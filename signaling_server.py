"""
DAMATH signaling broker - pairs two waiting players and tells them how to connect.

Run:   python signaling_server.py [--host 0.0.0.0] [--port 9009]

The broker never relays game traffic. It only introduces two players:
  1st player in the queue -> HOST  (opens a listening socket, reports its port)
  2nd player              -> GUEST (is given HOST's public IP + port and connects directly)

Players are only ever paired with someone who picked the SAME game mode
("variant"). Every variant has its own waiting line, so an INTEGERS player is
never matched with a WHOLE player, however long either one waits.

Wire protocol (newline-delimited JSON over TCP):
  client -> broker : {"type":"find","name":"PLAYER 1","variant":"INTEGERS"}
  broker -> HOST   : {"type":"assign","role":"host","opponent":"NAME","variant":"INTEGERS"}
  HOST   -> broker : {"type":"ready","port":54321}
  broker -> GUEST  : {"type":"assign","role":"guest","opponent":"NAME","variant":"INTEGERS",
                      "host":"1.2.3.4","port":54321}
  broker -> client : {"type":"error","reason":"..."}      (on any failure)

"variant" is required and must be one of VARIANTS (case-insensitive). A find
without it is answered with reason "MISSING VARIANT", an unknown one with
"UNKNOWN VARIANT"; the connection is then closed.

HOST's public IP is the address the broker sees the HOST connect from. For
play over the internet, the HOST's port must be reachable (port-forwarding,
or run the host with DAMATH_GAME_PORT set to a forwarded port).
"""

import argparse
import json
import select
import socket
import threading
import time

QUEUE_TIMEOUT = 180.0      # drop clients that wait longer than this (matches the game client)
READ_TIMEOUT = 10.0        # time allowed for "find" / "ready" messages
NAME_MAX = 10
VARIANTS = ("WHOLE", "INTEGERS", "FRACTIONS", "RATIONALS", "POLYNOMIALS")   # same as the game client

waiting = []               # list of dicts: {"sock", "name", "variant", "since"}
lock = threading.Lock()


class BadRequest(ValueError):
    """A rejected "find". The text is sent back to the client as the error reason."""


def log(*args):
    print(time.strftime("%H:%M:%S"), *args, flush=True)


def send(sock, obj):
    sock.sendall((json.dumps(obj) + "\n").encode("utf-8"))


def clean_name(raw):
    name = "".join(ch for ch in str(raw) if ch.isprintable()).strip()
    return (name or "PLAYER")[:NAME_MAX].upper()


def clean_variant(raw):
    """Normalise the requested game mode. Raises BadRequest if missing or unknown."""
    if raw is None:
        raise BadRequest("MISSING VARIANT")
    variant = raw.strip().upper() if isinstance(raw, str) else None
    if variant not in VARIANTS:
        raise BadRequest("UNKNOWN VARIANT")
    return variant


def is_alive(sock):
    """False if the waiting client has closed its end (e.g. pressed CANCEL)."""
    try:
        readable, _, _ = select.select([sock], [], [], 0)
        if readable:
            return sock.recv(1, socket.MSG_PEEK) != b""
        return True
    except OSError:
        return False


def close(sock):
    try:
        sock.close()
    except OSError:
        pass


def pop_live_waiter(variant, enqueue=None):
    """Pop the longest-waiting live client queued for the SAME variant (game mode).

    Clients waiting for any other variant are never touched. If nobody matches and
    `enqueue` is given, it is queued under the same lock, so two clients arriving
    at the same moment can never both miss each other.
    """
    with lock:
        for entry in list(waiting):
            if entry["variant"] != variant:
                continue
            waiting.remove(entry)
            if is_alive(entry["sock"]):
                return entry
            close(entry["sock"])
            log("dropped disconnected client", entry["name"])
        if enqueue is not None:
            waiting.append(enqueue)
    return None


def read_msg(sock, timeout):
    """Read one JSON line (max 4 KB). Raises on timeout, EOF or garbage."""
    sock.settimeout(timeout)
    buf = b""
    while b"\n" not in buf:
        chunk = sock.recv(1024)
        if not chunk:
            raise ConnectionError("closed")
        buf += chunk
        if len(buf) > 4096:
            raise ValueError("message too long")
    msg = json.loads(buf.split(b"\n", 1)[0].decode("utf-8"))
    if not isinstance(msg, dict):
        raise ValueError("not an object")
    return msg


def introduce(host, guest):
    """Run the HOST/GUEST handshake. Returns True on success."""
    try:
        send(host["sock"], {"type": "assign", "role": "host", "opponent": guest["name"],
                            "variant": host["variant"]})
        ready = read_msg(host["sock"], READ_TIMEOUT)
        port = int(ready.get("port", 0))
        if ready.get("type") != "ready" or not (0 < port < 65536):
            raise ValueError("bad ready message")
        host_ip = host["sock"].getpeername()[0]
    except (OSError, ValueError, json.JSONDecodeError) as exc:
        log("host", host["name"], "failed handshake:", exc)
        return False
    try:
        send(guest["sock"], {"type": "assign", "role": "guest", "opponent": host["name"],
                             "variant": host["variant"], "host": host_ip, "port": port})
    except OSError as exc:
        log("guest", guest["name"], "vanished:", exc)
        return False
    log(f"PAIRED  [{host['variant']}] host={host['name']} ({host_ip}:{port})  guest={guest['name']}")
    return True


def handle_client(sock, addr):
    try:
        sock.settimeout(READ_TIMEOUT)
        buf = b""
        while b"\n" not in buf:
            chunk = sock.recv(1024)
            if not chunk:
                raise ConnectionError("closed")
            buf += chunk
            if len(buf) > 4096:
                raise ValueError("message too long")

        # Extract the very first line to analyze its contents
        first_line_bytes = buf.split(b"\n", 1)[0]
        first_line = first_line_bytes.decode("utf-8", errors="ignore")

        # intercept Render's HTTP infrastructure checks safely
        if first_line.startswith("GET ") or first_line.startswith("HEAD "):
            http_response = (
                "HTTP/1.1 200 OK\(\r\\)n"
                "Content-Type: text/plain\(\r\\)n"
                "Content-Length: \(2\r\\)n"
                "Connection: close\(\r\n\r\\)n"
                "OK"
            )
            sock.sendall(http_response.encode("utf-8"))
            close(sock)
            return

        # Regular game traffic processing
        msg = json.loads(first_line)
        if not isinstance(msg, dict):
            raise ValueError("not an object")
        if msg.get("type") != "find":
            raise ValueError("expected find")

        me = {"sock": sock, "name": clean_name(msg.get("name", "")),
              "variant": clean_variant(msg.get("variant")), "since": time.monotonic()}

    except (OSError, ValueError, json.JSONDecodeError) as exc:
        if isinstance(exc, BadRequest):
            log("rejected game client", addr, exc)
            try:
                send(sock, {"type": "error", "reason": str(exc)})
            except OSError:
                pass
        close(sock)
        return

    log(f"QUEUED  [{me['variant']}] {me['name']} from {addr[0]}:{addr[1]}")
    while True:
        # pair only with someone waiting for the same game mode; otherwise queue as its HOST
        host = pop_live_waiter(me["variant"], enqueue=me)
        if host is None:
            return
        ok = introduce(host, me)
        close(host["sock"])
        if ok:
            close(sock)
            return
        # host dropped during the handshake: try the next waiting client


def reaper():
    """Drop waiting clients that disconnected or have waited too long."""
    while True:
        time.sleep(2.0)
        with lock:
            keep = []
            for e in waiting:
                if time.monotonic() - e["since"] > QUEUE_TIMEOUT:
                    try:
                        send(e["sock"], {"type": "error", "reason": "NO OPPONENT FOUND"})
                    except OSError:
                        pass
                    close(e["sock"])
                    log("timed out", e["name"], f"[{e['variant']}]")
                elif is_alive(e["sock"]):
                    keep.append(e)
                else:
                    close(e["sock"])
                    log("left queue", e["name"], f"[{e['variant']}]")
            waiting[:] = keep


def main():
    ap = argparse.ArgumentParser(description="DAMATH signaling broker")
    ap.add_argument("--host", default="0.0.0.0")
    ap.add_argument("--port", type=int, default=9009)
    args = ap.parse_args()

    server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    server.bind((args.host, args.port))
    server.listen(64)
    server.settimeout(1.0)
    threading.Thread(target=reaper, daemon=True).start()
    log(f"signaling broker listening on {args.host}:{args.port}  (Ctrl+C to stop)")
    log("game modes:", ", ".join(VARIANTS), "- players are only paired within the same mode")

    try:
        while True:
            try:
                sock, addr = server.accept()
            except socket.timeout:
                continue
            threading.Thread(target=handle_client, args=(sock, addr), daemon=True).start()
    except KeyboardInterrupt:
        log("shutting down")
    finally:
        server.close()
        with lock:
            for e in waiting:
                close(e["sock"])


if __name__ == "__main__":
    main()
              
