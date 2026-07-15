"""
backup_server.py -- Backup server for the replicated key-value store.

Wire format (see common.py): one JSON object per line over TCP.

Requests this server handles:
    {"op": "get", "key": <k>}
    {"op": "keys"}
    {"op": "replicate", "action": "put"|"delete", "key": <k>, "value": <v>}

Responses:
    {"status": "ok", "result": <value>}
    {"status": "error", "message": <reason>}

The backup never initiates writes on its own -- it only applies updates
sent to it by the primary via the internal "replicate" operation. It can
serve get/keys requests directly, independent of the primary, which is
what lets it keep answering reads if the primary goes down.
"""

import argparse
import json
import socket
import threading
import time
from pathlib import Path

from common import recv_line, send_json

LOG_PATH = Path(__file__).parent / "backup.log"
log_lock = threading.Lock()

store = {}
store_lock = threading.Lock()


def write_log(operation, args, result):
    ts = time.strftime("%Y-%m-%d %H:%M:%S")
    with log_lock:
        with open(LOG_PATH, "a") as f:
            f.write(f"{ts}\t{operation}\t{args}\t{result}\n")


def handle_line(conn, line):
    try:
        req = json.loads(line.decode("utf-8"))
    except json.JSONDecodeError:
        resp = {"status": "error", "message": "invalid JSON"}
        write_log("invalid_json", line, resp)
        send_json(conn, resp)
        return

    op = req.get("op")

    if op == "get":
        key = req.get("key")
        with store_lock:
            if key in store:
                resp = {"status": "ok", "result": store[key]}
            else:
                resp = {"status": "error", "message": f"key '{key}' not found"}
        write_log("get", req, resp)

    elif op == "keys":
        with store_lock:
            resp = {"status": "ok", "result": list(store.keys())}
        write_log("keys", req, resp)

    elif op == "replicate":
        action = req.get("action")
        key = req.get("key")
        value = req.get("value")
        if key is None:
            resp = {"status": "error", "message": "missing key"}
        elif action == "put":
            with store_lock:
                store[key] = value
            resp = {"status": "ok", "result": "replicated"}
        elif action == "delete":
            with store_lock:
                store.pop(key, None)
            resp = {"status": "ok", "result": "replicated"}
        else:
            resp = {"status": "error", "message": f"unsupported replicate action '{action}'"}
        write_log("replicate", req, resp)

    else:
        resp = {"status": "error", "message": f"unsupported operation '{op}'"}
        write_log(op, req, resp)

    send_json(conn, resp)


def handle_client(conn, addr):
    buffer = b""
    try:
        while True:
            line, buffer = recv_line(conn, buffer)
            if line is None:
                break
            handle_line(conn, line)
    except (ConnectionResetError, BrokenPipeError):
        pass
    finally:
        conn.close()


def main():
    parser = argparse.ArgumentParser(description="Backup key-value server")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=6001)
    args = parser.parse_args()

    server_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server_sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    server_sock.bind((args.host, args.port))
    server_sock.listen(5)
    print(f"[backup] listening on {args.host}:{args.port}")

    while True:
        conn, addr = server_sock.accept()
        t = threading.Thread(target=handle_client, args=(conn, addr), daemon=True)
        t.start()


if __name__ == "__main__":
    main()
