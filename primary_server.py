"""

Line-based message format:
- Each request and response is one JSON object encoded as UTF-8.
- Each JSON object is sent on one line and ends with "\\n".
- The server accepts these public client operations:

    {"op": "put", "key": "city", "value": "Toronto"}
    {"op": "get", "key": "city"}
    {"op": "delete", "key": "city"}
    {"op": "keys"}

- The primary sends this internal request to the backup for every write:

    {"op": "replicate", "action": "put", "key": "city", "value": "Toronto"}
    {"op": "replicate", "action": "delete", "key": "city", "value": null}

- Success response example:

    {"status": "ok", "result": "stored"}

- Error response example:

    {"status": "error", "message": "key not found"}

"""

import argparse
import json
import socket
import threading
import time
from pathlib import Path

from common import recv_line, send_json


# Shared key-value data for the primary server.
# The lock protects this dictionary from race conditions when multiple
# client-handler threads access it at the same time.
store = {}
store_lock = threading.Lock()

# Separate lock so multiple threads do not write messy mixed log lines.
log_lock = threading.Lock()

# These are set from command-line arguments in main().
BACKUP_HOST = "127.0.0.1"
BACKUP_PORT = 6001
LOG_PATH = Path(__file__).with_name("primary.log")


def write_log(operation, arguments, result):
    """Write one log line: timestamp, operation, arguments, result/error."""
    timestamp = time.strftime("%Y-%m-%d %H:%M:%S")
    with log_lock:
        with open(LOG_PATH, "a", encoding="utf-8") as log_file:
            log_file.write(f"{timestamp}\t{operation}\t{arguments}\t{result}\n")


def error(message):
    """Small helper for consistent error responses."""
    return {"status": "error", "message": message}


def ok(result):
    """Small helper for consistent success responses."""
    return {"status": "ok", "result": result}


def replicate_to_backup(action, key, value):
    """
    Send one write update to the backup server and wait for a response.

    Returns:
        (True, "replicated") if the backup accepts the update.
        (False, reason) if the backup is unreachable or returns an error.
    """
    request = {"op": "replicate", "action": action, "key": key, "value": value}

    try:
        with socket.create_connection((BACKUP_HOST, BACKUP_PORT), timeout=3) as sock:
            send_json(sock, request)
            buffer = b""
            line, buffer = recv_line(sock, buffer)

        if line is None:
            return False, "no response from backup"

        try:
            response = json.loads(line.decode("utf-8"))
        except json.JSONDecodeError:
            return False, "invalid JSON response from backup"

        if response.get("status") == "ok":
            return True, response.get("result", "replicated")

        return False, response.get("message", "backup returned an error")

    except (ConnectionRefusedError, socket.timeout, OSError) as exc:
        return False, f"backup unreachable: {exc}"


def handle_put(request):
    """Handle put(key, value)."""
    key = request.get("key")

    if key is None:
        return error("missing key")

    # JSON allows null as a value, so only key is required here.
    value = request.get("value")

    # Hold the lock while updating locally and replicating. This keeps the
    # write order the same on the primary and backup.
    with store_lock:
        old_value = store.get(key)
        existed_before = key in store

        store[key] = value
        replicated, message = replicate_to_backup("put", key, value)

        if not replicated:
            # Roll back the local change so the primary and backup do not
            # disagree after a failed write.
            if existed_before:
                store[key] = old_value
            else:
                store.pop(key, None)
            return error(f"put failed because replication failed: {message}")

    return ok("stored")


def handle_get(request):
    """Handle get(key)."""
    key = request.get("key")

    if key is None:
        return error("missing key")

    with store_lock:
        if key not in store:
            return error(f"key '{key}' not found")
        return ok(store[key])


def handle_delete(request):
    """Handle delete(key)."""
    key = request.get("key")

    if key is None:
        return error("missing key")

    with store_lock:
        if key not in store:
            return error(f"key '{key}' not found")

        old_value = store[key]
        del store[key]
        replicated, message = replicate_to_backup("delete", key, None)

        if not replicated:
            # Roll back the delete if the backup did not confirm it.
            store[key] = old_value
            return error(f"delete failed because replication failed: {message}")

    return ok("deleted")


def handle_keys():
    """Handle keys()."""
    with store_lock:
        return ok(sorted(store.keys()))


def process_request(raw_line):
    """
    Parse one client request and return one response dictionary.
    This function does not crash the server on bad input.
    """
    try:
        request = json.loads(raw_line.decode("utf-8"))
    except json.JSONDecodeError:
        return "invalid_json", raw_line.decode("utf-8", errors="replace"), error("invalid JSON")

    if not isinstance(request, dict):
        return "invalid_request", request, error("request must be a JSON object")

    operation = request.get("op")

    if operation == "put":
        response = handle_put(request)
    elif operation == "get":
        response = handle_get(request)
    elif operation == "delete":
        response = handle_delete(request)
    elif operation == "keys":
        response = handle_keys()
    else:
        response = error(f"unsupported operation '{operation}'")

    return operation, request, response


def handle_client(connection, address):
    """Serve one client connection. Each connection runs in its own thread."""
    buffer = b""

    try:
        while True:
            line, buffer = recv_line(connection, buffer)
            if line is None:
                break

            operation, arguments, response = process_request(line)
            write_log(operation, arguments, response)
            send_json(connection, response)

    except (ConnectionResetError, BrokenPipeError):
        write_log("connection_error", {"address": address}, "client disconnected")
    finally:
        connection.close()


def main():
    global BACKUP_HOST, BACKUP_PORT, LOG_PATH

    parser = argparse.ArgumentParser(description="Primary server for replicated key-value store")
    parser.add_argument("--host", default="127.0.0.1", help="host for the primary server")
    parser.add_argument("--port", type=int, default=6000, help="port for the primary server")
    parser.add_argument("--backup-host", default="127.0.0.1", help="host for the backup server")
    parser.add_argument("--backup-port", type=int, default=6001, help="port for the backup server")
    parser.add_argument("--log", default=str(LOG_PATH), help="path for the primary log file")
    args = parser.parse_args()

    BACKUP_HOST = args.backup_host
    BACKUP_PORT = args.backup_port
    LOG_PATH = Path(args.log)

    server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    server_socket.bind((args.host, args.port))
    server_socket.listen(20)

    print(f"[primary] listening on {args.host}:{args.port}")
    print(f"[primary] backup server expected at {BACKUP_HOST}:{BACKUP_PORT}")
    print(f"[primary] log file: {LOG_PATH}")

    try:
        while True:
            connection, address = server_socket.accept()
            thread = threading.Thread(target=handle_client, args=(connection, address), daemon=True)
            thread.start()
    except KeyboardInterrupt:
        print("\n[primary] shutting down")
    finally:
        server_socket.close()


if __name__ == "__main__":
    main()
