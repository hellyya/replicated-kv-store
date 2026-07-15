"""
Shared helpers for the replicated key-value store.

Wire format: each request/response is a single line of JSON, terminated
by "\n", sent over a TCP socket. Example:

    request:  {"op": "put", "key": "course", "value": "itec4100"}
    response: {"status": "ok", "result": "stored"}
"""

import json


def send_json(sock, obj):
    """Encode obj as one JSON line and send it over sock."""
    data = (json.dumps(obj) + "\n").encode("utf-8")
    sock.sendall(data)


def recv_line(sock, buffer):
    """
    Read one newline-terminated message from sock.

    `buffer` is a bytes object holding any data already read from the
    socket but not yet consumed (leftover from a previous call). This
    lets a caller keep reusing the same buffer across multiple messages
    on a persistent connection.

    Returns (line, remaining_buffer). `line` is None if the peer closed
    the connection before a full line arrived.
    """
    while b"\n" not in buffer:
        chunk = sock.recv(4096)
        if not chunk:
            return None, buffer
        buffer += chunk
    line, _, rest = buffer.partition(b"\n")
    return line, rest
