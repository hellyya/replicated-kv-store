"""
client.py -- Command-line client for the replicated key-value store.

Usage:
    python client.py --port 6000 put course itec4100
    python client.py --port 6000 get course
    python client.py --port 6000 delete course
    python client.py --port 6000 keys

--host/--port point at whichever server you want to talk to (primary for
writes; primary or backup for reads). Never hardcoded -- always passed on
the command line, defaulting to the primary's usual address.
"""

import argparse
import json
import socket

from common import recv_line, send_json


def send_request(host, port, req):
    with socket.create_connection((host, port), timeout=5) as sock:
        send_json(sock, req)
        buffer = b""
        line, buffer = recv_line(sock, buffer)
        if line is None:
            return {"status": "error", "message": "no response from server"}
        return json.loads(line.decode("utf-8"))


def build_request(args):
    if args.command == "put":
        return {"op": "put", "key": args.key, "value": args.value}
    if args.command == "get":
        return {"op": "get", "key": args.key}
    if args.command == "delete":
        return {"op": "delete", "key": args.key}
    if args.command == "keys":
        return {"op": "keys"}
    raise ValueError(f"unknown command {args.command}")


def main():
    parser = argparse.ArgumentParser(description="Key-value store client")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=6000)

    sub = parser.add_subparsers(dest="command", required=True)

    p_put = sub.add_parser("put")
    p_put.add_argument("key")
    p_put.add_argument("value")

    p_get = sub.add_parser("get")
    p_get.add_argument("key")

    p_del = sub.add_parser("delete")
    p_del.add_argument("key")

    sub.add_parser("keys")

    args = parser.parse_args()
    req = build_request(args)
    resp = send_request(args.host, args.port, req)
    print(json.dumps(resp))


if __name__ == "__main__":
    main()
