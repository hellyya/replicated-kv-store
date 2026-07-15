# Replicated Key-Value Store

Option A, Path 1 (Assignment 1 style RPC). Python standard library only —
TCP sockets, threads, JSON-line messages.

## Files

- `common.py` — shared send/receive helpers for the JSON-line wire format
- `backup_server.py` — backup server (holds a copy of the data, serves reads, accepts replication)
- `primary_server.py` — primary server (handles writes, replicates to backup before acking)
- `client.py` — command-line client

## Message format

One JSON object per line, terminated by `\n`, over TCP.

```
request:  {"op": "put", "key": "course", "value": "itec4100"}
response: {"status": "ok", "result": "stored"}
```

Client-facing operations: `put`, `get`, `delete`, `keys`.
Internal-only operation (primary → backup): `replicate`.

## Running

Start the backup first, then the primary (primary needs to know the backup's address):

```bash
python3 backup_server.py --port 6001
python3 primary_server.py --port 6000 --backup-port 6001
```

Then run client commands against the primary (default port 6000):

```bash
python3 client.py --port 6000 put city Toronto
python3 client.py --port 6000 get city
python3 client.py --port 6000 keys
python3 client.py --port 6000 delete city
```

Reads can also be sent directly to the backup:

```bash
python3 client.py --port 6001 get city
```

`--host`/`--port` are always passed on the command line — nothing is hardcoded.

## Demo scenario (from the proposal)

```bash
# terminal 1
python3 backup_server.py --port 6001

# terminal 2
python3 primary_server.py --port 6000 --backup-port 6001

# terminal 3
python3 client.py --port 6000 put city Toronto
python3 client.py --port 6000 get city
# kill terminal 2 (Ctrl+C) to simulate the primary going down
python3 client.py --port 6001 get city   # backup still answers
```

## Logs

Each process appends to its own log file (`primary.log`, `backup.log`) in
this directory: one line per operation with timestamp, operation name,
arguments, and result/error.

## Concurrency

Both servers use one thread per client connection. Access to the shared
in-memory dictionary is protected by a `threading.Lock` (`store_lock`) in
each process. This prevents a race condition where two client threads
read-modify-write the same key at the same time (e.g. one thread's `put`
being overwritten or read mid-update by another thread's `get`/`delete`).

On the primary, `store_lock` is held for the *entire* duration of a
`put`/`delete`, including the network round-trip to the backup. This is a
deliberate tradeoff: it guarantees the primary and backup apply writes in
the same order, and it lets a failed replication be rolled back cleanly
(see below) without another thread seeing the half-applied state in
between. The cost is that a slow or unresponsive backup blocks every
other client on the primary for the duration of that call, since no
other thread can acquire the lock until the write finishes or fails.
Releasing the lock before contacting the backup would improve
concurrency under a slow backup, at the cost of allowing interleaved
writes and making rollback race-prone.

## Replication failure handling

If the backup does not confirm a `put`/`delete` (unreachable, times out,
or returns an error), the primary rolls back its own local change so the
primary and backup never end up silently disagreeing, and returns an
error to the client explaining that replication failed. It does not
retry or attempt automatic recovery beyond that (out of scope per the
assignment).
