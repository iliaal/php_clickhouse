#!/usr/bin/env python3
"""
Record replay fixtures from real servers, eg older ClickHouse releases.
Proxy every connection, write server bytes of first closed connection
whose client bytes contain --match. Run until interrupted.

    tools/capture.py --listen 19234 --upstream 127.0.0.1:19233 \\
        --match 'FROM sparse_t' --out test/ch23_3_sparse.bin
"""

import argparse
import socket
import threading

lock = threading.Lock()
done = False


def pump(src, dst, buf):
    while chunk := src.recv(65536):
        buf += chunk
        dst.sendall(chunk)
    dst.shutdown(socket.SHUT_WR)


def serve(args, client):
    global done
    host, port = args.upstream.rsplit(":", 1)
    upstream = socket.create_connection((host, int(port)))
    c2s, s2c = bytearray(), bytearray()
    up = threading.Thread(target=pump, args=(client, upstream, c2s))
    up.start()
    try:
        pump(upstream, client, s2c)
    finally:
        up.join()
        client.close()
        upstream.close()
    with lock:
        if done or args.match.encode() not in c2s:
            return
        done = True
    with open(args.out, "wb") as f:
        f.write(s2c)
    print(f"wrote {len(s2c)} bytes to {args.out}", flush=True)


def main():
    p = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    p.add_argument("--listen", type=int, required=True)
    p.add_argument("--upstream", required=True, help="host:port")
    p.add_argument("--match", required=True, help="text client sends")
    p.add_argument("--out", required=True)
    args = p.parse_args()

    srv = socket.create_server(("127.0.0.1", args.listen))
    while True:
        client, _ = srv.accept()
        threading.Thread(target=serve, args=(args, client), daemon=True).start()


if __name__ == "__main__":
    main()
