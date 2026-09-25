#!/usr/bin/env python3
"""The marker's script from the assignment sheet: one socket, every request."""
import select, socket

s = socket.create_connection(("localhost", 8080))
buf = b""

def read_response():
    global buf
    while b"\r\n\r\n" not in buf:
        buf += s.recv(4096)
    head, buf = buf.split(b"\r\n\r\n", 1)
    lines = head.decode().split("\r\n")
    status = int(lines[0].split()[1])
    length = next(int(l.split(":")[1]) for l in lines if l.lower().startswith("content-length"))
    while len(buf) < length:
        buf += s.recv(4096)
    body, buf = buf[:length], buf[length:]
    return status, body.decode().strip()

requests = [
    ("GET", "/add?a=2&b=3"), ("GET", "/sub?a=10&b=4"), ("GET", "/mul?a=6&b=7"),
    ("GET", "/div?a=1&b=0"), ("GET", "/pow?a=2&b=8"), ("POST", "/add"),
]
for method, path in requests:
    s.sendall(f"{method} {path} HTTP/1.1\r\nHost: localhost\r\n\r\n".encode())
    status, body = read_response()
    print(f"  {method} {path:<18} -> {status}   {body if status == 200 else ''}")

r, _, _ = select.select([s], [], [], 0.2)
still_open = not r or s.recv(1, socket.MSG_PEEK) != b""
print(f"\n  socket still open: {still_open}")
print(f"  1 TCP handshake, {len(requests)} responses")
