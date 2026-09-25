#!/usr/bin/env python3
"""
Tests for calc_server.py -- written the way the marker will mark it:
one socket, every request.

Run:  python3 test_calc.py            (starts its own server on a free port)
      python3 test_calc.py --port 8080 --external   (test a running server)
"""

import argparse
import errno
import select
import socket
import sys
import threading
import time
import unittest

import calc_server


# --------------------------------------------------------------------------
# A tiny, strict HTTP/1.1 response reader.  It must *also* get framing right,
# otherwise the tests would be testing nothing.
# --------------------------------------------------------------------------
class Client:
    def __init__(self, host, port):
        self.sock = socket.create_connection((host, port), timeout=5)
        self.buf = b""

    def send(self, data):
        self.sock.sendall(data)

    def _fill(self):
        data = self.sock.recv(4096)
        if not data:
            raise EOFError("server closed connection")
        self.buf += data

    def read_response(self):
        while b"\r\n\r\n" not in self.buf:
            self._fill()
        head, self.buf = self.buf.split(b"\r\n\r\n", 1)
        lines = head.decode("iso-8859-1").split("\r\n")
        version, status, reason = lines[0].split(" ", 2)
        headers = {}
        for line in lines[1:]:
            k, v = line.split(":", 1)
            headers[k.strip().lower()] = v.strip()
        n = int(headers["content-length"])
        while len(self.buf) < n:
            self._fill()
        body, self.buf = self.buf[:n], self.buf[n:]
        return int(status), headers, body.decode()

    def request(self, raw):
        self.send(raw)
        return self.read_response()

    def is_open(self):
        """True if the peer has not sent FIN/RST.  Non-destructive peek."""
        r, _, _ = select.select([self.sock], [], [], 0.2)
        if not r:
            return True  # nothing to read -> no FIN pending
        try:
            data = self.sock.recv(1, socket.MSG_PEEK)
        except OSError:
            return False
        return bool(data)

    def close(self):
        self.sock.close()


def get(path, host=True, extra=""):
    h = "Host: localhost\r\n" if host else ""
    return ("GET %s HTTP/1.1\r\n%s%s\r\n" % (path, h, extra)).encode()


HOST, PORT = "localhost", 0
EXTERNAL = False
CONN_COUNT = [0]


def start_server():
    """Run the real server in-process on a free port; count accepts."""
    global PORT
    listener = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    listener.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    listener.bind(("127.0.0.1", 0))
    listener.listen(64)
    PORT = listener.getsockname()[1]
    calc_server.IDLE_TIMEOUT = 2.0  # short so the idle test is quick

    def loop():
        while True:
            c, a = listener.accept()
            CONN_COUNT[0] += 1
            threading.Thread(target=calc_server.handle_connection,
                             args=(c, a), daemon=True).start()
    threading.Thread(target=loop, daemon=True).start()


class MarkerScript(unittest.TestCase):
    """Exactly the sequence on the assignment sheet."""

    def test_marker_sequence_one_socket(self):
        before = CONN_COUNT[0]
        c = Client(HOST, PORT)
        s = c.request
        self.assertEqual(s(get("/add?a=2&b=3"))[::2], (200, "5"))
        self.assertEqual(s(get("/sub?a=10&b=4"))[::2], (200, "6"))
        self.assertEqual(s(get("/mul?a=6&b=7"))[::2], (200, "42"))
        self.assertEqual(s(get("/div?a=1&b=0"))[0], 400)
        self.assertEqual(s(get("/pow?a=2&b=8"))[0], 404)
        status, headers, _ = s(b"POST /add HTTP/1.1\r\nHost: localhost\r\n"
                               b"Content-Length: 0\r\n\r\n")
        self.assertEqual(status, 405)
        self.assertIn("GET", headers["allow"])
        self.assertTrue(c.is_open(), "socket still open: False")
        if not EXTERNAL:
            self.assertEqual(CONN_COUNT[0] - before, 1, "more than one TCP handshake")
        print("\n    socket still open: %s" % c.is_open())
        print("    1 TCP handshake, 6 responses", end=" ")
        c.close()

    def test_full_feature_table_one_socket(self):
        c = Client(HOST, PORT)
        cases = [
            (get("/add?a=2&b=3"), 200, "5"),
            (get("/sub?a=10&b=4"), 200, "6"),
            (get("/mul?a=6&b=7"), 200, "42"),
            (get("/div?a=9&b=3"), 200, "3"),
            (get("/div?a=1&b=0"), 400, None),
            (get("/add?a=x&b=3"), 400, None),
            (get("/pow?a=2&b=8"), 404, None),
            (b"POST /add HTTP/1.1\r\nHost: localhost\r\n\r\n", 405, None),
            (get("/add", host=False), 400, None),
        ]
        for raw, want_status, want_body in cases:
            status, _, body = c.request(raw)
            self.assertEqual(status, want_status, raw)
            if want_body is not None:
                self.assertEqual(body, want_body, raw)
        self.assertTrue(c.is_open())
        c.close()


class Arithmetic(unittest.TestCase):
    def setUp(self):
        self.c = Client(HOST, PORT)

    def tearDown(self):
        self.c.close()

    def check(self, path, status, body=None):
        st, _, b = self.c.request(get(path))
        self.assertEqual(st, status, path)
        if body is not None:
            self.assertEqual(b, body, path)

    def test_numbers(self):
        self.check("/add?a=-2&b=3", 200, "1")
        self.check("/div?a=7&b=2", 200, "3.5")
        self.check("/mul?a=1.5&b=2", 200, "3")
        self.check("/add?b=3&a=2", 200, "5")                 # order-free
        self.check("/add?a=%2B2&b=3", 200, "5")              # percent-decoded +2
        self.check("/mul?a=99999999999999999999&b=10", 200, "999999999999999999990")
        self.check("/div?a=0&b=5", 200, "0")

    def test_bad_input(self):
        for p in ["/add?a=2", "/add?b=2", "/add?a=&b=1", "/add?a=1e3&b=1",
                  "/add?a=nan&b=1", "/add?a=inf&b=1", "/div?a=1&b=0.0",
                  "/add?a=1&a=2&b=3", "/add"]:
            self.check(p, 400)
        self.assertTrue(self.c.is_open())

    def test_unknown_paths(self):
        for p in ["/pow?a=2&b=8", "/", "/ADD?a=1&b=2", "/add/extra?a=1&b=2"]:
            self.check(p, 404)

    def test_methods(self):
        for m in ["POST", "PUT", "DELETE", "PATCH"]:
            st, h, _ = self.c.request(("%s /add HTTP/1.1\r\nHost: x\r\n\r\n" % m).encode())
            self.assertEqual(st, 405)
            self.assertEqual(h["allow"], "GET, HEAD")
        # HEAD: headers only, Content-Length of what GET would send, no body
        self.c.send(b"HEAD /add?a=2&b=3 HTTP/1.1\r\nHost: x\r\n\r\n")
        while b"\r\n\r\n" not in self.c.buf:
            self.c._fill()
        head, rest = self.c.buf.split(b"\r\n\r\n", 1)
        self.assertIn(b"Content-Length: 1", head)
        self.assertEqual(rest, b"")
        self.c.buf = b""
        self.check("/add?a=1&b=1", 200, "2")  # stream still in sync


class Framing(unittest.TestCase):
    """The part that is actually hard."""

    def test_content_length_exact_and_next_request_follows(self):
        c = Client(HOST, PORT)
        # A POST body whose bytes *look* like a request.  If the server reads
        # one byte too many or too few, the next response is wrong.
        body = b"GET /pow?a=1&b=1 HTTP/1.1\r\nHost: x\r\n\r\n"
        raw = (b"POST /add HTTP/1.1\r\nHost: x\r\nContent-Length: %d\r\n\r\n" % len(body)
               + body + get("/add?a=2&b=3"))
        c.send(raw)
        self.assertEqual(c.read_response()[0], 405)
        self.assertEqual(c.read_response()[::2], (200, "5"))
        self.assertTrue(c.is_open())
        c.close()

    def test_byte_at_a_time(self):
        c = Client(HOST, PORT)
        c.sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
        for b in get("/mul?a=6&b=7"):
            c.send(bytes([b]))
            time.sleep(0.001)
        self.assertEqual(c.read_response()[::2], (200, "42"))
        c.close()

    def test_chunked_request_body(self):
        c = Client(HOST, PORT)
        raw = (b"POST /add HTTP/1.1\r\nHost: x\r\nTransfer-Encoding: chunked\r\n\r\n"
               b"5;ext=1\r\nhello\r\n"
               b"B\r\n world, GET\r\n"
               b"0\r\nX-Trailer: yes\r\n\r\n") + get("/sub?a=10&b=4")
        c.send(raw)
        self.assertEqual(c.read_response()[0], 405)
        self.assertEqual(c.read_response()[::2], (200, "6"))
        self.assertTrue(c.is_open())
        c.close()

    def test_pipelining_all_six_at_once(self):
        c = Client(HOST, PORT)
        reqs = [get("/add?a=2&b=3"), get("/sub?a=10&b=4"), get("/mul?a=6&b=7"),
                get("/div?a=1&b=0"), get("/pow?a=2&b=8"),
                b"POST /add HTTP/1.1\r\nHost: x\r\nContent-Length: 0\r\n\r\n"]
        c.send(b"".join(reqs))          # one write, six requests
        got = [c.read_response()[0] for _ in reqs]
        self.assertEqual(got, [200, 200, 200, 400, 404, 405])
        self.assertTrue(c.is_open())
        c.close()

    def test_leading_crlf_tolerated(self):
        c = Client(HOST, PORT)
        self.assertEqual(c.request(b"\r\n" + get("/add?a=1&b=2"))[::2], (200, "3"))
        c.close()

    def test_expect_100_continue(self):
        c = Client(HOST, PORT)
        c.send(b"POST /add HTTP/1.1\r\nHost: x\r\nContent-Length: 3\r\n"
               b"Expect: 100-continue\r\n\r\n")
        while b"\r\n\r\n" not in c.buf:
            c._fill()
        self.assertTrue(c.buf.startswith(b"HTTP/1.1 100 Continue\r\n\r\n"))
        c.buf = c.buf[len(b"HTTP/1.1 100 Continue\r\n\r\n"):]
        c.send(b"abc")
        self.assertEqual(c.read_response()[0], 405)
        c.close()


class ConnectionManagement(unittest.TestCase):
    def test_connection_close_honoured(self):
        c = Client(HOST, PORT)
        st, h, body = c.request(get("/add?a=2&b=3", extra="Connection: close\r\n"))
        self.assertEqual((st, body), (200, "5"))
        self.assertEqual(h["connection"], "close")
        self.assertEqual(c.sock.recv(1), b"")  # FIN
        c.close()

    def test_http10_closes_by_default_and_keepalive_optin(self):
        c = Client(HOST, PORT)
        st, h, _ = c.request(b"GET /add?a=1&b=1 HTTP/1.0\r\n\r\n")  # no Host OK in 1.0
        self.assertEqual(st, 200)
        self.assertEqual(h["connection"], "close")
        self.assertEqual(c.sock.recv(1), b"")
        c.close()
        c = Client(HOST, PORT)
        st, h, _ = c.request(b"GET /add?a=1&b=1 HTTP/1.0\r\nConnection: keep-alive\r\n\r\n")
        self.assertEqual(h["connection"], "keep-alive")
        self.assertEqual(c.request(get("/add?a=1&b=2"))[::2], (200, "3"))
        c.close()

    @unittest.skipIf(EXTERNAL, "timeout is configured by the in-process server")
    def test_idle_timeout(self):
        c = Client(HOST, PORT)
        self.assertEqual(c.request(get("/add?a=1&b=1"))[0], 200)
        c.sock.settimeout(calc_server.IDLE_TIMEOUT + 3)
        t0 = time.time()
        self.assertEqual(c.sock.recv(1), b"")  # server hangs up when idle
        waited = time.time() - t0
        self.assertGreater(waited, calc_server.IDLE_TIMEOUT - 0.5)
        c.close()

    def test_malformed_request_line_closes(self):
        for junk in [b"HELLO\r\n\r\n", b"GET /add\r\n\r\n", b"GET  /add HTTP/1.1\r\n\r\n",
                     b"GET /add HTTP/2.0\r\nHost: x\r\n\r\n"]:
            c = Client(HOST, PORT)
            st, h, _ = c.request(junk)
            self.assertIn(st, (400, 505), junk)
            self.assertEqual(h["connection"], "close")
            self.assertEqual(c.sock.recv(1), b"")
            c.close()

    def test_bad_framing_closes(self):
        for raw in [
            b"POST /add HTTP/1.1\r\nHost: x\r\nContent-Length: abc\r\n\r\n",
            b"POST /add HTTP/1.1\r\nHost: x\r\nContent-Length: 3\r\nContent-Length: 4\r\n\r\n",
            b"POST /add HTTP/1.1\r\nHost: x\r\nContent-Length: 3\r\n"
            b"Transfer-Encoding: chunked\r\n\r\n",
            b"GET /add HTTP/1.1\r\nHost : x\r\n\r\n",
            b"POST /add HTTP/1.1\r\nHost: x\r\nTransfer-Encoding: chunked\r\n\r\nzz\r\n",
        ]:
            c = Client(HOST, PORT)
            st, h, _ = c.request(raw)
            self.assertEqual(st, 400, raw)
            self.assertEqual(h["connection"], "close")
            c.close()

    def test_oversized_head(self):
        c = Client(HOST, PORT)
        st, _, _ = c.request(b"GET /add HTTP/1.1\r\nHost: x\r\nX: " + b"a" * 10000 + b"\r\n\r\n")
        self.assertEqual(st, 431)
        c.close()

    def test_duplicate_host(self):
        c = Client(HOST, PORT)
        st, _, _ = c.request(b"GET /add?a=1&b=1 HTTP/1.1\r\nHost: a\r\nHost: b\r\n\r\n")
        self.assertEqual(st, 400)
        self.assertTrue(c.is_open())
        c.close()

    def test_concurrent_connections(self):
        results = []

        def worker(i):
            c = Client(HOST, PORT)
            for j in range(20):
                results.append(c.request(get("/add?a=%d&b=%d" % (i, j)))[2] == str(i + j))
            c.close()
        ts = [threading.Thread(target=worker, args=(i,)) for i in range(10)]
        [t.start() for t in ts]
        [t.join() for t in ts]
        self.assertEqual(len(results), 200)
        self.assertTrue(all(results))


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--port", type=int)
    ap.add_argument("--external", action="store_true")
    args, rest = ap.parse_known_args()
    if args.external:
        EXTERNAL = True
        PORT = args.port or 8080
    else:
        start_server()
    unittest.main(argv=[sys.argv[0]] + rest, verbosity=2)
