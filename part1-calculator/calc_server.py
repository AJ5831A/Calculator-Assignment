#!/usr/bin/env python3
"""
A calculator that stays on the line.

HTTP/1.1 over a raw TCP socket -- no framework, no http.server.  One thread per
connection; each connection is a loop that pulls *exactly one* request off the
byte stream, answers it, and goes round again until the peer hangs up, asks us
to close, goes idle, or sends something we cannot frame.

The whole point is the question HTTP/1.0 never asked: where does this request
end and the next one begin?  Answer, per RFC 9112 section 6:

    1. the head ends at the first CRLF CRLF
    2. the body is then
         Transfer-Encoding: chunked  -> decode chunks until the 0-size chunk
         Content-Length: N           -> exactly N bytes, not one more
         neither                     -> zero bytes
    3. whatever is left in the buffer is the start of the *next* request
       (this is what makes pipelining work for free)

Usage:  python3 calc_server.py [--host 0.0.0.0] [--port 8080] [--idle 15]
"""

import argparse
import itertools
import re
import socket
import sys
import threading
import time
from urllib.parse import unquote_plus

# --------------------------------------------------------------------------
# Limits.  Each one is a defence against a peer that never stops talking.
# --------------------------------------------------------------------------
MAX_HEAD_BYTES = 8 * 1024          # request line + headers
MAX_BODY_BYTES = 1 * 1024 * 1024   # we never need a body; cap it anyway
MAX_CHUNK_LINE = 1024              # "1a;ext=foo\r\n"
RECV_SIZE = 4096

# Idle timeout: how long a kept-alive connection may sit silent *between*
# requests (and how long any single recv may stall mid-request).  15 s is
# long enough for a person typing into `nc`, long enough for any scripted
# client to send its next request, and short enough that idle sockets do not
# pile up threads.  (Apache's default KeepAliveTimeout is 5 s, nginx 75 s.)
DEFAULT_IDLE_TIMEOUT = 15.0

OPERATIONS = {
    "/add": lambda a, b: a + b,
    "/sub": lambda a, b: a - b,
    "/mul": lambda a, b: a * b,
    "/div": None,  # special-cased: division by zero, int vs float result
}
ALLOWED_METHODS = ("GET", "HEAD")

REASONS = {
    100: "Continue",
    200: "OK",
    400: "Bad Request",
    404: "Not Found",
    405: "Method Not Allowed",
    408: "Request Timeout",
    411: "Length Required",
    413: "Content Too Large",
    431: "Request Header Fields Too Large",
    501: "Not Implemented",
    505: "HTTP Version Not Supported",
}

NUMBER_RE = re.compile(r"^[+-]?(\d+)(\.\d+)?$")
TOKEN_RE = re.compile(r"^[!#$%&'*+\-.^_`|~0-9A-Za-z]+$")
REQUEST_LINE_RE = re.compile(r"^([!#$%&'*+\-.^_`|~0-9A-Za-z]+) (\S+) HTTP/(\d)\.(\d)$")

_conn_ids = itertools.count(1)
_log_lock = threading.Lock()


def log(msg):
    with _log_lock:
        sys.stderr.write(time.strftime("%H:%M:%S ") + msg + "\n")
        sys.stderr.flush()


# --------------------------------------------------------------------------
# Errors that mean "answer with this status".  `fatal` means the byte stream
# can no longer be trusted (we don't know where the next request begins), so
# we must close after replying.
# --------------------------------------------------------------------------
class HTTPError(Exception):
    def __init__(self, status, message, fatal=False, headers=None):
        super().__init__(message)
        self.status = status
        self.message = message
        self.fatal = fatal
        self.headers = headers or []


class PeerClosed(Exception):
    """Peer closed (or went idle) cleanly between requests."""


# --------------------------------------------------------------------------
# The byte stream.  A buffer plus a socket; everything reads through here so
# that bytes we over-read with recv() are kept for the next request.
# --------------------------------------------------------------------------
class Stream:
    def __init__(self, sock):
        self.sock = sock
        self.buf = bytearray()

    def _fill(self):
        """Read more bytes. Returns False on EOF."""
        data = self.sock.recv(RECV_SIZE)
        if not data:
            return False
        self.buf += data
        return True

    def read_until(self, delim, limit, on_limit):
        """Return bytes up to and including `delim`, consuming them."""
        start = 0
        while True:
            idx = self.buf.find(delim, start)
            if idx != -1:
                end = idx + len(delim)
                out = bytes(self.buf[:end])
                del self.buf[:end]
                return out
            if len(self.buf) > limit:
                raise on_limit
            start = max(0, len(self.buf) - len(delim) + 1)
            if not self._fill():
                raise EOFError

    def read_exact(self, n):
        """Consume exactly n bytes -- byte n+1 belongs to somebody else."""
        while len(self.buf) < n:
            if not self._fill():
                raise EOFError
        out = bytes(self.buf[:n])
        del self.buf[:n]
        return out


# --------------------------------------------------------------------------
# Request parsing
# --------------------------------------------------------------------------
class Request:
    __slots__ = ("method", "target", "version", "headers", "body")

    def header(self, name):
        """Single value of a header (lower-case name) or None."""
        vals = self.headers.get(name)
        return vals[0] if vals else None


def read_head(stream):
    """Read request line + header block. Raises PeerClosed if the peer went
    away (or idled out) before sending the first byte of a new request."""
    # RFC 9112 2.2: SHOULD ignore at least one empty line before the request
    # line (some clients send a stray CRLF after a POST body).
    while True:
        try:
            if not stream.buf and not stream._fill():
                raise PeerClosed()
        except socket.timeout:
            raise PeerClosed("idle timeout")
        if stream.buf.startswith(b"\r\n"):
            del stream.buf[:2]
            continue
        if stream.buf.startswith(b"\n"):
            del stream.buf[:1]
            continue
        break

    too_big = HTTPError(431, "request head too large", fatal=True)
    try:
        head = stream.read_until(b"\r\n\r\n", MAX_HEAD_BYTES, too_big)
    except EOFError:
        raise HTTPError(400, "connection closed mid-request", fatal=True)
    except socket.timeout:
        raise HTTPError(408, "timed out reading request head", fatal=True)
    if len(head) > MAX_HEAD_BYTES:
        raise too_big

    try:
        text = head[:-4].decode("iso-8859-1")
    except UnicodeDecodeError:  # pragma: no cover - latin-1 decodes anything
        raise HTTPError(400, "undecodable head", fatal=True)

    lines = text.split("\r\n")
    m = REQUEST_LINE_RE.match(lines[0])
    if not m:
        raise HTTPError(400, "malformed request line", fatal=True)
    req = Request()
    req.method, req.target = m.group(1), m.group(2)
    req.version = (int(m.group(3)), int(m.group(4)))
    if req.version[0] != 1:
        raise HTTPError(505, "only HTTP/1.x is spoken here", fatal=True)

    req.headers = {}
    for line in lines[1:]:
        if line[:1] in (" ", "\t"):
            # obs-fold: RFC 9112 5.2 says reject with 400.
            raise HTTPError(400, "obsolete line folding", fatal=True)
        name, sep, value = line.partition(":")
        if not sep or not TOKEN_RE.match(name):
            # includes "Name : value" (whitespace before colon) -- RFC 9112 5.1
            raise HTTPError(400, "malformed header line", fatal=True)
        req.headers.setdefault(name.lower(), []).append(value.strip(" \t"))
    req.body = b""
    return req


def read_body(stream, req, sock):
    """Consume exactly the request's body from the stream."""
    te = req.headers.get("transfer-encoding")
    cl = req.headers.get("content-length")

    if te is not None:
        if cl is not None:
            # RFC 9112 6.1: both present is a smuggling vector; reject.
            raise HTTPError(400, "both Transfer-Encoding and Content-Length", fatal=True)
        codings = [c.strip().lower() for v in te for c in v.split(",") if c.strip()]
        if not codings or codings[-1] != "chunked":
            # Can't find the end of the body -> can't find the next request.
            raise HTTPError(501 if codings else 400,
                            "unsupported transfer-coding", fatal=True)
        if codings != ["chunked"]:
            raise HTTPError(501, "only 'chunked' transfer-coding is supported", fatal=True)
        maybe_continue(req, sock)
        req.body = read_chunked(stream)
        return

    if cl is not None:
        values = {v.strip() for item in cl for v in item.split(",")}
        if len(values) != 1:
            raise HTTPError(400, "conflicting Content-Length", fatal=True)
        value = values.pop()
        if not value.isdigit():
            raise HTTPError(400, "invalid Content-Length", fatal=True)
        n = int(value)
        if n > MAX_BODY_BYTES:
            raise HTTPError(413, "body too large", fatal=True)
        if n:
            maybe_continue(req, sock)
        try:
            req.body = stream.read_exact(n)
        except EOFError:
            raise HTTPError(400, "connection closed mid-body", fatal=True)
        except socket.timeout:
            raise HTTPError(408, "timed out reading body", fatal=True)


def maybe_continue(req, sock):
    if req.version >= (1, 1) and (req.header("expect") or "").lower() == "100-continue":
        sock.sendall(b"HTTP/1.1 100 Continue\r\n\r\n")


def read_chunked(stream):
    """RFC 9112 7.1: chunk-size [; ext] CRLF data CRLF ... 0 CRLF trailers CRLF"""
    body = bytearray()
    bad_line = HTTPError(400, "chunk-size line too long", fatal=True)
    try:
        while True:
            line = stream.read_until(b"\r\n", MAX_CHUNK_LINE, bad_line)[:-2]
            size_txt = line.split(b";", 1)[0].strip()
            if not size_txt or not re.fullmatch(rb"[0-9A-Fa-f]+", size_txt):
                raise HTTPError(400, "invalid chunk size", fatal=True)
            size = int(size_txt, 16)
            if size == 0:
                break
            if len(body) + size > MAX_BODY_BYTES:
                raise HTTPError(413, "body too large", fatal=True)
            body += stream.read_exact(size)
            if stream.read_exact(2) != b"\r\n":
                raise HTTPError(400, "chunk data not followed by CRLF", fatal=True)
        # trailer section: header lines until an empty line
        while True:
            line = stream.read_until(b"\r\n", MAX_HEAD_BYTES, bad_line)
            if line == b"\r\n":
                break
    except EOFError:
        raise HTTPError(400, "connection closed mid-chunked-body", fatal=True)
    except socket.timeout:
        raise HTTPError(408, "timed out reading chunked body", fatal=True)
    return bytes(body)


# --------------------------------------------------------------------------
# The calculator (the part that is "not the point")
# --------------------------------------------------------------------------
def parse_number(text):
    m = NUMBER_RE.match(text)
    if not m:
        raise HTTPError(400, "not a number: %r" % text)
    return float(text) if m.group(2) else int(text)


def format_number(x):
    if isinstance(x, float):
        if x != x or x in (float("inf"), float("-inf")):
            raise HTTPError(400, "result is not a finite number")
        if x.is_integer() and abs(x) < 1e16:
            return str(int(x))
        return repr(x)
    return str(x)


def parse_query(query):
    params = {}
    if not query:
        return params
    for part in query.split("&"):
        if not part:
            continue
        key, _, value = part.partition("=")
        key, value = unquote_plus(key), unquote_plus(value)
        if key in params:
            raise HTTPError(400, "duplicate parameter %r" % key)
        params[key] = value
    return params


def calculate(path, query):
    params = parse_query(query)
    for name in ("a", "b"):
        if name not in params:
            raise HTTPError(400, "missing parameter %r" % name)
    a, b = parse_number(params["a"]), parse_number(params["b"])
    if path == "/div":
        if b == 0:
            raise HTTPError(400, "division by zero")
        if isinstance(a, int) and isinstance(b, int) and a % b == 0:
            return format_number(a // b)
        return format_number(a / b)
    return format_number(OPERATIONS[path](a, b))


def route(req):
    """Return (status, body_text, extra_headers) or raise HTTPError."""
    # HTTP/1.1 requires exactly one Host header (RFC 9112 3.2).
    hosts = req.headers.get("host")
    if req.version >= (1, 1) and hosts is None:
        raise HTTPError(400, "missing Host header")
    if hosts is not None and len(hosts) > 1:
        raise HTTPError(400, "multiple Host headers")

    target = req.target
    if target.startswith(("http://", "https://")):     # absolute-form
        target = "/" + target.split("://", 1)[1].partition("/")[2]
    if not target.startswith("/"):
        raise HTTPError(400, "bad request-target")
    path, _, query = target.partition("?")

    if path not in OPERATIONS:
        raise HTTPError(404, "no such operation: %s" % path)
    if req.method not in ALLOWED_METHODS:
        raise HTTPError(405, "method %s not allowed" % req.method,
                        headers=[("Allow", ", ".join(ALLOWED_METHODS))])
    return 200, calculate(path, query), []


# --------------------------------------------------------------------------
# Responses
# --------------------------------------------------------------------------
def build_response(status, body_text, keep_alive, extra_headers=(), head_only=False):
    body = body_text.encode("utf-8")
    lines = ["HTTP/1.1 %d %s" % (status, REASONS.get(status, "Unknown")),
             "Content-Type: text/plain; charset=utf-8",
             "Content-Length: %d" % len(body),
             "Connection: %s" % ("keep-alive" if keep_alive else "close")]
    if keep_alive:
        lines.append("Keep-Alive: timeout=%d" % int(IDLE_TIMEOUT))
    for k, v in extra_headers:
        lines.append("%s: %s" % (k, v))
    head = ("\r\n".join(lines) + "\r\n\r\n").encode("iso-8859-1")
    return head if head_only else head + body


def wants_keep_alive(req):
    tokens = {t.strip().lower() for v in req.headers.get("connection", [])
              for t in v.split(",")}
    if "close" in tokens:
        return False
    if req.version >= (1, 1):
        return True
    return "keep-alive" in tokens   # HTTP/1.0 opt-in


# --------------------------------------------------------------------------
# The connection loop
# --------------------------------------------------------------------------
IDLE_TIMEOUT = DEFAULT_IDLE_TIMEOUT


def handle_connection(sock, addr):
    cid = next(_conn_ids)
    log("conn#%d open   from %s:%d" % (cid, addr[0], addr[1]))
    sock.settimeout(IDLE_TIMEOUT)
    sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
    stream = Stream(sock)
    served = 0
    reason = "peer closed"
    try:
        while True:
            try:
                req = read_head(stream)
                read_body(stream, req, sock)
            except PeerClosed as e:
                reason = str(e) or "peer closed"
                break
            except HTTPError as e:
                # Framing is broken: answer, then hang up.
                sock.sendall(build_response(e.status, e.message + "\n", False, e.headers))
                served += 1
                log("conn#%d  #%d  ???  -> %d %s (closing)" % (cid, served, e.status, e.message))
                reason = "unframeable request"
                break

            keep = wants_keep_alive(req)
            try:
                status, body, extra = route(req)
            except HTTPError as e:
                status, body, extra = e.status, e.message + "\n", e.headers
            sock.sendall(build_response(status, body, keep, extra,
                                        head_only=(req.method == "HEAD")))
            served += 1
            log("conn#%d  #%d  %s %s -> %d" % (cid, served, req.method, req.target, status))
            if not keep:
                reason = "Connection: close"
                break
    except (ConnectionResetError, BrokenPipeError):
        reason = "connection reset"
    except socket.timeout:
        reason = "timeout"
    finally:
        try:
            sock.shutdown(socket.SHUT_WR)
        except OSError:
            pass
        sock.close()
        log("conn#%d closed after %d response(s): %s" % (cid, served, reason))


def serve(host, port):
    listener = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    listener.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    listener.bind((host, port))
    listener.listen(128)
    log("calculator listening on %s:%d (idle timeout %.0fs)" % (host, port, IDLE_TIMEOUT))
    try:
        while True:
            conn, addr = listener.accept()
            threading.Thread(target=handle_connection, args=(conn, addr),
                             daemon=True).start()
    except KeyboardInterrupt:
        log("shutting down")
    finally:
        listener.close()


def main():
    global IDLE_TIMEOUT
    ap = argparse.ArgumentParser(description="HTTP/1.1 keep-alive calculator")
    ap.add_argument("--host", default="0.0.0.0")
    ap.add_argument("--port", type=int, default=8080)
    ap.add_argument("--idle", type=float, default=DEFAULT_IDLE_TIMEOUT,
                    help="idle keep-alive timeout in seconds")
    args = ap.parse_args()
    IDLE_TIMEOUT = args.idle
    serve(args.host, args.port)


if __name__ == "__main__":
    main()
