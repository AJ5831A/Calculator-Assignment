# CN Assignment: staying on the line

Python 3 standard library only. No frameworks. Just sockets.

## Part 1: HTTP/1.1 calculator that stays on the line (`part1-calculator/`)

```
python3 part1-calculator/calc_server.py            # listens on :8080
python3 part1-calculator/marker.py                 # the marker's script: one socket, every request
python3 part1-calculator/test_calc.py              # 20 tests (starts its own server)
python3 part1-calculator/test_calc.py --external --port 8080   # same tests vs a running server
```

| Request | Response |
|---|---|
| `GET /add?a=2&b=3`, `/sub`, `/mul`, `/div` | `200` + result as `text/plain` |
| `GET /div?a=1&b=0`, `GET /add?a=x&b=3`, missing parameter | `400` |
| `GET /pow?...` (unknown path) | `404` |
| `POST /add` (any method except GET/HEAD) | `405` + `Allow: GET, HEAD` |
| HTTP/1.1 request without `Host` | `400` |

**Framing** (the hard part, see `read_head`/`read_body`): the head ends at the first
CRLFCRLF. The body is exactly `Content-Length` bytes, or de-chunked. Any bytes left
over stay in the buffer as the start of the next request.

**Stretch goals:** all four are done.

- `Connection: close` is honoured. HTTP/1.0 closes unless it sends `keep-alive`.
- A 15 s idle timeout, with the reasoning in the source.
- Chunked request bodies, including extensions and trailers.
- Pipelining. Six requests sent in one `write` are answered in order.

Also handled:

- `Expect: 100-continue`.
- A request that sets both CL and TE is rejected as a smuggling attempt.
- An obs-fold header is rejected.
- A 431 on an oversized head.

A malformed request line or bad framing gets an answer and then a close, because the
next request's start can no longer be found. A bad *value* (div by zero, `a=x`) gets an
answer and the connection stays open.
