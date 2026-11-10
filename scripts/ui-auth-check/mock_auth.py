#!/usr/bin/env python3
"""Auth-enforcing mock wrkspc operator API for check.mjs.

Serves the real UI page and fakes the ``/api/*`` reads the page's boot
flow touches, with the login-cookie contract: ``POST /api/login``
verifies the presented token and sets the HttpOnly ``wrkspc_token``
cookie (204); every other ``/api/*`` route accepts the cookie or a
bearer header; anything else gets a 401 with NO clearing Set-Cookie.

Scenario knobs (test levers, not part of the product). Responses are
DELAYED in-handler, never held on an open socket: a parked (never
answered) request makes Chrome serialize every later request behind it
and the page starves.

    POST /__delay {"path_prefix": "/api/conversations", "auth": false,
                   "status": 401, "delay_ms": 1500}
        The next matching request (any method) sleeps ``delay_ms``
        before answering with ``status``.
    POST /__fail {"path_prefix": "/api/conversations", "auth": false,
                  "status": 500, "skip": 1, "times": 1}
        The next ``times`` matching requests after ``skip`` matches are
        answered with ``status`` immediately.
    POST /__token {"token": "..."}
        Rotates the accepted credential (server-side rotation).
    POST /__reset
        Drops every pending rule.
    GET  /__state
        The request log and rule state.

Usage:
    python3 -I mock_auth.py <index.html> [--empty]

Prints ``PORT=<n>`` once listening on an ephemeral loopback port.
``--empty`` seeds zero conversations so the authenticated empty-state
boot path is exercised.
"""

import argparse
import json
import sys
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any
from urllib.parse import parse_qs, urlparse

# Synthetic credential. Never a live value; appears in test code only.
VALID_TOKEN = "wrkspc-auth-valid-token"
COOKIE_NAME = "wrkspc_token"

MESSAGE = {
    "id": "ma1",
    "sender_id": "codex",
    "content": "A-msg-1",
    "created_at": "2026-11-09T10:00:00Z",
    "deliveries": [{"peer_id": "codex", "state": "delivered"}],
}


class Rule:
    """One scenario knob: delay or fail matching requests."""

    def __init__(self, path_prefix: str, auth: bool | None, status: int,
                 delay_ms: int, skip: int, times: int, kind: str) -> None:
        self.path_prefix = path_prefix
        self.auth = auth
        self.status = status
        self.delay_ms = delay_ms
        self.skip = skip
        self.times = times
        self.kind = kind  # "delay" | "fail"
        self.consumed = 0


class MockState:
    """Request log and scenario rules, shared across handler threads."""

    def __init__(self, empty: bool) -> None:
        self.lock = threading.Lock()
        self.current_token = VALID_TOKEN
        self.requests: list[dict[str, Any]] = []
        self.rules: list[Rule] = []
        self.room: dict[str, Any] | None = None if empty else {
            "id": "room-1",
            "name": "Auth room",
            "members": ["operator", "codex"],
            "created_at": "2026-11-09T10:00:00Z",
        }

    def log(self, path: str, auth: str | None, cookie: str | None,
            status: int, delayed: bool, served: bool = False) -> None:
        with self.lock:
            self.requests.append({
                "seq": len(self.requests) + 1,
                "path": path,
                "auth": auth,
                "cookie": cookie,
                "status": status,
                "delayed": delayed,
                "served": served,
            })

    def rule_for(self, path: str, auth: bool) -> Rule | None:
        """The first rule that should consume this request, or None."""
        with self.lock:
            for rule in self.rules:
                if not path.startswith(rule.path_prefix):
                    continue
                if rule.auth is not None and rule.auth != auth:
                    continue
                if rule.skip > 0:
                    rule.skip -= 1
                    continue
                if rule.times <= 0:
                    continue
                rule.times -= 1
                rule.consumed += 1
                return rule
            return None

    def rotate_token(self, value: str) -> None:
        with self.lock:
            self.current_token = value

    def drop_rules(self) -> None:
        with self.lock:
            self.rules = []

    def stats(self) -> dict[str, Any]:
        with self.lock:
            return {
                "requests": list(self.requests),
                "rules": len(self.rules),
            }


def make_handler(page: bytes, state: MockState) -> type[BaseHTTPRequestHandler]:
    class Handler(BaseHTTPRequestHandler):
        def log_message(self, format: str, *args: Any) -> None:  # noqa: A002
            return

        def _send(self, status: int, body: Any, ctype: str = "application/json",
                  headers: dict[str, str] | None = None) -> None:
            data = body if isinstance(body, bytes) else json.dumps(body).encode()
            self.send_response(status)
            self.send_header("Content-Type", ctype)
            self.send_header("Content-Length", str(len(data)))
            for name, value in (headers or {}).items():
                self.send_header(name, value)
            self.end_headers()
            self.wfile.write(data)

        def _auth(self) -> str | None:
            header = self.headers.get("Authorization", "")
            if header.startswith("Bearer "):
                return header[len("Bearer "):]
            return None

        def _cookie(self) -> str | None:
            header = self.headers.get("Cookie", "")
            for part in header.split("; "):
                if part.startswith(COOKIE_NAME + "="):
                    return part[len(COOKIE_NAME) + 1:]
            return None

        def _authorized(self, auth: str | None, cookie: str | None) -> bool:
            with state.lock:
                current = state.current_token
            return (auth == current) or (cookie == current)

        def _api_get(self, parts: list[str], query: dict[str, list[str]],
                     auth: str | None, cookie: str | None) -> None:
            if not self._authorized(auth, cookie):
                state.log(self.path, auth, cookie, 401, False)
                return self._send(401, {"error": "unauthorized"})
            if parts == ["api", "conversations"]:
                state.log(self.path, auth, cookie, 200, False)
                rooms = [] if state.room is None else [state.room]
                return self._send(200, rooms)
            if parts == ["api", "peers"]:
                state.log(self.path, auth, cookie, 200, False)
                return self._send(200, [])
            if parts[:1] == ["api"] and parts[-1] == "moderation":
                state.log(self.path, auth, cookie, 200, False)
                return self._send(200, [])
            if parts == ["api", "attempts", "unknown"]:
                state.log(self.path, auth, cookie, 200, False)
                return self._send(200, [])
            if len(parts) >= 3 and parts[:2] == ["api", "conversations"]:
                cid = parts[2]
                if state.room is None or cid != state.room["id"]:
                    state.log(self.path, auth, cookie, 404, False)
                    return self._send(404, {"error": "not found"})
                if len(parts) == 3:
                    state.log(self.path, auth, cookie, 200, False)
                    return self._send(200, state.room)
                sub = parts[3]
                if sub == "messages":
                    state.log(self.path, auth, cookie, 200, False)
                    return self._send(200, [MESSAGE])
                if sub in ("fences", "guidance"):
                    state.log(self.path, auth, cookie, 200, False)
                    return self._send(200, [])
                if sub == "events":
                    state.log(self.path, auth, cookie, 200, False)
                    return self._send(200, [])
            state.log(self.path, auth, cookie, 404, False)
            self._send(404, {"error": "not found"})

        def _control(self, body: bytes) -> None:
            payload = {}
            try:
                payload = json.loads(body.decode() or "{}")
            except ValueError:
                pass
            path = urlparse(self.path).path
            if path in ("/__delay", "/__fail"):
                with state.lock:
                    state.rules.append(Rule(
                        payload.get("path_prefix", "/api/"),
                        payload.get("auth"),
                        int(payload.get("status", 500 if path == "/__fail" else 401)),
                        int(payload.get("delay_ms", 0)),
                        int(payload.get("skip", 0)),
                        int(payload.get("times", 1)),
                        "delay" if path == "/__delay" else "fail",
                    ))
                return self._send(200, {"ok": True})
            if path == "/__token":
                state.rotate_token(str(payload.get("token", "")))
                return self._send(200, {"ok": True})
            if path == "/__reset":
                state.drop_rules()
                return self._send(200, {"ok": True})
            self._send(404, {"error": "not found"})

        def do_GET(self) -> None:  # noqa: N802
            url = urlparse(self.path)
            parts = [p for p in url.path.split("/") if p]
            if not parts:
                return self._send(200, page, "text/html; charset=utf-8")
            if parts == ["__state"]:
                return self._send(200, state.stats())
            auth = self._auth()
            cookie = self._cookie()
            rule = state.rule_for(self.path, auth is not None)
            if rule is not None:
                state.log(self.path, auth, cookie, rule.status, True)
                if rule.delay_ms > 0:
                    time.sleep(rule.delay_ms / 1000.0)
                state.log(self.path, auth, cookie, rule.status, False, served=True)
                return self._send(rule.status, {"error": "scenario response"})
            self._api_get(parts, parse_qs(url.query), auth, cookie)

        def do_POST(self) -> None:  # noqa: N802
            length = int(self.headers.get("Content-Length", "0"))
            body = self.rfile.read(length)
            path = urlparse(self.path).path
            if path == "/api/login":
                auth = self._auth()
                cookie = self._cookie()
                rule = state.rule_for(self.path, auth is not None)
                if rule is not None:
                    state.log(self.path, auth, cookie, rule.status, True)
                    if rule.delay_ms > 0:
                        time.sleep(rule.delay_ms / 1000.0)
                    state.log(self.path, auth, cookie, rule.status, False, served=True)
                    # A scenario-delayed successful login still sets
                    # the cookie like the real route would.
                    if rule.status < 400:
                        with state.lock:
                            current = state.current_token
                        return self._send(rule.status, "", headers={
                            "Set-Cookie": (f"{COOKIE_NAME}={current}; HttpOnly; "
                                           "SameSite=Strict; Path=/"),
                        })
                    return self._send(rule.status, {"error": "scenario response"})
                payload = {}
                try:
                    payload = json.loads(body.decode() or "{}")
                except ValueError:
                    pass
                with state.lock:
                    current = state.current_token
                if payload.get("token") == current:
                    state.log(self.path, auth, cookie, 204, False)
                    return self._send(204, "", headers={
                        "Set-Cookie": (f"{COOKIE_NAME}={current}; HttpOnly; "
                                       "SameSite=Strict; Path=/"),
                    })
                state.log(self.path, auth, cookie, 401, False)
                return self._send(401, {"error": "unauthorized"})
            self._control(body)

    return Handler


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("page", help="the UI page HTML to serve")
    parser.add_argument("--empty", action="store_true",
                        help="seed zero conversations")
    args = parser.parse_args()
    with open(args.page, "rb") as handle:
        page = handle.read()
    state = MockState(args.empty)
    server = ThreadingHTTPServer(("127.0.0.1", 0), make_handler(page, state))
    print(f"PORT={server.server_address[1]}", flush=True)
    server.serve_forever()


if __name__ == "__main__":
    main()
