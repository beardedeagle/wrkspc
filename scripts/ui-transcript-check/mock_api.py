#!/usr/bin/env python3
"""Mock wrkspc operator API that serves the real UI page for check.mjs.

Serves ``index.html`` with a sampling probe injected and fakes the
``/api/*`` endpoints the page reads. Conversation A holds one finished
agent turn streamed as ``a_count`` chunks; conversation B holds a turn
still streaming. Events take the dispatcher's shape (``dispatch.started``,
``turn.chunk`` fragments sharing an ``attempt_id``, ``dispatch.stopped``)
and follow the real endpoint's contract: ``seq > after_seq``, ordered by
seq, at most ``limit`` (default 500). A's events endpoint is slower than
the page's 1 s poll, so polls overlap and a stale A response can land
after a switch to B.

Usage:
    python3 -I mock_api.py <index.html> <a_count>

Prints ``PORT=<n>`` once listening on an ephemeral loopback port.
"""

import json
import sys
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any
from urllib.parse import parse_qs, urlparse

# B's message: markdown the page must render, plus text that must stay inert
# (a javascript: link, a raw tag) and a local path that must not become a link.
B_MESSAGE = (
    "B-msg-1 with **bold**, `code`, [a site](https://example.com/), "
    "[a file](/Users/someone/SKILL.md), [bad](javascript:alert(1)) and "
    "<img src=x onerror=alert(1)>\n\n```text\nfenced\n```"
)

# When B's entries happened, in transcript order (message, dispatch.started,
# first chunk). The chunk uses the store's microsecond RFC 3339 form.
B_MESSAGE_AT = "2026-11-02T09:00:00Z"
B_STARTED_AT = "2026-11-02T09:01:00Z"
B_TURN_AT = "2026-11-02T09:01:05.123456+00:00"

# Seconds each conversation-A events page takes: longer than the 1 s poll.
SLOW_A_SECONDS = 1.5
# The real endpoint's default page size (crates/wrkspc-ui/src/api.rs).
DEFAULT_LIMIT = 500

# Records transcript state every 25 ms so check.mjs can see every frame's
# DOM, not just the final one. Seeds the token so the page boots past login.
PROBE = """<script>
localStorage.setItem("wrkspc_token", "check");
window.__probe = { t0: performance.now(), samples: [] };
setInterval(() => {
  const transcript = document.getElementById("transcript");
  if (!transcript) return;
  window.__probe.samples.push({
    t: performance.now() - window.__probe.t0,
    gate: !!document.getElementById("boot-gate"),
    bootHidden: document.getElementById("boot").hidden,
    loading: document.getElementById("transcript-scroll").classList.contains("loading"),
    msgs: transcript.querySelectorAll(".msg").length,
    events: transcript.querySelectorAll(".event").length,
    text: transcript.textContent,
  });
}, 25);
</script>
"""


def _event(cid: str, seq: int, kind: str, payload: dict[str, Any],
           created_at: str = "2026-11-02T09:00:00Z") -> dict[str, Any]:
    """Builds one event in the endpoint's wire shape.

    Args:
        cid (str): The conversation id.
        seq (int): The event sequence number.
        kind (str): The event kind.
        payload (dict[str, Any]): The event payload.
        created_at (str): When the event was committed (RFC 3339).

    Returns:
        dict[str, Any]: The event.
    """
    return {"seq": seq, "conversation_id": cid, "kind": kind,
            "payload": payload, "created_at": created_at}


def _chunk_text(cid: str, n: int) -> str:
    """Returns the ``n``-th streamed fragment of conversation ``cid``'s turn.

    Args:
        cid (str): The conversation id.
        n (int): The 1-based fragment number.

    Returns:
        str: The fragment, with the trailing space a token stream carries.
    """
    return f"{cid}-chunk-{n} "


def _chunk(cid: str, seq: int, n: int, message_id: str,
           created_at: str = "2026-11-02T09:00:00Z") -> dict[str, Any]:
    """Builds conversation ``cid``'s ``n``-th ``turn.chunk`` event.

    Args:
        cid (str): The conversation id.
        seq (int): The event sequence number.
        n (int): The 1-based fragment number.
        message_id (str): The agent message the fragment belongs to.
        created_at (str): When the event was committed (RFC 3339).

    Returns:
        dict[str, Any]: The event.
    """
    return _event(cid, seq, "turn.chunk", {"attempt_id": f"attempt-{cid}",
                                           "session_id": f"session-{cid}",
                                           "content": _chunk_text(cid, n),
                                           "message_id": message_id}, created_at)


class MockState:
    """Conversation data plus request counters, shared across threads.

    Attributes:
        lock (threading.Lock): Guards every mutable field below.
        events (dict[str, list[dict[str, Any]]]): Events per conversation.
        b_chunks (int): Fragments streamed so far in B's open turn.
        a_inflight (int): Conversation-A events requests in progress.
        a_from_zero (int): Conversation-A events requests with after_seq=0.
    """

    def __init__(self, a_count: int) -> None:
        """Seeds conversations A and B.

        Args:
            a_count (int): Number of fragments in A's finished codex turn
                (``a_count + 2`` events with its start and stop); B's
                claude turn follows in seq order, two fragments in.
        """
        self.lock = threading.Lock()
        self.conversations: list[dict[str, Any]] = [
            {"id": "A", "members": ["operator", "codex"], "created_at": "2026-11-01T00:00:00Z"},
            {"id": "B", "members": ["operator", "claude"], "created_at": "2026-11-01T00:00:00Z"},
        ]
        # A's message is newer, so the page's boot picks A.
        self.messages: dict[str, list[dict[str, Any]]] = {
            "A": [{"id": "ma1", "sender_id": "codex", "content": "A-msg-1",
                   "created_at": "2026-11-02T10:00:00Z", "deliveries": []}],
            "B": [{"id": "mb1", "sender_id": "claude", "content": B_MESSAGE,
                   "created_at": B_MESSAGE_AT, "deliveries": []}],
        }
        a_stop = a_count + 3
        self.events: dict[str, list[dict[str, Any]]] = {
            "A": [
                # A work report authored before the message, so the transcript
                # has to merge sorted rather than list messages then events.
                _event("A", 1, "work.report",
                       {"peer_id": "codex", "summary": "A-report-1"}, "2026-11-02T09:59:00Z"),
                _event("A", 2, "dispatch.started", {"attempt_id": "attempt-A", "recipient_id": "codex"}),
                *(_chunk("A", 2 + n, n, "a-1") for n in range(1, a_count + 1)),
                _event("A", a_stop, "dispatch.stopped", {"attempt_id": "attempt-A", "state": "completed"}),
                # One delivery: claim then ack, which the feed collapses to a
                # single line per delivery.
                # Historical shape: the claim carries no recipient, the ack does.
                _event("A", a_stop + 1, "delivery.claimed",
                       {"task_id": "task-A", "host": "node-test-1"},
                       "2026-11-02T10:00:05Z"),
                _event("A", a_stop + 2, "delivery.acked",
                       {"task_id": "task-A", "host": "node-test-1", "recipient_peer": "codex"},
                       "2026-11-02T10:00:06Z"),
            ],
            "B": [
                _event("B", a_stop + 3, "dispatch.started",
                       {"attempt_id": "attempt-B", "recipient_id": "claude"}, B_STARTED_AT),
                _chunk("B", a_stop + 4, 1, "b-1", B_TURN_AT),
                _chunk("B", a_stop + 5, 2, "b-1", B_TURN_AT),
            ],
        }
        self.b_chunks = 2
        self.added_events = 0
        self.a_inflight = 0
        self.a_from_zero = 0
        self.aliases: dict[str, str] = {}
        self.last_patch: dict[str, Any] = {}
        self.last_delete: dict[str, Any] = {}
        self.peer_status: dict[str, str] = {}
        self.forgotten: set[str] = set()
        self.moderation: list[dict[str, Any]] = []

    def _mod_event(self, peer_id: str, action: str, after: str | None) -> dict[str, Any]:
        """Appends and returns one moderation audit row (operator actor)."""
        event = {
            "id": len(self.moderation) + 1,
            "peer_id": peer_id,
            "actor": "operator",
            "action": action,
            "before": None,
            "after": after,
            "at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        }
        self.moderation.append(event)
        return event

    def peers_view(self) -> list[dict[str, Any]]:
        """Roster rows whose ``updated_at`` churns per request — the real
        nodes re-upload presence every poll tick, so the page's peers
        payload changes on every 1 s poll. A rename editor must survive
        that. Forgotten peers are gone; booted/banned statuses carry
        through from PATCHes."""
        now = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
        rows = []
        for pid, host in (("codex", "randys-macbook-pro.mtls.devlocal.home.arpa"),
                          ("operator", "wrkspc-pilot-host")):
            if pid in self.forgotten:
                continue
            rows.append({"peer_id": pid, "alias": self.aliases.get(pid),
                         "host_id": host, "state": "connected",
                         "status": self.peer_status.get(pid, "enrolled"),
                         "updated_at": now})
        return rows


def make_handler(page: str, state: MockState) -> type[BaseHTTPRequestHandler]:
    """Builds the request handler class bound to one page and state.

    Args:
        page (str): The UI page HTML, probe already injected.
        state (MockState): The shared mock state.

    Returns:
        type[BaseHTTPRequestHandler]: The handler class.
    """

    class Handler(BaseHTTPRequestHandler):
        """Routes the page's ``/api/*`` reads and the check's controls."""

        def log_message(self, format: str, *args: Any) -> None:  # noqa: A002
            """Silences per-request logging."""

        def _send(self, status: int, body: Any, ctype: str = "application/json") -> None:
            """Writes one complete response.

            Args:
                status (int): The HTTP status.
                body (Any): Raw bytes, or a value to encode as JSON.
                ctype (str): The Content-Type header.
            """
            data = body if isinstance(body, bytes) else json.dumps(body).encode()
            self.send_response(status)
            self.send_header("Content-Type", ctype)
            self.send_header("Content-Length", str(len(data)))
            self.end_headers()
            self.wfile.write(data)

        def _events(self, cid: str, query: dict[str, list[str]]) -> None:
            """Serves one events page with the real endpoint's contract.

            Args:
                cid (str): The conversation id.
                query (dict[str, list[str]]): The parsed query string.
            """
            try:
                after = int(query.get("after_seq", ["0"])[0])
                limit = int(query.get("limit", [str(DEFAULT_LIMIT)])[0])
            except ValueError:
                return self._send(400, {"error": "bad query"})
            if cid == "A":
                with state.lock:
                    state.a_inflight += 1
                    if after == 0:
                        state.a_from_zero += 1
                try:
                    time.sleep(SLOW_A_SECONDS)
                finally:
                    with state.lock:
                        state.a_inflight -= 1
            with state.lock:
                page_events = [e for e in state.events[cid] if e["seq"] > after][:limit]
            self._send(200, page_events)

        def do_GET(self) -> None:  # noqa: N802
            """Serves the page, the mock API, and ``/__stats``."""
            url = urlparse(self.path)
            parts = [p for p in url.path.split("/") if p]
            if not parts:
                return self._send(200, page.encode(), "text/html; charset=utf-8")
            if parts == ["__stats"]:
                with state.lock:
                    return self._send(200, {"a_inflight": state.a_inflight,
                                            "a_from_zero": state.a_from_zero,
                                            "last_patch": state.last_patch,
                                            "last_delete": state.last_delete})
            if parts == ["api", "conversations"]:
                return self._send(200, state.conversations)
            if parts == ["api", "peers"]:
                return self._send(200, state.peers_view())
            if parts == ["api", "moderation"]:
                return self._send(200, list(reversed(state.moderation)))
            if parts == ["api", "attempts", "unknown"]:
                return self._send(200, [])
            if len(parts) in (3, 4) and parts[:2] == ["api", "conversations"] and parts[2] in state.messages:
                cid = parts[2]
                if len(parts) == 3:
                    return self._send(200, next(c for c in state.conversations if c["id"] == cid))
                if parts[3] == "messages":
                    return self._send(200, state.messages[cid])
                if parts[3] == "fences":
                    return self._send(200, [])
                if parts[3] == "events":
                    return self._events(cid, parse_qs(url.query))
            self._send(404, {"error": "not found"})

        def do_POST(self) -> None:  # noqa: N802
            """Streams a fragment into B's turn, or adds a narration event.

            ``/__add_b_chunk`` appends a turn fragment; ``/__add_event`` appends
            a delivery ack with its own task id, so the system feed grows with a
            new line each call. Both are test levers, not part of the product.
            """
            if self.path == "/__add_b_chunk":
                with state.lock:
                    seq = max(e["seq"] for events in state.events.values() for e in events) + 1
                    state.b_chunks += 1
                    state.events["B"].append(_chunk("B", seq, state.b_chunks, "b-2"))
                    chunks = state.b_chunks
                return self._send(200, {"chunks": chunks})
            if self.path.startswith("/__add_event"):
                query = parse_qs(urlparse(self.path).query)
                cid = query.get("conversation", ["B"])[0]
                with state.lock:
                    seq = max(e["seq"] for events in state.events.values() for e in events) + 1
                    state.added_events += 1
                    task_id = f"task-added-{state.added_events}"
                    state.events[cid].append(_event(cid, seq, "delivery.acked", {
                        "task_id": task_id,
                        "host": "node-test-1",
                        "recipient_peer": "codex",
                    }, "2026-11-02T12:00:00Z"))
                    added = state.added_events
                return self._send(200, {"added": added})
            self._send(404, {"error": "not found"})

        def do_PATCH(self) -> None:  # noqa: N802
            """Records every ``/api/peers/{id}`` PATCH (content-type + raw
            body) so check.mjs can pin the wire shape, and applies an alias."""
            url = urlparse(self.path)
            parts = [p for p in url.path.split("/") if p]
            if len(parts) != 3 or parts[:2] != ["api", "peers"]:
                return self._send(404, {"error": "not found"})
            length = int(self.headers.get("Content-Length", "0"))
            raw = self.rfile.read(length).decode()
            with state.lock:
                state.last_patch = {
                    "path": self.path,
                    "content_type": self.headers.get("Content-Type", ""),
                    "body": raw,
                }
                try:
                    payload = json.loads(raw)
                    if payload.get("alias") is not None:
                        state.aliases[parts[2]] = payload["alias"]
                        state._mod_event(parts[2], "alias", payload["alias"])
                    if payload.get("status") is not None:
                        state.peer_status[parts[2]] = payload["status"]
                        action = {"booted": "boot", "banned": "ban"}.get(payload["status"], "restore")
                        state._mod_event(parts[2], action, payload["status"])
                except ValueError:
                    pass
            self._send(200, {"ok": True})

        def do_DELETE(self) -> None:  # noqa: N802
            """Records every ``/api/peers/{id}`` DELETE (the forget action)
            and drops the peer from the roster view."""
            url = urlparse(self.path)
            parts = [p for p in url.path.split("/") if p]
            if len(parts) != 3 or parts[:2] != ["api", "peers"]:
                return self._send(404, {"error": "not found"})
            with state.lock:
                state.last_delete = {"path": self.path, "method": "DELETE"}
                state.forgotten.add(parts[2])
                state._mod_event(parts[2], "forget", None)
            self._send(204, b"")

    return Handler


def main() -> None:
    """Starts the mock on an ephemeral loopback port and serves forever.

    Raises:
        SystemExit: On wrong arguments.
    """
    if len(sys.argv) != 3:
        raise SystemExit("usage: mock_api.py <index.html> <a_count>")
    with open(sys.argv[1], encoding="utf-8") as f:
        html = f.read()
    if "</head>" not in html:
        raise SystemExit("page has no </head> to inject the probe before")
    page = html.replace("</head>", PROBE + "</head>", 1)
    server = ThreadingHTTPServer(("127.0.0.1", 0), make_handler(page, MockState(int(sys.argv[2]))))
    print(f"PORT={server.server_address[1]}", flush=True)
    server.serve_forever()


if __name__ == "__main__":
    main()
