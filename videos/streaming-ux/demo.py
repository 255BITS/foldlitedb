"""Local SSE demo, real-browser captures, and native/Wasm verification.

The tiny HTTP transport and its integer document cursor belong to this example,
not to the FoldliteDB database API. Never use this fixture server in production.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import tempfile
import threading
from dataclasses import asdict
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import parse_qs, urlparse

HERE = Path(__file__).resolve().parent
ROOT = HERE.parents[1]
ASSETS = ROOT / "src/foldlitedb/portable_assets"
INITIAL = 7
PAYLOADS = [
    ("latency", 42),
    ("latency", 12),
    ("latency", 6),
    ("checkout", 120),
    ("checkout", 80),
    ("worker", 60),
    ("worker", 80),
    ("latency", 18),
    ("checkout", 40),
    ("latency", 10),
    ("worker", 40),
    ("latency", 8),
    ("checkout", 60),
]
DOCUMENTS = [
    {
        "id": f"reading-{i}",
        "source": "video",
        "position": i,
        "payload": {"metric": key, "value": value},
    }
    for i, (key, value) in enumerate(PAYLOADS)
]


class DemoServer(ThreadingHTTPServer):
    daemon_threads = True

    def __init__(self, address):
        self.published = INITIAL
        self.condition = threading.Condition()
        self.stopping = False
        super().__init__(address, Handler)

    def stop(self):
        with self.condition:
            self.stopping = True
            self.condition.notify_all()
        self.shutdown()
        self.server_close()


class Handler(BaseHTTPRequestHandler):
    def log_message(self, *_):
        pass

    def respond(self, data, content_type="application/json"):
        if not isinstance(data, bytes):
            data = json.dumps(data).encode()
        self.send_response(200)
        self.send_header("Content-Type", content_type)
        self.send_header("Content-Length", str(len(data)))
        self.send_header("Cache-Control", "no-store")
        self.end_headers()
        self.wfile.write(data)

    def do_GET(self):
        parsed = urlparse(self.path)
        if parsed.path == "/":
            return self.respond(
                (HERE / "demo.html").read_bytes(), "text/html; charset=utf-8"
            )
        if parsed.path.startswith("/portable/"):
            name = parsed.path.removeprefix("/portable/")
            if "/" in name or name not in {p.name for p in ASSETS.iterdir()}:
                return self.send_error(404)
            mime = (
                "application/wasm"
                if name.endswith(".wasm")
                else "text/javascript"
                if name.endswith(".js")
                else "application/json"
            )
            return self.respond((ASSETS / name).read_bytes(), mime)
        if parsed.path.startswith("/fonts/"):
            name = parsed.path.removeprefix("/fonts/")
            if name not in {
                "OpenSans-Regular.ttf",
                "OpenSans-Bold.ttf",
                "SourceCodePro-Regular.otf",
            }:
                return self.send_error(404)
            return self.respond(
                (HERE / "assets/fonts" / name).read_bytes(),
                "font/ttf" if name.endswith(".ttf") else "font/otf",
            )
        if parsed.path == "/bootstrap":
            # One consistent document prefix and its application watermark.
            with self.server.condition:
                end = self.server.published
            return self.respond({"documents": DOCUMENTS[:end], "cursor": end})
        if parsed.path != "/events":
            return self.send_error(404)
        try:
            after = int(parse_qs(parsed.query).get("after", ["0"])[0])
            if not 0 <= after <= self.server.published:
                raise ValueError("invalid document cursor")
        except ValueError:
            return self.send_error(400)
        self.send_response(200)
        self.send_header("Content-Type", "text/event-stream")
        self.send_header("Cache-Control", "no-cache")
        self.end_headers()
        try:
            self.wfile.write(b": connected\n\n")
            self.wfile.flush()
            while True:
                with self.server.condition:
                    if self.server.stopping:
                        return
                    end = self.server.published
                    if after == end:
                        self.server.condition.wait(timeout=1)
                        continue
                for i in range(after, end):
                    value = {"cursor": i + 1, "document": DOCUMENTS[i]}
                    self.wfile.write(
                        ("event: frame\ndata: " + json.dumps(value) + "\n\n").encode()
                    )
                    self.wfile.flush()
                    after = i + 1
        except (BrokenPipeError, ConnectionResetError):
            pass

    def do_POST(self):
        if self.path != "/publish":
            return self.send_error(404)
        with self.server.condition:
            self.server.published = min(len(DOCUMENTS), self.server.published + 1)
            self.server.condition.notify_all()
            count = self.server.published
        self.respond({"published": count})


def normalized(rows):
    return sorted(
        ({"key": r["key"], "result": r["result"], "state": r["state"]} for r in rows),
        key=lambda r: r["key"],
    )


def verify_native(browser_rows):
    from foldlitedb import CursorExpired, Database, Document, View

    # Use exactly the module bytes served to Chromium. Install this checkout in
    # the recording environment to exercise its matching native engine.
    view = View((ASSETS / "metrics.wasm").read_bytes())
    documents = [
        Document(d["id"], d["payload"], d["position"], d["source"]) for d in DOCUMENTS
    ]
    with tempfile.TemporaryDirectory(prefix="foldlite-video-") as directory:
        path = Path(directory) / "observations.fold"
        with Database(path, view) as db:
            db.ingest(documents[:INITIAL])
            snapshot = db.query()
            db.ingest(documents[INITIAL:])
            page = db.changes(since=snapshot.cursor, limit=1)
            applied = {r.key: r.result for r in snapshot.rows}
            pages = 0
            while True:
                applied.update((r.key, r.result) for r in page.rows)
                pages += 1
                if page.complete:
                    break
                page = db.changes(since=page.cursor, limit=1)
            expected = normalized([asdict(r) for r in db.rows()])
            assert expected == normalized(browser_rows), (
                "Native and browser results differ"
            )
            assert applied == {r["key"]: r["result"] for r in expected}
            before_retry = db.stats()["sequence"]
            retry = db.ingest(documents)
            assert retry.inserted == 0 and retry.replayed == len(documents)
            assert db.stats()["sequence"] == before_retry
            old_cursor = db.query().cursor
        digest = hashlib.sha256(path.read_bytes()).hexdigest()
        Path(str(path) + ".cache").unlink()
        with Database(path, view) as db:
            assert normalized([asdict(r) for r in db.rows()]) == expected
            assert db.stats()["cache"]["status"] == "missing"
            try:
                db.changes(since=old_cursor)
            except CursorExpired:
                pass
            else:
                raise AssertionError("Reconstruction must expire the previous cursor")
        assert hashlib.sha256(path.read_bytes()).hexdigest() == digest
    return {
        "native_browser_parity": True,
        "named_retry_noop": True,
        "cache_reconstruction": True,
        "expired_cursor_rejected": True,
        "log_unchanged": True,
        "native_change_pages": pages,
        "rows": expected,
    }


def capture(server, output):
    from playwright.sync_api import sync_playwright

    output.mkdir(parents=True, exist_ok=True)
    errors = []
    stages = []
    with sync_playwright() as p:
        # Reuse a locally installed Chromium; install through Playwright if absent.
        browser = p.chromium.launch(headless=True)
        page = browser.new_page(
            viewport={"width": 1440, "height": 540}, device_scale_factor=1.5
        )
        page.on("pageerror", lambda error: errors.append(str(error)))
        page.goto(f"http://127.0.0.1:{server.server_port}/")
        page.wait_for_function("window.demo?.ready === true")
        page.evaluate("document.fonts.ready")

        def shot(label, cursor):
            page.wait_for_function("n => window.demo.cursor === n", arg=cursor)
            # Finish the CSS highlight before the deterministic still capture.
            page.evaluate(
                "document.querySelectorAll('*').forEach(e => e.getAnimations().forEach(a => a.finish()))"
            )
            name = f"demo-{len(stages):02}.png"
            page.screenshot(path=str(output / name))
            stages.append({"file": name, "label": label, "cursor": cursor})

        shot("Bootstrap: a consistent prefix of 7 observations", INITIAL)
        checkout = page.locator('[data-key="checkout"] .mean').inner_text()
        page.request.post(f"http://127.0.0.1:{server.server_port}/publish")
        shot("Latency changes to 19.5 ms; checkout stays at 100 ms", 8)
        assert page.locator('[data-key="latency"] .mean').inner_text() == "19.5"
        assert page.locator('[data-key="checkout"] .mean').inner_text() == checkout
        page.request.post(f"http://127.0.0.1:{server.server_port}/publish")
        shot("A checkout observation updates only its keyed row", 9)
        page.click("#connection")
        page.wait_for_function("window.demo.paused")
        shot("Connection paused; the page retains its current state", 9)
        for _ in range(3):
            page.request.post(f"http://127.0.0.1:{server.server_port}/publish")
        shot("Three more facts arrive while the browser is disconnected", 9)
        assert page.evaluate("window.demo.cursor") == 9
        page.click("#connection")
        shot("Reconnect after cursor 9 and apply the missing observations", 12)
        page.request.post(f"http://127.0.0.1:{server.server_port}/publish")
        shot("Back on the live tail; no page reload", 13)
        browser_rows = page.evaluate("window.demo.db.rows()")
        replay = page.evaluate("window.demo.db.replay()")
        assert replay["equivalent"] is True
        assert not errors, errors
        browser.close()
    report = verify_native(browser_rows)
    report.update(
        {
            "browser_replay_equivalent": True,
            "page_errors": errors,
            "stages": stages,
            "module_sha256": hashlib.sha256(
                (ASSETS / "metrics.wasm").read_bytes()
            ).hexdigest(),
        }
    )
    (output / "verification.json").write_text(json.dumps(report, indent=2) + "\n")
    print(json.dumps(report, indent=2))


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--capture", action="store_true")
    parser.add_argument("--port", type=int, default=8765)
    parser.add_argument("--output", type=Path, default=HERE / ".build/demo")
    args = parser.parse_args()
    server = DemoServer(("127.0.0.1", 0 if args.capture else args.port))
    if args.capture:
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()
        try:
            capture(server, args.output)
        finally:
            server.stop()
            thread.join(timeout=5)
    else:
        print(f"Open http://127.0.0.1:{server.server_port}/", flush=True)
        try:
            server.serve_forever()
        except KeyboardInterrupt:
            server.server_close()


if __name__ == "__main__":
    main()
