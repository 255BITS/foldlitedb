"""Check static documentation links and optionally run Python snippets and Chromium."""
from __future__ import annotations

import argparse
from functools import partial
from html.parser import HTMLParser
from http.server import SimpleHTTPRequestHandler, ThreadingHTTPServer
import json
import os
from pathlib import Path
import tempfile
import threading
from urllib.parse import unquote, urlsplit


class Page(HTMLParser):
    def __init__(self):
        super().__init__()
        self.ids = set()
        self.links = []
        self.errors = []
        self.h1 = 0
        self.snippets = []
        self.native_snippets = []
        self.code = None
        self.code_kind = None

    def handle_starttag(self, tag, attrs):
        attrs = dict(attrs)
        identifier = attrs.get("id")
        if identifier:
            if identifier in self.ids:
                self.errors.append(f"duplicate ID {identifier}")
            self.ids.add(identifier)
        if tag == "h1":
            self.h1 += 1
        if tag in ("a", "link", "script", "img"):
            value = attrs.get("href", attrs.get("src"))
            if value:
                self.links.append(value)
        if tag == "pre" and attrs.get("data-run") in ("python", "native-python"):
            self.code = []
            self.code_kind = attrs["data-run"]

    def handle_data(self, data):
        if self.code is not None:
            self.code.append(data)

    def handle_endtag(self, tag):
        if tag == "pre" and self.code is not None:
            destination = self.native_snippets if self.code_kind == "native-python" else self.snippets
            destination.append("".join(self.code))
            self.code = None
            self.code_kind = None


def check(site: Path, *, execute=False, browser=False, native=False):
    site = site.resolve()
    pages = {}
    for filename in site.glob("*.html"):
        page = Page()
        source = filename.read_text()
        page.feed(source)
        assert not page.errors, (filename.name, page.errors)
        assert page.h1 == 1, f"{filename.name}: expected exactly one h1"
        assert '<meta name="viewport"' in source and "<title>" in source, filename
        assert "Development preview" in source, filename
        pages[filename.name] = page
    assert pages, f"No HTML pages found in {site}"
    links = 0
    for name, page in pages.items():
        for href in page.links:
            url = urlsplit(href)
            if url.scheme or url.netloc:
                assert url.scheme in ("https", "mailto"), (name, href)
                continue
            assert not url.path.startswith("/"), f"{name}: root-relative link breaks project hosting: {href}"
            target = (site / unquote(url.path or name)).resolve()
            assert site in target.parents, (name, href)
            assert target.is_file(), f"{name}: missing target {href}"
            if url.fragment:
                assert target.name in pages and unquote(url.fragment) in pages[target.name].ids, (name, href)
            links += 1
    for entry in json.loads((site / "search.json").read_text()):
        url = urlsplit(entry["url"])
        assert url.path in pages, entry
        assert not url.fragment or url.fragment in pages[url.path].ids, entry
    snippets = 0
    if execute:
        previous = Path.cwd()
        try:
            for name, page in pages.items():
                with tempfile.TemporaryDirectory(prefix="foldlitedb-doc-example-") as directory:
                    os.chdir(directory)
                    namespace = {"__name__": "__doc_example__"}
                    examples = page.snippets + (page.native_snippets if native else [])
                    for index, snippet in enumerate(examples):
                        exec(compile(snippet, f"{name}:example-{index + 1}", "exec"), namespace)
                        snippets += 1
        finally:
            os.chdir(previous)
    if browser:
        check_browser(site)
    print(f"Checked {len(pages)} pages, {links} local links, {snippets} executed Python examples")


def check_browser(site):
    from playwright.sync_api import sync_playwright

    class QuietHandler(SimpleHTTPRequestHandler):
        def log_message(self, *_):
            pass

    handler = partial(QuietHandler, directory=str(site))
    server = ThreadingHTTPServer(("127.0.0.1", 0), handler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        base = f"http://127.0.0.1:{server.server_port}"
        with sync_playwright() as playwright:
            chromium = playwright.chromium.launch()
            page = chromium.new_page(viewport={"width": 1440, "height": 1000})
            failures = []
            page.on("pageerror", lambda error: failures.append(str(error)))
            for path in ("index.html", "api.html", "concepts.html", "experiments.html"):
                page.goto(f"{base}/{path}")
                assert page.locator("h1").count() == 1
                assert page.evaluate("document.documentElement.scrollWidth <= innerWidth"), path
            page.goto(base + "/index.html")
            page.locator("#doc-search").fill("add_many")
            page.locator("#search-results a").first.wait_for()
            assert page.locator("#search-results a").count() > 0
            page.locator("#doc-search").fill("")
            page.set_viewport_size({"width": 390, "height": 844})
            assert page.evaluate("document.documentElement.scrollWidth <= innerWidth")
            menu = page.get_by_role("button", name="Menu", exact=True)
            menu.click()
            assert menu.get_attribute("aria-expanded") == "true"
            page.locator("#sidebar nav a[href='api.html']").click()
            page.wait_for_url("**/api.html")
            assert page.evaluate("document.documentElement.scrollWidth <= innerWidth")
            assert not failures, failures
            chromium.close()
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=5)
    print("Chromium checks passed at desktop and mobile widths")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--site", type=Path, default=Path(__file__).resolve().parents[1] / "_site")
    parser.add_argument("--execute", action="store_true", help="execute annotated Python examples")
    parser.add_argument("--browser", action="store_true", help="check interactions with installed Playwright Chromium")
    parser.add_argument("--native", action="store_true", help="also execute examples requiring the optional native package")
    args = parser.parse_args()
    check(args.site, execute=args.execute or args.native, browser=args.browser, native=args.native)
