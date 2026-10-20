"""Build the dependency-free GitHub Pages documentation into a static directory."""
from __future__ import annotations

import argparse
import html
from html.parser import HTMLParser
import json
from pathlib import Path
import shutil
import tempfile


ROOT = Path(__file__).resolve().parents[1]
SOURCE = ROOT / "docs"

class Outline(HTMLParser):
    def __init__(self):
        super().__init__()
        self.headings = []
        self.text = []
        self.current = None

    def handle_starttag(self, tag, attrs):
        if tag in ("h1", "h2", "h3"):
            self.current = [tag, dict(attrs).get("id"), []]

    def handle_data(self, data):
        self.text.append(data)
        if self.current is not None:
            self.current[2].append(data)

    def handle_endtag(self, tag):
        if self.current is not None and tag == self.current[0]:
            kind, identifier, pieces = self.current
            self.headings.append((kind, identifier, "".join(pieces)))
            self.current = None


def build(output: Path):
    output = output.resolve()
    if output == ROOT or output in ROOT.parents or output == SOURCE or SOURCE in output.parents:
        raise ValueError("Build output must be outside the documentation source and repository root")
    if output.exists() and not (output / ".foldlitedb-docs").is_file():
        raise ValueError(f"Refusing to replace an unrecognized directory: {output}")
    pages = json.loads((SOURCE / "pages.json").read_text())
    layout = (SOURCE / "layout.html").read_text()
    search = []
    output.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory(prefix="foldlitedb-docs-", dir=output.parent) as temporary:
        staged = Path(temporary) / "site"
        staged.mkdir()
        for page in pages:
            filename = page["file"]
            body = (SOURCE / filename).read_text()
            outline = Outline()
            outline.feed(body)
            navigation = "\n".join(
                f'<a href="{html.escape(other["file"])}"' +
                (' aria-current="page"' if other["file"] == filename else '') +
                f'>{html.escape(other["title"])}</a>' for other in pages)
            toc = "\n".join(f'<a href="#{html.escape(identifier)}">{html.escape(title)}</a>'
                            for kind, identifier, title in outline.headings
                            if kind == "h2" and identifier)
            rendered = layout
            for key, value in {"title": html.escape(page["title"]),
                               "description": html.escape(page["description"]),
                               "navigation": navigation, "toc": toc, "body": body}.items():
                rendered = rendered.replace("{{" + key + "}}", value)
            if "{{" in rendered:
                raise ValueError(f"Unresolved template field in {filename}")
            (staged / filename).write_text(rendered)
            search.append({"title": page["title"], "url": filename,
                           "text": " ".join(" ".join(outline.text).split())})
            for kind, identifier, title in outline.headings:
                if kind != "h1" and identifier:
                    search.append({"title": title, "url": filename + "#" + identifier,
                                   "text": title})
        for asset in (SOURCE / "static").iterdir():
            if not asset.is_file() or asset.is_symlink():
                raise ValueError(f"Only regular static assets are supported: {asset}")
            shutil.copy2(asset, staged / asset.name)
        portable = ROOT / "src" / "foldlitedb" / "portable_assets"
        if portable.is_dir():
            shutil.copytree(portable, staged / "portable", ignore=shutil.ignore_patterns("__pycache__", "*.pyc"))
        examples = staged / "examples"
        examples.mkdir()
        for example in sorted((ROOT / "examples").glob("*.py")):
            if example.is_symlink():
                raise ValueError(f"Only regular examples are supported: {example}")
            shutil.copy2(example, examples / example.name)
        (staged / "search.json").write_text(json.dumps(search, ensure_ascii=False) + "\n")
        (staged / ".nojekyll").touch()
        managed = [path.relative_to(staged).as_posix() for path in staged.rglob("*") if path.is_file()] + [".foldlitedb-docs"]
        (staged / ".foldlitedb-docs").write_text(json.dumps({
            "generator": "foldlitedb-docs/v1", "files": sorted(managed)}) + "\n")
        if output.exists():
            shutil.rmtree(output)
        shutil.copytree(staged, output)
    print(f"Built {len(pages)} documentation pages in {output}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, default=ROOT / "_site")
    build(parser.parse_args().output)
