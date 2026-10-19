"""Build the dependency-free GitHub Pages documentation into a static directory."""
from __future__ import annotations

import argparse
import html
from html.parser import HTMLParser
import json
from pathlib import Path
import shutil
import statistics
import tempfile


ROOT = Path(__file__).resolve().parents[1]
SOURCE = ROOT / "docs"


def performance(report):
    if report.get("complete") is not True:
        raise ValueError("Refusing to publish an incomplete benchmark report")
    trials = report["trials"]
    selected = [trial for trial in trials if trial["shape"] == "uniform"]
    settings = selected[0]
    runtime = report["runtime"]
    mount = runtime.get("storage_mount", {}).get("filesystems", [{}])[0]
    storage = " / ".join(str(mount.get(key, "unknown")) for key in ("fstype", "source"))
    repeats = sorted({trial["repeat"] for trial in selected})

    def median(cases, field):
        return statistics.median(trial[field] for trial in cases)

    result = [
        f'<p>Local medians from {len(trials)} fresh-process trials, with {len(repeats)} '
        f'repeats per configuration. The illustrated uniform workload has '
        f'{settings["documents"]:,} documents, {settings["keys"]} keys, '
        f'{settings["batch_size"]}-document durable batches, and {settings["live_documents"]} '
        f'live updates using <code>{html.escape(settings["reducer"])}</code>. '
        f'The host exposes {runtime["affinity_cpu_count"]} logical CPUs; storage is '
        f'<code>{html.escape(storage)}</code>.</p>',
        '<p>Ingestion timings exclude numeric runtime warm-up and include the same batch '
        'boundaries and durable acknowledgments. Within each matched workload, '
        'fixture and result hashes agree across engines and worker counts.</p>',
    ]
    for map_name, label in (("cheap", "Cheap field mapping"), ("synthetic", "Synthetic CPU-heavy mapping")):
        cases = [trial for trial in selected if trial["map"] == map_name]
        workers = sorted({trial["workers"] for trial in cases if trial["engine"] != "python-sqlite"})
        result += [f'<h3>{label}</h3>']
        if map_name == "synthetic":
            work = next(trial["work"] for trial in cases if trial["engine"] == "rust-log")
            result += [f'<p>The artificial map performs {work:,} integer-mixing rounds per '
                       'document. This measures available map parallelism, not an application speed guarantee.</p>']
        result += ['<div class="table-wrap"><table><thead><tr><th scope="col">Workers</th>'
                   '<th scope="col">Native SQLite docs/s</th><th scope="col">vs. 1 worker</th>'
                   '<th scope="col">Native log docs/s</th><th scope="col">vs. 1 worker</th>'
                   '</tr></thead><tbody>']
        for count in workers:
            row = [f'<tr><th scope="row">{count}</th>']
            for engine in ("rust-sqlite", "rust-log"):
                group = [trial for trial in cases if trial["engine"] == engine and trial["workers"] == count]
                baseline = [trial for trial in cases if trial["engine"] == engine and trial["workers"] == 1]
                rate = median(group, "ingest_documents_per_second")
                speedup = rate / median(baseline, "ingest_documents_per_second")
                row += [f'<td>{rate:,.0f}</td><td>{speedup:.2f}×</td>']
            result += ["".join(row) + '</tr>']
        result += ['</tbody></table></div>']
    result += [
        '<p>Cheap maps show no consistent multicore benefit. The synthetic case improves '
        'end-to-end ingestion by roughly two times, while its map phase scales further. '
        'Serial grouping, reduction, serialization, and commit costs remain. SQLite and '
        'the custom log are close in this fixture; neither is a categorical winner.</p>',
        '<h3>Startup and memory remain part of the cost</h3>',
        '<p>These one-worker rows use the cheap uniform workload. Setup includes explicit '
        'numeric runtime warm-up plus database opening; whole-process peak memory includes '
        'the Python host. The native backends replay their full journal on reopening.</p>',
        '<div class="table-wrap"><table><thead><tr><th scope="col">Engine</th>'
        '<th scope="col">Setup + warm-up</th><th scope="col">Reopen + full query</th>'
        '<th scope="col">Peak RSS</th><th scope="col">Storage</th></tr></thead><tbody>',
    ]
    for engine, label in (("python-sqlite", "Python / SQLite + WASM"),
                          ("rust-sqlite", "Native / SQLite"), ("rust-log", "Native / log")):
        group = [trial for trial in selected if trial["map"] == "cheap" and trial["engine"] == engine
                 and trial["workers"] == 1]
        setup = statistics.median(trial["runtime_warmup_seconds"] + trial["open_seconds"] for trial in group)
        result += [f'<tr><th scope="row">{label}</th><td>{setup * 1000:.2f} ms</td>'
                   f'<td>{median(group, "reopen_and_full_query_seconds") * 1000:.2f} ms</td>'
                   f'<td>{median(group, "peak_process_rss_bytes") / 1024**2:.1f} MiB</td>'
                   f'<td>{median(group, "database_bytes") / 1024:.0f} KiB</td></tr>']
    result += [
        '</tbody></table></div>',
        '<p>No OS page-cache eviction was performed. These small, two-repeat measurements '
        'do not establish large-history reopen cost, bounded memory, or production latency. '
        'The Python/native comparison also changes data layout and algorithms; worker-count '
        'comparisons within one native backend isolate parallelism more closely.</p>',
        '<p><a href="experimental-native-report.json">Download the complete NVMe/ext4 report</a> '
        'for every distribution, individual sample, latency, phase cost, and source hash. '
        'The separate tmpfs run is supplementary CPU-oriented evidence and is not used in '
        'these storage tables.</p>',
    ]
    return "\n".join(result)


def payload_performance(report):
    if report.get("complete") is not True:
        raise ValueError("Refusing to publish an incomplete payload report")
    trials = report["trials"]
    settings = trials[0]
    sizes = sorted({trial["payload_bytes"] for trial in trials})
    if len({trial["result_sha256"] for trial in trials}) != 1:
        raise ValueError("Payload probe result hashes must agree across sizes and engines")
    result = [
        f'<p>A separate {len(trials)}-trial NVMe/ext4 probe adds unused ASCII padding of '
        '0, 1 KiB, or 16 KiB per document while preserving numeric map results. '
        f'Each configuration uses {settings["documents"]} documents plus '
        f'{settings["live_documents"]} live records, {settings["batch_size"]}-document '
        f'durable batches, {settings["keys"]} keys, and two repeats. Compare sizes '
        'within this probe; its batch size differs from the worker-scaling matrix.</p>',
        '<div class="table-wrap"><table><thead><tr><th scope="col">Padding / document</th>'
        '<th scope="col">Python SQLite docs/s</th><th scope="col">Native SQLite, 1 worker</th>'
        '<th scope="col">Native SQLite, 4 workers</th><th scope="col">Native log, 1 worker</th>'
        '<th scope="col">Native log, 4 workers</th></tr></thead><tbody>',
    ]
    engines = [("python-sqlite", 1), ("rust-sqlite", 1), ("rust-sqlite", 4),
               ("rust-log", 1), ("rust-log", 4)]
    fixture_sizes = []
    for size in sizes:
        cases = [trial for trial in trials if trial["payload_bytes"] == size]
        if len({trial["fixture_sha256"] for trial in cases}) != 1:
            raise ValueError("Each payload size must use identical fixtures across engines")
        fixture_sizes.append(cases[0]["fixture_json_bytes"])
        label = f"{size // 1024} KiB" if size else "0 B"
        row = [f'<tr><th scope="row">{label}</th>']
        for engine, workers in engines:
            rate = statistics.median(trial["ingest_documents_per_second"] for trial in cases
                                     if trial["engine"] == engine and trial["workers"] == workers)
            row.append(f'<td>{rate:,.0f}</td>')
        result.append("".join(row) + '</tr>')
    result.append('</tbody></table></div>')
    result.append('<p>Actual encoded fixture sizes, including identities and live records, '
                  'are ' + ", ".join(f'{size:,}' for size in fixture_sizes) +
                  ' bytes respectively. At 16 KiB padding, median whole-process peak RSS '
                  'for one worker is ' + ", ".join(
                      f'{label}: {statistics.median(trial["peak_process_rss_bytes"] for trial in trials if trial["payload_bytes"] == max(sizes) and trial["engine"] == engine and trial["workers"] == 1) / 1024**2:.1f} MiB'
                      for engine, label in (("python-sqlite", "Python"), ("rust-sqlite", "native SQLite"), ("rust-log", "native log"))) + '.</p>')
    result.append('<p>Larger ignored payloads exercise JSON serialization, validation, copying, '
                  'retained storage, and durability together; these timings do not isolate one cause. '
                  'Peak RSS also includes fixture objects, encoded input, and the Python host. '
                  'This small padding probe does not establish behavior for every nested document '
                  'shape or high-fanout map.</p>')
    result.append('<p><a href="experimental-payloads-report.json">Download the complete payload probe</a>. '
                  'The original 156-trial results remain unchanged, with their measured runner '
                  'hash and source commit preserved in the primary report; the new padding option '
                  'is additive and defaults to zero.</p>')
    return "\n".join(result)


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
            if "benchmark_report" in page:
                report_path = ROOT / page["benchmark_report"]
                report = json.loads(report_path.read_text())
                body = body.replace("{{performance}}", performance(report))
                shutil.copy2(report_path, staged / "experimental-native-report.json")
            if "payload_report" in page:
                report_path = ROOT / page["payload_report"]
                report = json.loads(report_path.read_text())
                body = body.replace("{{payload_performance}}", payload_performance(report))
                shutil.copy2(report_path, staged / "experimental-payloads-report.json")
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
        (staged / "search.json").write_text(json.dumps(search, ensure_ascii=False) + "\n")
        (staged / ".nojekyll").touch()
        managed = [path.name for path in staged.iterdir()] + [".foldlitedb-docs"]
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
