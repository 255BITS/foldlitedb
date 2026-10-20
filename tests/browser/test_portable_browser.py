"""Real Chromium gate for portable view execution, native parity, and the demo."""
from functools import partial
from http.server import SimpleHTTPRequestHandler, ThreadingHTTPServer
import json
from pathlib import Path
import shutil
import subprocess
import sys
import threading

import pytest
from playwright.sync_api import expect, sync_playwright

from foldlitedb import Document, PortableDatabase, PortableView
from foldlitedb.portable import portable_assets

pytestmark = pytest.mark.browser


def test_map_preview_exposes_filtered_documents_without_retaining_them(portable_page):
    result = portable_page.evaluate("""async () => {
      const {BrowserDatabase} = await import('./portable/client.js');
      const db = await BrowserDatabase.connect({name:'catalogue'});
      const bad = {category:'books',id:'atlas',name:'Wrong adapter field'};
      const good = {category:'books',id:'atlas',title:'Field Guide'};
      try {
        const before = await db.stats();
        const filtered = await db.preview(bad), emitted = await db.preview(good);
        const after = await db.stats();
        await db.add(good,{id:'delivery-1'});
        return {before,after,filtered,emitted,documents:await db.export(),rows:await db.rows()};
      } finally {db.close();}
    }""")
    assert result["filtered"] == []
    assert result["emitted"] == [{"key": "books", "value": {"id": "atlas", "title": "Field Guide", "deleted": False}}]
    assert result["before"] == result["after"]
    assert len(result["documents"]) == 1
    assert result["documents"][0]["position"] == 0
    assert result["rows"][0]["result"] == {"count": 1, "items": {"atlas": "Field Guide"}}
    with PortableDatabase(":memory:", PortableView.bundled("catalogue")) as native:
        before = native.stats()
        assert native.preview({"category": "books", "id": "atlas", "name": "Wrong adapter field"}) == result["filtered"]
        assert native.preview({"category": "books", "id": "atlas", "title": "Field Guide"}) == result["emitted"]
        assert native.stats() == before


def test_browser_errors_have_stable_types_codes_and_recovery_guidance(portable_page):
    result = portable_page.evaluate("""async () => {
      const api = await import('./portable/client.js');
      const {BrowserDatabase,DatabaseError,DocumentConflict,SourceOrderError,CursorExpired} = api;
      const db = await BrowserDatabase.connect(), other = await BrowserDatabase.connect();
      const errors=[];
      async function failure(run, Type) {
        try {await run();throw new Error('expected a rejection');}
        catch(error) {
          errors.push({typed:error instanceof Type,base:error instanceof DatabaseError,
            name:error.name,code:error.code,message:error.message});
        }
      }
      try {
        await db.add({metric:'x',value:2},{id:'delivery-1',source:'events',position:2});
        const before=await db.stats();
        await failure(()=>db.add({metric:'x',value:3},{id:'delivery-1',source:'events'}),DocumentConflict);
        await failure(()=>db.add({metric:'x',value:3},{id:'older',source:'events',position:1}),SourceOrderError);
        const foreign=(await other.stats()).cursor;
        await failure(()=>db.changes(foreign),CursorExpired);
        await failure(()=>db.add({metric:'x',value:NaN}),DatabaseError);
        const after=await db.stats();
        db.close();
        await failure(()=>db.stats(),DatabaseError);
        return {errors,before,after};
      } finally {db.close();other.close();}
    }""")
    expected = ["DocumentConflict", "SourceOrderError", "CursorExpired", "DatabaseError", "DatabaseError"]
    assert [item["name"] for item in result["errors"]] == expected
    assert [item["code"] for item in result["errors"]] == expected
    assert all(item["typed"] and item["base"] for item in result["errors"])
    assert "DocumentConflict:" in result["errors"][0]["message"]
    assert "fresh snapshot" in result["errors"][2]["message"]
    assert result["before"] == result["after"]


@pytest.fixture(scope="module")
def portable_page(tmp_path_factory):
    root = tmp_path_factory.mktemp("portable-browser") / "site"
    repository = Path(__file__).resolve().parents[2]
    subprocess.run([sys.executable, str(repository / "scripts/build_docs.py"), "--output", str(root)], check=True, capture_output=True)
    # Exercise the installed package's portable assets, including worker imports.
    shutil.rmtree(root / "portable")
    (root / "portable").mkdir()
    for resource in portable_assets().iterdir():
        if resource.is_file():
            (root / "portable" / resource.name).write_bytes(resource.read_bytes())
    (root / "portable" / "metrics.v1.json").write_bytes((root / "portable" / "metrics.json").read_bytes())
    tampered = root / "portable" / "tampered"
    tampered.mkdir()
    (tampered / "metrics.json").write_bytes((root / "portable" / "metrics.json").read_bytes())
    binary = bytearray((root / "portable" / "metrics.wasm").read_bytes())
    binary[-1] ^= 1
    (tampered / "metrics.wasm").write_bytes(binary)

    class QuietHandler(SimpleHTTPRequestHandler):
        def log_message(self, *args):
            pass

    server = ThreadingHTTPServer(("127.0.0.1", 0), partial(QuietHandler, directory=str(root)))
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        with sync_playwright() as playwright:
            browser = playwright.chromium.launch()
            page = browser.new_page(accept_downloads=True)
            page.goto(f"http://127.0.0.1:{server.server_port}/playground.html")
            page.wait_for_function("window.foldliteDemo?.ready && document.getElementById('document-count').textContent === '12'")
            yield page
            browser.close()
    finally:
        server.shutdown()
        thread.join(timeout=5)
        server.server_close()


def test_browser_batches_match_native_wasm_state_sequences_and_replay(portable_page, tmp_path):
    batches = [
        [
            {"id": "a", "source": "events", "position": 0, "payload": {"metric": "latency", "value": 42}},
            {"id": "b", "source": "events", "position": 2, "payload": {"metric": "latency", "value": 12.5}},
            {"id": "a", "source": "other", "position": 0, "payload": {"metric": "\ue000", "value": -3}},
            {"id": "c", "source": "events", "position": 3, "payload": {"metric": "\U0001f680", "value": 1e-7}},
        ],
        [
            {"id": "d", "source": "events", "position": 4, "payload": {"metric": "latency", "value": 8}},
            {"id": "e", "source": "events", "position": 5, "payload": {"metric": "ignored", "value": "nope"}},
            {"id": "f", "source": "events", "position": 6, "payload": {"metric": "\U0001f680", "value": -0.0000001}},
        ],
    ]
    with PortableDatabase(tmp_path / "native.db", PortableView.bundled()) as native:
        native_initial = native.stats()["cursor"]
        first = native._call({"op": "ingest", "documents": batches[0]})
        snapshot = native.query(limit=1)
        native._call({"op": "ingest", "documents": batches[1]})
        expected = native._call({"op": "query", "limit": 1024})["rows"]
        old_rows = list(snapshot.rows)
        while not snapshot.complete:
            snapshot = native.query(limit=1, cursor=snapshot.cursor, after=snapshot.after)
            old_rows.extend(snapshot.rows)
        expected_old = [{"key": row.key, "state": row.state, "result": row.result, "sequence": row.sequence} for row in old_rows]
        native_changes = native.changes(since=native_initial, limit=1024)
        expected_changes = [{"key": row.key, "state": row.state, "result": row.result, "sequence": row.sequence} for row in native_changes.rows]

    result = portable_page.evaluate("""async batches => {
      const {BrowserDatabase} = await import('./portable/client.js');
      const db = await BrowserDatabase.connect();
      try {
        const initial = (await db.stats()).cursor;
        const first = await db.ingest(batches[0]);
        let snapshot = await db.query({limit:1});
        const oldRows = [...snapshot.rows];
        await db.ingest(batches[1]);
        while (!snapshot.complete) {
          snapshot = await db.query({limit:1,cursor:snapshot.cursor,after:snapshot.after});
          oldRows.push(...snapshot.rows);
        }
        const changes = []; let page = await db.changes(initial,{limit:1});
        changes.push(...page.rows);
        while (!page.complete) { page = await db.changes(page.cursor,{limit:1}); changes.push(...page.rows); }
        return {first,rows:await db.rows(),oldRows,changes,replay:await db.replay(),stats:await db.stats(),documents:await db.export()};
      } finally {db.close();}
    }""", batches)
    assert result["rows"] == expected
    assert result["oldRows"] == expected_old
    assert result["changes"] == expected_changes
    assert {key: value for key, value in result["first"].items() if key != "cursor"} == {key: value for key, value in first.items() if key != "cursor"}
    assert result["first"]["cursor"].endswith(f":{PortableView.bundled().sha256}:7")
    assert result["replay"]["equivalent"] is True
    assert result["stats"]["memory_bytes"] <= 16 * 1024 * 1024
    assert result["stats"]["module_sha256"] == PortableView.bundled().sha256
    # Browser-retained records replay directly into the public Python API.
    with PortableDatabase(tmp_path / "browser-rebuilt.db") as rebuilt:
        exported = [Document(d["id"], d["payload"], d["position"], d["source"]) for d in result["documents"]]
        replay = rebuilt.ingest(exported)
        assert replay.inserted == 7
        assert [(r.key, r.state, r.result) for r in rebuilt.rows()] == [(r["key"], r["state"], r["result"]) for r in expected]
        assert rebuilt.ingest(exported).replayed == 7


def test_browser_conflicts_overflow_ordering_and_finite_json_are_atomic(portable_page):
    result = portable_page.evaluate("""async () => {
      const {BrowserDatabase} = await import('./portable/client.js');
      const db = await BrowserDatabase.connect(); const errors=[];
      try {
        const saved = await db.add({metric:'x',value:2},{id:'named',source:'s',position:3});
        const replay = await db.add({value:2,metric:'x'},{id:'named',source:'s'});
        await db.add({metric:'x',value:4},{id:'named',source:'another'});
        const before = await db.stats(), rowsBefore = await db.rows();
        const attempts = [
          [{payload:{metric:'x',value:7}},{id:'named',source:'s',payload:{metric:'x',value:9}}],
          [{payload:{metric:'overflow',value:1e308}},{payload:{metric:'overflow',value:1e308}}],
          [{source:'s',position:2,payload:{metric:'x',value:1}}],
          Array.from({length:1025},() => ({payload:null})),
          [{payload:{metric:'x',value:NaN}}],
          [{payload:{metric:'x',value:Infinity}}],
          [{payload:undefined}],
          [{payload:{'\ud800':'invalid Unicode'}}],
          [{payload:{metric:'\udc00',value:1}}],
        ];
        for (const documents of attempts) { try {await db.ingest(documents);} catch(error) {errors.push(error.message);} }
        return {saved,replay,before,after:await db.stats(),rowsBefore,rowsAfter:await db.rows(),errors};
      } finally {db.close();}
    }""")
    assert result["saved"] == result["replay"]
    assert len(result["errors"]) == 9
    assert "conflict" in result["errors"][0].lower()
    assert "finite" in result["errors"][1].lower() or "overflow" in result["errors"][1].lower()
    assert "increase" in result["errors"][2]
    assert result["before"] == result["after"]
    assert result["rowsBefore"] == result["rowsAfter"]


def test_browser_rejects_cross_database_cursors_and_tampered_modules_without_webcrypto(portable_page):
    result = portable_page.evaluate("""async () => {
      const {BrowserDatabase} = await import('./portable/client.js');
      const {ViewModule, moduleDigest} = await import('./portable/host.js');
      const db = await BrowserDatabase.connect(), other = await BrowserDatabase.connect(); const errors=[];
      Object.defineProperty(crypto,'subtle',{value:undefined,configurable:true});
      try {
        const view = await ViewModule.load(new URL('./portable/',location.href));
        try {await ViewModule.load(new URL('./portable/tampered/',location.href));} catch(error) {errors.push(error.message);}
        try {await db.changes((await other.stats()).cursor);} catch(error) {errors.push(error.message);}
        const binary = new Uint8Array([0,97,115,109,1,0,0,0]);
        try {await ViewModule.fromBytes(binary,{abi:1,sha256:await moduleDigest(binary)});} catch(error) {errors.push(error.message);}
        const hugeTable = new Uint8Array([0,97,115,109,1,0,0,0,4,9,1,112,1,161,141,6,161,141,6,5,5,1,1,16,128,2]);
        try {await ViewModule.fromBytes(hugeTable,{abi:1,sha256:await moduleDigest(hugeTable)});} catch(error) {errors.push(error.message);}
        return {errors,descriptor:view.descriptor,identity:view.identity()};
      } finally {delete crypto.subtle;db.close();other.close();}
    }""")
    assert len(result["errors"]) == 4
    assert "digest mismatch" in result["errors"][0]
    assert "another database" in result["errors"][1]
    assert "bounded memory" in result["errors"][2]
    assert "table exceeds" in result["errors"][3]
    assert result["descriptor"]["name"] == "metrics"
    assert result["identity"]["count"] == 0


def test_browser_loads_dotted_artifact_names_from_custom_builds(portable_page):
    result = portable_page.evaluate("""async () => {
      const {BrowserDatabase} = await import('./portable/client.js');
      const db = await BrowserDatabase.connect({name:'metrics.v1'});
      try {
        await db.add({metric:'invoice',value:12.5});
        return {rows:await db.rows(),info:db.info};
      } finally {db.close();}
    }""")
    assert result["rows"][0]["result"]["sum"] == 12.5
    assert result["info"]["descriptor"]["name"] == "metrics"


def test_browser_deadline_terminates_worker_and_keeps_ui_responsive(portable_page):
    result = portable_page.evaluate("""async () => {
      const {BrowserDatabase} = await import('./portable/client.js');
      const url = URL.createObjectURL(new Blob(['while(true){}'],{type:'text/javascript'}));
      const db = new BrowserDatabase({timeoutMs:75,workerURL:url});
      let ticks=0; const timer=setInterval(()=>ticks++,5);
      try {await db.request({op:'stats'});return {error:'unexpected success'};}
      catch(error) {return {error:error.message,closed:db.closed,ticks};}
      finally {clearInterval(timer);db.close();URL.revokeObjectURL(url);}
    }""")
    assert "deadline" in result["error"]
    assert result["closed"] is True and result["ticks"] > 0


def test_browser_replay_preserves_accepted_large_document_batches(portable_page):
    result = portable_page.evaluate("""async () => {
      const {BrowserDatabase} = await import('./portable/client.js');
      const db = await BrowserDatabase.connect();
      try {
        for(let i=0;i<3;i++) await db.add({metric:'large',value:i,padding:'x'.repeat(100000)});
        const before = await db.stats(), replay = await db.replay();
        let error = '';
        try {await db.addMany(Array.from({length:3},(_,i)=>({metric:'large',value:i,padding:'x'.repeat(100000)})));}
        catch(failure) {error=failure.message;}
        return {before,replay,after:await db.stats(),error};
      } finally {db.close();}
    }""")
    assert result["replay"]["equivalent"] is True
    assert result["before"] == result["after"]
    assert "262144 bytes" in result["error"]


def test_ordered_catalogue_runs_the_same_module_in_browser_and_native(portable_page, tmp_path):
    batches = [
        [{"category": "books", "id": "__proto__", "title": "雪 🚀"}, {"category": "books", "id": "a", "title": "first"}],
        [{"category": "books", "id": "a", "title": "updated"}, {"category": "books", "id": "__proto__", "deleted": True}, {"category": "books", "id": "a\u0000b", "title": "escaped\ncontrol"}],
    ]
    with PortableDatabase(tmp_path / "catalogue.db", PortableView.bundled("catalogue")) as native:
        for batch in batches:
            native.add_many(batch)
        expected = native._call({"op": "query"})["rows"]
    result = portable_page.evaluate("""async batches => {
      const {BrowserDatabase} = await import('./portable/client.js');
      const db = await BrowserDatabase.connect({name:'catalogue'});
      try {
        for (const batch of batches) await db.addMany(batch);
        return {rows:await db.rows(),replay:await db.replay()};
      } finally {db.close();}
    }""", batches)
    assert result["rows"] == expected
    assert result["rows"][0]["result"] == {"count": 2, "items": {"a": "updated", "a\u0000b": "escaped\ncontrol"}}
    assert result["replay"]["equivalent"] is True


def test_playground_import_add_replay_export_and_mobile_layout(portable_page):
    page = portable_page
    page.get_by_role("button", name="Reset example").click()
    expect(page.locator("#document-count")).to_have_text("12")
    page.locator("#metric-input").fill("latency_ms")
    page.locator("#value-input").fill("100")
    page.locator("#measurement-form button").click()
    expect(page.locator("#document-count")).to_have_text("13")
    latency = page.locator("#view-rows tr").filter(has=page.get_by_role("rowheader", name="latency_ms", exact=True))
    expect(latency.locator("td").nth(0)).to_have_text("5")
    expect(latency.locator("td").nth(1)).to_have_text("59.2")
    page.locator("#document-file").set_input_files({"name": "metrics.jsonl", "mimeType": "application/json", "buffer": b'{"metric":"imported","value":3}\n{"metric":"imported","value":7}\n'})
    expect(page.locator("#document-count")).to_have_text("15")
    expect(page.locator("#group-count")).to_have_text("4")
    page.get_by_role("button", name="Rebuild & verify").click()
    expect(page.locator("#replay-state")).to_have_text("Verified ✓")
    with page.expect_download() as downloaded:
        page.get_by_role("button", name="Export documents").click()
    exported = Path(downloaded.value.path()).read_bytes()
    assert len(json.loads(exported)) == 15
    page.locator("#document-file").set_input_files({"name": "replay.json", "mimeType": "application/json", "buffer": exported})
    expect(page.locator("#playground-status")).to_contain_text("15 identical replays")
    expect(page.locator("#document-count")).to_have_text("15")
    # A dropped file follows the same local import path.
    page.evaluate("""() => {
      const transfer = new DataTransfer();
      transfer.items.add(new File(['[{"metric":"dropped","value":9}]'],'dropped.json',{type:'application/json'}));
      document.getElementById('file-drop').dispatchEvent(new DragEvent('drop',{dataTransfer:transfer,bubbles:true,cancelable:true}));
    }""")
    expect(page.locator("#document-count")).to_have_text("16")
    page.get_by_role("button", name="Start live stream").click()
    page.wait_for_function("Number(document.getElementById('document-count').textContent) > 16")
    page.get_by_role("button", name="Pause live stream").click()
    expect(page.locator("#toggle-stream")).to_have_attribute("aria-pressed", "false")
    page.set_viewport_size({"width": 375, "height": 900})
    assert page.evaluate("document.documentElement.scrollWidth <= innerWidth")
    expect(page.locator("#measurement-form button")).to_be_visible()
    page.set_viewport_size({"width": 1280, "height": 900})
