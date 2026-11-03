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


def test_shared_conformance_sources_depth_and_relocated_output(portable_page):
    fixture = json.loads((Path(__file__).resolve().parents[1] / "fixtures" / "portable_conformance.json").read_text())
    result = portable_page.evaluate("""async fixture => {
      const {BrowserDatabase} = await import('./portable/client.js');
      const {ViewModule,sha256Hex} = await import('./portable/host.js');
      const sources = [], depths = [], modules = [];
      for (const test of fixture.source_cases) {
        const db = await BrowserDatabase.connect();
        try {
          const record = {id:'one',source:test.source,payload:{metric:'x',value:2}};
          const accepted = await db.ingest([record]);
          const rows = await db.rows();
          const replay = await db.ingest([record]);
          sources.push({inserted:accepted.inserted,replayed:replay.replayed,
            rows,source:(await db.export())[0].source,equivalent:(await db.replay()).equivalent});
        } finally {db.close();}
      }
      for (const test of fixture.payload_depth_cases) {
        const db = await BrowserDatabase.connect();
        let payload = null;
        for (let i=0; i<test.depth; i++) payload=[payload];
        try {
          try {await db.add(payload);depths.push({accepted:true,documents:(await db.stats()).documents});}
          catch(error) {depths.push({accepted:false,documents:(await db.stats()).documents,error:error.message});}
        } finally {db.close();}
      }
      for (const test of fixture.output_modules) {
        const bytes = Uint8Array.from(test.hex.match(/../g), pair=>parseInt(pair,16));
        const spec = {abi:1,module:'moving.wasm',sha256:sha256Hex(bytes),descriptor:fixture.descriptor};
        try {await ViewModule.fromBytes(bytes,spec);modules.push({accepted:true});}
        catch(error) {modules.push({accepted:false,error:error.message});}
      }
      return {sources,depths,modules};
    }""", fixture)
    for case, actual in zip(fixture["source_cases"], result["sources"]):
        assert actual["inserted"] == actual["replayed"] == 1
        assert len(actual["rows"]) == int(case["mapped"])
        assert actual["source"] == case["source"] and actual["equivalent"]
        with PortableDatabase(":memory:") as native:
            native.add({"metric": "x", "value": 2}, id="one", source=case["source"])
            assert native._call({"op": "query"})["rows"] == actual["rows"]
    for case, actual in zip(fixture["payload_depth_cases"], result["depths"]):
        assert actual["accepted"] == case["accepted"], (case, actual)
        assert actual["documents"] == int(case["accepted"])
    for case, actual in zip(fixture["output_modules"], result["modules"]):
        assert actual["accepted"] == case["accepted"], (case["output_pointer"], actual)
        if not actual["accepted"]:
            assert "output buffer" in actual["error"]


def _query_keys_fixture():
    return json.loads((Path(__file__).resolve().parents[1] / "fixtures" / "query_keys.json").read_text(encoding="utf-8"))


def test_semantic_key_order_and_range_prefix_pages_match_native(portable_page):
    fixture = _query_keys_fixture()
    selections = [
        {},
        {"start_key": -20, "end_key": 10},
        {"start_key": -20, "end_key": 10, "inclusive_start": False, "inclusive_end": False},
        {"start_key": 2, "end_key": 2},
        {"start_key": 2, "end_key": 2, "inclusive_end": False},
        {"start_key": 10, "end_key": 2},
        {"start_key": None, "end_key": False},
        {"end_key": None},
        {"start_key": "\ue000", "end_key": "\U0001f680"},
        {"start_key": ["sales", "alice", 2], "end_key": ["sales", "alice", 10]},
        {"prefix": "al"},
        {"prefix": ""},
        {"prefix": ["sales"]},
        {"prefix": ["sales", "alice"]},
        {"prefix": []},
        {"prefix": [None]},
        {"prefix": ["absent"]},
        {"key": {"b": 2, "a": 1}},
        {"key": False},
        {"key": None},
        {"prefix": [False]},
        {"prefix": [0]},
        {"prefix": [[0]]},
        {"key": {"a": 10, "b": 0}},
    ]
    expected = []
    with PortableDatabase(":memory:", PortableView(bytes.fromhex(fixture["hex"]))) as native:
        native.add({})
        all_keys = [row.key for row in native.rows()]
        assert all_keys == fixture["keys"]
        for selection in selections:
            rows = []
            page = native.query(**selection, limit=1)
            rows.extend(page.rows)
            while not page.complete:
                page = native.query(**selection, limit=3, cursor=page.cursor, after=page.after)
                rows.extend(page.rows)
            expected.append([{"key": r.key, "state": r.state, "result": r.result, "sequence": r.sequence} for r in rows])

    actual = portable_page.evaluate("""async ({fixture,selections}) => {
      const {BrowserDatabase} = await import('./portable/client.js');
      const {sha256Hex,compareKeys} = await import('./portable/host.js');
      const bytes = Uint8Array.from(fixture.hex.match(/../g), pair=>parseInt(pair,16));
      const spec = {abi:1,module:'query-keys.wasm',sha256:sha256Hex(bytes),descriptor:fixture.descriptor};
      const db = await BrowserDatabase.connect({bytes,spec});
      try {
        await db.add({});
        const selectionsRows=[];
        for (const selection of selections) {
          let page=await db.query({...selection,limit:1}), rows=[...page.rows];
          while(!page.complete) {
            page=await db.query({...selection,limit:3,cursor:page.cursor,after:page.after});
            rows.push(...page.rows);
          }
          const convenience=await db.rows({...selection,limit:2});
          if(JSON.stringify(rows)!==JSON.stringify(convenience)) throw new Error('rows() differs from page iteration');
          selectionsRows.push(rows);
        }
        return {selectionsRows,sorted:[...fixture.keys].reverse().sort(compareKeys),replay:await db.replay()};
      } finally {db.close();}
    }""", {"fixture": fixture, "selections": selections})
    assert actual["sorted"] == fixture["keys"]
    assert actual["selectionsRows"] == expected
    assert actual["replay"]["equivalent"]
    assert [r["key"] for r in actual["selectionsRows"][11]] == [k for k in fixture["keys"] if isinstance(k, str)]
    assert [r["key"] for r in actual["selectionsRows"][14]] == [k for k in fixture["keys"] if isinstance(k, list)]


def test_browser_ranges_validate_selectors_and_bind_query_continuations(portable_page):
    fixture = _query_keys_fixture()
    invalid = [
        {"key": 2, "start_key": 1},
        {"key": 2, "prefix": []},
        {"prefix": [], "end_key": []},
        {"prefix": None}, {"prefix": 2}, {"prefix": {}},
        {"inclusive_start": 0}, {"inclusive_start": None},
        {"inclusive_end": "false"}, {"inclusive_end": 1},
    ]
    result = portable_page.evaluate("""async ({fixture,invalid}) => {
      const {BrowserDatabase,DatabaseError} = await import('./portable/client.js');
      const {canonical,sha256Hex} = await import('./portable/host.js');
      const bytes=Uint8Array.from(fixture.hex.match(/../g), pair=>parseInt(pair,16));
      const spec={abi:1,module:'query-keys.wasm',sha256:sha256Hex(bytes),descriptor:fixture.descriptor};
      const db=await BrowserDatabase.connect({bytes,spec}), other=await BrowserDatabase.connect({bytes,spec});
      const errors=[];
      async function rejected(selection) {
        try {await db.query(selection);return false;}
        catch(error) {errors.push({message:error.message,typed:error instanceof DatabaseError});return true;}
      }
      try {
        await db.add({});
        const before=await db.stats();
        const first=await db.query({prefix:'',limit:1});
        const token=JSON.parse(first.after);
        const attempts=[...invalid,
          {prefix:'al',cursor:first.cursor,after:first.after},
          {prefix:'',inclusive_end:false,cursor:first.cursor,after:first.after},
          {prefix:'',cursor:(await other.stats()).cursor,after:first.after},
          {prefix:'',cursor:first.cursor,after:JSON.stringify([first.cursor,token[3]])},
          {prefix:'',cursor:first.cursor,after:'not-json'},
          {prefix:'',cursor:first.cursor,after:JSON.stringify({})},
          {prefix:'',cursor:first.cursor,after:JSON.stringify([token[0],token[1],token[2],'null'])},
          {prefix:'',cursor:first.cursor,after:JSON.stringify([token[0],token[1],token[2],'"\\\\u0061"'])},
          {prefix:'',cursor:first.cursor,after:JSON.stringify([token[0],token[1],token[2],canonical(['wrong-type'])])},
        ];
        const rejectedAttempts=[];
        for(const attempt of attempts) rejectedAttempts.push(await rejected(attempt));
        const second=await db.query({prefix:'',limit:3,cursor:first.cursor,after:first.after});
        const docs=await db.request({op:'documents',limit:1});
        return {rejectedAttempts,errors,before,after:await db.stats(),first,second,docs};
      } finally {db.close();other.close();}
    }""", {"fixture": fixture, "invalid": invalid})
    assert all(result["rejectedAttempts"]), result
    assert all(error["typed"] for error in result["errors"])
    assert result["before"] == result["after"]
    assert result["first"]["rows"][0]["key"] != result["second"]["rows"][0]["key"]
    assert len(result["docs"]["documents"]) == 1
    assert result["docs"]["complete"]


def test_browser_filtered_snapshot_pages_keep_original_states_during_ingestion(portable_page):
    initial = [{"metric": "alpha", "value": 1}, {"metric": "alpine", "value": 2}, {"metric": "beta", "value": 3}]
    suffix = [{"metric": "alpha", "value": 100}, {"metric": "amber", "value": 5}, {"metric": "alpine", "value": 20}]
    selections = [{"prefix": "a"}, {"start_key": "a", "end_key": "az"}]
    expected = []
    for selection in selections:
        with PortableDatabase(":memory:") as native:
            native.add_many(initial)
            page = native.query(**selection, limit=1)
            rows = list(page.rows)
            native.add_many(suffix)
            while not page.complete:
                page = native.query(**selection, limit=1, cursor=page.cursor, after=page.after)
                rows.extend(page.rows)
            expected.append([{"key": r.key, "state": r.state, "result": r.result, "sequence": r.sequence} for r in rows])

    actual = portable_page.evaluate("""async ({initial,suffix,selections}) => {
      const {BrowserDatabase}=await import('./portable/client.js');
      const results=[];
      for(const selection of selections) {
        const db=await BrowserDatabase.connect();
        try {
          await db.addMany(initial);
          let page=await db.query({...selection,limit:1}), rows=[...page.rows];
          let documents=await db.request({op:'documents',limit:1}), retained=[...documents.documents];
          await db.addMany(suffix);
          while(!page.complete) {
            page=await db.query({...selection,limit:1,cursor:page.cursor,after:page.after});
            rows.push(...page.rows);
          }
          while(!documents.complete) {
            documents=await db.request({op:'documents',limit:2,cursor:documents.cursor,after:documents.after});
            retained.push(...documents.documents);
          }
          results.push({rows,current:await db.rows(selection),documents:retained});
        } finally {db.close();}
      }
      return results;
    }""", {"initial": initial, "suffix": suffix, "selections": selections})
    assert [item["rows"] for item in actual] == expected
    for item in actual:
        assert [r["key"] for r in item["rows"]] == ["alpha", "alpine"]
        assert [r["result"]["sum"] for r in item["rows"]] == [1, 2]
        assert [r["key"] for r in item["current"]] == ["alpha", "alpine", "amber"]
        assert [r["result"]["sum"] for r in item["current"]] == [101, 22, 5]
        assert [document["payload"] for document in item["documents"]] == initial


def test_browser_selector_depth_budget_is_per_key_and_matches_native(portable_page):
    fixture = _query_keys_fixture()
    fields = ["key", "start_key", "end_key", "prefix"]
    expected = []
    value = None
    for _ in range(64):
        value = [value]
    with PortableDatabase(":memory:", PortableView(bytes.fromhex(fixture["hex"]))) as native:
        native.add({})
        for field in fields:
            expected.append([row.key for row in native.query(**{field: value}, limit=100).rows])
    actual = portable_page.evaluate("""async ({fixture,fields}) => {
      const {BrowserDatabase,DatabaseError}=await import('./portable/client.js');
      const {sha256Hex}=await import('./portable/host.js');
      const bytes=Uint8Array.from(fixture.hex.match(/../g),pair=>parseInt(pair,16));
      const spec={abi:1,module:'query-keys.wasm',sha256:sha256Hex(bytes),descriptor:fixture.descriptor};
      const db=await BrowserDatabase.connect({bytes,spec});
      try {
        await db.add({});
        let key=null;for(let i=0;i<64;i++) key=[key];
        const rows=[],errors=[];
        for(const field of fields) {
          rows.push((await db.query({[field]:key})).rows.map(row=>row.key));
          try {await db.query({[field]:[key]});errors.push(false);}
          catch(error) {errors.push(error instanceof DatabaseError && /nesting/.test(error.message));}
        }
        return {rows,errors};
      } finally {db.close();}
    }""", {"fixture": fixture, "fields": fields})
    assert actual["rows"] == expected
    assert all(actual["errors"])


def test_browser_rows_snapshots_mutable_selectors_before_awaiting_pages(portable_page):
    fixture = _query_keys_fixture()
    actual = portable_page.evaluate("""async fixture => {
      const {BrowserDatabase,DatabaseError}=await import('./portable/client.js');
      const {sha256Hex}=await import('./portable/host.js');
      const bytes=Uint8Array.from(fixture.hex.match(/../g),pair=>parseInt(pair,16));
      const spec={abi:1,module:'query-keys.wasm',sha256:sha256Hex(bytes),descriptor:fixture.descriptor};
      const db=await BrowserDatabase.connect({bytes,spec});
      try {
        await db.add({});
        const prefix=['sales','alice'], options={prefix,limit:1};
        const pending=db.rows(options);
        prefix[1]='bob';options.prefix=['missing'];options.limit=0;
        const prefixKeys=(await pending).map(row=>row.key);
        const start=['sales','alice'],end=['sales','alice',10];
        const bounds={start_key:start,end_key:end,limit:1};
        const bounded=db.rows(bounds);
        start[1]='bob';end[2]=0;bounds.inclusive_end=false;
        const rangeKeys=(await bounded).map(row=>row.key);
        let typed=false;
        try {await db.rows({prefix:[()=>{}]});}
        catch(error) {typed=error instanceof DatabaseError;}
        return {prefixKeys,rangeKeys,typed};
      } finally {db.close();}
    }""", fixture)
    expected = [["sales", "alice"], ["sales", "alice", 2], ["sales", "alice", 10]]
    assert actual["prefixKeys"] == expected
    assert actual["rangeKeys"] == expected
    assert actual["typed"]
