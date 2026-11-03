"""Render the narrated FoldliteDB explainer from editable source.

No API keys or hosted rendering service. Downloads an open Piper voice once;
all synthesis and rendering then run locally. See README.md for prerequisites.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import math
import re
import subprocess
import textwrap
import urllib.request
import wave
from functools import lru_cache
from itertools import pairwise
from pathlib import Path

from PIL import Image, ImageDraw, ImageFont

HERE = Path(__file__).resolve().parent
BUILD = HERE / ".build"
FONTS = HERE / "assets/fonts"
MODEL = "en_US-ljspeech-high.onnx"
VOICE_URL = (
    "https://huggingface.co/rhasspy/piper-voices/resolve/main/en/en_US/ljspeech/high/"
)
VOICE_HASHES = {
    MODEL: "5d4f08ba6a2a48c44592eed3ce56bf85e9de3dd4e20df90541ae68a8310c029a",
    MODEL + ".json": "7e1f4634af596d83cca997fb7a931ba80b70f8a316a2655ee69c55365e0ace14",
    "MODEL_CARD": "289f7421072d689a3d91f6c632f486af46f9b8b04417536104a4ea667b8a394b",
}
BG = "#09131f"
PANEL = "#122233"
EDGE = "#2b4257"
WHITE = "#edf4f8"
MUTED = "#91aabd"
GREEN = "#5be2c0"
BLUE = "#80b9ff"
GOLD = "#ffca80"
SCALE = 1.5
DEMO_CUES = (0, 0.59, 0.705, 0.735, 0.79, 0.86, 0.96)


def ffmpeg():
    import imageio_ffmpeg

    return imageio_ffmpeg.get_ffmpeg_exe()


@lru_cache(maxsize=64)
def font(size, weight="regular"):
    name = {
        "regular": "OpenSans-Regular.ttf",
        "bold": "OpenSans-Bold.ttf",
        "semibold": "OpenSans-Semibold.ttf",
        "mono": "SourceCodePro-Regular.otf",
    }[weight]
    return ImageFont.truetype(str(FONTS / name), round(size * SCALE))


class Canvas:
    def __init__(self, image=None):
        self.image = (
            image.copy()
            if image is not None
            else Image.new("RGB", (round(1280 * SCALE), round(720 * SCALE)), BG)
        )
        self.draw = ImageDraw.Draw(self.image)
        self.flows = []

    def text(self, xy, value, size=22, color=WHITE, weight="regular", anchor=None):
        self.draw.text(
            tuple(round(v * SCALE) for v in xy),
            str(value),
            font=font(size, weight),
            fill=color,
            anchor=anchor,
        )

    def box(self, bounds, fill=PANEL, outline=EDGE, radius=14, width=1):
        self.draw.rounded_rectangle(
            tuple(round(v * SCALE) for v in bounds),
            radius=round(radius * SCALE),
            fill=fill,
            outline=outline,
            width=round(width * SCALE),
        )

    def line(self, points, color=EDGE, width=2):
        self.draw.line(
            [(round(x * SCALE), round(y * SCALE)) for x, y in points],
            fill=color,
            width=max(1, round(width * SCALE)),
        )

    def dot(self, xy, radius=4, color=GREEN):
        x, y = xy
        self.draw.ellipse(
            tuple(
                round(v * SCALE)
                for v in (x - radius, y - radius, x + radius, y + radius)
            ),
            fill=color,
        )

    def wrap(
        self,
        xy,
        value,
        size=22,
        color=WHITE,
        width=1040,
        leading=None,
        weight="regular",
    ):
        words = value.split()
        lines, current = [], ""
        for word in words:
            test = (current + " " + word).strip()
            if (
                self.draw.textlength(test, font=font(size, weight)) > width * SCALE
                and current
            ):
                lines.append(current)
                current = word
            else:
                current = test
        if current:
            lines.append(current)
        for i, line in enumerate(lines):
            self.text(
                (xy[0], xy[1] + i * (leading or size * 1.42)), line, size, color, weight
            )
        return len(lines)


def flow(c, points, t, color=GREEN):
    c.line(points, EDGE, 2)
    c.flows.append((points, t, color))


def particles(c, points, t, color):
    lengths = [math.dist(a, b) for a, b in pairwise(points)]
    length = sum(lengths)
    for phase in (0, 0.34, 0.68):
        cursor = ((t * 0.16 + phase) % 1) * length
        for (a, b), segment in zip(pairwise(points), lengths):
            if cursor <= segment:
                u = cursor / segment
                c.dot((a[0] + (b[0] - a[0]) * u, a[1] + (b[1] - a[1]) * u), 4, color)
                break
            cursor -= segment


def small_label(c, xy, value, color=MUTED):
    c.text(xy, value, 13, color, "mono")


def node(c, x, y, w, title, detail, accent=GREEN):
    c.box((x, y, x + w, y + 112))
    c.line([(x + 20, y + 23), (x + 45, y + 23)], accent, 3)
    c.text((x + 20, y + 37), title, 23, WHITE, "semibold")
    c.text((x + 20, y + 78), detail, 14, MUTED, "mono")


def stat(c, x, y, title, value, detail, accent=GREEN, w=340):
    c.box((x, y, x + w, y + 170), outline=accent)
    c.text((x + 22, y + 17), title, 17, MUTED)
    c.text((x + 20, y + 47), value, 54, WHITE, "bold")
    c.text((x + 22, y + 129), detail, 14, accent, "mono")


def code(c, x, y, lines, w=540, title="application pseudocode"):
    h = 69 + len(lines) * 30
    c.box((x, y, x + w, y + h), fill="#0c1b2a")
    for i, color in enumerate(("#f78d9a", GOLD, GREEN)):
        c.dot((x + 21 + i * 17, y + 20), 4, color)
    small_label(c, (x + 80, y + 11), title)
    for i, (line, color) in enumerate(lines):
        c.text((x + 22, y + 48 + i * 30), line, 17, color, "mono")


def content(c, scene, t, duration):
    p = min(1, t / duration)
    ident = scene["id"]
    if ident == "hook":
        c.text((74, 191), "Immutable documents.", 49, WHITE, "bold")
        c.text((74, 252), "An interface that moves.", 49, GREEN, "bold")
        stat(
            c,
            76,
            367,
            "REQUEST LATENCY",
            f"{20 if p < 0.5 else 19.5} ms",
            "1 new fact → 1 changed key",
            w=355,
        )
        for i, (title, detail) in enumerate(
            (
                ("Keep your place", "No page navigation"),
                ("Stay current", "Apply changes by key"),
                ("Reconnect deliberately", "Resume from applied progress"),
            )
        ):
            c.dot((508, 383 + i * 59), 4)
            c.text((528, 367 + i * 59), title, 23, WHITE, "semibold")
            c.text((528, 397 + i * 59), detail, 16, MUTED)
        for i in range(11):
            x = 995 + i * 15
            top = 500 - ((i * 31 + 20) % 135)
            c.line([(x, 505), (x, top)], GREEN if i < 8 else BLUE, 7)
        small_label(c, (981, 535), "LIVE VIEW")
    elif ident == "facts":
        nodes = [
            (76, "DOCUMENT LOG", "immutable observations"),
            (465, "WASM VIEW", "map → group → fold"),
            (854, "DERIVED STATE", "indexes + results"),
        ]
        for x, title, detail in nodes:
            node(c, x, 278, 348, title, detail)
        flow(c, [(424, 334), (465, 334)], t)
        flow(c, [(813, 334), (854, 334)], t)
        small_label(c, (76, 228), "NATIVE RUST ENGINE", GREEN)
        c.text((76, 435), "Validate + compute", 23, WHITE, "semibold")
        c.text((435, 435), "Append + sync", 23, WHITE, "semibold")
        c.text((824, 435), "Acknowledge batch", 23, WHITE, "semibold")
        c.line([(335, 450), (396, 450)], GREEN)
        c.line([(670, 450), (780, 450)], GREEN)
        c.text(
            (76, 520), "Retain the log. Reconstruct the cache.", 27, GREEN, "semibold"
        )
    elif ident == "mapreduce":
        small_label(c, (76, 205), "DOCUMENTS", BLUE)
        small_label(c, (458, 205), "KEYED CONTRIBUTIONS", GREEN)
        small_label(c, (840, 205), "REDUCED STATE", GOLD)
        for i, n in enumerate((42, 12, 6)):
            y = 250 + i * 79
            c.box((76, y, 385, y + 61))
            c.text((92, y + 18), f'{{metric:"latency", value:{n}}}', 14, WHITE, "mono")
            flow(c, [(386, y + 30), (457, y + 30)], t + i * 0.5)
            c.box((458, y, 729, y + 61))
            c.text((477, y + 16), f'("latency", {n})', 20, GREEN, "mono")
            c.line([(730, y + 30), (780, y + 30), (780, 360), (837, 360)], EDGE, 2)
        c.box((840, 250, 1204, 470))
        for i, (key, val) in enumerate(
            (("count", 3), ("sum", 60), ("min", 6), ("max", 42))
        ):
            c.text((865, 267 + i * 44), key, 22, MUTED, "mono")
            c.text((1167, 267 + i * 44), str(val), 24, WHITE, "mono", anchor="ra")
        c.box((76, 526, 1204, 583), fill="#14342f", outline="#2d6d5d")
        c.text(
            (97, 537), "finalize(state) → mean = sum / count = 20 ms", 24, GREEN, "mono"
        )
    elif ident == "incremental":
        stage = p > 0.43
        small_label(c, (76, 215), "ONLY NEW CONTRIBUTIONS ARE FOLDED", GREEN)
        stat(
            c,
            76,
            267,
            "LATENCY",
            "19.5 ms" if stage else "20 ms",
            "count 4 · sum 78" if stage else "count 3 · sum 60",
            w=340,
        )
        node(c, 470, 297, 320, "+ 18 ms", "new latency observation")
        flow(c, [(417, 352), (468, 352)], t)
        stat(c, 853, 267, "CHECKOUT", "100 ms", "same key state", BLUE, w=350)
        c.text((76, 484), "state′ = add(state, newValues)", 30, WHITE, "mono")
        c.text((76, 539), "Keep the previous answer. Fold the new evidence.", 24, MUTED)
    elif ident == "protocol":
        for i, (a, b) in enumerate(
            (
                ("SNAPSHOT", "pin + drain pages"),
                ("CHANGES", "after view cursor"),
                ("REPLACE KEY", "complete result"),
                ("SAVE", "state + cursor"),
            )
        ):
            x = 76 + i * 289
            node(c, x, 241, 260, a, b, BLUE if i == 0 else GREEN)
            if i < 3:
                flow(c, [(x + 261, 297), (x + 287, 297)], t + i)
        code(
            c,
            76,
            405,
            [
                ("snapshot = db.query()", BLUE),
                ("changes = db.changes(since=cursor)", WHITE),
                ("by_key[row.key] = row.result", GREEN),
            ],
            w=675,
            title="Python • pagination omitted here",
        )
        c.wrap(
            (803, 409),
            "Drain every page before saving progress.",
            25,
            WHITE,
            width=372,
            weight="semibold",
        )
        c.wrap(
            (803, 494),
            "Choose SSE, WebSocket, or another application transport.",
            19,
            MUTED,
            width=377,
        )
    elif ident == "demo":
        stages = json.loads((BUILD / "demo/verification.json").read_text())["stages"]
        # The actual browser states follow the narration: first the keyed update,
        # then disconnection, backlog, reconnect, and the resumed live tail.
        index = max(i for i, start in enumerate(DEMO_CUES) if p >= start)
        img = demo_image(stages[index]["file"])
        c.image.paste(img, (round(124 * SCALE), round(191 * SCALE)))
        c.text(
            (125, 175), "REAL CHROMIUM CAPTURE / BUNDLED METRICS.WASM", 10, BLUE, "mono"
        )
        c.box((76, 583, 1204, 614), fill="#14342f", outline="#2d6d5d", radius=6)
        c.text((93, 587), stages[index]["label"], 16, GREEN, "semibold")
    elif ident == "recovery":
        actions = [
            ("1", "Register subscriber", "Capture replay watermark after registration"),
            ("2", "Replay, then live tail", "Skip queued suffix duplicates"),
            ("3", "Apply, then acknowledge", "Advance only after state is applied"),
            (
                "4",
                "Gap or expired cursor?",
                "Reconnect or replace with a fresh bootstrap",
            ),
        ]
        for i, (n, title, detail) in enumerate(actions):
            y = 211 + i * 88
            c.box((76, y, 121, y + 49), fill="#17372f", outline="#397b68", radius=10)
            c.text((98, y + 12), n, 21, GREEN, "mono", anchor="ma")
            c.text((149, y + 1), title, 24, WHITE, "semibold")
            c.text((149, y + 36), detail, 18, MUTED)
        c.box((850, 221, 1204, 510), fill="#0d1d2b")
        c.text((871, 244), "TWO CURSOR DOMAINS", 14, GOLD, "mono")
        c.wrap(
            (871, 298),
            "Document stream progress",
            23,
            BLUE,
            width=300,
            weight="semibold",
        )
        c.wrap(
            (871, 398),
            "View generation + sequence",
            23,
            GREEN,
            width=300,
            weight="semibold",
        )
        c.text(
            (76, 579),
            "Bounded queues make interruption explicit.",
            21,
            GOLD,
            "semibold",
        )
    elif ident == "portable":
        node(c, 76, 308, 305, "AssemblyScript", "map + reducer + finalizer", BLUE)
        flow(c, [(382, 365), (444, 365)], t, BLUE)
        node(c, 445, 308, 300, "view.wasm", "same compiled bytes", GREEN)
        for y in (228, 440):
            flow(c, [(746, 365), (791, 365), (791, y + 56), (849, y + 56)], t)
        node(c, 850, 228, 354, "Rust host", "durable log + derived cache", BLUE)
        node(c, 850, 440, 354, "Browser worker", "in-memory JS host", GREEN)
        small_label(c, (445, 454), "EXPLICIT INPUT + STATE")
        small_label(c, (445, 483), "DETERMINISTIC VIEW")
    elif ident == "rebuild":
        node(c, 76, 270, 345, "observations.fold", "durable document batches", BLUE)
        flow(c, [(422, 327), (466, 327)], t)
        node(c, 467, 270, 345, "view.wasm", "exact SHA-256 identity", GREEN)
        flow(c, [(813, 327), (857, 327)], t)
        node(c, 858, 270, 346, ".cache", "replaceable checkpoint", GOLD)
        for i, (title, detail) in enumerate(
            (
                ("Valid cache", "Verify log → load prefix → compute suffix"),
                (
                    "Cache missing / module changed",
                    "Replay original batches → new cursor generation",
                ),
                (
                    "Identical named retry",
                    "Same identity + same content → no new observation",
                ),
            )
        ):
            y = 425 + i * 55
            c.text((76, y), title, 20, WHITE, "semibold")
            c.text((494, y + 1), detail, 17, MUTED, "mono")
    elif ident == "ordered":
        events = [
            ("ADD", "Field Guide"),
            ("RENAME", "Systems Guide"),
            ("DELETE", "removed from result"),
        ]
        for i, (a, b) in enumerate(events):
            x = 76 + i * 389
            node(c, x, 255, 347, a, b, BLUE if i == 0 else GREEN if i == 1 else GOLD)
            if i < 2:
                flow(c, [(x + 348, 311), (x + 388, 311)], t)
        active = min(2, int(p * 3))
        c.box((76, 412, 718, 555))
        small_label(c, (98, 430), "CATALOGUE RESULT", GREEN)
        c.text(
            (98, 471),
            "items: {}"
            if active == 2
            else 'items: {atlas: "' + events[active][1] + '"}',
            23,
            WHITE,
            "mono",
        )
        c.text((98, 515), "count: 0" if active == 2 else "count: 1", 17, MUTED, "mono")
        c.text((774, 421), "Order changes meaning.", 25, GOLD, "semibold")
        c.text((774, 469), "This fold is not mergeable.", 19, MUTED)
        c.text((774, 507), "Source facts remain intact.", 19, GREEN)
    elif ident == "boundaries":
        items = [
            ("NATIVE HOST", "One view per DB", "One active owner"),
            ("BROWSER HOST", "In-memory storage", "Export before reload"),
            ("APPLICATION", "Owns network transport", "Owns bootstrap + recovery"),
        ]
        for i, (a, b, d) in enumerate(items):
            x = 76 + i * 389
            c.box((x, 242, x + 347, 455))
            small_label(
                c, (x + 23, 265), a, BLUE if i == 0 else GREEN if i == 1 else GOLD
            )
            c.text((x + 23, 318), b, 21, WHITE, "semibold")
            c.text((x + 23, 372), d, 18, MUTED)
        c.text(
            (76, 504),
            "Host memory grows with retained data and history.",
            25,
            WHITE,
            "semibold",
        )
        c.text(
            (76, 554), "No built-in replication. Measure your own workload.", 22, MUTED
        )
    elif ident == "outro":
        c.text((76, 207), "Keep facts.", 59, WHITE, "bold")
        c.text((76, 280), "Maintain views.", 59, WHITE, "bold")
        c.text((76, 353), "Stream the experience.", 59, GREEN, "bold")
        c.text(
            (76, 478),
            "Telemetry dashboards  /  Activity feeds  /  Catalogues",
            20,
            MUTED,
        )
        c.box((76, 535, 1204, 591), fill="#14342f", outline="#2d6d5d")
        c.text((96, 547), "255bits.github.io/foldlitedb/", 23, GREEN, "mono")


@lru_cache(maxsize=8)
def demo_image(name):
    with Image.open(BUILD / "demo" / name) as im:
        return im.convert("RGB").resize(
            (round(1032 * SCALE), round(1032 * 540 / 1440 * SCALE)),
            Image.Resampling.LANCZOS,
        )


def content_stage(scene, t, duration):
    p = min(1, t / duration)
    if scene["id"] == "hook":
        return int(p >= 0.5)
    if scene["id"] == "incremental":
        return int(p > 0.43)
    if scene["id"] == "ordered":
        return min(2, int(p * 3))
    if scene["id"] == "demo":
        return max(i for i, start in enumerate(DEMO_CUES) if p >= start)
    return 0


@lru_cache(maxsize=32)
def background(scene_json, duration, stage, index):
    scene = json.loads(scene_json)
    representative = {
        "hook": (0.25, 0.75),
        "incremental": (0.2, 0.7),
        "ordered": (0.1, 0.5, 0.9),
        "demo": tuple(
            (start + end) / 2 for start, end in zip(DEMO_CUES, (*DEMO_CUES[1:], 1))
        ),
    }
    t = duration * representative.get(scene["id"], (0,))[stage]
    c = Canvas()
    # Restrained grid keeps diagrams legible while the progress marks move.
    for x in range(76, 1230, 64):
        for y in range(200, 604, 64):
            c.dot((x, y), 0.65, "#1a2a39")
    c.text((76, 28), "foldlite", 24, WHITE, "bold")
    c.text((166, 28), "db", 24, GREEN, "bold")
    small_label(c, (877, 37), "AN ENGINEER’S TOUR  /  " + f"{index + 1:02}")
    c.line([(76, 76), (1204, 76)], EDGE, 1)
    small_label(c, (76, 94), scene["label"], GREEN)
    c.text((74, 120), scene["title"], 39, WHITE, "bold")
    content(c, scene, t, duration)
    c.line([(76, 616), (1204, 616)], EDGE, 1)
    return c.image, c.flows, t


def frame(scene, t, duration, elapsed, total, caption="", index=0):
    # Text and diagram shapes are static between semantic changes. Cache them;
    # only particles, captions and the progress line need drawing each frame.
    key = json.dumps({k: scene[k] for k in ("id", "label", "title")}, sort_keys=True)
    picture, flows, origin = background(
        key, duration, content_stage(scene, t, duration), index
    )
    c = Canvas(picture)
    for points, offset, color in flows:
        particles(c, points, t - origin + offset, color)
    if caption:
        c.wrap((91, 633), caption, 19, WHITE, width=1098, leading=26)
    c.line([(0, 714), (1280, 714)], "#172839", 4)
    c.line([(0, 714), (max(1, 1280 * elapsed / total), 714)], GREEN, 4)
    return c.image


def download_voice():
    target = BUILD / "voice"
    target.mkdir(parents=True, exist_ok=True)
    for name in (MODEL, MODEL + ".json", "MODEL_CARD"):
        path = target / name
        if path.exists():
            if hashlib.sha256(path.read_bytes()).hexdigest() != VOICE_HASHES[name]:
                raise ValueError(f"Voice asset checksum mismatch: {path}")
            continue
        print(f"Downloading voice asset: {name}", flush=True)
        temporary = path.with_suffix(path.suffix + ".part")
        urllib.request.urlretrieve(VOICE_URL + name, temporary)
        if hashlib.sha256(temporary.read_bytes()).hexdigest() != VOICE_HASHES[name]:
            temporary.unlink()
            raise ValueError(f"Downloaded voice checksum mismatch: {name}")
        temporary.replace(path)
    return target / MODEL


def make_audio(story):
    from piper import PiperVoice, SynthesisConfig

    audio = BUILD / "audio"
    audio.mkdir(parents=True, exist_ok=True)
    model = download_voice()
    voice = None
    rate = 22050
    chunks = []
    scenes = []
    captions = []
    position = 0

    def silence(seconds):
        nonlocal position
        count = round(seconds * rate)
        chunks.append(b"\0\0" * count)
        position += count

    for entry in story:
        start = position / rate
        silence(0.65)
        for sentence in re.split(r"(?<=[.!?])\s+", entry["narration"]):
            digest = hashlib.sha256((MODEL + "|0.97|" + sentence).encode()).hexdigest()[
                :20
            ]
            path = audio / (digest + ".wav")
            if not path.exists():
                if voice is None:
                    voice = PiperVoice.load(model)
                print(f"Synthesizing: {sentence[:70]}", flush=True)
                with wave.open(str(path), "wb") as wav:
                    voice.synthesize_wav(
                        sentence, wav, SynthesisConfig(length_scale=0.97)
                    )
            with wave.open(str(path), "rb") as wav:
                if (
                    wav.getframerate() != rate
                    or wav.getnchannels() != 1
                    or wav.getsampwidth() != 2
                ):
                    raise ValueError("Expected mono 22.05kHz 16-bit voice")
                data = wav.readframes(wav.getnframes())
            # One sentence per subtitle, using its actual synthesized duration.
            begin = position / rate
            chunks.append(data)
            position += len(data) // 2
            captions.append({"start": begin, "end": position / rate, "text": sentence})
            silence(0.12)
        silence(0.8)
        scenes.append({**entry, "start": start, "duration": position / rate - start})
        print(f"Audio ready: {entry['id']} ({scenes[-1]['duration']:.1f}s)", flush=True)
    with wave.open(str(BUILD / "narration.wav"), "wb") as wav:
        wav.setparams((1, 2, rate, 0, "NONE", "not compressed"))
        for chunk in chunks:
            wav.writeframes(chunk)
    result = {"duration": position / rate, "scenes": scenes, "captions": captions}
    (BUILD / "timeline.json").write_text(json.dumps(result, indent=2) + "\n")
    return result


def stamp(seconds, separator="."):
    ms = round(seconds * 1000)
    h, ms = divmod(ms, 3600000)
    m, ms = divmod(ms, 60000)
    s, ms = divmod(ms, 1000)
    return f"{h:02}:{m:02}:{s:02}{separator}{ms:03}"


def write_text_tracks(timeline):
    srt = []
    vtt = ["WEBVTT\n"]
    for i, cap in enumerate(timeline["captions"], 1):
        caption = textwrap.fill(cap["text"], 90)
        srt.append(
            f"{i}\n{stamp(cap['start'], ',')} --> {stamp(cap['end'], ',')}\n{caption}\n"
        )
        vtt.append(f"{stamp(cap['start'])} --> {stamp(cap['end'])}\n{caption}\n")
    (HERE / "foldlitedb-streaming-ux.srt").write_text("\n".join(srt))
    (HERE / "foldlitedb-streaming-ux.vtt").write_text("\n".join(vtt))
    chapters = [
        {"start": round(s["start"], 3), "title": s["title"]} for s in timeline["scenes"]
    ]
    (HERE / "chapters.json").write_text(json.dumps(chapters, indent=2) + "\n")
    metadata = [
        ";FFMETADATA1",
        "title=Foldlite DB: from immutable facts to a live web UX",
        "artist=Foldlite DB",
        "comment=Generated from source; synthetic Piper narration. Demo uses actual bundled Wasm.",
    ]
    for i, scene in enumerate(timeline["scenes"]):
        end = (
            timeline["scenes"][i + 1]["start"]
            if i + 1 < len(timeline["scenes"])
            else timeline["duration"]
        )
        metadata.extend(
            [
                "[CHAPTER]",
                "TIMEBASE=1/1000",
                f"START={round(scene['start'] * 1000)}",
                f"END={round(end * 1000)}",
                "title=" + scene["title"],
            ]
        )
    (BUILD / "metadata.txt").write_text("\n".join(metadata) + "\n")


def render_video(timeline, fps):
    target = HERE / "foldlitedb-streaming-ux.mp4"
    raw = BUILD / "picture.mp4"
    total = timeline["duration"]
    command = [
        ffmpeg(),
        "-hide_banner",
        "-loglevel",
        "error",
        "-y",
        "-f",
        "rawvideo",
        "-pix_fmt",
        "rgb24",
        "-s",
        "1920x1080",
        "-r",
        str(fps),
        "-i",
        "-",
        "-an",
        "-c:v",
        "libx264",
        "-preset",
        "fast",
        "-crf",
        "25",
        "-pix_fmt",
        "yuv420p",
        "-threads",
        "4",
        str(raw),
    ]
    process = subprocess.Popen(command, stdin=subprocess.PIPE)
    caption_index = 0
    scene_index = 0
    try:
        for i in range(math.ceil(total * fps)):
            elapsed = i / fps
            while (
                scene_index + 1 < len(timeline["scenes"])
                and timeline["scenes"][scene_index + 1]["start"] <= elapsed
            ):
                scene_index += 1
                print(
                    f"Rendering {scene_index + 1}/{len(timeline['scenes'])}: {timeline['scenes'][scene_index]['id']}",
                    flush=True,
                )
            while (
                caption_index + 1 < len(timeline["captions"])
                and timeline["captions"][caption_index]["end"] <= elapsed
            ):
                caption_index += 1
            cap = timeline["captions"][caption_index]
            caption = cap["text"] if cap["start"] <= elapsed < cap["end"] else ""
            scene = timeline["scenes"][scene_index]
            picture = frame(
                scene,
                elapsed - scene["start"],
                scene["duration"],
                elapsed,
                total,
                caption,
                scene_index,
            )
            # Brief dips at chapter boundaries keep the narration continuous.
            local = elapsed - scene["start"]
            fade = min(1, local / 0.22, (scene["duration"] - local) / 0.22)
            if fade < 1:
                picture = Image.blend(
                    Image.new("RGB", picture.size, BG), picture, max(0, fade)
                )
            process.stdin.write(picture.tobytes())
    finally:
        process.stdin.close()
        code = process.wait()
    if code:
        raise subprocess.CalledProcessError(code, command)
    subprocess.run(
        [
            ffmpeg(),
            "-hide_banner",
            "-loglevel",
            "error",
            "-y",
            "-i",
            str(raw),
            "-i",
            str(BUILD / "narration.wav"),
            "-i",
            str(HERE / "foldlitedb-streaming-ux.srt"),
            "-i",
            str(BUILD / "metadata.txt"),
            "-map",
            "0:v",
            "-map",
            "1:a",
            "-map",
            "2:s",
            "-map_metadata",
            "3",
            "-map_chapters",
            "3",
            "-c:v",
            "copy",
            "-c:a",
            "aac",
            "-b:a",
            "128k",
            "-af",
            "loudnorm=I=-16:TP=-1.5:LRA=11",
            "-c:s",
            "mov_text",
            "-metadata:s:s:0",
            "language=eng",
            "-disposition:s:0",
            "0",
            "-movflags",
            "+faststart",
            "-t",
            str(total),
            str(target),
        ],
        check=True,
    )
    print(f"Video: {target} ({target.stat().st_size / 1024**2:.1f} MiB)", flush=True)


def previews(timeline):
    thumbs = []
    for i, scene in enumerate(timeline["scenes"]):
        local = scene["duration"] * 0.55
        picture = frame(
            scene,
            local,
            scene["duration"],
            scene["start"] + local,
            timeline["duration"],
            "",
            i,
        )
        picture.save(BUILD / f"preview-{scene['id']}.png")
        thumbs.append(picture.resize((640, 360), Image.Resampling.LANCZOS))
    contact = Image.new("RGB", (1920, 1440), BG)
    for i, picture in enumerate(thumbs):
        contact.paste(picture, ((i % 3) * 640, (i // 3) * 360))
    contact.save(BUILD / "storyboard.jpg", quality=92)
    frame(timeline["scenes"][0], 7, 20, 7, timeline["duration"], "", 0).save(
        HERE / "poster.jpg", quality=94
    )


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--audio-only", action="store_true")
    parser.add_argument(
        "--preview",
        action="store_true",
        help="Use a cached timeline to render stills only",
    )
    parser.add_argument("--fps", type=int, default=24)
    args = parser.parse_args()
    BUILD.mkdir(parents=True, exist_ok=True)
    if not (BUILD / "demo/verification.json").exists():
        raise SystemExit("First run: python videos/streaming-ux/demo.py --capture")
    if args.preview:
        timeline = json.loads((BUILD / "timeline.json").read_text())
    else:
        timeline = make_audio(json.loads((HERE / "story.json").read_text()))
    write_text_tracks(timeline)
    previews(timeline)
    if not args.audio_only and not args.preview:
        render_video(timeline, args.fps)


if __name__ == "__main__":
    main()
