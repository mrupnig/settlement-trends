from __future__ import annotations

import json
import re
from pathlib import Path
from typing import Iterable

VILLAGE_HEADER = re.compile(
    r"^\s*A\.\d+(?:\.\d+)*\s*-\s*(.+?)\s*\((CS\.\d+)\)\s*:\s*$",
    re.IGNORECASE,
)

SITE_HEADER = re.compile(
    r"""
    ^\s*
    (?P<name>.+?)                     # Site name (e.g. Jebel Qard)
    \s+
    (?P<code>CS\.\d+(?:\.\d+)*)       # CS code
    (?:                               # optional coordinate block
        \s*
        \(
        (?P<easting>\d{7})
        /
        (?P<northing>\d{7})
        \)
    )?
    (?:                               # optional trailing info (Figure, etc.)
        \s*,?\s*
        (?P<rest>.*?)
    )?
    \s*:\s*$
    """,
    re.IGNORECASE | re.VERBOSE,
)


SIZE_LINE = re.compile(r"\bSize\s*:\s*([\d.]+)\s*ha\b", re.IGNORECASE)
STRUCTURES_MARK = re.compile(r"\bStructures\s*:\s*", re.IGNORECASE)
FINDS_MARK = re.compile(r"\bFinds\s*:\s*", re.IGNORECASE)


def iter_lines(path: Path) -> Iterable[str]:
    for ln in path.read_text(encoding="utf-8", errors="replace").splitlines():
        yield ln.rstrip("\n")


def parse_gazetteer(cleaned_gazetteer_path: Path) -> list[dict]:
    current_village_name: str | None = None
    current_village_prefix: str | None = None

    sites: list[dict] = []

    block_lines: list[str] = []
    block_meta: dict | None = None
    block_index = 0

    def flush_block() -> None:
        nonlocal block_lines, block_meta, block_index
        if not block_meta:
            return

        text = " ".join(block_lines).strip()
        area_ha = None
        msize = SIZE_LINE.search(text)
        if msize:
            try:
                area_ha = float(msize.group(1))
            except ValueError:
                area_ha = None

        # Extract structures/finds text segments (best-effort)
        structures_text = None
        finds_text = None

        # Find "Structures:" and "Finds:" positions in the whole text
        sm = STRUCTURES_MARK.search(text)
        fm = FINDS_MARK.search(text)

        if sm and fm and sm.start() < fm.start():
            structures_text = text[sm.end() : fm.start()].strip()
            finds_text = text[fm.end() :].strip()
        elif fm:
            finds_text = text[fm.end() :].strip()
        else:
            # fallback: keep full block as notes for later improvement
            pass

        rec = {
            "code": block_meta["code"],
            "village_name": block_meta["village_name"],
            "village_code_prefix": block_meta["village_code_prefix"],
            "name": block_meta["name"],
            "coordinate_system": "UTM (as in thesis)",
            "utm_easting": block_meta["utm_easting"],
            "utm_northing": block_meta["utm_northing"],
            "area_ha": area_ha,
            "structures_text": structures_text,
            "finds_text": finds_text,
            "notes": None if (structures_text or finds_text) else text,
            "source_file": cleaned_gazetteer_path.name,
            "source_locator": f"site_block_{block_index:04d}",
        }
        sites.append(rec)

        block_lines = []
        block_meta = None
        block_index += 1

    for ln in iter_lines(cleaned_gazetteer_path):
        # Village header?
        mv = VILLAGE_HEADER.match(ln.strip())
        if mv:
            # new village context
            current_village_name = mv.group(1).strip()
            current_village_prefix = mv.group(2).strip()
            continue

        ms = SITE_HEADER.match(ln.strip())
        if ms:
            # start new site: flush previous
            flush_block()

            site_name = ms.group("name").strip()
            site_code = ms.group("code").strip()

            easting = ms.group("easting")
            northing = ms.group("northing")
            rest = ms.group("rest")

            block_meta = {
                "name": f"{site_name} {site_code}",
                "code": site_code,
                "utm_easting": int(easting) if easting else None,
                "utm_northing": int(northing) if northing else None,
                "village_name": current_village_name or "UNKNOWN_VILLAGE",
                "village_code_prefix": current_village_prefix,
            }

            block_lines = [rest.strip()] if rest else []
            continue

        # Collect lines inside current site block
        if block_meta is not None:
            if ln.strip():
                block_lines.append(ln.strip())

    flush_block()
    return sites


def write_jsonl(records: list[dict], out_path: Path) -> None:
    out_path.parent.mkdir(parents=True, exist_ok=True)
    with out_path.open("w", encoding="utf-8") as f:
        for rec in records:
            f.write(json.dumps(rec, ensure_ascii=False) + "\n")
