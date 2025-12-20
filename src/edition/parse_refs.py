from __future__ import annotations

import json
import re
from pathlib import Path
from typing import Iterable


CS_CODE = re.compile(r"\bCS\.\d+(?:\.\d+)*\b", re.IGNORECASE)


def iter_clean_lines(path: Path) -> Iterable[str]:
    for ln in path.read_text(encoding="utf-8", errors="replace").splitlines():
        ln = ln.strip()
        if ln:
            yield ln


def parse_numbered_list(cleaned_path: Path, label: str) -> tuple[list[dict], list[dict]]:
    """
    Parse lines like:
      Figure 88 some caption CS.1.1
      Map 3 ...
      Plate 12 ...
    Returns:
      items: [{"number": int, "caption": str, "page": None, "source_file": ...}, ...]
      links: [{"site_code": "CS.1.1", "<label>_number": 88}, ...]
    """
    line_re = re.compile(rf"^\s*{re.escape(label)}\s+(\d+)\s+(.*\S)\s*$", re.IGNORECASE)

    items: list[dict] = []
    links: list[dict] = []

    for ln in iter_clean_lines(cleaned_path):
        m = line_re.match(ln)
        if not m:
            continue

        number = int(m.group(1))
        caption = m.group(2)

        items.append(
            {
                "number": number,
                "caption": caption,
                "page": None,
                "source_file": cleaned_path.name,
            }
        )

        # Create links by extracting CS.* codes from caption
        for code in sorted(set(c.upper() for c in CS_CODE.findall(caption))):
            links.append({"site_code": code, f"{label.lower()}_number": number})

    return items, links


def write_jsonl(records: list[dict], out_path: Path) -> None:
    out_path.parent.mkdir(parents=True, exist_ok=True)
    with out_path.open("w", encoding="utf-8") as f:
        for rec in records:
            f.write(json.dumps(rec, ensure_ascii=False) + "\n")
