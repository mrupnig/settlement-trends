#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import re
from pathlib import Path
from typing import Dict, List, Tuple, Optional

from lxml import etree


FIELDS = ["object_no", "area", "site", "period", "description"]


def natural_key(p: Path) -> Tuple:
    parts = re.split(r"(\d+)", p.name)
    key: List[object] = []
    for part in parts:
        key.append(int(part) if part.isdigit() else part.lower())
    return tuple(key)


def reading_order_region_ids(page_root: etree._Element) -> List[str]:
    refs = page_root.xpath(
        ".//*[local-name()='ReadingOrder']//*[local-name()='RegionRefIndexed']/@regionRef"
    )
    if refs:
        return [r for r in refs if r]
    return [r for r in page_root.xpath(".//*[local-name()='TextRegion']/@id") if r]


def extract_region_text(region_el: etree._Element) -> Optional[str]:
    """
    Gibt den Text einer TextRegion zurück oder None,
    wenn die Region keine TextLines / keinen Unicode-Text enthält.
    """
    unicode_nodes = region_el.xpath(
        ".//*[local-name()='TextLine']/*[local-name()='TextEquiv']/*[local-name()='Unicode']"
    )
    lines: List[str] = []
    for n in unicode_nodes:
        txt = (n.text or "").strip()
        if txt:
            lines.append(txt)

    if not lines:
        return None
    return " ".join(lines)


def process_pages(input_dir: Path, output_path: Path) -> None:
    xml_files = sorted(input_dir.glob("*.xml"), key=natural_key)
    if not xml_files:
        raise FileNotFoundError(f"Keine *.xml Dateien gefunden in: {input_dir}")

    out: List[Dict[str, Optional[str]]] = []

    current: Dict[str, Optional[str]] = {k: None for k in FIELDS}
    field_idx = 0  # 0..4

    for xml_file in xml_files:
        tree = etree.parse(str(xml_file))
        root = tree.getroot()

        regions = root.xpath(".//*[local-name()='TextRegion']")
        region_by_id = {r.get("id"): r for r in regions if r.get("id")}

        for rid in reading_order_region_ids(root):
            region = region_by_id.get(rid)
            if region is None:
                continue

            text = extract_region_text(region)  # None, wenn leer

            key = FIELDS[field_idx]
            current[key] = text  # kann None sein → wird zu JSON null

            field_idx += 1
            if field_idx == len(FIELDS):
                out.append(current)
                current = {k: None for k in FIELDS}
                field_idx = 0

    # Restdatensatz (falls Anzahl der TextRegions nicht durch 5 teilbar ist)
    if any(v is not None for v in current.values()):
        out.append(current)

    output_path.parent.mkdir(parents=True, exist_ok=True)
    with output_path.open("w", encoding="utf-8") as f:
        for obj in out:
            f.write(json.dumps(obj, ensure_ascii=False) + "\n")


def main() -> None:
    ap = argparse.ArgumentParser(
        description="Positionsbasierter Export von PAGE-XML TextRegions nach JSONL (leere Regionen → null)."
    )
    ap.add_argument(
        "--input-dir",
        type=Path,
        default=Path("pages/h"),
        help="Ordner mit PAGE-XML-Dateien (*.xml).",
    )
    ap.add_argument(
        "--output",
        type=Path,
        default=Path("data/interim/finds.jsonl"),
        help="Ausgabedatei (JSONL).",
    )
    args = ap.parse_args()
    process_pages(args.input_dir, args.output)


if __name__ == "__main__":
    main()
