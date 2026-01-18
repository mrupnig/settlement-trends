#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, List, Optional, Tuple

from lxml import etree


TARGET_TYPES = {"header", "heading", "toc-entry", "credit", "paragraph", "footnote"}
STRUCTURAL_TYPES = {"header", "heading", "toc-entry"}
CONTENT_TYPES = {"credit", "paragraph", "footnote"}


def natural_key(p: Path) -> Tuple:
    parts = re.split(r"(\d+)", p.name)
    key: List[object] = []
    for part in parts:
        key.append(int(part) if part.isdigit() else part.lower())
    return tuple(key)


def parse_custom_type(custom: str) -> Optional[str]:
    if not custom:
        return None
    m = re.search(r"type\s*:\s*([A-Za-z0-9_-]+)", custom)
    return m.group(1).strip().lower() if m else None


def get_region_type(region_el: etree._Element) -> Optional[str]:
    t = (region_el.get("type") or "").strip().lower()
    if t:
        return t
    return parse_custom_type((region_el.get("custom") or "").strip())


def extract_region_text(region_el: etree._Element) -> str:
    unicode_nodes = region_el.xpath(
        ".//*[local-name()='TextLine']/*[local-name()='TextEquiv']/*[local-name()='Unicode']"
    )
    lines: List[str] = []
    for n in unicode_nodes:
        txt = (n.text or "").strip()
        if txt:
            lines.append(txt)
    return " ".join(lines).strip()


def reading_order_region_ids(page_root: etree._Element) -> List[str]:
    refs = page_root.xpath(
        ".//*[local-name()='ReadingOrder']//*[local-name()='RegionRefIndexed']/@regionRef"
    )
    if refs:
        return [r for r in refs if r]
    return [r for r in page_root.xpath(".//*[local-name()='TextRegion']/@id") if r]


@dataclass
class State:
    category: str = ""
    sub_category: str = ""
    sub_sub_category: str = ""


def new_record_from_state(st: State) -> Dict[str, str]:
    return {
        "category": st.category,
        "sub_category": st.sub_category,
        "sub_sub_category": st.sub_sub_category,
        "site_feature_type": "",
        "description": "",
        "sites": "",
    }


def append_text(existing: str, addition: str) -> str:
    if not addition:
        return existing
    if not existing:
        return addition
    return existing + "\n" + addition


def append_field(rec: Dict[str, str], field: str, text: str) -> None:
    if not text:
        return
    rec[field] = append_text(rec.get(field, ""), text)


def record_has_content(rec: Dict[str, str]) -> bool:
    # Wichtig: credit zählt jetzt als “Content”, weil ein Record pro credit entstehen soll
    return bool(rec.get("site_feature_type") or rec.get("description") or rec.get("sites"))


def process_pages(input_dir: Path, output_path: Path) -> None:
    xml_files = sorted(input_dir.glob("*.xml"), key=natural_key)
    if not xml_files:
        raise FileNotFoundError(f"Keine *.xml Dateien gefunden in: {input_dir}")

    state = State()
    current_record: Optional[Dict[str, str]] = None
    out_lines: List[Dict[str, str]] = []

    # Tracking für Dateigrenzen-Fortsetzung
    last_region_type: Optional[str] = None
    last_region_text: str = ""
    last_file: Optional[Path] = None

    def flush_record() -> None:
        nonlocal current_record
        if current_record and record_has_content(current_record):
            out_lines.append(current_record)
        current_record = None

    for xml_file in xml_files:
        tree = etree.parse(str(xml_file))
        root = tree.getroot()

        regions = root.xpath(".//*[local-name()='TextRegion']")
        region_by_id: Dict[str, etree._Element] = {r.get("id"): r for r in regions if r.get("id")}

        ro_ids = reading_order_region_ids(root)

        # Relevante Regionen extrahieren (damit wir “erste relevante” erkennen können)
        relevant: List[Tuple[str, str, str]] = []
        for rid in ro_ids:
            region = region_by_id.get(rid)
            if region is None:
                continue
            rtype = get_region_type(region)
            if not rtype:
                continue
            rtype = rtype.lower()
            if rtype not in TARGET_TYPES:
                continue
            text = extract_region_text(region)
            if not text:
                continue
            relevant.append((rid, rtype, text))

        for idx, (_rid, rtype, text) in enumerate(relevant):
            is_first_relevant_of_file = idx == 0
            crossed_file_boundary = last_file is not None and last_file != xml_file
            continuation_across_files = (
                crossed_file_boundary
                and is_first_relevant_of_file
                and last_region_type == rtype
            )

            # identische Wiederholung am Seitenanfang (Running header etc.) ignorieren
            if continuation_across_files and text.strip() == last_region_text.strip():
                last_region_type = rtype
                last_region_text = text
                continue

            # echte Fortsetzung struktureller Region über Dateigrenze: anhängen statt resetten
            if continuation_across_files and rtype in STRUCTURAL_TYPES:
                if rtype == "header":
                    state.category = append_text(state.category, text) if state.category else text
                elif rtype == "heading":
                    state.sub_category = append_text(state.sub_category, text) if state.sub_category else text
                elif rtype == "toc-entry":
                    state.sub_sub_category = append_text(state.sub_sub_category, text) if state.sub_sub_category else text
                    # existing record (falls offen) State synchron halten
                    if current_record is not None:
                        current_record["sub_sub_category"] = state.sub_sub_category

                last_region_type = rtype
                last_region_text = text
                continue

            # --- Strukturwechsel ---
            if rtype == "header":
                flush_record()
                state.category = text
                state.sub_category = ""
                state.sub_sub_category = ""
                current_record = None

            elif rtype == "heading":
                flush_record()
                state.sub_category = text
                state.sub_sub_category = ""
                current_record = None

            elif rtype == "toc-entry":
                # NUR den State setzen; Record wird NICHT automatisch erzeugt.
                # Record-Start hängt jetzt primär an credit.
                flush_record()
                state.sub_sub_category = text
                current_record = None

            # --- Content ---
            elif rtype == "credit":
                # NEUE ZEILE STARTEN, wenn credit kommt:
                # bisherigen Record abschließen und neuen anlegen
                flush_record()
                current_record = new_record_from_state(state)
                append_field(current_record, "site_feature_type", text)

            elif rtype == "paragraph":
                if current_record is None:
                    # Falls paragraph ohne vorherigen credit auftaucht: Record mit leerem credit starten
                    current_record = new_record_from_state(state)
                append_field(current_record, "description", text)

            elif rtype == "footnote":
                if current_record is None:
                    current_record = new_record_from_state(state)
                append_field(current_record, "sites", text)

            last_region_type = rtype
            last_region_text = text

        last_file = xml_file

    flush_record()

    output_path.parent.mkdir(parents=True, exist_ok=True)
    with output_path.open("w", encoding="utf-8") as f:
        for obj in out_lines:
            f.write(json.dumps(obj, ensure_ascii=False) + "\n")


def main() -> None:
    ap = argparse.ArgumentParser(
        description="Extrahiert PAGE-XML TextRegions nach ReadingOrder und schreibt typology.jsonl."
    )
    ap.add_argument(
        "--input-dir",
        type=Path,
        default=Path("/home/martin/ocr4all/data/settlement_B/results/2025-12-21-19-20-17_xml/pages"),
        help="Ordner mit PAGE-XML Dateien (*.xml).",
    )
    ap.add_argument(
        "--output",
        type=Path,
        default=Path("typology.jsonl"),
        help="Ausgabedatei im JSONL-Format.",
    )
    args = ap.parse_args()
    process_pages(args.input_dir, args.output)


if __name__ == "__main__":
    main()
