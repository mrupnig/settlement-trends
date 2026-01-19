from __future__ import annotations

import argparse
import json
import re
from pathlib import Path
from typing import Dict, List, Optional, Tuple

FIELD_PREFIXES: List[Tuple[str, str]] = [
    ("Class Code:", "class_code"),
    ("Class Name:", "class_name"),
    ("Period:", "period"),
    ("Description:", "description"),
    ("Origin:", "origin"),
    ("Sites:", "sites"),
    ("Total sherds:", "total_sherds"),
    ("Figures:", "figures"),
    ("Parallels:", "parallels"),
]

PREFIX_TO_KEY = {p: k for p, k in FIELD_PREFIXES}
PREFIX_RE = re.compile(
    r"^(Class Code:|Class Name:|Period:|Description:|Origin:|Sites:|Total sherds:|Figures:|Parallels:)\s*"
)


def starts_with_known_prefix(line: str) -> Optional[str]:
    m = PREFIX_RE.match(line)
    return m.group(1) if m else None


def flush_record(out: List[Dict[str, object]], record: Optional[Dict[str, object]]) -> None:
    if not record:
        return
    if record.get("class_code"):
        out.append(record)


def normalize_value(text: str) -> str:
    lines = [ln.rstrip() for ln in text.splitlines()]
    return " ".join(lines).strip()


def export_classification(
    input_path: Path = Path("data/raw/appendix_c_pottery_classification.txt"),
    output_path: Path = Path("data/interim/classification.jsonl"),
) -> None:
    if not input_path.exists():
        raise FileNotFoundError(f"Input file not found: {input_path}")

    lines = input_path.read_text(encoding="utf-8", errors="replace").splitlines()

    out: List[Dict[str, object]] = []

    # Wichtig: class erst "für den nächsten Record" übernehmen
    pending_class: str = ""
    record: Optional[Dict[str, object]] = None

    current_field_key: Optional[str] = None
    buffer: List[str] = []

    def commit_buffer() -> None:
        nonlocal buffer, current_field_key, record
        if record is None or current_field_key is None:
            buffer = []
            return
        val = normalize_value("\n".join(buffer))
        record[current_field_key] = val if val != "" else None
        buffer = []

    for raw in lines:
        stripped = raw.strip()

        if stripped == "":
            if current_field_key is not None:
                buffer.append("")
            continue

        # 1) "# ..." setzt nur den pending_class (gilt ab nächstem Class Code)
        if stripped.startswith("# "):
            commit_buffer()
            pending_class = stripped[2:].strip()
            # KEIN Update des laufenden records!
            continue

        # 2) Prefix erkennen
        prefix = starts_with_known_prefix(stripped)
        if prefix is not None:
            key = PREFIX_TO_KEY[prefix]

            # Neuer Record bei Class Code:
            if prefix == "Class Code:":
                commit_buffer()
                flush_record(out, record)

                record = {"class": pending_class}
                current_field_key = None
                buffer = []

            # Feldwechsel
            commit_buffer()
            current_field_key = key

            after = stripped[len(prefix):].strip()
            buffer = [after] if after != "" else []
            continue

        # 3) sonst: Fortsetzung des aktuellen Feldes
        if current_field_key is not None:
            buffer.append(stripped)
        else:
            # außerhalb bekannter Felder ignorieren
            continue

    commit_buffer()
    flush_record(out, record)

    output_path.parent.mkdir(parents=True, exist_ok=True)
    with output_path.open("w", encoding="utf-8") as f:
        for obj in out:
            f.write(json.dumps(obj, ensure_ascii=False) + "\n")


def main() -> None:
    ap = argparse.ArgumentParser(description="Export pottery classification TXT to JSONL.")
    ap.add_argument(
        "--input",
        type=Path,
        default=Path("data/raw/appendix_c_pottery_classification.txt"),
        help="Path to the input txt file.",
    )
    ap.add_argument(
        "--output",
        type=Path,
        default=Path("data/interim/classification.jsonl"),
        help="Path to the output jsonl file.",
    )
    args = ap.parse_args()
    export_classification(args.input, args.output)


if __name__ == "__main__":
    main()
