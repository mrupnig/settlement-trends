from __future__ import annotations

import re
from pathlib import Path

PAGE_NUMBER_LINE = re.compile(r"^\s*\d+\s*$")


def clean_text(text: str) -> str:
    lines = text.splitlines()
    out: list[str] = []
    for ln in lines:
        # remove pure page number lines
        if PAGE_NUMBER_LINE.match(ln):
            continue

        # normalize soft hyphen
        ln = ln.replace("\u00ad", "")

        out.append(ln.rstrip())

    cleaned = "\n".join(out)
    # collapse many empty lines
    cleaned = re.sub(r"\n{3,}", "\n\n", cleaned)
    cleaned = cleaned.strip() + "\n"
    return cleaned


def clean_file(src: Path, dst: Path) -> None:
    dst.parent.mkdir(parents=True, exist_ok=True)
    text = src.read_text(encoding="utf-8", errors="replace")
    dst.write_text(clean_text(text), encoding="utf-8")


def clean_dir(raw_dir: Path, cleaned_dir: Path) -> list[Path]:
    cleaned_dir.mkdir(parents=True, exist_ok=True)
    produced: list[Path] = []
    for p in sorted(raw_dir.glob("*.txt")):
        out = cleaned_dir / f"{p.stem}.cleaned.txt"
        clean_file(p, out)
        produced.append(out)
    return produced
