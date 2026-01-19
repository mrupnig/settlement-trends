from __future__ import annotations

import json
from pathlib import Path
import hashlib
import re


from .db import connect


def read_jsonl(path: Path) -> list[dict]:
    records: list[dict] = []
    if not path.exists():
        return records
    with path.open("r", encoding="utf-8") as f:
        for ln in f:
            ln = ln.strip()
            if not ln:
                continue
            records.append(json.loads(ln))
    return records

CS_CODE_RE = re.compile(r"\bCS\.\d+(?:\.\d+)*\b", re.IGNORECASE)
PLATE_RE = re.compile(r"\bPlate\s+(\d+)\b", re.IGNORECASE)

# Matches "4 sherds", "I sherd", "II sherds", OCR variants
SHERD_COUNT_RE = re.compile(r"\b([IVXLCDM]+|\d+|[Il])\s*sherd", re.IGNORECASE)

# Extract P-codes like P00681, P02326 etc (OCR may vary: P0..., PO..., P.123)
P_CODE_RE = re.compile(r"\bP[0O]?\s*\.?\s*\d{3,6}\b", re.IGNORECASE)

# Extract plate numbers inside "Plates: 382 (A-B); 387 (A-C)"
PLATE_NUM_RE = re.compile(r"\b(\d{1,4})\b")

# Figure references: can be digits or roman numerals like "III"
FIG_LABEL_RE = re.compile(r"\b(\d{1,4}|[IVXLCDM]{1,8})\b", re.IGNORECASE)


_ROMAN = {"I":1,"II":2,"III":3,"IV":4,"V":5,"VI":6,"VII":7,"VIII":8,"IX":9,"X":10,"XI":11,"XII":12}
def _parse_count_token(tok: str) -> int | None:
    if not tok:
        return None
    t = tok.strip().upper()
    if t.isdigit():
        return int(t)
    # OCR: "l" often used for "1"
    if t in {"L", "I"}:
        return 1
    return _ROMAN.get(t)


def split_site_entries(sites_text: str | None) -> list[str]:
    """
    Example:
      "CS.5.2.1= 4 sherds (...); CS.5.1.2= 2 sherds (...); ..."
    We split on semicolons first. Also handle stray ":" as separators.
    """
    if not sites_text:
        return []
    text = sites_text.replace(":", ";")
    parts = [p.strip() for p in text.split(";")]
    return [p for p in parts if p]


def parse_site_entry(entry: str) -> tuple[str | None, int | None, str | None, str | None]:
    """
    Returns: (site_code, sherd_count, evidence_text, sample_codes_text)
    """
    m = CS_CODE_RE.search(entry)
    if not m:
        return None, None, None, None

    site_code = m.group(0).upper()

    evidence = entry[m.end():].strip()
    evidence = evidence.lstrip(" =,-").strip() or None

    # best-effort count
    sherd_count = None
    mc = SHERD_COUNT_RE.search(entry)
    if mc:
        sherd_count = _parse_count_token(mc.group(1))

    # extract P-codes (raw list)
    p_codes = [re.sub(r"\s+", "", x.upper().replace("O", "0")) for x in P_CODE_RE.findall(entry)]
    sample_codes_text = ", ".join(sorted(set(p_codes))) if p_codes else None

    return site_code, sherd_count, evidence, sample_codes_text


def extract_plate_numbers_from_figures_field(figures_text: str | None) -> list[int]:
    """
    The field mixes "Plates: 382 (A-B)" and figures.
    We'll only pull plate numbers after the word 'Plates' if present,
    else fallback to extracting numbers conservatively.
    """
    if not figures_text:
        return []
    # Try to isolate substring after "Plates"
    lower = figures_text.lower()
    idx = lower.find("plates")
    if idx == -1:
        return []
    sub = figures_text[idx:]
    nums = []
    for m in PLATE_NUM_RE.finditer(sub):
        n = int(m.group(1))
        nums.append(n)
    # unique preserving order
    seen = set()
    out = []
    for n in nums:
        if n not in seen:
            out.append(n); seen.add(n)
    return out


def extract_figure_labels(figures_text: str | None) -> list[str]:
    """
    Extract labels like '122', 'III' from figures_text, excluding plate-only part when possible.
    """
    if not figures_text:
        return []
    # Remove everything after "Plates" to avoid mixing plate numbers into figure labels
    lower = figures_text.lower()
    idx = lower.find("plates")
    sub = figures_text[:idx] if idx != -1 else figures_text

    labels = []
    for m in FIG_LABEL_RE.finditer(sub):
        labels.append(m.group(1).upper())

    # De-dup preserving order
    seen = set()
    out = []
    for x in labels:
        if x not in seen:
            out.append(x); seen.add(x)
    return out



def parse_site_field(site_raw: str | None) -> tuple[str | None, str | None]:
    """
    Input examples:
      "CS.4.13, Tomb 8"
      "CS.1.2.1"
      "CS.4.13, Tomb 8, Chamber 2" (we keep rest as context)
    Returns:
      (site_code, context)
    """
    if not site_raw:
        return None, None

    m = CS_CODE_RE.search(site_raw)
    if not m:
        return None, site_raw.strip() or None

    site_code = m.group(0).upper()
    # context = everything after the CS.* code, stripped of leading separators
    rest = site_raw[m.end():].strip()
    rest = rest.lstrip(" ,;:-").strip()
    context = rest or None
    return site_code, context


def extract_plate_numbers(text: str | None) -> list[int]:
    if not text:
        return []
    nums = []
    for m in PLATE_RE.finditer(text):
        try:
            nums.append(int(m.group(1)))
        except ValueError:
            continue
    # preserve order but unique
    seen = set()
    out = []
    for n in nums:
        if n not in seen:
            out.append(n)
            seen.add(n)
    return out


def sha256_file(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def image_info_for(
    kind: str,
    raw_dir: Path,
    number: int,
    project_root: Path,
) -> tuple[str | None, str | None, str | None]:
    """
    Returns (relative_path, mime, sha256) or (None, None, None)
    """
    prefix = {"figure": "fig", "map": "map", "plate": "plate"}[kind]
    filename = f"{prefix}_{number:03d}.png"
    abs_path = raw_dir / filename

    if not abs_path.exists():
        return None, None, None

    # relative path
    rel_path = abs_path.relative_to(project_root).as_posix()

    mime = "image/png"
    digest = sha256_file(abs_path)

    return rel_path, mime, digest



def upsert_village(con, name: str, code_prefix: str | None) -> int:
    con.execute(
        """
        INSERT INTO village (name, code_prefix)
        VALUES (?, ?)
        ON CONFLICT(name) DO UPDATE SET
          code_prefix = COALESCE(excluded.code_prefix, village.code_prefix),
          updated_at = datetime('now')
        """,
        (name, code_prefix),
    )
    row = con.execute("SELECT id FROM village WHERE name = ?", (name,)).fetchone()
    return int(row["id"])


def upsert_site(con, site: dict, village_id: int) -> int:
    con.execute(
        """
        INSERT INTO site (
          code, village_id, name, coordinate_system, utm_easting, utm_northing,
          area_ha, structures_text, finds_text, notes, source_file, source_locator
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(code) DO UPDATE SET
          village_id = excluded.village_id,
          name = COALESCE(excluded.name, site.name),
          coordinate_system = COALESCE(excluded.coordinate_system, site.coordinate_system),
          utm_easting = COALESCE(excluded.utm_easting, site.utm_easting),
          utm_northing = COALESCE(excluded.utm_northing, site.utm_northing),
          area_ha = COALESCE(excluded.area_ha, site.area_ha),
          structures_text = COALESCE(excluded.structures_text, site.structures_text),
          finds_text = COALESCE(excluded.finds_text, site.finds_text),
          notes = COALESCE(excluded.notes, site.notes),
          source_file = COALESCE(excluded.source_file, site.source_file),
          source_locator = COALESCE(excluded.source_locator, site.source_locator),
          updated_at = datetime('now')
        """,
        (
            site["code"],
            village_id,
            site.get("name"),
            site.get("coordinate_system"),
            site.get("utm_easting"),
            site.get("utm_northing"),
            site.get("area_ha"),
            site.get("structures_text"),
            site.get("finds_text"),
            site.get("notes"),
            site.get("source_file"),
            site.get("source_locator"),
        ),
    )
    row = con.execute("SELECT id FROM site WHERE code = ?", (site["code"],)).fetchone()
    return int(row["id"])


def upsert_figure(con, fig: dict) -> int:
    con.execute(
        """
        INSERT INTO figure (number, caption, page, source_file, image_path, image_mime, image_sha256)
        VALUES (?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(number) DO UPDATE SET
          caption = excluded.caption,
          page = COALESCE(excluded.page, figure.page),
          source_file = COALESCE(excluded.source_file, figure.source_file),

          image_path = COALESCE(excluded.image_path, figure.image_path),
          image_mime = COALESCE(excluded.image_mime, figure.image_mime),
          image_sha256 = COALESCE(excluded.image_sha256, figure.image_sha256),

          updated_at = datetime('now')
        """,
        (
            fig["number"],
            fig["caption"],
            fig.get("page"),
            fig.get("source_file"),
            fig.get("image_path"),
            fig.get("image_mime"),
            fig.get("image_sha256"),
        ),
    )
    row = con.execute("SELECT id FROM figure WHERE number = ?", (fig["number"],)).fetchone()
    return int(row["id"])


def upsert_map(con, m: dict) -> int:
    con.execute(
        """
        INSERT INTO map (number, caption, page, source_file, image_path, image_mime, image_sha256)
        VALUES (?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(number) DO UPDATE SET
          caption = excluded.caption,
          page = COALESCE(excluded.page, map.page),
          source_file = COALESCE(excluded.source_file, map.source_file),

          image_path = COALESCE(excluded.image_path, map.image_path),
          image_mime = COALESCE(excluded.image_mime, map.image_mime),
          image_sha256 = COALESCE(excluded.image_sha256, map.image_sha256),

          updated_at = datetime('now')
        """,
        (
            m["number"],
            m["caption"],
            m.get("page"),
            m.get("source_file"),
            m.get("image_path"),
            m.get("image_mime"),
            m.get("image_sha256"),
        ),
    )
    row = con.execute("SELECT id FROM map WHERE number = ?", (m["number"],)).fetchone()
    return int(row["id"])


def upsert_plate(con, p: dict) -> int:
    con.execute(
        """
        INSERT INTO plate (number, caption, page, source_file, image_path, image_mime, image_sha256)
        VALUES (?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(number) DO UPDATE SET
          caption = excluded.caption,
          page = COALESCE(excluded.page, plate.page),
          source_file = COALESCE(excluded.source_file, plate.source_file),

          image_path = COALESCE(excluded.image_path, plate.image_path),
          image_mime = COALESCE(excluded.image_mime, plate.image_mime),
          image_sha256 = COALESCE(excluded.image_sha256, plate.image_sha256),

          updated_at = datetime('now')
        """,
        (
            p["number"],
            p["caption"],
            p.get("page"),
            p.get("source_file"),
            p.get("image_path"),
            p.get("image_mime"),
            p.get("image_sha256"),
        ),
    )
    row = con.execute("SELECT id FROM plate WHERE number = ?", (p["number"],)).fetchone()
    return int(row["id"])


def upsert_typology(con, t: dict) -> int:
    con.execute(
        """
        INSERT INTO typology (
          category, sub_category, sub_sub_category, site_feature_type, description, source_file
        )
        VALUES (?, ?, ?, ?, ?, ?)
        ON CONFLICT(category, sub_category, sub_sub_category, site_feature_type) DO UPDATE SET
          description = COALESCE(excluded.description, typology.description),
          source_file = COALESCE(excluded.source_file, typology.source_file),
          updated_at = datetime('now')
        """,
        (
            t["category"],
            t.get("sub_category"),
            t.get("sub_sub_category"),
            t["site_feature_type"],
            t.get("description"),
            t.get("source_file"),
        ),
    )
    row = con.execute(
        """
        SELECT id FROM typology
        WHERE category = ?
          AND COALESCE(sub_category, '') = COALESCE(?, '')
          AND COALESCE(sub_sub_category, '') = COALESCE(?, '')
          AND site_feature_type = ?
        """,
        (t["category"], t.get("sub_category"), t.get("sub_sub_category"), t["site_feature_type"]),
    ).fetchone()
    return int(row["id"])


def upsert_find_item(con, rec: dict) -> int:
    con.execute(
        """
        INSERT INTO find_item (
          object_no, area, site_raw, site_code, context, site_id,
          period, description, source_file
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(object_no) DO UPDATE SET
          area = COALESCE(excluded.area, find_item.area),
          site_raw = COALESCE(excluded.site_raw, find_item.site_raw),
          site_code = COALESCE(excluded.site_code, find_item.site_code),
          context = COALESCE(excluded.context, find_item.context),
          site_id = COALESCE(excluded.site_id, find_item.site_id),
          period = COALESCE(excluded.period, find_item.period),
          description = COALESCE(excluded.description, find_item.description),
          source_file = COALESCE(excluded.source_file, find_item.source_file),
          updated_at = datetime('now')
        """,
        (
            rec["object_no"],
            rec.get("area"),
            rec.get("site_raw"),
            rec.get("site_code"),
            rec.get("context"),
            rec.get("site_id"),
            rec.get("period"),
            rec.get("description"),
            rec.get("source_file"),
        ),
    )
    row = con.execute("SELECT id FROM find_item WHERE object_no = ?", (rec["object_no"],)).fetchone()
    return int(row["id"])

def upsert_pottery_class(con, r: dict) -> int:
    con.execute(
        """
        INSERT INTO pottery_class (
          class_group, class_code, class_name, period, description, origin,
          total_sherds_text, parallels, figures_text, sites_text, source_file
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(class_code) DO UPDATE SET
          class_group = COALESCE(excluded.class_group, pottery_class.class_group),
          class_name = COALESCE(excluded.class_name, pottery_class.class_name),
          period = COALESCE(excluded.period, pottery_class.period),
          description = COALESCE(excluded.description, pottery_class.description),
          origin = COALESCE(excluded.origin, pottery_class.origin),
          total_sherds_text = COALESCE(excluded.total_sherds_text, pottery_class.total_sherds_text),
          parallels = COALESCE(excluded.parallels, pottery_class.parallels),
          figures_text = COALESCE(excluded.figures_text, pottery_class.figures_text),
          sites_text = COALESCE(excluded.sites_text, pottery_class.sites_text),
          source_file = COALESCE(excluded.source_file, pottery_class.source_file),
          updated_at = datetime('now')
        """,
        (
            r.get("class"),
            r["class_code"],
            r["class_name"],
            r.get("period"),
            r.get("description"),
            r.get("origin"),
            r.get("total_sherds"),
            r.get("parallels"),
            r.get("figures"),
            r.get("sites"),
            r.get("source_file"),
        ),
    )
    row = con.execute("SELECT id FROM pottery_class WHERE class_code = ?", (r["class_code"],)).fetchone()
    return int(row["id"])



def load_sites(db_path: Path, sites_jsonl: Path) -> None:
    sites = read_jsonl(sites_jsonl)
    with connect(db_path) as con:
        for s in sites:
            village_id = upsert_village(con, s["village_name"], s.get("village_code_prefix"))
            upsert_site(con, s, village_id)


def load_figures(db_path: Path, figures_jsonl: Path, raw_figures_dir: Path, project_root: Path) -> None:
    figs = read_jsonl(figures_jsonl)
    with connect(db_path) as con:
        for f in figs:
            img_path, img_mime, img_sha = image_info_for("figure", raw_figures_dir, int(f["number"]), project_root)
            f["image_path"] = img_path
            f["image_mime"] = img_mime
            f["image_sha256"] = img_sha
            upsert_figure(con, f)


def load_maps(db_path: Path, maps_jsonl: Path, raw_maps_dir: Path, project_root: Path) -> None:
    maps = read_jsonl(maps_jsonl)
    with connect(db_path) as con:
        for m in maps:
            img_path, img_mime, img_sha = image_info_for("map", raw_maps_dir, int(m["number"]), project_root)
            m["image_path"] = img_path
            m["image_mime"] = img_mime
            m["image_sha256"] = img_sha
            upsert_map(con, m)


def load_plates(db_path: Path, plates_jsonl: Path, raw_plates_dir: Path, project_root: Path) -> None:
    plates = read_jsonl(plates_jsonl)
    with connect(db_path) as con:
        for p in plates:
            img_path, img_mime, img_sha = image_info_for("plate", raw_plates_dir, int(p["number"]), project_root)
            p["image_path"] = img_path
            p["image_mime"] = img_mime
            p["image_sha256"] = img_sha
            upsert_plate(con, p)


def load_site_map_links(db_path: Path, links_jsonl: Path) -> None:
    links = read_jsonl(links_jsonl)
    with connect(db_path) as con:
        for link in links:
            site = con.execute("SELECT id FROM site WHERE code = ?", (link["site_code"],)).fetchone()
            mp = con.execute("SELECT id FROM map WHERE number = ?", (link["map_number"],)).fetchone()
            if not site or not mp:
                continue
            con.execute(
                "INSERT OR IGNORE INTO site_map (site_id, map_id) VALUES (?, ?)",
                (int(site["id"]), int(mp["id"])),
            )


def load_site_plate_links(db_path: Path, links_jsonl: Path) -> None:
    links = read_jsonl(links_jsonl)
    with connect(db_path) as con:
        for link in links:
            site = con.execute("SELECT id FROM site WHERE code = ?", (link["site_code"],)).fetchone()
            pl = con.execute("SELECT id FROM plate WHERE number = ?", (link["plate_number"],)).fetchone()
            if not site or not pl:
                continue
            con.execute(
                "INSERT OR IGNORE INTO site_plate (site_id, plate_id) VALUES (?, ?)",
                (int(site["id"]), int(pl["id"])),
            )
            

def load_site_figure_links(db_path: Path, links_jsonl: Path) -> None:
    links = read_jsonl(links_jsonl)
    with connect(db_path) as con:
        for link in links:
            site = con.execute("SELECT id FROM site WHERE code = ?", (link["site_code"],)).fetchone()
            fig = con.execute("SELECT id FROM figure WHERE number = ?", (link["figure_number"],)).fetchone()
            if not site or not fig:
                # site may not exist yet, or figure not loaded; skip silently for now
                continue
            con.execute(
                """
                INSERT OR IGNORE INTO site_figure (site_id, figure_id)
                VALUES (?, ?)
                """,
                (int(site["id"]), int(fig["id"])),
            )

def _split_sites_field(sites_field: str | None) -> list[str]:
    if not sites_field:
        return []
    # supports "CS.1.1, CS.1.3, CS.14, ..."
    parts = [p.strip() for p in sites_field.split(",")]
    # normalize to uppercase (matches how you store codes elsewhere)
    return [p.upper() for p in parts if p]


def load_typology(db_path: Path, typology_jsonl: Path) -> None:
    records = read_jsonl(typology_jsonl)
    with connect(db_path) as con:
        for t in records:
            # annotate provenance if absent
            t.setdefault("source_file", typology_jsonl.name)

            typology_id = upsert_typology(con, t)

            for site_code in _split_sites_field(t.get("sites")):
                site = con.execute("SELECT id FROM site WHERE code = ?", (site_code,)).fetchone()
                if not site:
                    # site missing in DB (maybe not parsed yet) -> skip for now
                    continue
                con.execute(
                    "INSERT OR IGNORE INTO site_typology (site_id, typology_id) VALUES (?, ?)",
                    (int(site["id"]), typology_id),
                )
                
def load_finds(db_path: Path, finds_jsonl: Path) -> None:
    records = read_jsonl(finds_jsonl)
    with connect(db_path) as con:
        for r in records:
            object_no = r.get("object_no")
            if not object_no:
                continue

            site_raw = r.get("site")
            site_code, context = parse_site_field(site_raw)

            site_id = None
            if site_code:
                row = con.execute("SELECT id FROM site WHERE code = ?", (site_code,)).fetchone()
                if row:
                    site_id = int(row["id"])

            rec = {
                "object_no": object_no,
                "area": r.get("area"),
                "site_raw": site_raw,
                "site_code": site_code,
                "context": context,
                "site_id": site_id,
                "period": r.get("period"),
                "description": r.get("description"),
                "source_file": finds_jsonl.name,
            }

            find_id = upsert_find_item(con, rec)

            # Link to plates (if referenced in description)
            for plate_number in extract_plate_numbers(r.get("description")):
                plate_row = con.execute(
                    "SELECT id FROM plate WHERE number = ?",
                    (plate_number,),
                ).fetchone()
                plate_id = int(plate_row["id"]) if plate_row else None

                con.execute(
                    """
                    INSERT OR IGNORE INTO find_plate (find_id, plate_number, plate_id)
                    VALUES (?, ?, ?)
                    """,
                    (find_id, plate_number, plate_id),
                )


def load_classification(db_path: Path, classification_jsonl: Path) -> None:
    records = read_jsonl(classification_jsonl)
    with connect(db_path) as con:
        for r in records:
            if not r.get("class_code") or not r.get("class_name"):
                continue

            r.setdefault("source_file", classification_jsonl.name)
            class_id = upsert_pottery_class(con, r)

            # 1) Site occurrences
            for entry in split_site_entries(r.get("sites")):
                site_code, sherd_count, evidence, sample_codes_text = parse_site_entry(entry)
                if not site_code:
                    continue

                site_row = con.execute("SELECT id FROM site WHERE code = ?", (site_code,)).fetchone()
                site_id = int(site_row["id"]) if site_row else None

                con.execute(
                    """
                    INSERT INTO pottery_class_site (
                      pottery_class_id, site_id, site_code, sherd_count, evidence_text, sample_codes_text
                    )
                    VALUES (?, ?, ?, ?, ?, ?)
                    ON CONFLICT(pottery_class_id, site_code) DO UPDATE SET
                      site_id = COALESCE(excluded.site_id, pottery_class_site.site_id),
                      sherd_count = COALESCE(excluded.sherd_count, pottery_class_site.sherd_count),
                      evidence_text = COALESCE(excluded.evidence_text, pottery_class_site.evidence_text),
                      sample_codes_text = COALESCE(excluded.sample_codes_text, pottery_class_site.sample_codes_text)
                    """,
                    (class_id, site_id, site_code, sherd_count, evidence, sample_codes_text),
                )

            # 2) Plate links (from figures field "Plates: ...")
            for plate_number in extract_plate_numbers_from_figures_field(r.get("figures")):
                plate_row = con.execute("SELECT id FROM plate WHERE number = ?", (plate_number,)).fetchone()
                plate_id = int(plate_row["id"]) if plate_row else None

                con.execute(
                    """
                    INSERT OR IGNORE INTO pottery_class_plate (pottery_class_id, plate_number, plate_id)
                    VALUES (?, ?, ?)
                    """,
                    (class_id, plate_number, plate_id),
                )

            # 3) Figure references (roman numerals + ints)
            for label in extract_figure_labels(r.get("figures")):
                fig_num = int(label) if label.isdigit() else None
                fig_id = None
                if fig_num is not None:
                    row = con.execute("SELECT id FROM figure WHERE number = ?", (fig_num,)).fetchone()
                    fig_id = int(row["id"]) if row else None

                con.execute(
                    """
                    INSERT OR IGNORE INTO pottery_class_figure_ref
                      (pottery_class_id, figure_label, figure_number, figure_id)
                    VALUES (?, ?, ?, ?)
                    """,
                    (class_id, label, fig_num, fig_id),
                )
