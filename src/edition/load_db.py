from __future__ import annotations

import json
from pathlib import Path
import hashlib

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
