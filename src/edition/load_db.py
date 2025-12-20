from __future__ import annotations

import json
from pathlib import Path

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
        INSERT INTO figure (number, caption, page, source_file)
        VALUES (?, ?, ?, ?)
        ON CONFLICT(number) DO UPDATE SET
          caption = excluded.caption,
          page = COALESCE(excluded.page, figure.page),
          source_file = COALESCE(excluded.source_file, figure.source_file),
          updated_at = datetime('now')
        """,
        (fig["number"], fig["caption"], fig.get("page"), fig.get("source_file")),
    )
    row = con.execute("SELECT id FROM figure WHERE number = ?", (fig["number"],)).fetchone()
    return int(row["id"])

def upsert_map(con, m: dict) -> int:
    con.execute(
        """
        INSERT INTO map (number, caption, page, source_file)
        VALUES (?, ?, ?, ?)
        ON CONFLICT(number) DO UPDATE SET
          caption = excluded.caption,
          page = COALESCE(excluded.page, map.page),
          source_file = COALESCE(excluded.source_file, map.source_file),
          updated_at = datetime('now')
        """,
        (m["number"], m["caption"], m.get("page"), m.get("source_file")),
    )
    row = con.execute("SELECT id FROM map WHERE number = ?", (m["number"],)).fetchone()
    return int(row["id"])


def upsert_plate(con, p: dict) -> int:
    con.execute(
        """
        INSERT INTO plate (number, caption, page, source_file)
        VALUES (?, ?, ?, ?)
        ON CONFLICT(number) DO UPDATE SET
          caption = excluded.caption,
          page = COALESCE(excluded.page, plate.page),
          source_file = COALESCE(excluded.source_file, plate.source_file),
          updated_at = datetime('now')
        """,
        (p["number"], p["caption"], p.get("page"), p.get("source_file")),
    )
    row = con.execute("SELECT id FROM plate WHERE number = ?", (p["number"],)).fetchone()
    return int(row["id"])



def load_sites(db_path: Path, sites_jsonl: Path) -> None:
    sites = read_jsonl(sites_jsonl)
    with connect(db_path) as con:
        for s in sites:
            village_id = upsert_village(con, s["village_name"], s.get("village_code_prefix"))
            upsert_site(con, s, village_id)


def load_figures(db_path: Path, figures_jsonl: Path) -> None:
    figs = read_jsonl(figures_jsonl)
    with connect(db_path) as con:
        for f in figs:
            upsert_figure(con, f)

def load_maps(db_path: Path, maps_jsonl: Path) -> None:
    maps = read_jsonl(maps_jsonl)
    with connect(db_path) as con:
        for m in maps:
            upsert_map(con, m)


def load_plates(db_path: Path, plates_jsonl: Path) -> None:
    plates = read_jsonl(plates_jsonl)
    with connect(db_path) as con:
        for p in plates:
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
