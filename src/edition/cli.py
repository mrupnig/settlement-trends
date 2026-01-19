from __future__ import annotations

import argparse
from pathlib import Path

from .clean import clean_dir
from .db import init_db
from .parse_gazetteer import parse_gazetteer, write_jsonl as write_jsonl_sites
from .parse_refs import parse_numbered_list, write_jsonl as write_jsonl_refs
from .load_db import (
    load_sites,
    load_figures,
    load_site_figure_links,
    load_maps,
    load_site_map_links,
    load_plates,
    load_site_plate_links,
    load_typology,
    load_finds,
    load_classification,
)



PROJECT_ROOT = Path(__file__).resolve().parents[2]
DATA_DIR = PROJECT_ROOT / "data"
RAW_DIR = DATA_DIR / "raw"
CLEANED_DIR = DATA_DIR / "cleaned"
INTERIM_DIR = DATA_DIR / "interim"
DB_PATH = DATA_DIR / "db" / "edition.sqlite"
SCHEMA_PATH = PROJECT_ROOT / "src" / "schema.sql"
RAW_FIGURES_DIR = RAW_DIR / "figures"
RAW_MAPS_DIR = RAW_DIR / "maps"
RAW_PLATES_DIR = RAW_DIR / "plates"


def cmd_clean() -> None:
    produced = clean_dir(RAW_DIR, CLEANED_DIR)
    print(f"Cleaned {len(produced)} files into {CLEANED_DIR}")


def cmd_init_db() -> None:
    init_db(DB_PATH, SCHEMA_PATH)
    print(f"Initialized DB: {DB_PATH}")


def cmd_parse_refs() -> None:
    # Figures
    fig_path = CLEANED_DIR / "list_of_figures.cleaned.txt"
    figures, fig_links = parse_numbered_list(fig_path, "Figure")
    write_jsonl_refs(figures, INTERIM_DIR / "figures.jsonl")
    write_jsonl_refs(fig_links, INTERIM_DIR / "site_figure.jsonl")
    print(f"Parsed figures: {len(figures)} | links: {len(fig_links)}")

    # Maps
    map_path = CLEANED_DIR / "list_of_maps.cleaned.txt"
    maps, map_links = parse_numbered_list(map_path, "Map")
    write_jsonl_refs(maps, INTERIM_DIR / "maps.jsonl")
    write_jsonl_refs(map_links, INTERIM_DIR / "site_map.jsonl")
    print(f"Parsed maps: {len(maps)} | links: {len(map_links)}")

    # Plates
    plate_path = CLEANED_DIR / "list_of_plates.cleaned.txt"
    plates, plate_links = parse_numbered_list(plate_path, "Plate")
    write_jsonl_refs(plates, INTERIM_DIR / "plates.jsonl")
    write_jsonl_refs(plate_links, INTERIM_DIR / "site_plate.jsonl")
    print(f"Parsed plates: {len(plates)} | links: {len(plate_links)}")



def cmd_parse_gazetteer() -> None:
    gaz_path = CLEANED_DIR / "appendix_a_sites_gazetteer.cleaned.txt"
    sites = parse_gazetteer(gaz_path)

    write_jsonl_sites(sites, INTERIM_DIR / "sites.jsonl")
    print(f"Parsed sites: {len(sites)}")

def cmd_load() -> None:
    load_sites(DB_PATH, INTERIM_DIR / "sites.jsonl")

    load_figures(DB_PATH, INTERIM_DIR / "figures.jsonl", RAW_FIGURES_DIR, PROJECT_ROOT,)
    load_maps(DB_PATH, INTERIM_DIR / "maps.jsonl", RAW_MAPS_DIR, PROJECT_ROOT,)
    load_plates(DB_PATH, INTERIM_DIR / "plates.jsonl", RAW_PLATES_DIR, PROJECT_ROOT,)

    load_site_figure_links(DB_PATH, INTERIM_DIR / "site_figure.jsonl")
    load_site_map_links(DB_PATH, INTERIM_DIR / "site_map.jsonl")
    load_site_plate_links(DB_PATH, INTERIM_DIR / "site_plate.jsonl")
    load_typology(DB_PATH, INTERIM_DIR / "typology.jsonl")
    load_finds(DB_PATH, INTERIM_DIR / "finds.jsonl")
    load_classification(DB_PATH, INTERIM_DIR / "classification.jsonl")


    print("Loaded interim JSONL into SQLite (including image metadata).")


def main() -> None:
    parser = argparse.ArgumentParser(prog="edition")
    sub = parser.add_subparsers(dest="cmd", required=True)

    sub.add_parser("clean")
    sub.add_parser("init-db")
    sub.add_parser("parse-refs")
    sub.add_parser("parse-gazetteer")
    sub.add_parser("load")
    sub.add_parser("all")

    args = parser.parse_args()

    if args.cmd == "clean":
        cmd_clean()
    elif args.cmd == "init-db":
        cmd_init_db()
    elif args.cmd == "parse-refs":
        cmd_parse_refs()
    elif args.cmd == "parse-gazetteer":
        cmd_parse_gazetteer()
    elif args.cmd == "load":
        cmd_load()
    elif args.cmd == "all":
        cmd_clean()
        cmd_init_db()
        cmd_parse_refs()
        cmd_parse_gazetteer()
        cmd_load()
    else:
        raise SystemExit(f"Unknown cmd: {args.cmd}")
