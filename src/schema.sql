PRAGMA foreign_keys = ON;

CREATE TABLE IF NOT EXISTS village (
  id           INTEGER PRIMARY KEY,
  name         TEXT NOT NULL,
  code_prefix  TEXT,
  notes        TEXT,
  created_at   TEXT NOT NULL DEFAULT (datetime('now')),
  updated_at   TEXT NOT NULL DEFAULT (datetime('now')),
  CONSTRAINT uq_village_name UNIQUE (name),
  CONSTRAINT uq_village_code_prefix UNIQUE (code_prefix)
);

CREATE TABLE IF NOT EXISTS site (
  id                 INTEGER PRIMARY KEY,
  code               TEXT NOT NULL,
  village_id         INTEGER NOT NULL,
  name               TEXT,
  coordinate_system  TEXT,
  utm_easting        INTEGER,
  utm_northing       INTEGER,
  area_ha            REAL,
  structures_text    TEXT,
  finds_text         TEXT,
  notes              TEXT,
  source_file        TEXT,
  source_locator     TEXT,
  created_at         TEXT NOT NULL DEFAULT (datetime('now')),
  updated_at         TEXT NOT NULL DEFAULT (datetime('now')),
  CONSTRAINT uq_site_code UNIQUE (code),
  CONSTRAINT fk_site_village FOREIGN KEY (village_id)
    REFERENCES village(id)
    ON UPDATE CASCADE
    ON DELETE RESTRICT
);

CREATE INDEX IF NOT EXISTS ix_site_village_id ON site(village_id);
CREATE INDEX IF NOT EXISTS ix_site_code ON site(code);

CREATE TABLE IF NOT EXISTS figure (
  id          INTEGER PRIMARY KEY,
  number      INTEGER NOT NULL,
  caption     TEXT NOT NULL,
  page        INTEGER,
  source_file TEXT,

  image_path   TEXT,
  image_mime   TEXT,
  image_sha256 TEXT,

  created_at  TEXT NOT NULL DEFAULT (datetime('now')),
  updated_at  TEXT NOT NULL DEFAULT (datetime('now')),
  CONSTRAINT uq_figure_number UNIQUE (number)
);

CREATE TABLE IF NOT EXISTS site_figure (
  site_id   INTEGER NOT NULL,
  figure_id INTEGER NOT NULL,
  PRIMARY KEY (site_id, figure_id),
  CONSTRAINT fk_sf_site FOREIGN KEY (site_id)
    REFERENCES site(id)
    ON UPDATE CASCADE
    ON DELETE CASCADE,
  CONSTRAINT fk_sf_figure FOREIGN KEY (figure_id)
    REFERENCES figure(id)
    ON UPDATE CASCADE
    ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS ix_site_figure_figure_id ON site_figure(figure_id);

CREATE TABLE IF NOT EXISTS map (
  id          INTEGER PRIMARY KEY,
  number      INTEGER NOT NULL,
  caption     TEXT NOT NULL,
  page        INTEGER,
  source_file TEXT,

  image_path   TEXT,
  image_mime   TEXT,
  image_sha256 TEXT,

  created_at  TEXT NOT NULL DEFAULT (datetime('now')),
  updated_at  TEXT NOT NULL DEFAULT (datetime('now')),
  CONSTRAINT uq_map_number UNIQUE (number)
);

CREATE TABLE IF NOT EXISTS site_map (
  site_id INTEGER NOT NULL,
  map_id  INTEGER NOT NULL,
  PRIMARY KEY (site_id, map_id),
  CONSTRAINT fk_sm_site FOREIGN KEY (site_id)
    REFERENCES site(id)
    ON UPDATE CASCADE
    ON DELETE CASCADE,
  CONSTRAINT fk_sm_map FOREIGN KEY (map_id)
    REFERENCES map(id)
    ON UPDATE CASCADE
    ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS ix_site_map_map_id ON site_map(map_id);

CREATE TABLE IF NOT EXISTS plate (
  id          INTEGER PRIMARY KEY,
  number      INTEGER NOT NULL,
  caption     TEXT NOT NULL,
  page        INTEGER,
  source_file TEXT,

  image_path   TEXT,
  image_mime   TEXT,
  image_sha256 TEXT,

  created_at  TEXT NOT NULL DEFAULT (datetime('now')),
  updated_at  TEXT NOT NULL DEFAULT (datetime('now')),
  CONSTRAINT uq_plate_number UNIQUE (number)
);

CREATE TABLE IF NOT EXISTS site_plate (
  site_id  INTEGER NOT NULL,
  plate_id INTEGER NOT NULL,
  PRIMARY KEY (site_id, plate_id),
  CONSTRAINT fk_sp_site FOREIGN KEY (site_id)
    REFERENCES site(id)
    ON UPDATE CASCADE
    ON DELETE CASCADE,
  CONSTRAINT fk_sp_plate FOREIGN KEY (plate_id)
    REFERENCES plate(id)
    ON UPDATE CASCADE
    ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS ix_site_plate_plate_id ON site_plate(plate_id);

CREATE TABLE IF NOT EXISTS typology (
  id                INTEGER PRIMARY KEY,
  category          TEXT NOT NULL,
  sub_category      TEXT,
  sub_sub_category  TEXT,
  site_feature_type TEXT NOT NULL,   -- z.B. "Beehive Tomb"
  description       TEXT,
  source_file       TEXT,

  created_at        TEXT NOT NULL DEFAULT (datetime('now')),
  updated_at        TEXT NOT NULL DEFAULT (datetime('now')),

  CONSTRAINT uq_typology_key UNIQUE (category, sub_category, sub_sub_category, site_feature_type)
);

CREATE INDEX IF NOT EXISTS ix_typology_feature_type ON typology(site_feature_type);

CREATE TABLE IF NOT EXISTS site_typology (
  site_id     INTEGER NOT NULL,
  typology_id INTEGER NOT NULL,

  PRIMARY KEY (site_id, typology_id),

  CONSTRAINT fk_st_site FOREIGN KEY (site_id)
    REFERENCES site(id)
    ON UPDATE CASCADE
    ON DELETE CASCADE,

  CONSTRAINT fk_st_typology FOREIGN KEY (typology_id)
    REFERENCES typology(id)
    ON UPDATE CASCADE
    ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS ix_site_typology_typology_id ON site_typology(typology_id);

-- Individual finds (e.g. W.D.001)
CREATE TABLE IF NOT EXISTS find_item (
  id          INTEGER PRIMARY KEY,

  object_no   TEXT NOT NULL,     -- e.g. "W.D.001"
  area        TEXT,              -- e.g. "al-Fulayj"
  site_raw    TEXT,              -- original "site" field from JSONL (may include "Tomb 8")
  site_code   TEXT,              -- extracted CS.* code (e.g. "CS.4.13")
  context     TEXT,              -- e.g. "Tomb 8" (best-effort)
  site_id     INTEGER,           -- optional FK to site table

  period      TEXT,
  description TEXT,

  source_file TEXT,

  created_at  TEXT NOT NULL DEFAULT (datetime('now')),
  updated_at  TEXT NOT NULL DEFAULT (datetime('now')),

  CONSTRAINT uq_find_object_no UNIQUE (object_no),
  CONSTRAINT fk_find_site FOREIGN KEY (site_id)
    REFERENCES site(id)
    ON UPDATE CASCADE
    ON DELETE SET NULL
);

CREATE INDEX IF NOT EXISTS ix_find_site_code ON find_item(site_code);
CREATE INDEX IF NOT EXISTS ix_find_site_id ON find_item(site_id);
CREATE INDEX IF NOT EXISTS ix_find_period ON find_item(period);

-- Link finds to plates (find can reference multiple plates)
CREATE TABLE IF NOT EXISTS find_plate (
  find_id      INTEGER NOT NULL,
  plate_number INTEGER NOT NULL,  -- store even if plate record missing
  plate_id     INTEGER,           -- nullable FK to plate

  PRIMARY KEY (find_id, plate_number),

  CONSTRAINT fk_fp_find FOREIGN KEY (find_id)
    REFERENCES find_item(id)
    ON UPDATE CASCADE
    ON DELETE CASCADE,

  CONSTRAINT fk_fp_plate FOREIGN KEY (plate_id)
    REFERENCES plate(id)
    ON UPDATE CASCADE
    ON DELETE SET NULL
);

CREATE INDEX IF NOT EXISTS ix_find_plate_plate_id ON find_plate(plate_id);
