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
