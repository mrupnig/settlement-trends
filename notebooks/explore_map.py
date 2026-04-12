import sqlite3
import pandas as pd
from pathlib import Path

database_path = Path("../data/db/edition.sqlite")

q = """
SELECT
  s.id AS site_id,
  s.code AS site_code,
  s.name AS site_name,
  v.name AS village_name,
  c.latitude,
  c.longitude
FROM coords c
JOIN site s ON s.id = c.site_id
LEFT JOIN village v ON v.id = s.village_id
WHERE c.latitude IS NOT NULL
  AND c.longitude IS NOT NULL
  AND c.latitude BETWEEN -90 AND 90
  AND c.longitude BETWEEN -180 AND 180;
"""

with sqlite3.connect(database_path) as conn:
    df = pd.read_sql_query(q, conn)

df.head()