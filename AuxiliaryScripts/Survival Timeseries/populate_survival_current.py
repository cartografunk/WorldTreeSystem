# scripts/populate_survival_current.py

from core.db import get_engine
from sqlalchemy.sql import text

engine = get_engine()

SQL = """
TRUNCATE masterdatabase.survival_current;

INSERT INTO masterdatabase.survival_current (
    contract_code,
    current_survival_pct,
    current_surviving_trees,
    survival_metric_source
)
SELECT DISTINCT ON (s.contract_code)
    s.contract_code,
    s.survival_pct::NUMERIC,
    s.survival_count::INTEGER,
    s.survival_metric_source
FROM masterdatabase.survival_timeseries s
JOIN public.cat_survival_fields c
    ON TRIM(s.survival_metric_source) = TRIM(c.survival_metric_source)
WHERE s.survival_pct IS NOT NULL
  AND s.survival_count IS NOT NULL
ORDER BY s.contract_code, c.priority DESC;
"""

with engine.connect() as conn:
    conn.execute(text(SQL))
    print("✅ Tabla poblada: masterdatabase.survival_current")
