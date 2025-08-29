from core.libs import pd
from sqlalchemy import text

SQL = """
WITH imc_latest AS (
  SELECT
    imc.*,
    ROW_NUMBER() OVER (
      PARTITION BY imc.contract_code
      ORDER BY imc.inventory_date DESC NULLS LAST,
               imc.inventory_year DESC NULLS LAST
    ) AS rn_imc
  FROM masterdatabase.inventory_metrics_current imc
),
imc1 AS (
  SELECT * FROM imc_latest WHERE rn_imc = 1
)
SELECT
  cc.contract_code                                        AS contract_code,
  fpi.farmer_number                                       AS farmer_number,
  fpi.contract_name                                       AS contract_name,
  fpi.representative                                      AS representative,

  /* Campos que hoy no existen en tus schemas → NULL explícito */
  NULL::text                                              AS status,
  imc1.inventory_year::INT                                AS etp_year,
  NULL::bigint                                            AS planted,
  NULL::text                                              AS planting_obligation,
  imc1.planting_year::INT                                 AS planting_year,
  NULL::text                                              AS region,
  NULL::text                                              AS species,
  NULL::text                                              AS strain,
  NULL::bigint                                            AS trees_contract,
  NULL::date                                              AS planting_date,

  /* Métricas disponibles en IMC */
  imc1.inventory_date                                     AS inventory_date,
  imc1.survival                                           AS current_survival,
  NULL::bigint                                            AS surviving_trees,
  NULL::double precision                                  AS mean_dbh,
  imc1.tht_mean                                           AS mean_height
FROM masterdatabase.farmer_personal_information  fpi
LEFT JOIN LATERAL unnest(fpi.contract_codes) AS cc(contract_code) ON TRUE
LEFT JOIN imc1
  ON imc1.contract_code = cc.contract_code
/* 👇 Activo = el contrato aparece en CTI o tiene métricas en IMC */
LEFT JOIN masterdatabase.contract_tree_information cti
  ON cti.contract_code = cc.contract_code
WHERE
  cc.contract_code IS NOT NULL
  AND (cti.contract_code IS NOT NULL OR imc1.contract_code IS NOT NULL)
ORDER BY cc.contract_code;
"""

COLUMNS_ORDER = [
  "contract_code","farmer_number","contract_name","representative","status",
  "etp_year","planted","planting_obligation","planting_year","region",
  "species","strain","trees_contract","planting_date","inventory_date",
  "current_survival","surviving_trees","mean_dbh","mean_height",
]

def fetch_active_contracts_query_df(engine) -> pd.DataFrame:
    df = pd.read_sql(text(SQL), engine)
    cols = [c for c in COLUMNS_ORDER if c in df.columns]
    return df.reindex(columns=cols)
