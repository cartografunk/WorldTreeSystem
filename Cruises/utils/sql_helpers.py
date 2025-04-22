# utils/sql_helpers.py
from sqlalchemy import Text, Float, Numeric, BigInteger
from utils.libs import pd

# Mapeo de renombrado para que el DataFrame encaje con el esquema SQL final
RENAMING = {
    "contractcode":            "Contract Code",
    "contract_code":           "Contract Code",
    "Stand#":                  "Stand#",
    "Plot#":                   "Plot#",
    "PlotCoordinate":          "PlotCoordinate",
    # Captura 'Tree #' (limpio) y 'tree'
    "Tree#":                   "Tree#",
    "Tree #":                  "Tree#",
    "tree":                    "Tree#",
    "tree_number":             "Tree#",
    "Defect HT (ft)":          "Defect HT(ft)",
    "DBH (in)":                "DBH (in)",
    "THT (ft)":                "THT (ft)",
    "Merch. HT (ft)":          "Merch. HT (ft)",
    "Short Note":              "Short Note",
    "status_id":               "status_id",
    "species_id":              "cat_species_id",
    "defect_id":               "cat_defect_id",
    "pests_id":                "cat_pest_id",
    "coppiced_id":             "cat_coppiced_id",
    "permanent_plot_id":       "cat_permanent_plot_id",
    "disease_id":              "cat_disease_id",
    "doyle_bf":                "doyle_bf",
    "dead_tree":               "dead_tree",
    "alive_tree":              "alive_tree",
    # Columnas adicionales para auditoría y metadatos
    "farmername":              "FarmerName",
    "cruisedate":              "CruiseDate",
}

# Orden final de columnas en la tabla SQL (idéntico a inventory_us_2025)
FINAL_ORDER = [
    "Contract Code",
    "Stand#",
    "Plot#",
    "PlotCoordinate",
    "Tree#",
    "Defect HT(ft)",
    "DBH (in)",
    "THT (ft)",
    "Merch. HT (ft)",
    "Short Note",
    "status_id",
    "cat_species_id",
    "cat_defect_id",
    "cat_pest_id",
    "cat_coppiced_id",
    "cat_permanent_plot_id",
    "doyle_bf",
    "cat_disease_id",
    "dead_tree",
    "alive_tree",
]

# Tipos explícitos para cada columna en la base de datos
DTYPES = {
    "Contract Code":       Text(),
    "Stand#":              Float(),
    "Plot#":               Float(),
    "PlotCoordinate":      Text(),
    "Tree#":               Float(),
    "Defect HT(ft)":       Numeric(),
    "DBH (in)":            Numeric(),
    "THT (ft)":            Numeric(),
    "Merch. HT (ft)":      Numeric(),
    "Short Note":          Text(),
    "status_id":           BigInteger(),
    "cat_species_id":      BigInteger(),
    "cat_defect_id":       BigInteger(),
    "cat_pest_id":         BigInteger(),
    "cat_coppiced_id":     BigInteger(),
    "cat_permanent_plot_id": BigInteger(),
    "doyle_bf":            Numeric(),
    "cat_disease_id":      BigInteger(),
    "dead_tree":           Float(),
    "alive_tree":          Float(),
}

def prepare_df_for_sql(df):
    """
    Renombra, reordena y convierte el DataFrame para encajar con el esquema SQL final.
    Devuelve: (df_preparado, dtype_dict) para to_sql.
    """
    # Renombrado de columnas
    df2 = df.rename(columns=RENAMING)
    # Filtrar y reordenar
    cols = [c for c in FINAL_ORDER if c in df2.columns]
    df2 = df2[cols].copy()

    # Convertir columnas enteras (BigInteger)
    int_cols = [c for c, dtype in DTYPES.items() if isinstance(dtype, BigInteger) and c in df2.columns]
    for col in int_cols:
        df2[col] = (
            df2[col].astype(str)
                .str.extract(r"(\d+)", expand=False)
                .pipe(pd.to_numeric, errors='coerce')
                .fillna(0)
                .astype(int)
        )

    # Convertir columnas numéricas (Float y Numeric)
    num_cols = [c for c, dtype in DTYPES.items()
                if (isinstance(dtype, (Float, Numeric)) or dtype.__class__.__name__=='Numeric') and c in df2.columns]
    for col in num_cols:
        df2[col] = pd.to_numeric(df2[col], errors='coerce')

    # Preparar dict de tipos para to_sql
    dtype_for_sql = {c: DTYPES[c] for c in cols if c in DTYPES}
    return df2, dtype_for_sql