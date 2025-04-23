# utils/sql_helpers.py
from sqlalchemy import Text, Float, Numeric, SmallInteger, Date
from utils.libs import pd
from utils.column_mapper import SQL_COLUMNS   # ← importa el mapeo que definiste

# Orden final de columnas en la tabla SQL (idéntico a inventory_us_2025)
FINAL_ORDER = [
    SQL_COLUMNS["contractcode"],
    SQL_COLUMNS["farmername"],
    SQL_COLUMNS["cruisedate"],
    SQL_COLUMNS["id"],

    SQL_COLUMNS["stand"],
    SQL_COLUMNS["plot"],
    SQL_COLUMNS["plot_coordinate"],
    SQL_COLUMNS["tree_number"],

    SQL_COLUMNS["defect_ht_ft"],
    SQL_COLUMNS["dbh_in"],
    SQL_COLUMNS["tht_ft"],
    SQL_COLUMNS["merch_ht_ft"],

    SQL_COLUMNS["short_note"],

    SQL_COLUMNS["status_id"],
    SQL_COLUMNS["species_id"],
    SQL_COLUMNS["defect_id"],
    SQL_COLUMNS["pests_id"],
    SQL_COLUMNS["coppiced_id"],
    SQL_COLUMNS["permanent_plot_id"],
    SQL_COLUMNS["disease_id"],

    SQL_COLUMNS["doyle_bf"],
    SQL_COLUMNS["dead_tree"],
    SQL_COLUMNS["alive_tree"],
]

# Tipos explícitos para cada columna en la base de datos
DTYPES = {
    "Contract Code":       Text(),
    "FarmerName":          Text(),
    "CruiseDate":          Date(),
    "Stand#":              Float(),
    "Plot#":               Float(),
    "PlotCoordinate":      Text(),
    "Tree#":               Float(),
    "Defect HT(ft)":       Numeric(),
    "DBH (in)":            Numeric(),
    "THT (ft)":            Numeric(),
    "Merch. HT (ft)":      Numeric(),
    "Short Note":          Text(),
    "status_id":           SmallInteger(),
    "cat_species_id":      SmallInteger(),
    "cat_defect_id":       SmallInteger(),
    "cat_pest_id":         SmallInteger(),
    "cat_coppiced_id":     SmallInteger(),
    "cat_permanent_plot_id": SmallInteger(),
    "doyle_bf":            Numeric(),
    "cat_disease_id":      SmallInteger(),
    "dead_tree":           Float(),
    "alive_tree":          Float(),
}


def prepare_df_for_sql(df):
    # 1) Renombrar
    df2 = df.rename(columns=SQL_COLUMNS)
    # 2) Quitar duplicados
    df2 = df2.loc[:, ~df2.columns.duplicated()]
    # 3) Filtrar y reordenar
    from utils.sql_helpers import FINAL_ORDER
    cols = [c for c in FINAL_ORDER if c in df2.columns]
    df2 = df2[cols].copy()

    # 4️⃣ Conversión de tipos en base a DTYPES
    from sqlalchemy import Float, Numeric, SmallInteger

    # Enteros pequeños
    int_cols = [c for c, dtype in DTYPES.items()
                if isinstance(dtype, SmallInteger) and c in df2.columns]
    for col in int_cols:
        df2[col] = (
            df2[col].astype(str)
            .str.extract(r"(\d+)", expand=False)
            .pipe(pd.to_numeric, errors="coerce")
            .fillna(0)
            .astype(int)
        )

    # Flotantes / numéricos
    num_cols = [c for c, dtype in DTYPES.items()
                if isinstance(dtype, (Float, Numeric)) and c in df2.columns]
    for col in num_cols:
        df2[col] = pd.to_numeric(df2[col], errors="coerce")

    # 5️⃣ Prepara dtype_for_sql a partir de DTYPES
    dtype_for_sql = {c: DTYPES[c] for c in df2.columns if c in DTYPES}

    return df2, dtype_for_sql