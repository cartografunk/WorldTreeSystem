from Cruises.utils.cleaners import clean_cruise_dataframe, remove_blank_rows, standardize_units
from Cruises.filldown import forward_fill_headers
from Cruises.catalog_normalizer import normalize_catalogs
from core.doyle_calculator import calculate_doyle
from Cruises.dead_alive_calculator import calculate_dead_alive
from Cruises.dead_tree_imputer import add_imputed_dead_rows
from Cruises.tree_id import split_by_id_validity

def process_inventory_dataframe(df, engine, country_code):
    df = clean_cruise_dataframe(df)
    df = remove_blank_rows(df)
    df = forward_fill_headers(df)
    if "sheet_used" in df.columns and country_code in {"CR", "MX", "GT"}:
        input2_mask = df["sheet_used"].str.lower().eq("input (2)")
        if input2_mask.any():
            print(f"🧪 Saltando normalización de unidades para {input2_mask.sum()} filas de Input (2)")
            df.loc[~input2_mask, :] = standardize_units(df.loc[~input2_mask, :])
        else:
            df = standardize_units(df)
    else:
        df = standardize_units(df)

    df = calculate_dead_alive(df, engine)

    df = normalize_catalogs(
        df,
        engine,
        country_code
    )

    df = calculate_doyle(df)

    df = add_imputed_dead_rows(df, contract_col="contractcode", plot_col="plot", dead_col="dead_tree")

    # Eliminar columnas temporales
    df = df.drop(columns=["Status", "status_id", "status_text_raw"], errors="ignore")

    df_good, df_bad = split_by_id_validity(df)
    return df_good, df_bad
