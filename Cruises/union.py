# union.py
from utils.libs import os, pd, warnings
from utils.extractors import extract_metadata_from_excel
from utils.cleaners   import clean_column_name, get_column

warnings.filterwarnings(
    "ignore",
    message="Data Validation extension is not supported and will be removed",
    category=UserWarning,
    module="openpyxl",
)
warnings.filterwarnings(
    "ignore",
    message="Conditional Formatting extension is not supported and will be removed",
    category=UserWarning,
    module="openpyxl",
)


# ────────────────────────────────
#  Hoja INPUT → DataFrame limpio
# ────────────────────────────────
def read_input_sheet(file_path: str) -> pd.DataFrame | None:
    """
    Lee la hoja 'Input' (o 'DataInput') y renombra columnas
    únicamente con COLUMN_LOOKUP + get_column.
    """
    try:
        xls     = pd.ExcelFile(file_path)
        target  = next(
            (s for s in xls.sheet_names if s.lower().strip() in ("input", "datainput")),
            xls.sheet_names[0],
        )

        with warnings.catch_warnings():
            warnings.simplefilter("ignore", UserWarning)
            df = pd.read_excel(
                file_path,
                sheet_name=target,
                engine="openpyxl",
                dtype=str,
                na_filter=False,
            )

        print(f"\n=== DEBUG {os.path.basename(file_path)} ===")
        print("Columnas originales:", df.columns.tolist())

        # Limpieza bruta
        df.columns = [clean_column_name(c) for c in df.columns]
        print("Columnas limpiadas:", df.columns.tolist())

        # Campos lógicos → nombres internos
        campos = {
            "Tree #":            "tree_number",
            "Stand #":           "stand",
            "Plot #":            "plot",
            "Plot Coordinate":   "plot_coordinate",
            "Status":            "status_id",
            "Species":           "species_id",
            "Defect":            "defect_id",
            "Defect HT (ft)":    "defect_ht_ft",
            "DBH (in)":          "dbh_in",
            "THT (ft)":          "tht_ft",
            "Merch. HT (ft)":    "merch_ht_ft",
            "Pests":             "pests_id",
            "Disease":           "disease_id",
            "Coppiced":          "coppiced_id",
            "Permanent Plot":    "permanent_plot_id",
            "Short Note":        "short_note",
        }

        rename_dict = {}
        for logical, internal in campos.items():
            try:
                real = get_column(df, logical)          # usa COLUMN_LOOKUP
                rename_dict[real] = internal
            except KeyError:
                print(f"   ⚠️  '{logical}' no presente")

        df = df.rename(columns=rename_dict)
        print("Columnas tras renombrado:", df.columns.tolist())
        return df

    except Exception as e:
        print(f"[ERROR] {file_path}: {e}")
        return None


# ────────────────────────────────
#  Recorre carpeta y concatena
# ────────────────────────────────
def combine_files(base_path: str, filter_func=None) -> pd.DataFrame | None:
    """
    Recorre todos los .xlsx, inyecta metadata y concatena.
    """
    df_list = []

    for root, _, files in os.walk(base_path):
        for file in files:
            if file.lower().startswith("~$") or "combined_inventory" in file.lower():
                continue
            if not file.lower().endswith(".xlsx"):
                continue

            file_path = os.path.join(root, file)
            print(f"\n📄 Procesando archivo: {file}")

            # ── 1) Metadata desde Summary ─────────────────────────
            meta = extract_metadata_from_excel(file_path) or {}
            print("   ▶ Metadata:", meta)

            contract = meta.get("contract_code")
            farmer   = meta.get("farmer_name")
            cdate    = meta.get("cruise_date", pd.NaT)

            # allowed_codes → skip file?
            if filter_func and not filter_func(contract):
                print(f"   ⏭️  {contract} no está en allowed_codes")
                continue

            # ── 2) Hoja Input ─────────────────────────────────────
            df = read_input_sheet(file_path)
            if df is None or df.empty:
                print("   ⚠️  Sin datos válidos, se omite")
                continue                                   # ← NO vuelve a faltar

            # ── 3) Inyectar metadata y volver a normalizar nombres ─
            meta_cols = {
                "contractcode": contract,
                "farmername":   farmer,
                "cruisedate":   cdate,
            }
            for col, val in meta_cols.items():
                df[col] = val

            # (los nombres ya están limpios → no es necesario renombrar)
            print("   ▶ Columnas finales:", df.columns.tolist())
            df_list.append(df)

    if not df_list:
        print("❌ No se encontró ningún archivo válido.")
        return None

    combined = pd.concat(df_list, ignore_index=True)
    print("\n=== FINAL COMBINED CHECK ===")
    print("Columns:", combined.columns.tolist())
    print("Unique contractcodes:", combined.contractcode.unique()[:5])
    return combined
