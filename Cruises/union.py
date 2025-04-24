# union.py
from utils.libs import os, pd, warnings
from utils.extractors import extract_metadata_from_excel
from utils.schema import COLUMNS
from utils.cleaners import get_column
from utils.normalizers import clean_column_name

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

print("📂 Leyendo archivos XLSX...")

def read_input_sheet(file_path: str) -> pd.DataFrame | None:
    try:
        xls = pd.ExcelFile(file_path)
        target = next(
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

        # Limpieza básica de columnas
        df.columns = [clean_column_name(c) for c in df.columns]

        # Renombrar columnas que están definidas como "input"
        rename_dict = {}
        for col in COLUMNS:
            if col.get("source") != "input":
                continue
            try:
                real = get_column(df, col["key"])
                rename_dict[real] = col["key"]
            except KeyError:
                pass  # omitir advertencias si no es input

        df = df.rename(columns=rename_dict)
        return df

    except Exception as e:
        print(f"[ERROR] {file_path}: {e}")
        return None


def combine_files(base_path: str, filter_func=None) -> pd.DataFrame | None:
    df_list = []

    for root, _, files in os.walk(base_path):
        for file in files:
            if file.lower().startswith("~$") or "combined_inventory" in file.lower():
                continue
            if not file.lower().endswith(".xlsx"):
                continue

            file_path = os.path.join(root, file)
            meta = extract_metadata_from_excel(file_path) or {}

            contract = meta.get("contract_code")
            farmer = meta.get("farmer_name")
            cdate = meta.get("cruise_date", pd.NaT)

            if filter_func and not filter_func(contract):
                print(f"   ⏭️  {contract} no está en allowed_codes")
                continue

            df = read_input_sheet(file_path)
            if df is None or df.empty:
                print("   ⚠️  Sin datos válidos, se omite")
                continue

            df["contractcode"] = contract
            df["farmername"]   = farmer
            df["cruisedate"]   = cdate

            df_list.append(df)

    if not df_list:
        print("❌ No se encontró ningún archivo válido.")
        return None

    combined = pd.concat(df_list, ignore_index=True)
    print("📂 Combinación finalizada")
    return combined
