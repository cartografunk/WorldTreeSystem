# union.py
from utils.libs import os, pd, warnings
from utils.extractors import extract_metadata_from_excel
from utils.schema import COLUMNS
from utils.cleaners import get_column
from utils.normalizers import clean_column_name
from tqdm import tqdm

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

def read_input_sheet(file_path: str) -> pd.DataFrame | None:
    try:
        # Mostrar inicio de lectura
        print(f"📄 Leyendo: {file_path}")

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

        # Barra de progreso para limpieza básica de columnas
        df.columns = [clean_column_name(c) for c in tqdm(df.columns, desc="🔠 Limpiando columnas", leave=False)]

        # Renombrar columnas basadas en COLUMNS
        rename_dict = {}
        for col in COLUMNS:
            if col.get("source") != "input":
                continue
            try:
                real = get_column(df, col["key"])
                rename_dict[real] = col["key"]
            except KeyError:
                pass  # omitir advertencias si no es input

        # Aplicar renombramiento
        if rename_dict:
            df = df.rename(columns=rename_dict)

        return df

    except Exception as e:
        print(f"[ERROR] {file_path}: {e}")
        return None


def combine_files(base_path: str, filter_func=None) -> pd.DataFrame | None:
    df_list = []

    for root, _, files in os.walk(base_path):
        files = [f for f in files if f.lower().endswith(".xlsx") and not f.lower().startswith("~$") and "combined_inventory" not in f.lower()]

        for file in tqdm(files, desc="📄 Leyendo archivos", unit="archivo"):
            file_path = os.path.join(root, file)
            meta = extract_metadata_from_excel(file_path) or {}

            contract = meta.get("contract_code")
            farmer = meta.get("farmer_name")
            cdate = meta.get("cruise_date", pd.NaT)

            if filter_func and not filter_func(contract):
                tqdm.write(f"   ⏭️  {contract} no está en allowed_codes")
                continue

            df = read_input_sheet(file_path)
            if df is None or df.empty:
                tqdm.write(f"   ⚠️  Sin datos válidos en {file}")
                continue

            df["contractcode"] = contract
            df["farmername"] = farmer
            df["cruisedate"] = cdate

            df_list.append(df)

    if not df_list:
        print("❌ No se encontró ningún archivo válido.")
        return None

    combined = pd.concat(df_list, ignore_index=True)
    print("📂 Combinación finalizada")
    if "contractcode" in combined.columns:
        total_arboles = len(combined)
        print(f"🌳 Total de árboles combinados: {total_arboles:,}")
    return combined
