# union.py
from core.libs    import pd, warnings, Path
from Cruises.utils.extractors import extract_metadata_from_excel
from core.schema import COLUMNS
from Cruises.utils.cleaners  import get_column
from Cruises.utils.normalizers import clean_column_name
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
warnings.filterwarnings(
    "ignore",
    message="The behavior of DataFrame concatenation with empty or all-NA entries is deprecated",
    category=FutureWarning,
    module="pandas.core.reshape.concat"
)

def read_metadata_and_input(file_path: str) -> tuple[pd.DataFrame | None, dict]:
    """
    Abre un XLSX, extrae:
     - df_input: la hoja 'Input' o 'DataInput'
     - meta: dict con contract_code, farmer_name, cruise_date
    """
    try:
        xls = pd.ExcelFile(file_path)
        raw_sheets = xls.sheet_names
        # buscar hoja de input de forma case‐insensitive
        for s in raw_sheets:
            if s.lower().strip() in ("input", "datainput"):
                target = s
                break
        else:
            target = raw_sheets[0]

        df = pd.read_excel(xls, sheet_name=target, dtype=str, na_filter=False)
        # limpieza de nombres
        df.columns = [clean_column_name(c) for c in df.columns]

        # renombrar según schema
        rename_dict = {}
        for col in COLUMNS:
            if col.get("source") != "input":
                continue
            try:
                real = get_column(df, col["key"])
                rename_dict[real] = col["key"]
            except KeyError:
                pass
        df = df.rename(columns=rename_dict)

        # metadatos con tu extractor existente
        meta = extract_metadata_from_excel(file_path) or {}

        return df, meta

    except Exception as e:
        print(f"[ERROR] {file_path}: {e}")
        return None, {}

from tqdm import tqdm
import os

def combine_files(file_paths, filter_func=None):
    """
    Combina archivos .xlsx a partir de una lista de rutas individuales.
    """
    df_list = []

    print("⚙️ Iniciando procesamiento de archivos...")
    for path_str in tqdm(file_paths, unit="archivo"):
        path = Path(path_str)
        file = path.name

        # Filtrar si no es .xlsx o si contiene "combined_inventory"
        if (
            not path.suffix.lower() == ".xlsx"
            or path.name.startswith("~$")
            or "combined_inventory" in file.lower()
        ):
            continue

        # Filtro personalizado opcional
        if filter_func and not filter_func(path):
            continue

        try:
            df, meta = read_metadata_and_input(path)
        except Exception as e:
            tqdm.write(f"   ❌ Error al leer {file}: {e}")
            continue

        if df is None or df.empty:
            tqdm.write(f"   ⚠️  Sin datos válidos en {file}")
            continue

        for col in ("tree_number", "Status"):
            df[col] = df.get(col, pd.NA).replace("", pd.NA)

        mask = df["tree_number"].isna() & df["Status"].isna()
        if mask.any():
            tqdm.write(f"   🧹 {mask.sum()} filas vacías en {file}")
            df = df.loc[~mask]

        df["contractcode"] = meta.get("contract_code")
        df["farmername"]   = meta.get("farmer_name")
        df["cruisedate"]   = meta.get("cruise_date", pd.NaT)

        df_list.append(df)

    if not df_list:
        print("❌ No se encontró ningún archivo válido.")
        return None

    combined = pd.concat(df_list, ignore_index=True)
    print("📂 Combinación finalizada")
    print(f"🌳 Total de árboles combinados: {len(combined):,}")
    return combined
