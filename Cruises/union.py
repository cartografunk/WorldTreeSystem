# union.py
from core.libs    import pd, warnings, Path, os
from Cruises.utils.extractors import extract_metadata_from_excel
from core.schema import COLUMNS, cast_dataframe
from Cruises.utils.cleaners import get_column
from Cruises.utils.normalizers import clean_column_name
from tqdm import tqdm
import traceback
from Cruises.onedriver import force_download

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
    from pathlib import Path
    from time import sleep

    try:
        path = Path(file_path)
        max_retries = 3
        delay_base = 2  # segundos

        for attempt in range(1, max_retries + 1):
            success = force_download(path)

            # Validación real de archivo descargado
            if success and path.exists() and path.is_file() and path.stat().st_size > 0:
                break

            print(f"🔁 Retry {attempt}/{max_retries} para {path.name}...")
            sleep(delay_base * attempt)
        else:
            print(f"⛔ No se pudo acceder a: {file_path} tras {max_retries} intentos")
            return None, {}

        # Proceder con la lectura
        xls = pd.ExcelFile(file_path)
        raw_sheets = xls.sheet_names

        for s in raw_sheets:
            if s.lower().strip() in ("input", "datainput"):
                target = s
                break
        else:
            target = raw_sheets[0]

        df = pd.read_excel(xls, sheet_name=target, dtype=str, na_filter=False)
        df.columns = [clean_column_name(c) for c in df.columns]

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

        # 1️⃣  Normaliza dtypes (solo una vez)
        df = cast_dataframe(df)  # <- aquí


        meta = extract_metadata_from_excel(file_path) or {}
        return df, meta

    except Exception as e:
        print(f"[ERROR] {file_path}: {e}")
        traceback.print_exc()
        return None, {}



def combine_files(base_path, filter_func=None, explicit_files=None):
    """Combina archivos XLSX de inventario forestal.

    Args:
        base_path (str/Path): Ruta base (se ignora si explicit_files está presente)
        filter_func (callable, optional): Función para filtrar metadatos
        explicit_files (list, optional): Lista explícita de paths de archivos a procesar

    Returns:
        pd.DataFrame: DataFrame combinado
    """

    df_list = []
    all_files = []

    # Priorizar archivos explícitos si existen
    if explicit_files:
        from core.paths import INVENTORY_BASE

        # 🔧 Armar rutas completas + limpiar nombres invisibles
        all_files = [INVENTORY_BASE / Path(f) for f in explicit_files]


        #print(f"🗂️ Procesando {len(all_files)} archivos explícitos")

    else:  # Modo automático: buscar en directorio
        base_path = Path(base_path)
        print(f"📁 Buscando archivos en: {base_path}")

        for root, _, files in os.walk(base_path):
            for f in files:
                if (f.lower().endswith(".xlsx")
                        and not f.startswith("~$")
                        and "combined_inventory" not in f.lower()):
                    all_files.append(Path(root) / f)

        print(f"🔍 Encontrados {len(all_files)} archivos XLSX")

    if not all_files:
        print("❌ No hay archivos para procesar")
        return pd.DataFrame()

    print("⚙️ Iniciando procesamiento de archivos...")
    for path in tqdm(all_files, unit="archivo"):
        # ⬇️ Verifica y descarga si está en la nube
        if not force_download(path):
            continue  # Silencioso

        file = path.name
        try:
            # Leer archivo y metadatos
            df, meta = read_metadata_and_input(path)

            if df is None:
                print(f"   ⚠️  Archivo no procesado: {file}")
                continue

            if df.empty:
                print(f"   ⚠️  Archivo vacío: {file}")
                continue

            # Validación básica de columnas
            required_cols = ["tree_number", "Status"]
            missing = [c for c in required_cols if c not in df.columns]
            if missing:
                print(f"   ❌ Faltan columnas clave: {', '.join(missing)}")
                continue

            # Filtrado por metadatos
            if filter_func and not filter_func(meta):
                print(f"   🚫 Filtrado por códigos: {file}")
                continue

            # Limpieza inicial
            df = df.dropna(subset=["tree_number", "Status"], how="all")
            df = df.reset_index(drop=True)

            # Añadir metadatos
            df["contractcode"] = meta.get("contract_code", "DESCONOCIDO")
            df["farmername"] = meta.get("farmer_name", "SIN_NOMBRE")
            df["cruisedate"] = meta.get("cruise_date", pd.NaT)

            df_list.append(df)
            #print(f"   ✅ Procesado exitoso: {len(df)} filas")

        except Exception as e:
            print(f"   🔥 Error crítico en {file}: {str(e)}")
            continue

    if not df_list:
        print("❌ Ningún archivo pudo ser procesado")
        return pd.DataFrame()

    combined = pd.concat(df_list, ignore_index=True)
    print("\n📊 Combinación finalizada")
    print(f"🌳 Total de árboles procesados: {len(combined):,}")
    #print(f"📅 Rango de fechas: {combined['cruisedate'].min()} a {combined['cruisedate'].max()}")

    return combined

