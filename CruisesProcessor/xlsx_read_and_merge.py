#CruisesProcessor/xlsx_read_and_merge.py
from core.schema import (
    COLUMNS,
)
from core.schema_helpers import rename_columns_using_schema, clean_column_name, get_column as get_column
from core.libs import pd, warnings, Path, os, tqdm, traceback, sleep

from CruisesProcessor.utils.metadata_extractor import extract_metadata_from_excel
from CruisesProcessor.utils.onedriver import force_download
from CruisesProcessor.general_importer import cast_dataframe


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
    A la vez aplica:
     1) Limpieza de nombres
     2) casteo de tipos (cast_dataframe)
     3) renombrado global basado en schema (rename_columns_using_schema)
     4) eliminación de cat_*_id viejas
     5) verificación rápida de columnas lógicas de catálogo
    """
    try:
        path = Path(file_path)
        max_retries = 3
        delay_base = 8  # segundos

        for attempt in range(1, max_retries + 1):
            success = force_download(path)

            # Validación real de archivo descargado
            if success and path.exists() and path.is_file() and path.stat().st_size > 0:
                break

            print(f"🔁 Retry {attempt}/{max_retries} para {path.name}…")
            sleep(delay_base * attempt)
        else:
            print(f"⛔ No se pudo acceder a: {file_path} tras {max_retries} intentos")
            return None, {}

        # Proceder con la lectura
        xls = pd.ExcelFile(file_path)
        raw_sheets = xls.sheet_names

        preferred_sheets = ["input (2)", "input", "datainput", "Sheet1"]
        target = None
        for pref in preferred_sheets:
            matches = [s for s in raw_sheets if s.lower().strip() == pref]
            if matches:
                target = matches[0]
                break
        if not target:
            target = raw_sheets[0]

        df = pd.read_excel(xls, sheet_name=target, dtype=str, na_filter=False)

        # 1️⃣  Normaliza dtypes (solo una vez)
        df = cast_dataframe(df)

        # 2️⃣  Renombrado global según schema.py
        #     Esto convierte columnas “defecto”→“Defect”, “especie”→“Species”, “plagas”→“Pests”, etc.
        # ANTES del rename
        #print(f"🔍 Columnas ANTES de rename_columns_using_schema: {df.columns.tolist()}")

        df = rename_columns_using_schema(df)

        # INMEDIATAMENTE DESPUÉS del rename
        #print(f"🔍 Columnas INMEDIATAMENTE DESPUÉS de rename_columns_using_schema: {df.columns.tolist()}")
        #print(">>> Columnas tras rename_columns_using_schema:", df.columns.tolist())

        # 3️⃣  Borrar columnas residuales cat_*_id (si quedaron de corridas anteriores)
        for c in [
            "cat_defect_id",
            "cat_pest_id",
            "cat_disease_id",
            "cat_coppiced_id",
            "cat_permanent_plot_id",
        ]:
            if c in df.columns:
                df = df.drop(columns=[c])
                print(f"🔸 Eliminada columna residual {c!r}")

        #print(">>> Columnas DESPUÉS de quitar cat_*_id viejas:", df.columns.tolist())

        # 5️⃣ Devolver el DataFrame listo para pasar a normalize_catalogs
        meta = extract_metadata_from_excel(file_path) or {}
        meta["sheet_used"] = target  # ← esto es clave
        return df, meta

    except Exception as e:
        print(f"[ERROR] {file_path}: {e}")
        traceback.print_exc()
        return None, {}

def combine_files(explicit_files=None, base_path=None, filter_func=None):
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
        all_files = [Path(f) for f in explicit_files]

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

            # En xlsx_read_and_merge.py línea ~175
            df = rename_columns_using_schema(df)

            # Validación básica de columnas - PASAR df
            required_cols = [get_column("tree_number", df), get_column("status", df)]
            missing = [c for c in required_cols if c not in df.columns]
            if missing:
                print(f"   ❌ Faltan columnas clave: {', '.join(missing)}")
                continue

            # Filtrado por metadatos
            if filter_func and not filter_func(meta):
                print(f"   🚫 Filtrado por códigos: {file}")
                continue

            # Limpieza inicial
            df = df.dropna(subset=required_cols, how="all")
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