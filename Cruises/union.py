from utils.libs import os, pd, warnings
from utils.extractors import extract_metadata_from_excel
from utils.cleaners import clean_column_name, get_column

# Suprime warnings benignos de openpyxl
warnings.filterwarnings(
    "ignore",
    message="Data Validation extension is not supported and will be removed",
    category=UserWarning,
    module="openpyxl"
)
warnings.filterwarnings(
    "ignore",
    message="Conditional Formatting extension is not supported and will be removed",
    category=UserWarning,
    module="openpyxl"
)


def read_input_sheet(file_path):
    """
    Lee la hoja 'Input' o 'DataInput' y normaliza columnas
    usando únicamente COLUMN_LOOKUP + get_column.
    """
    try:
        # 1) Selecciona la hoja correcta
        xls = pd.ExcelFile(file_path)
        sheets = xls.sheet_names
        target = next(
            (s for s in sheets if s.lower().strip() in ("input", "datainput")),
            sheets[0] if sheets else None
        )

        # 2) Lee el Excel crudo
        with warnings.catch_warnings():
            warnings.simplefilter("ignore", UserWarning)
            df = pd.read_excel(
                file_path,
                sheet_name=target,
                engine="openpyxl",
                dtype=str,
                na_filter=False
            )

        print(f"\n=== DEBUG {os.path.basename(file_path)} ===")
        print("Columnas originales:", df.columns.tolist())

        # 3) Limpia todos los nombres de columna con tu normalizador
        df.columns = [clean_column_name(col) for col in df.columns]
        print("Columnas limpiadas:", df.columns.tolist())

        # 4) Define aquí los campos lógicos que quieres capturar
        #    y el nombre interno que les vas a dar
        campos = {
            "Tree #": "tree_number",
            "Stand #": "stand",
            "Plot #": "plot",
            "Plot Coordinate": "plot_coordinate",
            "Status": "status_id",
            "Species": "species_id",
            "Defect": "defect_id",
            "Defect HT (ft)": "defect_ht_ft",
            "DBH (in)": "dbh_in",
            "THT (ft)": "tht_ft",
            "Merch. HT (ft)": "merch_ht_ft",
            "Pests": "pests_id",
            "Disease": "disease_id",
            "Coppiced": "coppiced_id",
            "Permanent Plot": "permanent_plot_id",
            "Short Note": "short_note",
            "ContractCode": "contractcode",
            "FarmerName": "farmername",
            "CruiseDate": "cruisedate"
        }

        # 5) Usa get_column para descubrir cada alias, y crea el dict de renombrado
        rename_dict = {}
        for logical_name, internal_name in campos.items():
            try:
                actual = get_column(df, logical_name)
                rename_dict[actual] = internal_name
            except KeyError:
                # si no lo encuentra, avisas o simplemente sigues
                print(f"⚠️ No encontré columna lógica '{logical_name}'")

        # 6) Renombra de golpe
        df = df.rename(columns=rename_dict)
        print("Columnas tras renombrado:", df.columns.tolist())

        return df

    except Exception as e:
        print(f"[ERROR] No se pudo leer {file_path}: {e}")
        return None

def combine_files(base_path, filter_func=None):
    """
    Recorre base_path, procesa todos los .xlsx (excluye temporales y combinados),
    extrae metadatos y concatena DataFrames.
    """
    df_list = []

    for root, dirs, files in os.walk(base_path):
        for file in files:
            if file.lower().startswith("~$") or "combined_inventory" in file.lower():
                continue
            if not file.lower().endswith(".xlsx"):
                continue

            file_path = os.path.join(root, file)
            print(f"\n📄 Procesando archivo: {file}")

            # Logging de hojas disponibles
            xls = pd.ExcelFile(file_path)
            print(f"   ▶ Hojas encontradas: {xls.sheet_names}")

            # Extraer metadatos
            metadata = extract_metadata_from_excel(file_path) or {}
            contract_code = metadata.get("contract_code")
            farmer_name = metadata.get("farmer_name")
            cruise_date = metadata.get("cruise_date", pd.NaT)

            # Leer contenido de hoja
            df = read_input_sheet(file_path)
            if df is not None and not df.empty:
                # Verificar si el renombrado fue exitoso
                if "tree_number" not in df.columns:
                    print(f"🛑 Error crítico: Columna 'tree_number' no encontrada en {file}")
                    continue  # O maneja el error según tu lógica

                # Añadir metadatos (sin cambios)
                df["contractcode"] = contract_code
                df["farmername"] = farmer_name
                df["cruisedate"] = cruise_date
                df_list.append(df)
            else:
                print(f"   ⚠️ Archivo omitido (sin datos válidos): {file}")

    if not df_list:
        return None
    return pd.concat(df_list, ignore_index=True)
