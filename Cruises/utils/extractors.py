# utils/extractors.py
from utils.libs import load_workbook, range_boundaries, re


def extract_metadata_from_excel(file_path):
    try:
        wb = load_workbook(file_path, data_only=True)
        ws = wb.active
        metadata = {}
        for row in ws.iter_rows(values_only=True):
            if "Código de Contrato" in row:
                idx = row.index("Código de Contrato")
                metadata["contract_code"] = row[idx + 1]
            if "Nombre del Crucero" in row:
                idx = row.index("Nombre del Crucero")
                metadata["farmer_name"] = row[idx + 1]
            if "Fecha de Inicio" in row:
                idx = row.index("Fecha de Inicio")
                metadata["cruise_date"] = row[idx + 1]
        return metadata
    except Exception as e:
        print(f"❌ Error extrayendo metadatos: {e}")
        return {}