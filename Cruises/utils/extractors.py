# utils/extractors.py
from core.libs import load_workbook, range_boundaries, re


# define all your labels, in Spanish and English, normalized to lowercase without accents
LABELS = {
    "contract_code": [
        "código de contrato", "codigo de contrato",
        "contract code"
    ],
    "farmer_name": [
        "nombre del productor", "nombre del productor",
        "rep. del productor", "producer name",
        "farmer name"
    ],
    "cruise_date": [
        "fecha de inicio", "start date"
    ],
    # you can add more here if you ever need e.g. audit type, region, etc.
}

print("📄 Extrayendo metadatos...")

def extract_metadata_from_excel(path):


    """
    Returns a dict with keys: contract_code, farmer_name, cruise_date.
    Scans the 'Summary' sheet for any of the LABELS, then walks
    right from the label cell until it finds a non-empty value.
    """
    try:
        wb = load_workbook(path, data_only=True)
        # pick a sheet named "Summary" (any casing), or fall back to first sheet
        summary = next((s for s in wb.sheetnames if s.lower()=="summary"), wb.sheetnames[0])
        ws = wb[summary]

        metadata = {}

        # Helper to normalize cell text
        def norm(txt):
            return str(txt).strip().lower().rstrip(":") if txt is not None else ""

        # Build a flat mapping of all cells (row, col) → normalized text
        # so we can find label positions even if they’re merged
        positions = {}
        for row in ws.iter_rows():
            for cell in row:
                txt = norm(cell.value)
                if txt:
                    positions[(cell.row, cell.column)] = txt

        # For each metadata field, look for any of its labels
        for field, candidates in LABELS.items():
            if field in metadata:
                continue
            # scan every label possibility
            for (r, c), cell_txt in positions.items():
                if cell_txt in candidates:
                    # walk to the right until we find a real value
                    for offset in range(1, 6):  # look up to 5 cols over
                        val = ws.cell(row=r, column=c+offset).value
                        if val not in (None, "",):
                            metadata[field] = val
                            break
                    break

        return metadata

    except Exception as e:
        print(f"❌ Error extrayendo metadatos: {e}")
        return {}