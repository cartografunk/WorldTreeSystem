# forest_inventory/cruiseDater.py

from utils.libs import os, pd, load_workbook, range_boundaries, execute_values
from utils.db import get_engine


def extract_cruise_info(file_path):
    """
    Extrae el 'Contract Code' y 'Start Date' de la hoja 'Summary' de un archivo Excel.

    Returns:
        tuple: (contract_code, cruise_date)
    """
    try:
        wb = load_workbook(file_path, data_only=True)
        sheet_name = next((s for s in wb.sheetnames if s.lower().strip() == 'summary'), None)
        if not sheet_name:
            return None, None

        ws = wb[sheet_name]
        cruise_date, contract_code = None, None

        # Buscar en celdas combinadas
        for merged_range in ws.merged_cells.ranges:
            min_col, min_row, max_col, max_row = range_boundaries(str(merged_range))
            value = ws.cell(row=min_row, column=min_col).value
            if not value:
                continue
            val = str(value).strip().lower()
            if val.startswith("start date"):
                cruise_date = ws.cell(row=min_row, column=max_col + 1).value
            elif val.startswith("contract code"):
                contract_code = ws.cell(row=min_row, column=max_col + 1).value

        # Fallback: buscar en celdas normales
        if not cruise_date or not contract_code:
            for row in ws.iter_rows():
                for cell in row:
                    val = str(cell.value).strip().lower() if cell.value else ""
                    if not cruise_date and val.startswith("start date"):
                        cruise_date = ws.cell(row=cell.row, column=cell.column + 1).value
                    if not contract_code and val.startswith("contract code"):
                        contract_code = ws.cell(row=cell.row, column=cell.column + 1).value
                if cruise_date and contract_code:
                    break

        return contract_code, cruise_date

    except Exception as e:
        print(f"❌ Error procesando '{file_path}': {e}")
        return None, None


def process_folder(base_folder, table_name, connection_string):
    """
    Recorre recursivamente una carpeta de archivos XLSX, extrae fechas de inicio por contrato
    y las actualiza en la tabla SQL especificada.
    """
    parsed_data = []

    for root, dirs, files in os.walk(base_folder):
        for file in files:
            if file.lower().endswith((".xlsx", ".xlsm")) and not file.startswith("~$"):
                file_path = os.path.join(root, file)
                print(f"\n📄 Analizando: {file_path}")
                contract_code, cruise_date = extract_cruise_info(file_path)
                if cruise_date and contract_code:
                    print(f"✅ {contract_code} → {cruise_date}")
                    parsed_data.append((cruise_date, contract_code))
                else:
                    print("⚠️ No se encontró 'Start Date' o 'Contract Code'.")

    if not parsed_data:
        print("\n⚠️ No se encontraron fechas válidas.")
        return

    # Actualizar base de datos
    engine = get_engine(connection_string)
    conn = engine.raw_connection()
    cursor = conn.cursor()

    update_query = f"""
        UPDATE {table_name} AS t
        SET cruise_start_date = data.cruise_date
        FROM (VALUES %s) AS data (cruise_date, contract_code)
        WHERE t."ContractCode" = data.contract_code;
    """
    try:
        execute_values(cursor, update_query, parsed_data)
        conn.commit()
        print(f"\n✅ {len(parsed_data)} registros actualizados en '{table_name}'.")
    except Exception as e:
        print(f"❌ Error actualizando SQL: {e}")
    finally:
        cursor.close()
        conn.close()
