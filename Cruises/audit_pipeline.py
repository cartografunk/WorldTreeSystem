# pipeline_utils.py
from Cruises.audit_generator import create_audit_table
from core.libs import os, re

def run_audit(engine, inventory_table_name: str, output_xlsx: str | None):
    """
    Lanza audit_generator sobre la tabla de inventario ya grabada.
    - inventory_table_name: p.e. 'inventory_mx_2025'
    - output_xlsx: carpeta donde soltar el .xlsx de auditoría (o None)
    """
    # Extraer carpeta
    folder = os.path.dirname(output_xlsx) if output_xlsx else None
    # Ejecutar
    return create_audit_table(engine, inventory_table_name, folder)
