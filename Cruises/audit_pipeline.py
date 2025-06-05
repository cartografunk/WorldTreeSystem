# Cruises/audit_pipeline.py
from Cruises.audit_generator import create_audit_table

def run_audit(engine, inventory_table_name: str, output_xlsx: str | None = None):
    """
    Lanza audit_generator sobre la tabla de inventario ya grabada.
    - inventory_table_name: p.e. 'inventory_mx_2025'
    - output_xlsx: carpeta donde soltar el .xlsx de auditoría (o None)
    """
    folder = None
    # Si en el futuro quieres guardar Excel, descomenta esto:
    # if output_xlsx is not None and isinstance(output_xlsx, str) and output_xlsx.strip() != "":
    #     import os
    #     folder = os.path.dirname(output_xlsx)
    return create_audit_table(engine, inventory_table_name, folder)
