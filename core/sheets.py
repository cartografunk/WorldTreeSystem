# core/sheets.py
from __future__ import annotations
from typing import Dict, List, Optional, Iterable
from openpyxl import load_workbook
from core.libs import pd
from core.schema_helpers_db_management import read_cell_by_key
from core.schema_helpers import clean_column_name

# ---------- Clase util para hojas ----------
class Sheet:
    def __init__(self, path, sheet_name: str):
        self.path = path
        self.wb = load_workbook(path)
        if sheet_name not in self.wb.sheetnames:
            raise ValueError(f"❌ La hoja '{sheet_name}' no existe en {path}")
        self.ws = self.wb[sheet_name]
        self.headers: List[str] = [c.value if c.value is not None else "" for c in self.ws[1]]
        self.hdr_df = pd.DataFrame(columns=self.headers)
        self._rebuild_header_map()

    def _rebuild_header_map(self):
        self.header_map: Dict[str, int] = {h: i + 1 for i, h in enumerate(self.headers)}
        self.header_map_norm: Dict[str, int] = {clean_column_name(h): i for i, h in enumerate(self.headers)}

    def ensure_column(self, name: str) -> int:
        if name in self.header_map:
            return self.header_map[name]
        col = len(self.headers) + 1
        self.ws.cell(row=1, column=col, value=name)
        self.headers.append(name)
        self._rebuild_header_map()
        return col

    def index_of(self, header_name: str) -> Optional[int]:
        if header_name in self.header_map:
            return self.header_map[header_name]
        norm = clean_column_name(header_name)
        idx0 = self.header_map_norm.get(norm)
        return (idx0 + 1) if idx0 is not None else None

    def iter_rows(self):
        for r, row in enumerate(self.ws.iter_rows(min_row=2, max_row=self.ws.max_row), start=2):
            yield r, row

    def read(self, row, logical_key: str):
        try:
            return read_cell_by_key(row, self.headers, self.hdr_df, logical_key)
        except Exception:
            return None

    def get_cell(self, r: int, c: int):
        return self.ws.cell(row=r, column=c)

    def mark_done(self, r: int, done_col_index: int, value: str = "Done"):
        self.ws.cell(row=r, column=done_col_index, value=value)

    def save(self):
        self.wb.save(self.path)


# ---------- Catálogos del changelog ----------
def read_changelog_catalogs(catalog_file) -> tuple[pd.DataFrame, pd.DataFrame]:
    fields = pd.read_excel(catalog_file, sheet_name="FieldsCatalog")
    reasons = pd.read_excel(catalog_file, sheet_name="ChangeReasonsCatalog")
    return fields, reasons

def get_table_for_field(fields_catalog: pd.DataFrame, field: str) -> str | None:
    matches = fields_catalog[fields_catalog["target_field"] == field]
    return matches["target_table"].iloc[0] if not matches.empty else None


# ---------- Utils de DataFrame / export ----------
def remove_tz(df: pd.DataFrame) -> pd.DataFrame:
    for col in df.columns:
        if pd.api.types.is_datetime64tz_dtype(df[col]):
            df[col] = df[col].dt.tz_localize(None)
    return df

def export_tables_to_excel(engine, tables: Iterable[str], out_path, order_by: str = "contract_code"):
    """
    Exporta tablas SQL a un Excel (una hoja por tabla). Sobrescribe el archivo.
    """
    from core.libs import Path
    out_path = Path(out_path)
    print("⏳ Sobrescribiendo Excel...")
    with pd.ExcelWriter(out_path, engine="openpyxl", mode="w") as writer:
        for table in tables:
            df = pd.read_sql(f'SELECT * FROM masterdatabase."{table}"', engine)
            df = remove_tz(df)
            if order_by in df.columns:
                df = df.sort_values(order_by)
            df.to_excel(writer, sheet_name=table[:31], index=False)
            print(f"Exportado: {table}")
    print(f"✅ Exportación finalizada: {out_path}")
