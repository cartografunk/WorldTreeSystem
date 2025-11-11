#MasterdatabaseManagement/tools/database_management_helpers.py

from core.libs import pd, Path  # ya usas pd aquí
from core.paths import DATABASE_EXPORTS_DIR
from core.sheets import Sheet, STATUS_DONE
from datetime import datetime
from typing import Iterable, Mapping, Optional


def remove_tz(df: pd.DataFrame) -> pd.DataFrame:
    # Quita tz de columnas datetime (robusto)
    for col in df.columns:
        if pd.api.types.is_datetime64tz_dtype(df[col]):
            df[col] = df[col].dt.tz_localize(None)
    return df


def _next_row(sheet: Sheet) -> int:
    return int(sheet.ws.max_row) + 1

def _set(sheet: Sheet, row: int, col_idx: int, value):
    sheet.get_cell(row, col_idx).value = value

def log_new_contract_to_changelog(
    contract_code: str,
    scenario: str,                 # 'nuevo' | 'clonado'
    farmer_number: Optional[str],
    source_row: Optional[int],
    catalog_path: Optional[Path] = None,
    sheet_name: str = "ChangeLog",
) -> None:
    """
    Inserta una línea en changelog.xlsx → ChangeLog usando POSICIONES fijas:
    1=requester, 2=change_date, 3=Contract Code, penúltima=change_in_db, última=Notes
    """
    path = catalog_path or (Path(DATABASE_EXPORTS_DIR) / "changelog.xlsx")
    sh = Sheet(path, sheet_name)

    # Columnas por POSICIÓN fija (1-based)
    ncols = len(sh.headers)
    if ncols < 5:
        raise RuntimeError(f"ChangeLog tiene muy pocas columnas ({ncols}).")

    col_requester     = 1
    col_change_date   = 2
    col_contract_code = 3
    col_notes         = ncols
    col_status        = ncols - 1

    r = _next_row(sh)
    date_str = datetime.now().strftime("%d/%m/%Y")

    note = f"new-contracts | {scenario}"
    if farmer_number:
        note += f" | farmer={farmer_number}"
    if source_row is not None:
        note += f" | src_row={source_row}"

    _set(sh, r, col_requester,     "new-contracts")
    _set(sh, r, col_change_date,   date_str)
    _set(sh, r, col_contract_code, contract_code)
    _set(sh, r, col_status,        STATUS_DONE)   # 'Done'
    _set(sh, r, col_notes,         note)

    sh.save()