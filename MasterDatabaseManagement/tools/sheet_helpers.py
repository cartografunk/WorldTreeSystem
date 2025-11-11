# MasterDatabaseManagement/tools/sheet_helpers.py
from core.sheets import Sheet, STATUS_DONE
from datetime import datetime
from typing import Dict, Iterable, Optional

# ==== filas/columnas ====

def next_free_row(sh: Sheet) -> int:
    """Devuelve la siguiente fila libre (1-based)."""
    return int(sh.ws.max_row) + 1

def set_cell(sh: Sheet, row: int, col_idx: int, value):
    """Asigna valor a una celda por índice (1-based)."""
    sh.get_cell(row, col_idx).value = value

def write_row_by_positions(sh: Sheet, row: int, mapping: Dict[int, object]) -> None:
    """Escribe varios valores en una fila, por posiciones de columna (1-based)."""
    for col_idx, val in mapping.items():
        set_cell(sh, row, col_idx, val)

def mark_done(sh: Sheet, row: int, status_col_idx: int) -> None:
    """Marca STATUS_DONE en la columna de estatus."""
    sh.mark_status(row, status_col_idx, STATUS_DONE)

def today_ddmmyyyy() -> str:
    """Fecha actual dd/mm/AAAA (como usas en ChangeLog)."""
    return datetime.now().strftime("%d/%m/%Y")

# ==== ChangeLog “hardcode” por posiciones fijas ====

def append_changelog_line_hardcoded(
    sh: Sheet, *, contract_code: str, notes: str, requester_val: str = "new-contracts"
) -> None:
    """
    Escribe una línea nueva en ChangeLog usando POSICIONES FIJAS:
      1=requester, 2=change_date, 3=Contract Code, (ncols-1)=change_in_db, (ncols)=Notes
    """
    ncols = len(sh.headers)
    r = next_free_row(sh)
    write_row_by_positions(sh, r, {
        1: requester_val,
        2: today_ddmmyyyy(),
        3: contract_code,
        ncols - 1: STATUS_DONE,
        ncols: notes
    })

def append_changelog_batch_hardcoded(
    sh: Sheet,
    rows: Iterable[dict],
    requester_val: str = "contract_replacements",
) -> int:
    """
    Batch para ContractReplacements u otros flujos.
    rows: [{ "contract_code": "...", "notes": "..." }, ...]
    """
    ncols = len(sh.headers)
    count = 0
    for row in rows:
        cc = row.get("contract_code")
        notes = row.get("notes", "")
        if not cc:
            continue
        r = next_free_row(sh)
        write_row_by_positions(sh, r, {
            1: requester_val,
            2: today_ddmmyyyy(),
            3: cc,
            ncols - 1: STATUS_DONE,
            ncols: notes
        })
        count += 1
    return count
