# MasterDatabaseManagement/tools/changelog_bridge.py
from core.paths import DATABASE_EXPORTS_DIR
from core.sheets import Sheet
from pathlib import Path
from typing import Iterable, Dict
from .sheet_helpers import append_changelog_line_hardcoded, append_changelog_batch_hardcoded

CATALOG_FILE = Path(DATABASE_EXPORTS_DIR) / "changelog.xlsx"
CHANGELOG_SHEET = "ChangeLog"

def append_new_contract_line(contract_code: str, notes: str, requester: str="new-contracts") -> None:
    sh = Sheet(CATALOG_FILE, CHANGELOG_SHEET)
    append_changelog_line_hardcoded(sh, contract_code=contract_code, notes=notes, requester_val=requester)
    sh.save()

def append_batch_lines(rows: Iterable[Dict], requester: str="contract_replacements") -> int:
    sh = Sheet(CATALOG_FILE, CHANGELOG_SHEET)
    n = append_changelog_batch_hardcoded(sh, rows, requester_val=requester)
    if n:
        sh.save()
    return n
