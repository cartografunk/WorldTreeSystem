# MasterDatabaseManagement/tools/ensure_views.py
import argparse
from core.db import get_engine
from core.db_objects import ensure_fpi_expanded_view

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--grant-to", default=None)
    args = ap.parse_args()

    eng = get_engine()
    ensure_fpi_expanded_view(eng, grant_to=args.grant_to)
    print("✅ View masterdatabase.fpi_contracts_expanded lista.")

if __name__ == "__main__":
    main()
