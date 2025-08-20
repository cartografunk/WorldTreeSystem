# MasterDatabaseManagement/Changes/__main__.py
import argparse
from MasterDatabaseManagement.Changes.changelog_activation import main as run_changelog
from MasterDatabaseManagement.Changes.new_contract_input_activation import main as run_new_contracts

def parse_args():
    p = argparse.ArgumentParser("MasterDatabaseManagement.Changes")
    sub = p.add_subparsers(dest="cmd", required=True)

    s1 = sub.add_parser("changelog")
    s1.add_argument("--dry-run", action="store_true")

    s2 = sub.add_parser("new-contracts")
    s2.add_argument("--dry-run", action="store_true")

    return p.parse_args()

def main():
    args = parse_args()
    if args.cmd == "changelog":
        run_changelog(dry_run=args.dry_run)
    elif args.cmd == "new-contracts":
        run_new_contracts(dry_run=args.dry_run)

if __name__ == "__main__":
    main()
