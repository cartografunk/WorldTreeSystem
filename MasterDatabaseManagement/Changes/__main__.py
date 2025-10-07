# MasterDatabaseManagement/Changes/__main__.py
import argparse
from datetime import datetime

from core.db import get_engine
from MasterDatabaseManagement.sanidad import preflight, postflight, ensure_constraints
from MasterDatabaseManagement.sanidad.reporters import export_report
from MasterDatabaseManagement.sanidad.fixers import backfill_survival_current

EXPORTS_DIR = "Exports"

def _tag(prefix: str) -> str:
    return f"{prefix}_{datetime.now().strftime('%Y%m%d_%H%M%S')}"

def parse_args():
    p = argparse.ArgumentParser("MasterDatabaseManagement.Changes")
    sub = p.add_subparsers(dest="cmd", required=True)

    s1 = sub.add_parser("changelog")
    s1.add_argument("--dry-run", action="store_true")

    s2 = sub.add_parser("new-contracts")
    s2.add_argument("--dry-run", action="store_true")

    return p.parse_args()

def run_with_sanity(job_name: str, run_job_callable, dry_run: bool, do_backfill: bool = False):
    """
    job_name: 'new_contracts' | 'changelog'
    run_job_callable: función que ejecuta tu job actual (acepta kwarg dry_run)
    dry_run:
      - True  => Solo corre preflight (strict=False) y exporta reporte. NO muta.
      - False => ensure_constraints -> preflight(strict=True) -> job -> (backfill opcional) -> postflight(strict=True) -> exporta reporte.
    """
    engine = get_engine()

    if dry_run:
        tag = _tag(f"dryrun_{job_name}")
        results = preflight(engine, job=job_name, strict=False)
        path = export_report(results, outdir=EXPORTS_DIR, tag=tag)
        print(f"🧪 Dry-run {job_name}: checks exportados en {path}")
        # Ejecuta el job en modo dry_run para que imprima/loggee lo que haría, sin tocar DB
        run_job_callable(dry_run=True)
        return

    # Camino normal (con sanidad estricta)
    ensure_constraints(engine)
    preflight(engine, job=job_name, strict=True)

    # Ejecuta tu job real
    run_job_callable(dry_run=False)

    # Para altas de contratos, rellenamos survival_current faltantes si aplica
    if do_backfill:
        with engine.begin() as conn:
            backfill_survival_current(conn)

    results = postflight(engine, job=job_name, strict=True)
    tag = _tag(f"post_{job_name}")
    path = export_report(results, outdir=EXPORTS_DIR, tag=tag)
    print(f"✅ Postflight {job_name}: checks exportados en {path}")

def main():
    args = parse_args()

    if args.cmd == "changelog":
        from .changelog_activation import main as run_changelog
        run_with_sanity(
            job_name="changelog",
            run_job_callable=run_changelog,
            dry_run=args.dry_run,
            do_backfill=False,  # no es necesario en changelog
        )

    elif args.cmd == "new-contracts":
        from .new_contract_input_activation import main as run_new_contracts
        run_with_sanity(
            job_name="new_contracts",
            run_job_callable=run_new_contracts,
            dry_run=args.dry_run,
            do_backfill=True,   # tras altas, asegura SC con backfill
        )

if __name__ == "__main__":
    main()
