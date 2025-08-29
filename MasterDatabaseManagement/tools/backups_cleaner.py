# MasterDatabaseManagement/Housekeeping/cleanup_backups.py
from core.db import get_engine
from core.libs import pd, re, argparse
from sqlalchemy import text  # ← importa esto



"""
Limpia tablas backup en un esquema, conservando el backup más reciente por grupo.
Agrupación por: (base_table, tag), donde:
  - base_table: ej. contract_tree_information
  - tag:        ej. pre_changelog, pre_newcontracts, bak

Detecta estos patrones comunes:
  1) <base>_pre_<tag>_<YYYYMMDD>_<HHMMSS>
     - ej. contract_tree_information_pre_newcontracts_20250826_230056
     - ej. contract_allocation_pre_changelog_20250827_001738
  2) <base>__bak  ó  <base>_bak
     - ej. contract_farmer_information__bak

Puedes ajustar --keep por grupo (default=1) y hacer --dry-run false para ejecutar.
"""

PATTERNS = [
    # <base>_pre_<tag>_<YYYYMMDD>_<HHMMSS>
    re.compile(r'^(?P<base>.+?)_pre_(?P<tag>[a-z0-9_]+?)_(?P<ts>\d{8}_\d{6})$', re.IGNORECASE),
    # <base>__bak  ó  <base>_bak
    re.compile(r'^(?P<base>.+?)__(?P<tag>bak)$', re.IGNORECASE),
    re.compile(r'^(?P<base>.+?)_(?P<tag>bak)$', re.IGNORECASE),
]

def parse_table(tname: str):
    for rx in PATTERNS:
        m = rx.match(tname)
        if m:
            base = m.group('base')
            tag  = m.group('tag').lower()
            ts   = m.groupdict().get('ts')  # solo tiene valor en el patrón _pre_<tag>_<ts>
            # 👇 si viene del patrón con timestamp (o sea, del "pre_"), normalizamos el tag a "pre_<tag>"
            if ts:
                tag = f"pre_{tag}"
            return base, tag, ts
    return None, None, None

def main():
    ap = argparse.ArgumentParser(description="Limpia backups en un esquema, conservando el más reciente por grupo.")
    ap.add_argument("--schema", default="masterdatabase", help="Esquema objetivo (default: masterdatabase)")
    ap.add_argument("--keep", type=int, default=1, help="Cuántos backups conservar por (base, tag). Default=1")
    ap.add_argument("--include", default="pre_changelog,pre_newcontracts,bak",
                    help="Tags a considerar (coma-sep). Default: pre_changelog,pre_newcontracts,bak")
    ap.add_argument("--dry-run", dest="dry_run", default="true",
                    help="true/false. Si false, ejecuta DROP TABLE. Default=true")
    ap.add_argument("--verbose", action="store_true", help="Imprime detalle de grupos y decisiones")
    args = ap.parse_args()

    include_tags = {t.strip().lower() for t in args.include.split(",") if t.strip()}
    dry_run = str(args.dry_run).lower() in {"1","true","yes","y"}

    eng = get_engine()
    q = """
    SELECT table_name
    FROM information_schema.tables
    WHERE table_schema = :schema
    ORDER BY table_name;
    """
    df = pd.read_sql(text(q), eng, params={"schema": args.schema})  # ← usa text(q)
    tables = df["table_name"].tolist()

    # Clasificar por (base, tag)
    rows = []
    for t in tables:
        base, tag, ts = parse_table(t)
        if tag and tag in include_tags:
            rows.append({"table_name": t, "base": base, "tag": tag, "ts": ts})

    if not rows:
        print("No se encontraron backups que coincidan con los patrones y tags indicados.")
        return

    meta = pd.DataFrame(rows)

    # Ordenar por timestamp descendente para identificar "más recientes".
    # Las entradas sin ts (p. ej. _bak) van al final en el orden.
    meta["ts_sort"] = meta["ts"].fillna("")
    # Convertir YYYYMMDD_HHMMSS a sortable; sin ts queda "" (menor)
    meta = meta.sort_values(by=["base", "tag", "ts_sort"], ascending=[True, True, False], kind="stable")

    # Marcar cuáles conservar vs eliminar por grupo (base, tag)
    meta["rank"] = meta.groupby(["base", "tag"]).cumcount() + 1
    to_keep   = meta[meta["rank"] <= args.keep]
    to_delete = meta[meta["rank"]  > args.keep]

    # Reporte
    print(f"🔎 Esquema: {args.schema}")
    print(f"🧹 Tags incluidos: {sorted(include_tags)}")
    print(f"📦 Total backups detectados: {len(meta)}")
    print(f"✅ Se conservarán: {len(to_keep)} (keep={args.keep} por grupo)")
    print(f"🗑️  Se eliminarán: {len(to_delete)}")
    if args.verbose:
        print("\n— Conservar —")
        for _, r in to_keep.iterrows():
            print(f"  [KEEP] {r['table_name']}  (base={r['base']} tag={r['tag']} ts={r['ts']})")
        print("\n— Eliminar —")
        for _, r in to_delete.iterrows():
            print(f"  [DROP] {r['table_name']}  (base={r['base']} tag={r['tag']} ts={r['ts']})")

    if not len(to_delete):
        print("\nNada por borrar. Listo.")
        return

    if dry_run:
        print("\nDRY-RUN activo → no se ejecutan DROP TABLE. "
              "Ejecuta con --dry-run false para aplicar cambios.")
        return

    with eng.begin() as conn:
        failures = []
        for _, r in to_delete.iterrows():
            tname = r["table_name"]
            # por si algún nombre trae comillas raras
            safe_tname = tname.replace('"', '""')
            ddl = f'DROP TABLE IF EXISTS "{args.schema}"."{safe_tname}" CASCADE;'
            print(f"🗑️  {ddl}")
            try:
                # Opción A (recomendada con SQLAlchemy 2.x): usa exec_driver_sql
                conn.exec_driver_sql(ddl)
                # Opción B: conn.execute(text(ddl))   ← también sirve
            except Exception as e:
                print(f"⚠️  Falló {tname}: {e}")
                failures.append((tname, str(e)))

    if failures:
        print("\n❗Resumen de fallos:")
        for t, msg in failures:
            print(f"  - {t}: {msg}")
    else:
        print("\n✅ Limpieza completada sin fallos.")
if __name__ == "__main__":
    main()
