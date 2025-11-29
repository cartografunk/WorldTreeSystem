import os
import re

PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
OUTPUT = os.path.join(os.path.dirname(__file__), "audit_backups_true.md")

# ==============================================================
# PATRONES DEFINITIVOS DE CREACIÓN DE RESPALDOS / DUPLICADOS
# ==============================================================
SQL_BACKUP_PATTERNS = [
    r"CREATE\s+TABLE\s+.*AS\s+TABLE",     # CREATE TABLE backup AS TABLE original
    r"CREATE\s+TABLE\s+.*AS\s+SELECT",    # CREATE TABLE backup AS SELECT * FROM original
    r"SELECT\s+.*INTO\s+.*",              # SELECT * INTO backup FROM original
    r"INSERT\s+INTO\s+.*SELECT",          # INSERT INTO backup SELECT * FROM original
    r"ALTER\s+TABLE\s+.*RENAME\s+TO",     # ALTER TABLE original RENAME TO backup
    r"DROP\s+TABLE\s+.*",                 # DROP TABLE old_backup
]

PYTHON_BACKUP_PATTERNS = [
    r"conn\.execute\(.*CREATE TABLE",     # conn.execute("CREATE TABLE ...")
    r"engine\.execute\(.*CREATE TABLE",   # engine.execute("CREATE TABLE ...")
    r"cursor\.execute\(.*CREATE TABLE",   # cursor.execute("CREATE TABLE ...")
    r"to_sql\(",                          # df.to_sql(...) but we'll filter manually
]

FILE_EXTENSIONS = [".sql", ".py"]

results = []

for root, dirs, files in os.walk(PROJECT_ROOT):
    for file in files:
        if any(file.endswith(ext) for ext in FILE_EXTENSIONS):
            filepath = os.path.join(root, file)
            try:
                with open(filepath, "r", encoding="utf8", errors="ignore") as f:
                    lines = f.readlines()
            except:
                continue

            for i, line in enumerate(lines, start=1):
                # revisar SQL patterns
                for pattern in SQL_BACKUP_PATTERNS:
                    if re.search(pattern, line, re.IGNORECASE):
                        results.append({
                            "file": filepath,
                            "line": i,
                            "pattern": pattern,
                            "text": line.strip()
                        })
                # revisar Python patterns
                for pattern in PYTHON_BACKUP_PATTERNS:
                    if re.search(pattern, line, re.IGNORECASE):
                        results.append({
                            "file": filepath,
                            "line": i,
                            "pattern": pattern,
                            "text": line.strip()
                        })

with open(OUTPUT, "w", encoding="utf8") as out:
    out.write("# TRUE BACKUP OPERATIONS AUDIT\n\n")

    if not results:
        out.write("No se encontraron operaciones reales de duplicado/respaldo.\n")
    else:
        for r in results:
            out.write(f"### {r['file']}\n")
            out.write(f"- Línea {r['line']}\n")
            out.write(f"- Coincidencia: `{r['pattern']}`\n")
            out.write(f"- Código: `{r['text']}`\n\n")

print(f"Auditoría completada → {OUTPUT}")
