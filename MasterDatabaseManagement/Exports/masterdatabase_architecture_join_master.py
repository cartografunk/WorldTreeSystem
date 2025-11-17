"""
masterdatabase_architecture_join_master.py
------------------------------------------
Audita las Dependencies (FKs, vistas, triggers y joins manuales)
del esquema masterdatabase y genera un join maestro exportado
a CSV y Excel ULTRA-COMPLETO para presentaciones ejecutivas.

Autor: cartografunk
Fecha: 2025-11-11
Ubicación: Querétaro, MX
"""

from core.db import get_engine
from core.libs import pd, text, Path
from core.paths import DATABASE_EXPORTS_DIR, safe_mkdir
import xlsxwriter
from datetime import datetime
import shutil
import json

# === CONFIG ===
SCHEMA = "masterdatabase"
EXPORT_DIR = Path(DATABASE_EXPORTS_DIR)
BACKUP_DIR = EXPORT_DIR / "backups"
safe_mkdir(EXPORT_DIR)
safe_mkdir(BACKUP_DIR)

# Límite de backups (rotación)
MAX_BACKUPS = 10

# Mapa de colores por tipo de relación (COLORES CORPORATIVOS)
COLOR_MAP = {
    "physical": "#88ad21",  # Verde secundario
    "logical": "#3668ba",   # Azul corporativo
    "trigger": "#baa61a",   # Amarillo acento
    "manual": "#313233"     # Gris oscuro
}

# === FUNCIONES AUXILIARES ===

def rotate_backups(prefix: str):
    """Mantiene solo los últimos MAX_BACKUPS archivos."""
    backups = sorted(BACKUP_DIR.glob(f"{prefix}_*.csv"), key=lambda x: x.stat().st_mtime)
    while len(backups) >= MAX_BACKUPS:
        oldest = backups.pop(0)
        oldest.unlink()
        print(f"🗑️  Backup eliminado: {oldest.name}")

def backup_previous_export():
    """Respalda el último export antes de sobreescribirlo."""
    csv_path = EXPORT_DIR / f"{SCHEMA}_join_master.csv"
    if csv_path.exists():
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        backup_path = BACKUP_DIR / f"{SCHEMA}_join_master_{timestamp}.csv"
        shutil.copy2(csv_path, backup_path)
        print(f"💾 Backup creado: {backup_path.name}")
        rotate_backups(f"{SCHEMA}_join_master")

def create_sql_view(engine):
    """Crea o reemplaza la vista SQL vw_master_join_dependencies."""
    view_sql = f"""
    CREATE OR REPLACE VIEW {SCHEMA}.vw_master_join_dependencies AS
    
    -- Physical FK relationships
    SELECT 
        conrelid::regclass::text AS parent_table,
        confrelid::regclass::text AS child_table,
        conname AS link_name,
        'physical' AS relation_type,
        '{SCHEMA}'::text AS schema_name,
        CURRENT_DATE AS last_audit,
        0 AS dependency_depth
    FROM pg_constraint
    WHERE contype = 'f'
      AND connamespace::regnamespace::text = '{SCHEMA}'
    
    UNION ALL
    
    -- Logical view dependencies
    SELECT 
        view_name AS parent_table,
        table_name AS child_table,
        'view_dependency' AS link_name,
        'logical' AS relation_type,
        '{SCHEMA}'::text AS schema_name,
        CURRENT_DATE AS last_audit,
        1 AS dependency_depth
    FROM information_schema.view_table_usage
    WHERE view_schema = '{SCHEMA}'
    
    UNION ALL
    
    -- Trigger relationships
    SELECT 
        event_object_table AS parent_table,
        trigger_name AS child_table,
        trigger_name AS link_name,
        'trigger' AS relation_type,
        '{SCHEMA}'::text AS schema_name,
        CURRENT_DATE AS last_audit,
        2 AS dependency_depth
    FROM information_schema.triggers
    WHERE trigger_schema = '{SCHEMA}'
    
    UNION ALL
    
    -- Manual relationships (si existe la tabla)
    SELECT 
        parent_table, 
        child_table, 
        COALESCE(link_name, 'manual_join') AS link_name,
        'manual' AS relation_type,
        '{SCHEMA}'::text AS schema_name,
        CURRENT_DATE AS last_audit,
        3 AS dependency_depth
    FROM {SCHEMA}.relationships_manual
    WHERE EXISTS (
        SELECT 1 FROM information_schema.tables 
        WHERE table_schema = '{SCHEMA}' AND table_name = 'relationships_manual'
    );
    """

    with engine.begin() as conn:
        conn.execute(text(view_sql))
    print(f"✅ Vista SQL creada: {SCHEMA}.vw_master_join_dependencies")

def generate_metrics(df: pd.DataFrame) -> dict:
    """Calcula métricas del grafo de Dependencies."""
    metrics = {
        "total_dependencies": len(df),
        "by_type": df["relation_type"].value_counts().to_dict(),
        "unique_parents": df["parent_table"].nunique(),
        "unique_children": df["child_table"].nunique(),
        "most_connected_parent": df["parent_table"].mode()[0] if len(df) > 0 else None,
        "most_connected_child": df["child_table"].mode()[0] if len(df) > 0 else None,
        "avg_depth": float(df["dependency_depth"].mean()) if len(df) > 0 else 0,
        "timestamp": datetime.now().isoformat()
    }
    return metrics

def export_metrics(metrics: dict):
    """Exporta métricas a JSON."""
    metrics_path = EXPORT_DIR / f"{SCHEMA}_metrics.json"
    with open(metrics_path, 'w', encoding='utf-8') as f:
        json.dump(metrics, f, indent=2, ensure_ascii=False)
    print(f"📊 Métricas exportadas: {metrics_path}")

def create_dependency_matrix(df: pd.DataFrame) -> pd.DataFrame:
    """Crea matriz de Dependencies table-to-table."""
    all_tables = sorted(set(df["parent_table"]).union(set(df["child_table"])))
    matrix = pd.DataFrame(0, index=all_tables, columns=all_tables)

    for _, row in df.iterrows():
        if row["parent_table"] in matrix.index and row["child_table"] in matrix.columns:
            matrix.loc[row["parent_table"], row["child_table"]] += 1

    return matrix

def create_connection_summary(df: pd.DataFrame) -> pd.DataFrame:
    """Resumen de conexiones por tabla."""
    parent_counts = df.groupby("parent_table").size().reset_index(name="outgoing_links")
    child_counts = df.groupby("child_table").size().reset_index(name="incoming_links")

    summary = pd.merge(
        parent_counts,
        child_counts,
        left_on="parent_table",
        right_on="child_table",
        how="outer"
    ).fillna(0)

    summary["table_name"] = summary["parent_table"].combine_first(summary["child_table"])
    summary["total_connections"] = summary["outgoing_links"] + summary["incoming_links"]
    summary = summary[["table_name", "outgoing_links", "incoming_links", "total_connections"]]
    summary = summary.sort_values("total_connections", ascending=False)

    return summary.astype({"outgoing_links": int, "incoming_links": int, "total_connections": int})

# === CONSULTAS ===

def extract_dependencies(engine, filter_type=None):
    """Extrae todas las Dependencies con filtro opcional."""
    queries = {
        "physical": f"""
            SELECT 
                conrelid::regclass::text AS parent_table,
                confrelid::regclass::text AS child_table,
                conname AS link_name,
                'physical' AS relation_type
            FROM pg_constraint
            WHERE contype = 'f'
              AND connamespace::regnamespace::text = '{SCHEMA}'
        """,
        "logical": f"""
            SELECT 
                view_name AS parent_table,
                table_name AS child_table,
                'view_dependency' AS link_name,
                'logical' AS relation_type
            FROM information_schema.view_table_usage
            WHERE view_schema = '{SCHEMA}'
        """,
        "trigger": f"""
            SELECT 
                event_object_table AS parent_table,
                trigger_name AS child_table,
                trigger_name AS link_name,
                'trigger' AS relation_type
            FROM information_schema.triggers
            WHERE trigger_schema = '{SCHEMA}'
        """,
        "manual": f"""
            SELECT 
                parent_table, 
                child_table, 
                COALESCE(link_name, 'manual_join') AS link_name,
                'manual' AS relation_type
            FROM {SCHEMA}.relationships_manual
        """
    }

    if filter_type:
        queries = {k: v for k, v in queries.items() if k == filter_type}

    dfs = []
    with engine.begin() as conn:
        for rel_type, query in queries.items():
            try:
                df = pd.read_sql(text(query), conn)
                dfs.append(df)
            except Exception as e:
                if rel_type != "manual":
                    print(f"⚠️  Error en {rel_type}: {e}")

    return pd.concat(dfs, ignore_index=True) if dfs else pd.DataFrame()

# === EXCEL ULTRA-COMPLETO ===

def create_executive_excel(df_master: pd.DataFrame, metrics: dict, xlsx_path: Path):
    """Genera Excel ejecutivo con múltiples hojas y visualizaciones."""

    with pd.ExcelWriter(xlsx_path, engine="xlsxwriter") as writer:
        wb = writer.book

        # === FORMATOS (COLORES CORPORATIVOS) ===
        fmt_title = wb.add_format({
            "bold": True, "font_size": 18, "font_color": "#3668ba",
            "align": "left", "valign": "vcenter"
        })

        fmt_header = wb.add_format({
            "bold": True, "bg_color": "#1f1f1f", "font_color": "white",
            "border": 1, "align": "center", "valign": "vcenter"
        })

        fmt_metric_label = wb.add_format({
            "bold": True, "font_size": 11, "align": "right",
            "valign": "vcenter", "border": 1, "bg_color": "#e0e0e0"
        })

        fmt_metric_value = wb.add_format({
            "font_size": 11, "align": "left", "valign": "vcenter",
            "border": 1, "num_format": "#,##0"
        })

        fmt_legend = wb.add_format({
            "bold": True, "font_size": 10, "align": "center",
            "valign": "vcenter", "border": 1
        })

        # === HOJA 1: JOIN MAESTRO ===
        df_master.to_excel(writer, index=False, sheet_name="🔗 Join Master")
        ws_master = writer.sheets["🔗 Join Master"]

        # Formato header
        for col_num, value in enumerate(df_master.columns.values):
            ws_master.write(0, col_num, value, fmt_header)

        # Formato condicional por tipo
        for rel, color in COLOR_MAP.items():
            fmt = wb.add_format({"bg_color": color, "font_color": "white" if rel != "trigger" else "#1f1f1f"})
            ws_master.conditional_format(1, 3, len(df_master), 3, {
                "type": "cell", "criteria": "==", "value": f'"{rel}"', "format": fmt
            })

        # Autofiltro y anchos
        ws_master.autofilter(0, 0, len(df_master), len(df_master.columns) - 1)
        ws_master.set_column("A:B", 30)
        ws_master.set_column("C:D", 25)
        ws_master.set_column("E:H", 18)
        ws_master.freeze_panes(1, 0)

        # === HOJA 2: DASHBOARD ===
        # === HOJA 2: DASHBOARD ===
        ws_dash = wb.add_worksheet("📊 Dashboard")
        ws_dash.set_column("A:A", 3)
        ws_dash.set_column("B:B", 30)
        ws_dash.set_column("C:D", 20)

        # Título
        ws_dash.merge_range("B2:D2", f"DEPENDENCY AUDIT: {SCHEMA.upper()}", fmt_title)
        ws_dash.write("B3", f"Date: {datetime.now().strftime('%Y-%m-%d %H:%M')}")

        # Distribución por tipo
        row = 5
        ws_dash.merge_range(row, 1, row, 2, "DISTRIBUTION BY TYPE", fmt_header)
        row += 1

        for rel_type, count in metrics["by_type"].items():
            color_fmt = wb.add_format({
                "bg_color": COLOR_MAP[rel_type],
                "font_color": "white" if rel_type != "trigger" else "#1f1f1f",
                "bold": True, "align": "center", "border": 1
            })
            ws_dash.write(row, 1, rel_type.upper(), color_fmt)
            ws_dash.write(row, 2, count, fmt_metric_value)
            row += 1

        # Leyenda de colores
        row += 2
        ws_dash.merge_range(row, 1, row, 3, "COLOR LEGEND", fmt_header)
        row += 1

        legends = {
            "physical": "Physical Foreign Keys (referential integrity)",
            "logical": "Dependencies between views and tables",
            "trigger": "Functional relationships activated by triggers",
            "manual": "Manually documented joins"
        }

        for rel_type, description in legends.items():
            color_fmt = wb.add_format({
                "bg_color": COLOR_MAP[rel_type],
                "font_color": "white" if rel_type != "trigger" else "#1f1f1f",
                "bold": True, "align": "center", "border": 1
            })
            ws_dash.write(row, 1, rel_type.upper(), color_fmt)
            ws_dash.write(row, 2, description)
            row += 1

        # Gráfico de barras (si hay datos)
        if metrics["total_dependencies"] > 0:
            chart = wb.add_chart({"type": "column"})
            chart.add_series({
                "name": "Dependencies by Type",
                "categories": f"='📊 Dashboard'!$B${row-len(legends)+1}:$B${row}",
                "values": f"='📊 Dashboard'!$C${row-len(legends)+1}:$C${row}",
                "fill": {"color": "#2196F3"}
            })
            chart.set_title({"name": "Dependency Distribution"})
            chart.set_x_axis({"name": "Relationship Type"})
            chart.set_y_axis({"name": "Count"})
            chart.set_style(11)
            ws_dash.insert_chart(f"E5", chart, {"x_scale": 1.5, "y_scale": 1.5})

        # === HOJA 2: Dependencies DETALLADAS ===
        df_master.to_excel(writer, index=False, sheet_name="📋 Dependencies")
        ws_deps = writer.sheets["📋 Dependencies"]

        # Formato header
        for col_num, value in enumerate(df_master.columns.values):
            ws_deps.write(0, col_num, value, fmt_header)

        # Formato condicional por tipo
        for rel, color in COLOR_MAP.items():
            fmt = wb.add_format({"bg_color": color, "font_color": "white" if rel != "trigger" else "#1f1f1f"})
            ws_deps.conditional_format(1, 3, len(df_master), 3, {
                "type": "cell", "criteria": "==", "value": f'"{rel}"', "format": fmt
            })

        # Autofiltro y anchos
        ws_deps.autofilter(0, 0, len(df_master), len(df_master.columns) - 1)
        ws_deps.set_column("A:B", 30)
        ws_deps.set_column("C:D", 25)
        ws_deps.set_column("E:H", 18)
        ws_deps.freeze_panes(1, 0)

        # === HOJA 4: RESUMEN POR TABLA ===
        df_summary = create_connection_summary(df_master)
        df_summary.to_excel(writer, index=False, sheet_name="📌 Summary by Table")
        ws_summary = writer.sheets["📌 Summary by Table"]

        for col_num, value in enumerate(df_summary.columns.values):
            ws_summary.write(0, col_num, value, fmt_header)

        # Formato condicional para total_connections (escala de colores corporativos)
        ws_summary.conditional_format(1, 3, len(df_summary), 3, {
            "type": "3_color_scale",
            "min_color": "#e0e0e0",
            "mid_color": "#baa61a",
            "max_color": "#88ad21"
        })

        ws_summary.set_column("A:A", 35)
        ws_summary.set_column("B:D", 18)
        ws_summary.autofilter(0, 0, len(df_summary), len(df_summary.columns) - 1)
        ws_summary.freeze_panes(1, 0)

        # === HOJA 5: MATRIZ DE Dependencies ===
        df_matrix = create_dependency_matrix(df_master)

        # Solo crear si no es demasiado grande
        if len(df_matrix) <= 100:
            df_matrix.to_excel(writer, sheet_name="🔗 Matrix")
            ws_matrix = writer.sheets["🔗 Matrix"]

            # Formato para matriz
            for row_num in range(len(df_matrix)):
                for col_num in range(len(df_matrix.columns)):
                    value = df_matrix.iloc[row_num, col_num]
                    if value > 0:
                        fmt_cell = wb.add_format({
                            "bg_color": "#88ad21",
                            "align": "center",
                            "border": 1
                        })
                        ws_matrix.write(row_num + 1, col_num + 1, value, fmt_cell)

            ws_matrix.set_column("A:A", 25)
            ws_matrix.freeze_panes(1, 1)

        # === HOJA 6: INSTRUCCIONES ===
        ws_inst = wb.add_worksheet("📖 Instructions")
        ws_inst.set_column("A:A", 3)
        ws_inst.set_column("B:B", 80)

        instructions = [
            ("", ""),
            ("USER GUIDE - DEPENDENCY AUDIT", fmt_title),
            ("", ""),
            ("1. JOIN MASTER", fmt_metric_label),
            ("   - Complete unified join table with all dependencies", None),
            ("   - Use FILTERS in the first row to search", None),
            ("   - Colors indicate relationship type (see legend)", None),
            ("   - Main output for export and analysis", None),
            ("", ""),
            ("2. DASHBOARD", fmt_metric_label),
            ("   - Visual distribution of dependency types", None),
            ("   - Color legend for quick interpretation", None),
            ("   - Bar chart showing relationship breakdown", None),
            ("", ""),
            ("3. DEPENDENCIES", fmt_metric_label),
            ("   - Detailed list of all detected relationships", None),
            ("   - Same data as Join Master with enhanced formatting", None),
            ("", ""),
            ("4. SUMMARY BY TABLE", fmt_metric_label),
            ("   - Ranking of most connected tables", None),
            ("   - Outgoing: connections leaving the table", None),
            ("   - Incoming: connections arriving to the table", None),
            ("   - Color intensity = connection strength", None),
            ("", ""),
            ("5. MATRIX (if applicable)", fmt_metric_label),
            ("   - Cross-view table-to-table", None),
            ("   - Green indicates dependency exists", None),
            ("", ""),
            ("RELATIONSHIP TYPES:", fmt_header),
            ("", ""),
            ("🟢 PHYSICAL: Foreign Keys declared in DB", None),
            ("🔵 LOGICAL: Views that depend on tables", None),
            ("🟡 TRIGGER: Relationships via automatic triggers", None),
            ("⚫ MANUAL: Manually documented joins", None),
        ]

        for row, (text, fmt) in enumerate(instructions, start=1):
            if text and fmt:
                ws_inst.write(row, 1, text, fmt)
            elif text:
                ws_inst.write(row, 1, text)

    print(f"✅ Excel ejecutivo generado: {xlsx_path}")

# === MAIN ===

def main(filter_type=None):
    """
    Ejecuta el proceso completo de auditoría.

    Args:
        filter_type: 'physical', 'logical', 'trigger', 'manual' o None (todos)
    """
    print(f"\n{'='*60}")
    print(f"🔍 AUDITORÍA DE Dependencies: {SCHEMA}")
    print(f"{'='*60}\n")

    # Conectar
    engine = get_engine()

    # Backup del export anterior
    backup_previous_export()

    # Extraer Dependencies
    print("📥 Extrayendo Dependencies...")
    df_master = extract_dependencies(engine, filter_type)

    if df_master.empty:
        print("⚠️  No se encontraron Dependencies.")
        return

    # Enriquecer datos
    df_master["schema_name"] = SCHEMA
    df_master["last_audit"] = pd.Timestamp.now().normalize()
    df_master["dependency_depth"] = df_master["relation_type"].map({
        "physical": 0, "logical": 1, "trigger": 2, "manual": 3
    }).fillna(99).astype(int)

    # Ordenar
    df_master = df_master.sort_values(["dependency_depth", "parent_table"])

    # Métricas
    metrics = generate_metrics(df_master)
    export_metrics(metrics)

    print(f"\n📊 RESUMEN:")
    print(f"  Total Dependencies: {metrics['total_dependencies']}")
    print(f"  Por tipo: {metrics['by_type']}")
    print(f"  Profundidad promedio: {metrics['avg_depth']:.2f}")

    # Export CSV
    csv_path = EXPORT_DIR / f"{SCHEMA}_join_master.csv"
    df_master.to_csv(csv_path, index=False, encoding="utf-8-sig")
    print(f"\n✅ CSV: {csv_path}")

    # Export Excel EJECUTIVO
    xlsx_path = EXPORT_DIR / f"{SCHEMA}_join_master.xlsx"
    create_executive_excel(df_master, metrics, xlsx_path)

    # Crear vista SQL
    try:
        create_sql_view(engine)
    except Exception as e:
        print(f"⚠️  Error creando vista SQL: {e}")

    print(f"\n{'='*60}")
    print("✨ PROCESO COMPLETADO")
    print(f"{'='*60}\n")

if __name__ == "__main__":
    # Ejecutar auditoría completa
    main()

    # Ejemplos de uso con filtros:
    # main(filter_type="physical")  # Solo FKs
    # main(filter_type="logical")   # Solo vistas