"""
CruisesProcessorHybrid - Enhanced Version
=========================================
Procesa campañas anuales de inventario forestal con integración híbrida:
- Raw data (CSV/Excel crudos) como scaffolding estructural
- Human-validated Excel como fuente de verdad (priority override)
- QA checks especializados
- Outputs estandarizados por país-año

Pipeline:
1. Discover countries & validated files
2. Load raw inputs
3. Load validated Excel (priority)
4. Standardize validated data
5. Merge raw + validated (validated wins)
6. Compute inventory metrics
7. QA checks
8. Export country-year outputs
"""

import pandas as pd
import numpy as np
from pathlib import Path
import argparse
from tqdm import tqdm
import re
import warnings
import json
from datetime import datetime
from typing import Dict, List, Tuple, Optional

warnings.filterwarnings('ignore', category=UserWarning, module='openpyxl')

# ============================================================================
# CONFIGURATION
# ============================================================================

INVENTORY_YEAR = 2024

INPUT_DIRECTORIES = {
    "CR": r"C:\Users\HeyCe\World Tree Technologies Inc\Forest Inventory - Documentos\Costa Rica\2023_ForestInventory\3-WT Cruises",
    "GT": r"C:\Users\HeyCe\World Tree Technologies Inc\Forest Inventory - Documentos\Guatemala\2023_ForestInventory\3-WT Cruises",
    "US": r"C:\Users\HeyCe\World Tree Technologies Inc\Forest Inventory - Documentos\USA\2023_ForestInventory\3-WT Cruises",
    "US_contractor": r"C:\Users\HeyCe\World Tree Technologies Inc\Forest Inventory - Documentos\USA\2023_ForestInventory\4-Contractor Cruises",
    "MX": r"C:\Users\HeyCe\World Tree Technologies Inc\Forest Inventory - Documentos\Mexico\2023_ForestInventory\3-WT Cruises"
}

VALIDATED_EXCEL_PATTERN = r"inventory_([A-Z]{2,3})_(\d{4})\.xlsx"

# ============================================================================
# STEP 1: DISCOVER COUNTRIES & VALIDATED FILES
# ============================================================================

def discover_countries_and_validated(base_paths: Dict[str, str]) -> Dict:
    """
    Detecta países disponibles y sus Excel validados

    Returns:
        {
            'CR': {
                'raw_path': Path,
                'validated_excel': Path or None,
                'has_validated': bool
            },
            ...
        }
    """
    discovered = {}

    print("\n📍 STEP 1: Discovering countries and validated files")
    print("=" * 60)

    for country, path_str in base_paths.items():
        path = Path(path_str)

        if not path.exists():
            print(f"⚠️  {country}: Path not found - {path}")
            continue

        # Buscar Excel validado en la carpeta
        validated_excel = None
        pattern = f"inventory_{country}_{INVENTORY_YEAR}.xlsx"

        for file in path.parent.rglob(pattern):
            if not file.name.startswith("~$"):
                validated_excel = file
                break

        discovered[country] = {
            'raw_path': path,
            'validated_excel': validated_excel,
            'has_validated': validated_excel is not None
        }

        status = "✅ Validated found" if validated_excel else "📁 Raw only"
        print(f"{status}: {country}")
        if validated_excel:
            print(f"   → {validated_excel.name}")

    return discovered


# ============================================================================
# STEP 2: LOAD RAW INPUTS (from original merge logic)
# ============================================================================

def extract_contract_from_filename(filename: str) -> str:
    """Extrae contract_code del nombre del archivo"""
    match = re.search(r'([A-Z]{2,3}\d{4})', filename.upper())
    if match:
        return match.group(1)
    return Path(filename).stem


def read_input_sheet(file_path: Path, sheet_name: str) -> Optional[pd.DataFrame]:
    """Lee una hoja Input o Input(2)"""
    try:
        df = pd.read_excel(file_path, sheet_name=sheet_name, header=0)
        df = df.dropna(how='all').dropna(axis=1, how='all')
        return df if not df.empty else None
    except:
        return None


def get_best_sheet(file_path: Path) -> Tuple[Optional[pd.DataFrame], Optional[str]]:
    """
    Retorna el mejor sheet disponible.
    Priority: Input (2) > Input
    """
    df_input2 = read_input_sheet(file_path, "Input (2)")
    if df_input2 is not None:
        return df_input2, "Input (2)"

    df_input = read_input_sheet(file_path, "Input")
    if df_input is not None:
        return df_input, "Input"

    return None, None


def load_raw_inputs(raw_path: Path, country: str) -> pd.DataFrame:
    """
    Carga todos los Excel crudos de una carpeta (merge original)
    """
    print(f"\n📂 STEP 2: Loading raw inputs for {country}")

    all_files = [
        f for f in raw_path.rglob("*.xlsx")
        if not f.name.startswith("~$")
           and "combined" not in f.name.lower()
           and "merged" not in f.name.lower()
           and "plantilla" not in f.name.lower()
           and "inventory_" not in f.name.lower()
    ]

    print(f"   Found {len(all_files)} raw Excel files")

    if not all_files:
        return pd.DataFrame()

    all_data = []

    for file_path in tqdm(all_files, desc=f"  Loading {country}", unit="file"):
        contract_code = extract_contract_from_filename(file_path.name)
        df, sheet_name = get_best_sheet(file_path)

        if df is None:
            continue

        # Add metadata
        df.insert(0, "contract_code", contract_code)
        df.insert(1, "source_file", file_path.name)
        df.insert(2, "source_sheet", sheet_name)
        df.insert(3, "data_source", "raw")

        all_data.append(df)

    if not all_data:
        return pd.DataFrame()

    merged = pd.concat(all_data, ignore_index=True)
    print(f"   ✅ Loaded {len(merged):,} raw records")

    return merged


# ============================================================================
# STEP 3: LOAD VALIDATED EXCEL (Human-validated, priority override)
# ============================================================================

def load_validated_excel(validated_path: Path, country: str) -> pd.DataFrame:
    """
    Carga Excel validado por humano.
    REGLA CRÍTICA: No modificar nombres ni valores - es el oráculo.
    """
    print(f"\n📋 STEP 3: Loading validated Excel for {country}")
    print(f"   Source: {validated_path.name}")

    try:
        df = pd.read_excel(validated_path, sheet_name=0)
        df = df.dropna(how='all').dropna(axis=1, how='all')

        # Marcar como validado
        if 'data_source' not in df.columns:
            df.insert(0, 'data_source', 'validated')

        print(f"   ✅ Loaded {len(df):,} validated records")
        print(f"   Columns: {', '.join(df.columns[:10].tolist())}...")

        return df

    except Exception as e:
        print(f"   ❌ Error loading validated Excel: {e}")
        return pd.DataFrame()


# ============================================================================
# STEP 4: STANDARDIZE VALIDATED DATA
# ============================================================================

def standardize_validated_data(df: pd.DataFrame) -> pd.DataFrame:
    """
    Estandariza tipos y unidades respetando valores del Excel validado
    """
    print(f"\n🔧 STEP 4: Standardizing validated data")

    df = df.copy()

    # Convertir columnas numéricas esperadas
    numeric_cols = ['dbh', 'tht', 'height', 'volume', 'basal_area']

    for col in numeric_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce')

    # Asegurar que hay identificadores básicos
    if 'tree_id' not in df.columns and 'id' in df.columns:
        df['tree_id'] = df['id']

    print(f"   ✅ Standardized {len(df):,} records")

    return df


# ============================================================================
# STEP 5: MERGE RAW + VALIDATED (Validated wins)
# ============================================================================

def merge_raw_and_validated(raw_df: pd.DataFrame,
                            validated_df: pd.DataFrame,
                            country: str) -> pd.DataFrame:
    """
    Combina raw + validated con precedencia a validated.

    Strategy:
    - Si existe validated: usar 100% validated (raw solo como backup estructural)
    - Si no existe validated: usar raw
    """
    print(f"\n🔀 STEP 5: Merging raw + validated for {country}")

    if validated_df.empty:
        print("   ℹ️  No validated data - using raw only")
        return raw_df

    if raw_df.empty:
        print("   ℹ️  No raw data - using validated only")
        return validated_df

    # PRECEDENCE: validated overrides raw
    print(f"   📊 Raw records: {len(raw_df):,}")
    print(f"   📋 Validated records: {len(validated_df):,}")
    print(f"   🎯 Using validated as primary source")

    # Use validated as base, append any raw contracts not in validated
    validated_contracts = set(validated_df.get('contract_code', pd.Series()).unique())
    raw_only = raw_df[~raw_df['contract_code'].isin(validated_contracts)] if 'contract_code' in raw_df.columns else pd.DataFrame()

    if not raw_only.empty:
        print(f"   ➕ Adding {len(raw_only):,} records from raw (non-validated contracts)")
        merged = pd.concat([validated_df, raw_only], ignore_index=True)
    else:
        merged = validated_df

    print(f"   ✅ Final dataset: {len(merged):,} records")

    return merged


# ============================================================================
# STEP 6: COMPUTE INVENTORY METRICS
# ============================================================================

def compute_inventory_metrics(df: pd.DataFrame) -> pd.DataFrame:
    """
    Calcula métricas estándar de inventario
    """
    print(f"\n📐 STEP 6: Computing inventory metrics")

    df = df.copy()

    # Basal area (m²) from DBH (cm)
    if 'dbh' in df.columns and 'basal_area' not in df.columns:
        df['basal_area'] = np.pi * (df['dbh'] / 200) ** 2

    # Volume estimation (simplified - replace with actual formula)
    if 'volume' not in df.columns and 'dbh' in df.columns and 'tht' in df.columns:
        df['volume'] = df['basal_area'] * df['tht'] * 0.5  # Form factor approximation

    print(f"   ✅ Computed metrics for {len(df):,} records")

    return df


# ============================================================================
# STEP 7: QA CHECKS
# ============================================================================

def run_qa_checks(df: pd.DataFrame, country: str) -> Dict:
    """
    Ejecuta validaciones QA especializadas
    """
    print(f"\n🔍 STEP 7: Running QA checks for {country}")

    qa_report = {
        'country': country,
        'timestamp': datetime.now().isoformat(),
        'total_records': len(df),
        'issues': {}
    }

    # Check 1: Missing DBH
    if 'dbh' in df.columns:
        missing_dbh = df['dbh'].isna().sum()
        qa_report['issues']['missing_dbh'] = {
            'count': int(missing_dbh),
            'percentage': round(missing_dbh / len(df) * 100, 2)
        }

    # Check 2: THT out of range
    if 'tht' in df.columns:
        tht_out_of_range = ((df['tht'] < 0) | (df['tht'] > 50)).sum()
        qa_report['issues']['tht_out_of_range'] = {
            'count': int(tht_out_of_range),
            'percentage': round(tht_out_of_range / len(df) * 100, 2)
        }

    # Check 3: Zero volume suspicious
    if 'volume' in df.columns:
        zero_volume = (df['volume'] == 0).sum()
        qa_report['issues']['zero_volume'] = {
            'count': int(zero_volume),
            'percentage': round(zero_volume / len(df) * 100, 2)
        }

    # Check 4: Duplicates
    if 'tree_id' in df.columns:
        duplicates = df['tree_id'].duplicated().sum()
        qa_report['issues']['duplicate_tree_ids'] = {
            'count': int(duplicates),
            'percentage': round(duplicates / len(df) * 100, 2)
        }

    # Summary
    total_issues = sum(issue['count'] for issue in qa_report['issues'].values())
    qa_report['summary'] = {
        'total_issues': total_issues,
        'data_quality_score': round((1 - total_issues / (len(df) * 4)) * 100, 2)
    }

    print(f"   ✅ QA Score: {qa_report['summary']['data_quality_score']}%")
    print(f"   Issues found: {total_issues:,}")

    return qa_report


# ============================================================================
# DATA QUALITY DETECTOR (Excel type mismatches)
# ============================================================================

def detect_mixed_types(df: pd.DataFrame, include_location: bool = False) -> Dict:
    """
    Detecta columnas con tipos mixtos causados por errores de Excel.
    Retorna reporte detallado para corrección manual.

    Args:
        df: DataFrame a analizar
        include_location: Si True, incluye country/source_file para rastrear errores
    """
    mixed_type_report = {}

    for col in df.columns:
        if df[col].dtype == 'object':
            # Detectar tipos únicos en la columna
            types_found = df[col].dropna().apply(type).unique()

            if len(types_found) > 1:
                # Hay tipos mixtos!
                type_counts = df[col].dropna().apply(lambda x: type(x).__name__).value_counts()

                # Encontrar ejemplos de cada tipo
                examples = {}
                for type_name in type_counts.index:
                    sample = df[col].dropna()[df[col].dropna().apply(lambda x: type(x).__name__ == type_name)].head(3).tolist()
                    examples[type_name] = sample

                # Identificar filas con datetime (probable error)
                datetime_mask = df[col].apply(lambda x: isinstance(x, pd.Timestamp) or isinstance(x, datetime))
                datetime_rows = df[datetime_mask].index.tolist()

                error_info = {
                    'type_counts': type_counts.to_dict(),
                    'examples': examples,
                    'datetime_error_rows': datetime_rows[:10],  # Primeras 10 filas con error
                    'total_datetime_errors': len(datetime_rows)
                }

                # Si se pidió ubicación, agregar detalles de country/file
                if include_location and len(datetime_rows) > 0:
                    error_locations = []
                    for idx in datetime_rows[:10]:  # Primeras 10
                        location = {
                            'row_index': int(idx),
                            'value': str(df.loc[idx, col])
                        }
                        if 'country' in df.columns:
                            location['country'] = str(df.loc[idx, 'country'])
                        if 'source_file' in df.columns:
                            location['source_file'] = str(df.loc[idx, 'source_file'])
                        if 'contract_code' in df.columns:
                            location['contract_code'] = str(df.loc[idx, 'contract_code'])
                        error_locations.append(location)

                    error_info['error_locations'] = error_locations

                mixed_type_report[col] = error_info

    return mixed_type_report


def sanitize_for_parquet(df: pd.DataFrame, verbose: bool = True) -> Tuple[pd.DataFrame, Dict]:
    """
    Limpia DataFrame para exportación a Parquet.
    Retorna: (df_clean, mixed_types_report)
    """
    df = df.copy()
    mixed_types_report = detect_mixed_types(df)

    if verbose and mixed_types_report:
        print(f"\n   ⚠️  MIXED TYPES DETECTED - Excel format errors:")
        for col, info in mixed_types_report.items():
            print(f"   📊 Column: '{col}'")
            for type_name, count in info['type_counts'].items():
                print(f"      - {type_name}: {count} values")
            if info['total_datetime_errors'] > 0:
                print(f"      🔴 {info['total_datetime_errors']} datetime errors (should be numeric)")
                print(f"      📍 Error rows: {info['datetime_error_rows']}")

    # Limpiar columnas con tipos mixtos
    for col in df.columns:
        if df[col].dtype == 'object':
            try:
                # Intentar convertir a numérico primero (sin el deprecated 'ignore')
                numeric_attempt = pd.to_numeric(df[col], errors='coerce')

                # Si encontró al menos algunos números, usar esa conversión
                if numeric_attempt.notna().sum() > 0:
                    df[col] = numeric_attempt
                else:
                    # Si no hay números, convertir todo a string
                    df[col] = df[col].apply(
                        lambda x: str(x) if not pd.isna(x) else None
                    )
            except:
                df[col] = df[col].astype(str).replace('nan', None)

    return df, mixed_types_report


# ============================================================================
# STEP 8: EXPORT COUNTRY-YEAR OUTPUTS
# ============================================================================

def export_country_year_outputs(df: pd.DataFrame,
                                qa_report: Dict,
                                country: str,
                                output_dir: Path):
    """
    Exporta outputs estandarizados por país-año
    """
    print(f"\n💾 STEP 8: Exporting outputs for {country}_{INVENTORY_YEAR}")

    country_dir = output_dir / f"inventory_{country}_{INVENTORY_YEAR}"
    country_dir.mkdir(parents=True, exist_ok=True)

    # Detectar errores de tipos mixtos para reportar
    mixed_types_report = detect_mixed_types(df)

    if mixed_types_report:
        print(f"\n   ⚠️  MIXED TYPES DETECTED - Excel format errors:")
        for col, info in mixed_types_report.items():
            print(f"   📊 Column: '{col}'")
            for type_name, count in info['type_counts'].items():
                print(f"      - {type_name}: {count} values")
            if info['total_datetime_errors'] > 0:
                print(f"      🔴 {info['total_datetime_errors']} datetime errors (should be numeric)")
                print(f"      📍 Error rows: {info['datetime_error_rows']}")

        # Agregar al QA report (serializable)
        serializable_excel_errors = {}
        for col, info in mixed_types_report.items():
            serializable_excel_errors[col] = {
                'type_counts': info['type_counts'],
                'examples': {k: [str(v) for v in vals] for k, vals in info['examples'].items()},
                'datetime_error_rows': info['datetime_error_rows'],
                'total_datetime_errors': info['total_datetime_errors']
            }
        qa_report['excel_format_errors'] = serializable_excel_errors

    # 1. Inventory final (Excel - main output)
    excel_path = country_dir / "inventory_final.xlsx"
    df.to_excel(excel_path, index=False, engine='openpyxl')
    print(f"   ✅ {excel_path.name} ({len(df):,} records)")

    # 2. QA Report (JSON)
    qa_path = country_dir / "qa_report.json"
    with open(qa_path, 'w') as f:
        json.dump(qa_report, f, indent=2)
    print(f"   ✅ {qa_path.name}")

    # 3. Summary stats (CSV) - FIXED: construir dict dinámicamente
    if 'contract_code' in df.columns:
        # Construir diccionario de agregación solo con columnas existentes
        agg_dict = {}

        if 'tree_id' in df.columns:
            agg_dict['tree_id'] = 'count'

        if 'dbh' in df.columns:
            agg_dict['dbh'] = 'mean'

        if 'volume' in df.columns:
            agg_dict['volume'] = 'sum'

        # Solo crear summary si hay algo que agregar
        if agg_dict:
            summary = df.groupby('contract_code').agg(agg_dict).round(2)
            summary_path = country_dir / "summary_by_contract.csv"
            summary.to_csv(summary_path)
            print(f"   ✅ {summary_path.name}")
        else:
            print(f"   ⚠️  Skipping summary - no aggregatable columns found")

    print(f"\n   📁 All outputs saved to: {country_dir}")


# ============================================================================
# MAIN PIPELINE
# ============================================================================

def process_country(country: str,
                    country_info: Dict,
                    output_dir: Path) -> bool:
    """
    Procesa un país completo siguiendo el pipeline de 8 pasos
    """
    print(f"\n{'='*60}")
    print(f"🌎 PROCESSING: {country}")
    print(f"{'='*60}")

    # Step 2: Load raw
    raw_df = load_raw_inputs(country_info['raw_path'], country)

    # Step 3: Load validated (if exists)
    validated_df = pd.DataFrame()
    if country_info['has_validated']:
        validated_df = load_validated_excel(country_info['validated_excel'], country)

        # Step 4: Standardize validated
        if not validated_df.empty:
            validated_df = standardize_validated_data(validated_df)

    # Step 5: Merge
    merged_df = merge_raw_and_validated(raw_df, validated_df, country)

    if merged_df.empty:
        print(f"   ⚠️  No data available for {country}")
        return False

    # Step 6: Compute metrics
    final_df = compute_inventory_metrics(merged_df)

    # Step 7: QA checks
    qa_report = run_qa_checks(final_df, country)

    # Step 8: Export
    export_country_year_outputs(final_df, qa_report, country, output_dir)

    return True


def main():
    parser = argparse.ArgumentParser(
        description="CruisesProcessorHybrid - Enhanced annual inventory processor"
    )
    parser.add_argument(
        "--output",
        default="./public",
        help="Output directory for country-year inventories"
    )
    parser.add_argument(
        "--countries",
        nargs="+",
        help="Specific countries to process (default: all available)"
    )

    args = parser.parse_args()

    print("🌳 CruisesProcessorHybrid - Enhanced Version")
    print("=" * 60)
    print(f"Inventory Year: {INVENTORY_YEAR}")
    print(f"Output Directory: {args.output}")
    print("=" * 60)

    # Step 1: Discover
    discovered = discover_countries_and_validated(INPUT_DIRECTORIES)

    if not discovered:
        print("❌ No countries discovered")
        return

    # Filter countries if specified
    if args.countries:
        discovered = {k: v for k, v in discovered.items() if k in args.countries}

    # Process each country
    output_dir = Path(args.output)
    output_dir.mkdir(parents=True, exist_ok=True)

    results = {}
    all_country_data = []

    for country, info in discovered.items():
        success = process_country(country, info, output_dir)
        results[country] = success

        # Collect data for consolidated merge
        if success:
            country_file = output_dir / f"inventory_{country}_{INVENTORY_YEAR}" / "inventory_final.xlsx"
            if country_file.exists():
                df_country = pd.read_excel(country_file)
                df_country.insert(0, 'country', country)
                all_country_data.append(df_country)

    # Create consolidated inventory_merge file
    if all_country_data:
        print("\n" + "=" * 60)
        print("📦 Creating consolidated merge file")
        print("=" * 60)

        consolidated_df = pd.concat(all_country_data, ignore_index=True)

        # Detectar errores de formato en el consolidado (CON ubicación)
        print("\n🔍 Checking for Excel format errors in consolidated data...")
        consolidated_mixed_types = detect_mixed_types(consolidated_df, include_location=True)

        # Save in CruisesProcessorHybrid folder
        script_dir = Path(__file__).parent
        merge_filename = f"inventory_merge_{INVENTORY_YEAR}.xlsx"
        merge_path = script_dir / merge_filename

        print(f"\n   Creating Excel with multiple sheets...")

        # Crear Excel con múltiples hojas
        with pd.ExcelWriter(merge_path, engine='openpyxl') as writer:
            # Hoja 1: Datos principales
            consolidated_df.to_excel(writer, sheet_name='Inventory', index=False)
            print(f"   ✅ Sheet 1: Inventory ({len(consolidated_df):,} records)")

            # Hoja 2: Errores de formato Excel (datetime en columnas numéricas)
            if consolidated_mixed_types:
                error_records = []
                for col, info in consolidated_mixed_types.items():
                    if info['total_datetime_errors'] > 0:
                        # Obtener todas las filas con datetime en esta columna
                        datetime_mask = consolidated_df[col].apply(
                            lambda x: isinstance(x, pd.Timestamp) or isinstance(x, datetime)
                        )
                        error_rows = consolidated_df[datetime_mask].copy()
                        error_rows.insert(0, 'error_column', col)
                        error_rows.insert(1, 'error_value', error_rows[col].astype(str))
                        error_records.append(error_rows)

                if error_records:
                    errors_df = pd.concat(error_records, ignore_index=True)
                    # Reordenar columnas para poner los identificadores primero
                    id_cols = ['error_column', 'error_value', 'country', 'contract_code', 'source_file', 'source_sheet']
                    id_cols = [c for c in id_cols if c in errors_df.columns]
                    other_cols = [c for c in errors_df.columns if c not in id_cols]
                    errors_df = errors_df[id_cols + other_cols]
                    errors_df.to_excel(writer, sheet_name='Excel_Format_Errors', index=False)
                    print(f"   ✅ Sheet 2: Excel_Format_Errors ({len(errors_df):,} records)")

            # Hoja 3: Contratos source sheet tracker (Input vs Input (2))
            if 'source_sheet' in consolidated_df.columns and 'contract_code' in consolidated_df.columns:
                # Usar size() en lugar de agg para evitar conflictos de columnas
                sheet_summary = consolidated_df.groupby(
                    ['country', 'contract_code', 'source_sheet', 'source_file']).size().reset_index(
                    name='record_count')
                sheet_summary = sheet_summary.sort_values(['country', 'contract_code'])
                sheet_summary.to_excel(writer, sheet_name='Source_Sheet_Tracker', index=False)
                print(f"   ✅ Sheet 3: Source_Sheet_Tracker ({len(sheet_summary):,} contracts)")

        print(f"\n   📊 Total records: {len(consolidated_df):,}")
        print(f"   🌎 Countries: {', '.join(sorted(consolidated_df['country'].unique()))}")
        print(f"   📁 Saved to: {merge_path.absolute()}")

        # Guardar reporte de errores de formato Excel
        if consolidated_mixed_types:
            excel_errors_path = script_dir / f"excel_format_errors_{INVENTORY_YEAR}.json"

            # Hacer el reporte serializable
            serializable_report = {}
            for col, info in consolidated_mixed_types.items():
                serializable_report[col] = {
                    'type_counts': info['type_counts'],
                    'examples': {k: [str(v) for v in vals] for k, vals in info['examples'].items()},
                    'datetime_error_rows': info['datetime_error_rows'],
                    'total_datetime_errors': info['total_datetime_errors']
                }
                # Agregar ubicaciones si existen
                if 'error_locations' in info:
                    serializable_report[col]['error_locations'] = info['error_locations']

            with open(excel_errors_path, 'w') as f:
                json.dump({
                    'timestamp': datetime.now().isoformat(),
                    'inventory_year': INVENTORY_YEAR,
                    'total_columns_with_errors': len(serializable_report),
                    'errors_by_column': serializable_report
                }, f, indent=2)

            print(f"\n   🔴 EXCEL FORMAT ERRORS DETECTED!")
            print(f"   📋 {len(consolidated_mixed_types)} columns with mixed types")
            print(f"   📄 Error report: {excel_errors_path.name}")
            print(f"\n   💡 These errors should be fixed in source Excel files:")
            for col in list(consolidated_mixed_types.keys())[:5]:
                print(f"      - {col}")

    # Final summary
    print("\n" + "=" * 60)
    print("🏁 PROCESSING COMPLETE")
    print("=" * 60)

    for country, success in results.items():
        status = "✅" if success else "❌"
        print(f"{status} {country}")

    print(f"\n📂 Country-specific outputs: {output_dir.absolute()}")
    if all_country_data:
        print(f"📦 Consolidated merge: {merge_path.absolute()}")


if __name__ == "__main__":
    main()