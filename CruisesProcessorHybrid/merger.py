"""
InventoryMerger - Enhanced Version with Schema Standardization
===============================================================
Procesa campañas anuales de inventario forestal con:
- Raw data (CSV/Excel crudos) como scaffolding estructural
- Estandarización de columnas usando schema_helpers
- QA checks especializados
- Outputs estandarizados por país-año

Pipeline:
1. Load config from JSON
2. Discover countries & files
3. Load raw inputs
4. Standardize columns using schema_helpers
5. Merge all countries
6. QA checks
7. Export consolidated merge
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

# Import schema helpers
import sys
sys.path.append(str(Path(__file__).parent.parent))
from core.schema_helpers import (
    rename_columns_using_schema,
    get_column,
    clean_column_name
)

warnings.filterwarnings('ignore', category=UserWarning, module='openpyxl')


# ============================================================================
# CONFIGURATION
# ============================================================================

def load_config(config_path: str = "inventory_config.json") -> Dict:
    """Carga configuración desde JSON"""
    config_file = Path(__file__).parent / config_path

    if not config_file.exists():
        print(f"⚠️  Config file not found: {config_path}")
        print(f"   Creating default config...")

        default_config = {
            "inventory_year": 2025,
            "input_directories": {},
            "output_directory": "./public"
        }

        with open(config_file, 'w') as f:
            json.dump(default_config, f, indent=2)

        return default_config

    with open(config_file, 'r') as f:
        config = json.load(f)

    return config


# ============================================================================
# STEP 1: DISCOVER FILES
# ============================================================================

def discover_raw_files(base_paths: Dict[str, str]) -> Dict:
    """
    Detecta archivos Excel disponibles por país
    """
    discovered = {}

    print("\n🔍 STEP 1: Discovering raw files")
    print("=" * 60)

    for country, path_str in base_paths.items():
        path = Path(path_str)

        if not path.exists():
            print(f"⚠️  {country}: Path not found - {path}")
            continue

        # Buscar Excel files (excluir temporales y merged)
        raw_files = [
            f for f in path.rglob("*.xlsx")
            if not f.name.startswith("~$")
               and "combined" not in f.name.lower()
               and "merged" not in f.name.lower()
               and "plantilla" not in f.name.lower()
               and "inventory_" not in f.name.lower()
               and "backup" not in f.name.lower()
        ]

        discovered[country] = {
            'raw_path': path,
            'files': raw_files,
            'file_count': len(raw_files)
        }

        print(f"✅ {country}: {len(raw_files)} files found")

    return discovered


# ============================================================================
# STEP 2: LOAD RAW INPUTS
# ============================================================================

def extract_contract_from_filename(filename: str) -> str:
    """Extrae contract_code del nombre del archivo"""
    match = re.search(r'([A-Z]{2,3}\d{4}[a-zA-Z]?)', filename.upper())
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


def load_raw_inputs(files: List[Path], country: str) -> pd.DataFrame:
    """
    Carga todos los Excel crudos de un país
    """
    print(f"\n📂 STEP 2: Loading raw inputs for {country}")

    if not files:
        print(f"   ⚠️  No files found")
        return pd.DataFrame()

    all_data = []

    for file_path in tqdm(files, desc=f"  Loading {country}", unit="file"):
        contract_code = extract_contract_from_filename(file_path.name)
        df, sheet_name = get_best_sheet(file_path)

        if df is None:
            continue

        # Add metadata
        df.insert(0, "contract_code", contract_code)
        df.insert(1, "source_file", file_path.name)
        df.insert(2, "source_sheet", sheet_name)
        df.insert(3, "country", country)

        all_data.append(df)

    if not all_data:
        return pd.DataFrame()

    merged = pd.concat(all_data, ignore_index=True)
    print(f"   ✅ Loaded {len(merged):,} raw records")

    return merged


# ============================================================================
# STEP 3: STANDARDIZE COLUMNS USING SCHEMA
# ============================================================================

def standardize_columns(df: pd.DataFrame, country: str) -> pd.DataFrame:
    """
    Estandariza nombres de columnas usando schema_helpers
    """
    print(f"\n🔧 STEP 3: Standardizing columns for {country}")

    df = df.copy()

    # Mostrar columnas originales
    print(f"   Original columns ({len(df.columns)}): {', '.join(df.columns[:10].tolist())}...")

    # Aplicar renombre usando schema
    df = rename_columns_using_schema(df)

    # Mostrar columnas después del renombre
    print(f"   Standardized columns ({len(df.columns)}): {', '.join(df.columns[:10].tolist())}...")

    return df


def drop_unknown_columns(df: pd.DataFrame) -> pd.DataFrame:
    """
    Elimina columnas que no están en el schema
    """
    # Obtener columnas conocidas del schema
    from core.schema import COLUMNS
    known_keys = {col["key"] for col in COLUMNS}
    known_sql = {col["sql_name"] for col in COLUMNS}
    known_aliases = set()
    for col in COLUMNS:
        known_aliases.update(col.get("aliases", []))

    all_known = known_keys | known_sql | known_aliases

    # Agregar columnas de metadata que queremos mantener
    metadata_cols = {'contract_code', 'source_file', 'source_sheet', 'country'}
    all_known = all_known | metadata_cols

    # Identificar columnas desconocidas
    unknown_cols = [col for col in df.columns if col not in all_known]

    if unknown_cols:
        print(f"   🗑️  Dropping {len(unknown_cols)} unknown columns: {', '.join(unknown_cols[:5])}...")
        df = df.drop(columns=unknown_cols)

    return df


# ============================================================================
# STEP 4: DATA TYPE CONVERSION
# ============================================================================

def convert_data_types(df: pd.DataFrame) -> pd.DataFrame:
    """
    Convierte tipos de datos según schema
    """
    print(f"\n🔄 STEP 4: Converting data types")

    df = df.copy()

    # Intentar convertir columnas numéricas conocidas
    numeric_cols = ['dbh_in', 'tht_ft', 'mht_ft', 'merch_ht_ft', 'defect_ht_ft', 'doyle_bf']

    for col in numeric_cols:
        try:
            # Buscar la columna usando get_column (maneja aliases)
            real_col = get_column(col, df)
            if real_col in df.columns:
                df[real_col] = pd.to_numeric(df[real_col], errors='coerce')
        except KeyError:
            # Columna no existe en este df
            pass

    # Convertir fechas
    date_cols = ['cruisedate']
    for col in date_cols:
        try:
            real_col = get_column(col, df)
            if real_col in df.columns:
                df[real_col] = pd.to_datetime(df[real_col], errors='coerce')
        except KeyError:
            pass

    print(f"   ✅ Data types converted")

    return df


# ============================================================================
# STEP 5: CALCULATE DERIVED COLUMNS
# ============================================================================

def calculate_derived_columns(df: pd.DataFrame) -> pd.DataFrame:
    """
    Calcula columnas derivadas (doyle_bf, survival status, etc)
    """
    print(f"\n📊 STEP 5: Calculating derived columns")

    df = df.copy()

    # Doyle Board Feet (si no existe)
    try:
        dbh_col = get_column('dbh_in', df)
        mht_col = get_column('mht_ft', df)

        if 'doyle_bf' not in df.columns:
            # Fórmula: ((DBH - 4) / 4)^2 * MHT
            df['doyle_bf'] = ((df[dbh_col] - 4) / 4) ** 2 * df[mht_col]
            df.loc[df[dbh_col] < 4, 'doyle_bf'] = 0
            df.loc[df['doyle_bf'] < 0, 'doyle_bf'] = 0
            print(f"   ✅ Calculated doyle_bf")
    except KeyError:
        print(f"   ⚠️  Cannot calculate doyle_bf - missing columns")

    # Survival status columns
    status_candidates = ['status', 'status2', 'condition', 'tree_status']
    found_status = None

    for candidate in status_candidates:
        if candidate in df.columns:
            found_status = candidate
            break

    if found_status:
        # alive_tree: 1 si vivo, 0 si muerto
        df['alive_tree'] = df[found_status].str.contains('viv|alive|live', case=False, na=False).astype(int)
        # dead_tree: inverso
        df['dead_tree'] = 1 - df['alive_tree']
        print(f"   ✅ Calculated survival status from '{found_status}'")
    else:
        print(f"   ⚠️  No status column found - cannot determine survival")

    return df


# ============================================================================
# STEP 6: QA CHECKS
# ============================================================================

def run_qa_checks(df: pd.DataFrame) -> Dict:
    """
    Ejecuta validaciones QA
    """
    print(f"\n🔍 STEP 6: Running QA checks")

    qa_report = {
        'timestamp': datetime.now().isoformat(),
        'total_records': len(df),
        'issues': {}
    }

    # Check 1: Missing critical columns
    try:
        dbh_col = get_column('dbh_in', df)
        missing_dbh = df[dbh_col].isna().sum()
        qa_report['issues']['missing_dbh'] = {
            'count': int(missing_dbh),
            'percentage': round(missing_dbh / len(df) * 100, 2)
        }
    except KeyError:
        qa_report['issues']['missing_dbh_column'] = True

    # Check 2: Out of range values
    try:
        tht_col = get_column('tht_ft', df)
        out_of_range = ((df[tht_col] < 0) | (df[tht_col] > 150)).sum()
        qa_report['issues']['height_out_of_range'] = {
            'count': int(out_of_range),
            'percentage': round(out_of_range / len(df) * 100, 2)
        }
    except KeyError:
        pass

    # Check 3: Duplicate tree IDs
    try:
        tree_col = get_column('tree_number', df)
        contract_col = get_column('contractcode', df)

        df['temp_id'] = df[contract_col].astype(str) + '_' + df[tree_col].astype(str)
        duplicates = df['temp_id'].duplicated().sum()
        df.drop(columns=['temp_id'], inplace=True)

        qa_report['issues']['duplicate_tree_ids'] = {
            'count': int(duplicates),
            'percentage': round(duplicates / len(df) * 100, 2)
        }
    except KeyError:
        pass

    # Summary
    total_issues = sum(
        issue.get('count', 0) if isinstance(issue, dict) else 0
        for issue in qa_report['issues'].values()
    )
    qa_report['summary'] = {
        'total_issues': total_issues,
        'data_quality_score': round(max(0, 100 - (total_issues / len(df) * 10)), 2)
    }

    print(f"   ✅ QA Score: {qa_report['summary']['data_quality_score']}%")
    print(f"   Issues found: {total_issues:,}")

    return qa_report


# ============================================================================
# STEP 7: EXPORT CONSOLIDATED MERGE
# ============================================================================

def export_consolidated_merge(df: pd.DataFrame,
                              qa_report: Dict,
                              config: Dict,
                              output_path: Path):
    """
    Exporta el merge consolidado multi-sheet
    """
    print(f"\n💾 STEP 7: Exporting consolidated merge")

    inventory_year = config['inventory_year']

    print(f"   Creating Excel with multiple sheets...")

    # Obtener nombres reales de columnas usando get_column
    try:
        contract_col = get_column('contractcode', df)
    except KeyError:
        contract_col = None

    country_col = 'country' if 'country' in df.columns else None

    with pd.ExcelWriter(output_path, engine='openpyxl') as writer:
        # Sheet 1: Inventory merged (nombre exacto para metrics calculator)
        sheet_name = f'inventory_merged_{inventory_year}'
        df.to_excel(writer, sheet_name=sheet_name, index=False)
        print(f"   ✅ Sheet 1: {sheet_name} ({len(df):,} records)")

        # Sheet 2: QA Report Summary
        qa_df = pd.DataFrame([
            {'metric': k, 'value': v}
            for k, v in qa_report['summary'].items()
        ])
        qa_df.to_excel(writer, sheet_name='QA_Summary', index=False)
        print(f"   ✅ Sheet 2: QA_Summary")

        # Sheet 3: Contract Summary
        if contract_col and country_col:
            try:
                # Preparar columnas para agregación
                agg_dict = {}

                # Buscar tree_number
                try:
                    tree_col = get_column('tree_number', df)
                    agg_dict[tree_col] = 'count'
                except KeyError:
                    pass

                # Buscar DBH
                try:
                    dbh_col = get_column('dbh_in', df)
                    agg_dict[dbh_col] = 'mean'
                except KeyError:
                    pass

                # Buscar Doyle BF
                if 'doyle_bf' in df.columns:
                    agg_dict['doyle_bf'] = 'sum'

                if agg_dict:
                    contract_summary = df.groupby([country_col, contract_col]).agg(agg_dict).round(2).reset_index()
                    contract_summary.to_excel(writer, sheet_name='Contract_Summary', index=False)
                    print(f"   ✅ Sheet 3: Contract_Summary ({len(contract_summary)} contracts)")
                else:
                    print(f"   ⚠️  Sheet 3: No aggregatable columns for Contract_Summary")
            except Exception as e:
                print(f"   ⚠️  Sheet 3: Error creating Contract_Summary: {e}")
        else:
            print(f"   ⚠️  Sheet 3: Skipping Contract_Summary - missing columns")

        # Sheet 4: Source File Tracker
        if 'source_file' in df.columns and 'source_sheet' in df.columns and contract_col and country_col:
            try:
                source_summary = df.groupby(
                    [country_col, contract_col, 'source_sheet', 'source_file']
                ).size().reset_index(name='record_count')
                source_summary = source_summary.sort_values([country_col, contract_col])
                source_summary.to_excel(writer, sheet_name='Source_Tracker', index=False)
                print(f"   ✅ Sheet 4: Source_Tracker ({len(source_summary)} files)")
            except Exception as e:
                print(f"   ⚠️  Sheet 4: Error creating Source_Tracker: {e}")
        else:
            print(f"   ⚠️  Sheet 4: Skipping Source_Tracker - missing columns")

    print(f"\n   📊 Total records: {len(df):,}")
    if country_col:
        print(f"   🌎 Countries: {', '.join(sorted(df[country_col].unique()))}")
    print(f"   📁 Saved to: {output_path.absolute()}")

    # Guardar QA report como JSON
    qa_json_path = output_path.parent / f"qa_report_{inventory_year}.json"
    with open(qa_json_path, 'w') as f:
        json.dump(qa_report, f, indent=2)
    print(f"   📄 QA Report: {qa_json_path.name}")


# ============================================================================
# MAIN PIPELINE
# ============================================================================

def main():
    parser = argparse.ArgumentParser(
        description="InventoryMerger - Enhanced with schema standardization"
    )
    parser.add_argument(
        "--config",
        default="inventory_config.json",
        help="Path to config JSON file"
    )
    parser.add_argument(
        "--countries",
        nargs="+",
        help="Specific countries to process (default: all in config)"
    )

    args = parser.parse_args()

    print("🌳 InventoryMerger - Enhanced Version")
    print("=" * 60)

    # Load configuration
    config = load_config(args.config)
    inventory_year = config['inventory_year']
    input_dirs = config['input_directories']

    print(f"Inventory Year: {inventory_year}")
    print(f"Countries configured: {', '.join(input_dirs.keys())}")
    print("=" * 60)

    # Filter countries if specified
    if args.countries:
        input_dirs = {k: v for k, v in input_dirs.items() if k in args.countries}

    # Step 1: Discover files
    discovered = discover_raw_files(input_dirs)

    if not discovered:
        print("❌ No files discovered")
        return

    # Process each country
    all_country_data = []

    for country, info in discovered.items():
        if info['file_count'] == 0:
            print(f"\n⚠️  Skipping {country} - no files found")
            continue

        print(f"\n{'='*60}")
        print(f"🌎 PROCESSING: {country}")
        print(f"{'='*60}")

        # Step 2: Load raw inputs
        raw_df = load_raw_inputs(info['files'], country)

        if raw_df.empty:
            print(f"   ⚠️  No data loaded for {country}")
            continue

        # Step 3: Standardize columns
        std_df = standardize_columns(raw_df, country)

        # Drop unknown columns
        std_df = drop_unknown_columns(std_df)

        # Step 4: Convert data types
        std_df = convert_data_types(std_df)

        # Step 5: Calculate derived columns
        std_df = calculate_derived_columns(std_df)

        all_country_data.append(std_df)

    # Merge all countries
    if not all_country_data:
        print("\n❌ No data to merge")
        return

    print(f"\n{'='*60}")
    print("🔗 MERGING ALL COUNTRIES")
    print(f"{'='*60}")

    consolidated_df = pd.concat(all_country_data, ignore_index=True)

    print(f"\n   📊 Total records: {len(consolidated_df):,}")
    if 'country' in consolidated_df.columns:
        print(f"   🌎 Countries: {', '.join(sorted(consolidated_df['country'].unique()))}")

    # Step 6: QA checks on consolidated data
    qa_report = run_qa_checks(consolidated_df)

    # Step 7: Export
    script_dir = Path(__file__).parent
    merge_filename = config.get('merge_output_filename', 'inventory_merge_{year}.xlsx').format(year=inventory_year)
    output_path = script_dir / merge_filename

    export_consolidated_merge(consolidated_df, qa_report, config, output_path)

    # Final summary
    print("\n" + "=" * 60)
    print("✅ MERGE COMPLETE")
    print("=" * 60)
    print(f"\n📦 Next steps:")
    print(f"   1. Review QA report: qa_report_{inventory_year}.json")
    print(f"   2. Validate data in: {merge_filename}")
    print(f"   3. Run metrics calculator: python inventory_metrics_calculator.py")


if __name__ == "__main__":
    main()