"""
InventoryMetricsCalculator
==========================
Calcula métricas anuales de inventario a partir del merge consolidado.

Input:
- inventory_merge_2025.xlsx (generado por InventoryMerger)
- masterdatabase.inventory_metrics (DB, histórico, para recuperar inventory_date)
- masterdatabase.contract_tree_information (DB, para planting_year)

Output:
- masterdatabase.inventory_metrics (DB, INSERT año actual)

Métricas calculadas:
- survival, mortality
- tht_mean, tht_std
- mht_mean, mht_std
- dbh_mean, dbh_std
- doyle_bf_mean, doyle_bf_std, doyle_bf_total
- noncom_dbh_count
- tree_age (calculado)
"""

import pandas as pd
import numpy as np
from pathlib import Path
from datetime import datetime
from sqlalchemy import text
import argparse
import warnings
import json

warnings.filterwarnings('ignore')

# ============================================================================
# CONFIGURATION
# ============================================================================

def load_config(config_path: str = "inventory_config.json") -> dict:
    """Carga configuración desde JSON"""
    config_file = Path(__file__).parent / config_path

    if not config_file.exists():
        return {
            "inventory_year": 2024,
            "merge_output_filename": "inventory_merge_{year}.xlsx"
        }

    with open(config_file, 'r') as f:
        config = json.load(f)

    return config

CONFIG = load_config()
INVENTORY_YEAR = CONFIG['inventory_year']
MERGE_FILE = CONFIG.get('merge_output_filename', 'inventory_merge_{year}.xlsx').format(year=INVENTORY_YEAR)

# ============================================================================
# STEP 1: LOAD MERGE DATA
# ============================================================================

def load_inventory_merge(merge_path: Path) -> pd.DataFrame:
    """Carga el Excel consolidado generado por Merger"""
    print(f"\n📊 STEP 1: Loading inventory merge")
    print(f"   Source: {merge_path.name}")

    sheet_name = f'inventory_merged_{INVENTORY_YEAR}'

    try:
        df = pd.read_excel(merge_path, sheet_name=sheet_name)
    except ValueError:
        print(f"   ℹ️  Sheet '{sheet_name}' not found, trying 'Inventory'...")
        df = pd.read_excel(merge_path, sheet_name='Inventory')

    # Normalizar nombre de columna contract
    if 'contractcode' in df.columns and 'contract_code' not in df.columns:
        df.rename(columns={'contractcode': 'contract_code'}, inplace=True)
        print(f"   🔧 Renamed 'contractcode' → 'contract_code'")

    print(f"   ✅ Loaded {len(df):,} records")

    if 'contract_code' in df.columns:
        contracts = df['contract_code'].nunique()
        print(f"   📊 Unique contracts: {contracts}")

    if 'country' in df.columns:
        countries = df['country'].dropna().unique()
        if len(countries) > 0:
            print(f"   🌍 Countries: {', '.join(sorted(countries))}")

    return df


# ============================================================================
# STEP 2: LOAD REFERENCE DATA FROM DATABASE
# ============================================================================

def load_historical_inventory_dates(engine) -> pd.DataFrame:
    """Carga inventory_date desde masterdatabase.inventory_metrics"""
    print(f"\n📅 STEP 2: Loading historical inventory_date from database")

    query = text("""
        SELECT DISTINCT 
            contract_code, 
            inventory_date
        FROM masterdatabase.inventory_metrics
        WHERE inventory_year = :year
          AND inventory_date IS NOT NULL
    """)

    with engine.connect() as conn:
        df = pd.read_sql(query, conn, params={"year": INVENTORY_YEAR})

    if not df.empty:
        print(f"   ✅ Found inventory_date for {len(df)} contracts from {INVENTORY_YEAR}")
    else:
        print(f"   ℹ️  No existing inventory_date found for {INVENTORY_YEAR}")

    return df


def load_cti_data(engine) -> pd.DataFrame:
    """Carga planting info desde masterdatabase.contract_tree_information"""
    print(f"\n🌱 STEP 3: Loading CTI data from database")

    query = text("""
        SELECT DISTINCT 
            contract_code, 
            planting_date, 
            planting_year
        FROM masterdatabase.contract_tree_information
        WHERE contract_code IS NOT NULL
    """)

    with engine.connect() as conn:
        df = pd.read_sql(query, conn)

    df['planting_date'] = pd.to_datetime(df['planting_date'], errors='coerce')

    print(f"   ✅ Loaded {len(df)} contracts from CTI")

    return df


# ============================================================================
# STEP 3: STANDARDIZE UNITS
# ============================================================================

def standardize_units(df: pd.DataFrame) -> pd.DataFrame:
    """Verifica que las columnas existan con los nombres correctos"""
    print(f"\n🔧 STEP 4: Verifying unit columns")

    df = df.copy()

    expected_cols = {
        'dbh_in': 'DBH in inches',
        'tht_ft': 'Total height in feet',
        'mht_ft': 'Merchantable height in feet (optional)'
    }

    found = []
    missing = []

    for col, desc in expected_cols.items():
        if col in df.columns:
            found.append(f"{col} ({desc})")
        else:
            missing.append(f"{col} ({desc})")

    if found:
        print(f"   ✅ Found: {', '.join(found)}")
    if missing:
        print(f"   ⚠️  Missing: {', '.join(missing)}")

    return df


# ============================================================================
# STEP 4: CALCULATE DOYLE BOARD FEET
# ============================================================================

def calculate_doyle_bf(df: pd.DataFrame) -> pd.DataFrame:
    """Calcula Doyle Board Feet: ((DBH_in - 4) / 4)^2 * MHT_ft"""
    print(f"\n📏 STEP 5: Calculating Doyle Board Feet")

    df = df.copy()

    if 'dbh_in' not in df.columns:
        print(f"   ⚠️  DBH not found - cannot calculate Doyle BF")
        df['doyle_bf'] = np.nan
        return df

    # Usar MHT preferentemente, fallback a merch_ht_ft o THT
    if 'mht_ft' in df.columns:
        height_ft = df['mht_ft'].fillna(df.get('tht_ft', 0))
        height_source = 'MHT (fallback THT)'
    elif 'merch_ht_ft' in df.columns:
        height_ft = df['merch_ht_ft'].fillna(df.get('tht_ft', 0))
        height_source = 'merch_ht_ft (fallback THT)'
    elif 'tht_ft' in df.columns:
        height_ft = df['tht_ft']
        height_source = 'THT'
    else:
        print(f"   ⚠️  No height data - cannot calculate Doyle BF")
        df['doyle_bf'] = np.nan
        return df

    df['doyle_bf'] = ((df['dbh_in'] - 4) / 4) ** 2 * height_ft
    df.loc[df['dbh_in'] < 4, 'doyle_bf'] = 0
    df.loc[df['doyle_bf'] < 0, 'doyle_bf'] = 0

    valid_count = (df['doyle_bf'] > 0).sum()
    print(f"   ✅ Calculated Doyle BF using {height_source}")
    print(f"   📊 {valid_count:,} trees with commercial volume (DBH ≥ 4\")")

    return df


# ============================================================================
# STEP 5: CALCULATE SURVIVAL & MORTALITY
# ============================================================================

def calculate_survival_mortality(df: pd.DataFrame) -> pd.DataFrame:
    """Calcula survival y mortality por contrato"""
    print(f"\n💚 STEP 6: Calculating survival & mortality")

    metrics = []

    contract_col = 'contract_code' if 'contract_code' in df.columns else 'contractcode'

    if contract_col not in df.columns:
        print(f"   ❌ ERROR: No contract column found!")
        return pd.DataFrame()

    for contract in df[contract_col].unique():
        df_contract = df[df[contract_col] == contract]
        total_trees = len(df_contract)

        if 'alive_tree' in df_contract.columns:
            surviving = df_contract['alive_tree'].sum()
            dead = df_contract['dead_tree'].sum() if 'dead_tree' in df_contract.columns else (total_trees - surviving)
        elif 'surviving_tree' in df_contract.columns:
            surviving = df_contract['surviving_tree'].sum()
            dead = total_trees - surviving
        elif 'dead_trees' in df_contract.columns:
            dead = df_contract['dead_trees'].sum()
            surviving = total_trees - dead
        elif 'status' in df_contract.columns:
            surviving = df_contract['status'].str.contains('Viv|alive|live', case=False, na=False).sum()
            dead = total_trees - surviving
        else:
            surviving = total_trees
            dead = 0

        survival = round((surviving / total_trees), 4) if total_trees > 0 else 0
        mortality = round((dead / total_trees), 4) if total_trees > 0 else 0

        metrics.append({
            'contract_code': contract,
            'total_trees': total_trees,
            'surviving': surviving,
            'dead': dead,
            'survival': survival,
            'mortality': mortality
        })

    metrics_df = pd.DataFrame(metrics)

    print(f"   ✅ Calculated survival for {len(metrics_df)} contracts")
    print(f"   📊 Average survival: {metrics_df['survival'].mean():.2%}")

    return metrics_df


# ============================================================================
# STEP 6: CALCULATE STATISTICAL METRICS
# ============================================================================

def calculate_contract_metrics(df: pd.DataFrame) -> pd.DataFrame:
    """Calcula métricas estadísticas por contrato"""
    print(f"\n📈 STEP 7: Calculating statistical metrics")

    metrics = []

    contract_col = 'contract_code' if 'contract_code' in df.columns else 'contractcode'

    if contract_col not in df.columns:
        print(f"   ❌ ERROR: No contract column found!")
        return pd.DataFrame()

    for contract in df[contract_col].unique():
        df_contract = df[df[contract_col] == contract]

        if 'alive_tree' in df_contract.columns:
            df_alive = df_contract[df_contract['alive_tree'] == 1]
        elif 'surviving_tree' in df_contract.columns:
            df_alive = df_contract[df_contract['surviving_tree'] == 1]
        else:
            df_alive = df_contract

        metric = {'contract_code': contract}

        # THT metrics
        if 'tht_ft' in df_alive.columns:
            tht_valid = pd.to_numeric(df_alive['tht_ft'], errors='coerce').dropna()
            metric['tht_mean'] = round(tht_valid.mean(), 2) if len(tht_valid) > 0 else np.nan
            metric['tht_std'] = round(tht_valid.std(), 2) if len(tht_valid) > 1 else np.nan

        # MHT metrics
        if 'mht_ft' in df_alive.columns:
            mht_valid = pd.to_numeric(df_alive['mht_ft'], errors='coerce').dropna()
            metric['mht_mean'] = round(mht_valid.mean(), 2) if len(mht_valid) > 0 else np.nan
            metric['mht_std'] = round(mht_valid.std(), 2) if len(mht_valid) > 1 else np.nan
        elif 'merch_ht_ft' in df_alive.columns:
            mht_valid = pd.to_numeric(df_alive['merch_ht_ft'], errors='coerce').dropna()
            metric['mht_mean'] = round(mht_valid.mean(), 2) if len(mht_valid) > 0 else np.nan
            metric['mht_std'] = round(mht_valid.std(), 2) if len(mht_valid) > 1 else np.nan

        # DBH metrics
        if 'dbh_in' in df_alive.columns:
            dbh_valid = pd.to_numeric(df_alive['dbh_in'], errors='coerce').dropna()
            metric['dbh_mean'] = round(dbh_valid.mean(), 2) if len(dbh_valid) > 0 else np.nan
            metric['dbh_std'] = round(dbh_valid.std(), 2) if len(dbh_valid) > 1 else np.nan
            metric['noncom_dbh_count'] = int((df_alive['dbh_in'] < 8).sum())

        # Doyle BF metrics
        if 'doyle_bf' in df_alive.columns:
            doyle_valid = pd.to_numeric(df_alive['doyle_bf'], errors='coerce').dropna()
            doyle_positive = doyle_valid[doyle_valid > 0]
            metric['doyle_bf_mean'] = round(doyle_positive.mean(), 2) if len(doyle_positive) > 0 else np.nan
            metric['doyle_bf_std'] = round(doyle_positive.std(), 2) if len(doyle_positive) > 1 else np.nan
            metric['doyle_bf_total'] = round(doyle_positive.sum(), 2) if len(doyle_positive) > 0 else 0

        metrics.append(metric)

    metrics_df = pd.DataFrame(metrics)
    print(f"   ✅ Calculated metrics for {len(metrics_df)} contracts")

    return metrics_df


# ============================================================================
# STEP 7: CALCULATE TREE AGE
# ============================================================================

def calculate_tree_age(planting_date: pd.Timestamp, inventory_date: pd.Timestamp) -> str:
    """Calcula tree_age en formato 'X Y/12'"""
    if pd.isna(planting_date) or pd.isna(inventory_date):
        return np.nan

    delta = inventory_date - planting_date
    total_months = delta.days / 30.44
    years = int(total_months // 12)
    months = int(total_months % 12)

    return f"{years} {months}/12"


# ============================================================================
# STEP 8: ASSEMBLE FINAL METRICS
# ============================================================================

def assemble_inventory_metrics(merge_df: pd.DataFrame,
                               historical_dates: pd.DataFrame,
                               cti_df: pd.DataFrame) -> pd.DataFrame:
    """Ensambla el DataFrame final de inventory_metrics"""
    print(f"\n🔨 STEP 8: Assembling final inventory_metrics")

    merge_df = standardize_units(merge_df)
    merge_df = calculate_doyle_bf(merge_df)

    survival_df = calculate_survival_mortality(merge_df)
    stats_df = calculate_contract_metrics(merge_df)

    final_df = survival_df.merge(stats_df, on='contract_code', how='outer')

    # Agregar inventory_date
    if not historical_dates.empty:
        final_df = final_df.merge(historical_dates, on='contract_code', how='left')
        dates_found = final_df['inventory_date'].notna().sum()
        print(f"   ✅ Merged inventory_date for {dates_found}/{len(final_df)} contracts")

        missing_dates = len(final_df) - dates_found
        if missing_dates > 0:
            print(f"   ℹ️  {missing_dates} contracts will have NULL inventory_date")
    else:
        final_df['inventory_date'] = np.nan
        print(f"   ⚠️  No historical inventory_date available")

    # Agregar planting info
    if not cti_df.empty:
        final_df = final_df.merge(cti_df, on='contract_code', how='left')
        planting_found = final_df['planting_year'].notna().sum()
        print(f"   ✅ Merged planting info for {planting_found} contracts")
    else:
        final_df['planting_date'] = np.nan
        final_df['planting_year'] = np.nan

    # Calcular tree_age
    final_df['inventory_date'] = pd.to_datetime(final_df['inventory_date'], errors='coerce')
    final_df['tree_age'] = final_df.apply(
        lambda row: calculate_tree_age(row['planting_date'], row['inventory_date']),
        axis=1
    )

    age_found = final_df['tree_age'].notna().sum()
    print(f"   📊 Calculated tree_age for {age_found}/{len(final_df)} contracts")

    missing_age = len(final_df) - age_found
    if missing_age > 0:
        print(f"   ℹ️  {missing_age} contracts have NULL tree_age")

    # Campos fijos
    final_df['inventory_year'] = INVENTORY_YEAR
    final_df['type_of_metric'] = 'inventory'
    final_df['pkid'] = final_df['contract_code'] + ' ' + str(INVENTORY_YEAR)
    final_df['progress'] = 'OK'
    final_df['rel_path'] = ''
    final_df['mht_pct_of_target'] = np.nan
    final_df['dbh_pct_of_target'] = np.nan
    final_df['projected_dbh'] = np.nan
    final_df['projected_doyle_bf'] = np.nan

    # Reordenar columnas
    column_order = [
        'rel_path', 'contract_code', 'tree_age', 'inventory_year', 'inventory_date',
        'tht_mean', 'tht_std', 'mht_mean', 'mht_std',
        'dbh_mean', 'dbh_std', 'noncom_dbh_count',
        'doyle_bf_mean', 'doyle_bf_std', 'doyle_bf_total',
        'pkid', 'progress', 'total_trees', 'type_of_metric',
        'planting_date', 'planting_year', 'survival',
        'mht_pct_of_target', 'dbh_pct_of_target',
        'projected_dbh', 'projected_doyle_bf', 'mortality'
    ]

    for col in column_order:
        if col not in final_df.columns:
            final_df[col] = np.nan

    final_df = final_df[column_order]

    print(f"   ✅ Assembled {len(final_df)} contract metrics")

    return final_df


# ============================================================================
# STEP 9: VALIDATE FOREIGN KEY CONSTRAINTS
# ============================================================================

def validate_contracts_in_cti(metrics_df: pd.DataFrame, engine) -> pd.DataFrame:
    """Valida que todos los contract_code existan en CTI"""
    print(f"\n🔍 STEP 9: Validating contracts against CTI (Foreign Key check)")

    query = text("""
        SELECT DISTINCT contract_code 
        FROM masterdatabase.contract_tree_information
        WHERE contract_code IS NOT NULL
    """)

    with engine.connect() as conn:
        valid_contracts = pd.read_sql(query, conn)

    valid_set = set(valid_contracts['contract_code'])
    metrics_set = set(metrics_df['contract_code'])
    missing_contracts = metrics_set - valid_set

    if missing_contracts:
        print(f"   ⚠️  WARNING: {len(missing_contracts)} contracts NOT in CTI:")
        for contract in sorted(missing_contracts):
            print(f"      - {contract}")
        print(f"   🚫 These contracts will be EXCLUDED from insert")

        valid_metrics_df = metrics_df[metrics_df['contract_code'].isin(valid_set)].copy()
        print(f"   ✅ Will insert {len(valid_metrics_df)}/{len(metrics_df)} contracts")

        if len(missing_contracts) > 0:
            excluded_df = metrics_df[metrics_df['contract_code'].isin(missing_contracts)]
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            excluded_file = Path(f"excluded_contracts_not_in_cti_{timestamp}.csv")
            excluded_df.to_csv(excluded_file, index=False)
            print(f"   💾 Excluded contracts saved to: {excluded_file.name}")

        return valid_metrics_df
    else:
        print(f"   ✅ All {len(metrics_df)} contracts exist in CTI")
        return metrics_df


# ============================================================================
# STEP 10: UPDATE DATABASE
# ============================================================================

def safe_update_inventory_metrics_db(new_metrics_df: pd.DataFrame, engine):
    """Actualiza masterdatabase.inventory_metrics de forma segura"""

    new_metrics_df = validate_contracts_in_cti(new_metrics_df, engine)

    if len(new_metrics_df) == 0:
        print(f"   ❌ No valid contracts to insert after FK validation!")
        return

    print(f"\n💾 STEP 10: Updating database (masterdatabase.inventory_metrics)")

    query_check = text("""
        SELECT * FROM masterdatabase.inventory_metrics
        WHERE inventory_year = :year
    """)

    with engine.connect() as conn:
        df_current_year = pd.read_sql(query_check, conn, params={"year": INVENTORY_YEAR})

    if len(df_current_year) > 0:
        print(f"   ⚠️  Found {len(df_current_year)} existing {INVENTORY_YEAR} records")
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        backup_file = Path(f"backup_inventory_metrics_{INVENTORY_YEAR}_{timestamp}.csv")
        df_current_year.to_csv(backup_file, index=False)
        print(f"   💾 Backed up to: {backup_file.name}")

    query_delete = text("""
        DELETE FROM masterdatabase.inventory_metrics
        WHERE inventory_year = :year
    """)

    with engine.begin() as conn:
        result = conn.execute(query_delete, {"year": INVENTORY_YEAR})
        deleted_count = result.rowcount
        print(f"   🗑️  Deleted {deleted_count} old {INVENTORY_YEAR} records")

    print(f"   📥 Inserting {len(new_metrics_df)} new {INVENTORY_YEAR} records...")

    new_metrics_df.to_sql(
        'inventory_metrics',
        engine,
        schema='masterdatabase',
        if_exists='append',
        index=False,
        method='multi',
        chunksize=1000
    )

    print(f"   ✅ Successfully inserted {len(new_metrics_df)} records")
    print(f"   🔔 Trigger will update inventory_metrics_current automatically")


# ============================================================================
# MAIN
# ============================================================================

def main():
    parser = argparse.ArgumentParser(
        description="InventoryMetricsCalculator - Generate annual inventory metrics"
    )
    parser.add_argument(
        "--merge-file",
        default=MERGE_FILE,
        help="Path to inventory_merge_2025.xlsx"
    )
    parser.add_argument(
        "--output",
        default=None,
        help="Optional: also export to CSV"
    )

    args = parser.parse_args()

    print("📊 InventoryMetricsCalculator")
    print("=" * 60)
    print(f"Inventory Year: {INVENTORY_YEAR}")
    print(f"Target: masterdatabase.inventory_metrics")
    print("=" * 60)

    from core.db import get_engine
    engine = get_engine()

    merge_path = Path(args.merge_file)
    if not merge_path.exists():
        print(f"❌ Merge file not found: {merge_path}")
        return

    merge_df = load_inventory_merge(merge_path)
    historical_dates = load_historical_inventory_dates(engine)
    cti_df = load_cti_data(engine)

    final_metrics = assemble_inventory_metrics(merge_df, historical_dates, cti_df)

    print(f"\n📊 Will process ALL {len(final_metrics)} contracts from merge")
    print(f"   - With inventory_date: {final_metrics['inventory_date'].notna().sum()}")
    print(f"   - Without inventory_date (NULL): {final_metrics['inventory_date'].isna().sum()}")

    safe_update_inventory_metrics_db(final_metrics, engine)

    if args.output:
        output_path = Path(args.output)
        final_metrics.to_csv(output_path, index=False)
        print(f"\n   💾 Also exported to CSV: {output_path.name}")

    print(f"\n{'=' * 60}")
    print(f"✅ COMPLETE")
    print(f"{'=' * 60}")
    print(f"📊 Updated contracts: {len(final_metrics)}")
    print(f"🗄️  Database: masterdatabase.inventory_metrics")
    print(f"\n📈 Summary Statistics:")
    print(f"   Average survival: {final_metrics['survival'].mean():.2%}")
    print(f"   Average mortality: {final_metrics['mortality'].mean():.2%}")
    print(f"   Contracts with inventory_date: {final_metrics['inventory_date'].notna().sum()}")
    print(f"   Contracts with planting_year: {final_metrics['planting_year'].notna().sum()}")
    print(f"   Contracts with tree_age: {final_metrics['tree_age'].notna().sum()}")


if __name__ == "__main__":
    main()