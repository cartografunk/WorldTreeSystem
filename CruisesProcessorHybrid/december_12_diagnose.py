# december_12_diagnose.py - FIXED
import pandas as pd
from core.db import get_engine

print("=" * 60)
print("📊 INVENTORY MERGE DIAGNOSTIC")
print("=" * 60)

# Step 1: Load merge and inspect columns
print("\n1️⃣ Loading merge file...")
df_merge = pd.read_excel('inventory_merge_2025.xlsx', sheet_name='inventory_merged_2025')

print(f"   Total records: {len(df_merge):,}")
print(f"   Total columns: {len(df_merge.columns)}")

print("\n📋 ALL COLUMNS IN MERGE:")
for i, col in enumerate(df_merge.columns, 1):
    print(f"   {i:2d}. {col}")

# Step 2: Find contract code column
print("\n🔍 Looking for contract identifier...")
contract_candidates = [col for col in df_merge.columns if 'contract' in col.lower() or 'code' in col.lower()]
print(f"   Candidates: {contract_candidates}")

# Try to find the actual contract column
if contract_candidates:
    contract_col = contract_candidates[0]
    print(f"   ✅ Using: '{contract_col}'")

    contracts_in_merge = set(df_merge[contract_col].unique())
    print(f"   Total unique contracts: {len(contracts_in_merge)}")
else:
    print("   ❌ No contract column found!")
    print("\n   First 5 rows sample:")
    print(df_merge.head())
    exit(1)

# Step 3: Check DB
print("\n2️⃣ Checking database...")
engine = get_engine()
df_db = pd.read_sql(
    "SELECT DISTINCT contract_code FROM masterdatabase.inventory_metrics WHERE inventory_year = 2025",
    engine
)
contracts_in_db = set(df_db['contract_code'].unique())
print(f"   Contracts in DB: {len(contracts_in_db)}")

# Step 4: Compare
print("\n3️⃣ Comparing merge vs DB...")
missing = contracts_in_merge - contracts_in_db
extra = contracts_in_db - contracts_in_merge

print(f"\n🚨 MISSING in DB (in merge, not in DB): {len(missing)}")
if missing:
    for contract in sorted(missing):
        print(f"   - {contract}")

print(f"\n⚠️  EXTRA in DB (in DB, not in merge): {len(extra)}")
if extra:
    for contract in sorted(extra):
        print(f"   - {contract}")

# Step 5: Check for cruise_date
print("\n4️⃣ Checking for cruise_date columns...")
date_cols = [col for col in df_merge.columns if 'date' in col.lower() or 'cruise' in col.lower()]
print(f"   Date-related columns: {date_cols}")

if date_cols:
    for col in date_cols:
        non_null = df_merge[col].notna().sum()
        print(f"   - {col}: {non_null}/{len(df_merge)} non-null ({non_null / len(df_merge) * 100:.1f}%)")

# Step 6: Check country breakdown
print("\n5️⃣ Merge breakdown by country...")
if 'country' in df_merge.columns:
    country_counts = df_merge.groupby('country')[contract_col].nunique()
    print(country_counts)
else:
    print("   ⚠️  No 'country' column found")

# Step 7: Save missing contracts if any
if missing:
    print(f"\n💾 Saving missing contracts analysis...")
    df_missing = df_merge[df_merge[contract_col].isin(missing)]

    summary_cols = [contract_col]
    if 'country' in df_merge.columns:
        summary_cols.append('country')
    if 'source_file' in df_merge.columns:
        summary_cols.append('source_file')

    output = df_missing[summary_cols].drop_duplicates()
    output.to_csv('missing_contracts_analysis.csv', index=False)
    print(f"   Saved to: missing_contracts_analysis.csv")

print("\n" + "=" * 60)
print("✅ DIAGNOSTIC COMPLETE")
print("=" * 60)