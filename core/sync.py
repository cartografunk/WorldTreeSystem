# core/sync.py
"""
Funciones de sincronización entre tablas de la base de datos.
Mantiene la consistencia de datos derivados cuando se modifican tablas fuente.
"""

from core.libs import pd, np
from core.db import get_engine
from sqlalchemy import text as sqltext


def sync_ca_from_cti(contract_codes: list[str] | None = None):
    """
    CORRECTED VERSION: Sync contract_allocation from CTI.

    CHANGES FROM ORIGINAL:
    - REMOVED: usa_allocation_pct based splits
    - REMOVED: usa_trees_*, canada_trees_* calculations
    - REMOVED: etp_type='ETP/COP' percentage logic
    - ADDED: Proper allocation based on etp_year
    - ADDED: Modern year protection (2024+ always ETP)

    SIGNATURE: UNCHANGED (preserves compatibility)
    """

    engine = get_engine()

    # Construir filtro SQL
    where_clause = ""
    if contract_codes:
        codes_str = "', '".join(contract_codes)
        where_clause = f"WHERE cti.contract_code IN ('{codes_str}')"

    # Leer datos necesarios (NO leemos usa_allocation_pct)
    query = f"""
    SELECT 
        cti.contract_code,
        cti.trees_contract,
        cti.planted,
        cti.etp_year,
        ca.etp_type,
        ca.contracted_etp,
        ca.contracted_cop
    FROM masterdatabase.contract_tree_information cti
    LEFT JOIN masterdatabase.contract_allocation ca ON cti.contract_code = ca.contract_code
    {where_clause}
    """

    df = pd.read_sql(query, engine)

    if df.empty:
        print("⚠️  No hay contratos para sincronizar")
        return 0

    # Asegurar tipos numéricos
    df["trees_contract"] = pd.to_numeric(df["trees_contract"], errors="coerce").fillna(0).astype(int)
    df["planted"] = pd.to_numeric(df["planted"], errors="coerce").fillna(0).astype(int)
    df["etp_year"] = pd.to_numeric(df["etp_year"], errors="coerce").fillna(0).astype(int)
    df["contracted_etp"] = pd.to_numeric(df["contracted_etp"], errors="coerce").fillna(0).astype(int)
    df["contracted_cop"] = pd.to_numeric(df["contracted_cop"], errors="coerce").fillna(0).astype(int)
    df["etp_type"] = df["etp_type"].astype(str).str.strip()

    # ──────────────────────────────────────────────────────────────────────
    # CORRECTED LOGIC: Calculate allocation based on etp_year ONLY
    # ──────────────────────────────────────────────────────────────────────

    # Initialize all columns
    df["contracted_etp_new"] = 0
    df["contracted_cop_new"] = 0
    df["planted_etp_new"] = 0
    df["planted_cop_new"] = 0
    df["canada_2017_trees_new"] = 0
    df["etp_type_new"] = 'ETP'

    # Year 2015: ALL COP
    mask_2015 = df["etp_year"] == 2015
    df.loc[mask_2015, "contracted_etp_new"] = 0
    df.loc[mask_2015, "contracted_cop_new"] = df.loc[mask_2015, "trees_contract"]
    df.loc[mask_2015, "planted_etp_new"] = 0
    df.loc[mask_2015, "planted_cop_new"] = df.loc[mask_2015, "planted"]
    df.loc[mask_2015, "etp_type_new"] = 'COP'

    # Year 2017: ALL COP (Canada)
    mask_2017 = df["etp_year"] == 2017
    df.loc[mask_2017, "contracted_etp_new"] = 0
    df.loc[mask_2017, "contracted_cop_new"] = df.loc[mask_2017, "trees_contract"]
    df.loc[mask_2017, "planted_etp_new"] = 0
    df.loc[mask_2017, "planted_cop_new"] = df.loc[mask_2017, "planted"]
    df.loc[mask_2017, "canada_2017_trees_new"] = df.loc[mask_2017, "trees_contract"]
    df.loc[mask_2017, "etp_type_new"] = 'COP'

    # Years 2016, 2018: Keep existing split IF it exists, otherwise default to ETP
    mask_split_years = df["etp_year"].isin([2016, 2018])

    # Check if contract already has a valid split
    df["has_valid_split"] = (df["contracted_etp"] + df["contracted_cop"]) == df["trees_contract"]

    # Keep existing split if valid
    mask_keep_existing = mask_split_years & df["has_valid_split"]
    df.loc[mask_keep_existing, "contracted_etp_new"] = df.loc[mask_keep_existing, "contracted_etp"]
    df.loc[mask_keep_existing, "contracted_cop_new"] = df.loc[mask_keep_existing, "contracted_cop"]
    df.loc[mask_keep_existing, "planted_etp_new"] = df.loc[mask_keep_existing, "planted_etp"]
    df.loc[mask_keep_existing, "planted_cop_new"] = df.loc[mask_keep_existing, "planted_cop"]
    df.loc[mask_keep_existing, "etp_type_new"] = df.loc[mask_keep_existing, "etp_type"]

    # Default to ETP if no valid split
    mask_default_etp = mask_split_years & ~df["has_valid_split"]
    df.loc[mask_default_etp, "contracted_etp_new"] = df.loc[mask_default_etp, "trees_contract"]
    df.loc[mask_default_etp, "contracted_cop_new"] = 0
    df.loc[mask_default_etp, "planted_etp_new"] = df.loc[mask_default_etp, "planted"]
    df.loc[mask_default_etp, "planted_cop_new"] = 0
    df.loc[mask_default_etp, "etp_type_new"] = 'ETP'

    # All other years (including 2024+): ALL ETP
    mask_other = ~(mask_2015 | mask_2017 | mask_split_years)
    df.loc[mask_other, "contracted_etp_new"] = df.loc[mask_other, "trees_contract"]
    df.loc[mask_other, "contracted_cop_new"] = 0
    df.loc[mask_other, "planted_etp_new"] = df.loc[mask_other, "planted"]
    df.loc[mask_other, "planted_cop_new"] = 0
    df.loc[mask_other, "etp_type_new"] = 'ETP'

    # ──────────────────────────────────────────────────────────────────────
    # CRITICAL: Ensure modern years (2024+) have COP=0
    # ──────────────────────────────────────────────────────────────────────
    mask_modern = df["etp_year"] >= 2024
    df.loc[mask_modern, "contracted_cop_new"] = 0
    df.loc[mask_modern, "planted_cop_new"] = 0
    df.loc[mask_modern, "etp_type_new"] = 'ETP'

    # ──────────────────────────────────────────────────────────────────────
    # WARNING: Log contracts where we're changing existing allocations
    # ──────────────────────────────────────────────────────────────────────
    df["total_existing"] = df["contracted_etp"] + df["contracted_cop"]
    df["total_new"] = df["contracted_etp_new"] + df["contracted_cop_new"]
    df["will_change"] = (df["total_existing"] != df["total_new"]) & (df["total_existing"] > 0)

    changing = df[df["will_change"]]
    if not changing.empty:
        print(f"⚠️  WARNING: {len(changing)} contracts have manual allocations that will be overwritten:")
        for _, row in changing.head(10).iterrows():
            print(
                f"   {row['contract_code']}: {row['contracted_etp']}+{row['contracted_cop']} → {row['contracted_etp_new']}+{row['contracted_cop_new']}")
        if len(changing) > 10:
            print(f"   ... and {len(changing) - 10} more")

    # ──────────────────────────────────────────────────────────────────────
    # Update database (REMOVED usa_trees_*, canada_trees_*)
    # ──────────────────────────────────────────────────────────────────────
    update_query = text("""
        UPDATE masterdatabase.contract_allocation
        SET 
            contracted_etp = :etp_c,
            planted_etp = :etp_p,
            contracted_cop = :cop_c,
            planted_cop = :cop_p,
            canada_2017_trees = :can2017,
            etp_type = :etp_type
        WHERE contract_code = :code
    """)

    count = 0
    with engine.begin() as conn:
        for _, row in df.iterrows():
            conn.execute(update_query, {
                'code': row['contract_code'],
                'etp_c': int(row['contracted_etp_new']),
                'etp_p': int(row['planted_etp_new']),
                'cop_c': int(row['contracted_cop_new']),
                'cop_p': int(row['planted_cop_new']),
                'can2017': int(row['canada_2017_trees_new']),
                'etp_type': row['etp_type_new']
            })
            count += 1

    print(f"✅ Sincronizados {count} contratos en contract_allocation")
    return count


def sync_surviving_split(contract_codes: list[str] | None = None):
    """
    CORRECTED VERSION: Recalcula surviving_etp y surviving_cop.

    CHANGES FROM ORIGINAL:
    - REMOVED: usa_allocation_pct based logic for 'ETP/COP' type
    - Uses contracted_etp/cop ratio instead of percentage

    SIGNATURE: UNCHANGED (preserves compatibility)
    """

    engine = get_engine()

    where_clause = ""
    if contract_codes:
        codes_str = "', '".join(contract_codes)
        where_clause = f"AND ca.contract_code IN ('{codes_str}')"

    # CORRECTED: Use contracted_etp/cop ratio instead of usa_allocation_pct
    q = text(f"""
        WITH sc AS (
            SELECT contract_code, current_surviving_trees
            FROM masterdatabase.survival_current
        ),
        ca_totals AS (
            SELECT 
                contract_code,
                CASE 
                    WHEN (contracted_etp + contracted_cop) > 0 
                    THEN contracted_etp::FLOAT / (contracted_etp + contracted_cop)
                    ELSE 1.0
                END as etp_ratio
            FROM masterdatabase.contract_allocation
        )
        UPDATE masterdatabase.contract_allocation ca
        SET surviving_etp = CASE
                WHEN etp_type = 'ETP' THEN sc.current_surviving_trees
                WHEN etp_type = 'COP' THEN 0
                -- CORRECTED: Use ratio of contracted values, not percentage
                WHEN etp_type = 'ETP/COP' THEN CEIL(sc.current_surviving_trees * cat.etp_ratio)
                ELSE 0
            END,
            surviving_cop = CASE
                WHEN etp_type = 'ETP' THEN 0
                WHEN etp_type = 'COP' THEN sc.current_surviving_trees
                -- CORRECTED: Use ratio of contracted values, not percentage
                WHEN etp_type = 'ETP/COP' THEN sc.current_surviving_trees - CEIL(sc.current_surviving_trees * cat.etp_ratio)
                ELSE 0
            END
        FROM sc
        JOIN ca_totals cat ON ca.contract_code = cat.contract_code
        WHERE ca.contract_code = sc.contract_code {where_clause}
    """)

    with engine.begin() as conn:
        result = conn.execute(q)
        count = result.rowcount

    print(f"✅ Sincronizados {count} contratos (surviving split)")
    return count


def sync_contract_allocation_full(contract_codes: list[str] | None = None):
    """
    UNCHANGED: Same function signature and behavior.
    Calls corrected versions of sync_ca_from_cti and sync_surviving_split.
    """
    print(f"\n🔄 Iniciando sincronización completa de contract_allocation...")
    if contract_codes:
        print(f"   Contratos: {', '.join(contract_codes)}")
    else:
        print(f"   Contratos: TODOS")

    results = {
        'ca_from_cti': sync_ca_from_cti(contract_codes),
        'surviving_split': sync_surviving_split(contract_codes)
    }

    print(f"\n✅ Sincronización completa finalizada")
    return results
