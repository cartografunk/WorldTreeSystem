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
    Sincroniza contract_allocation (CA) con los valores actuales de contract_tree_information (CTI).

    Recalcula y actualiza en la BD:
    - usa_trees_contracted, canada_trees_contracted (split por usa_allocation_pct)
    - usa_trees_planted, canada_trees_planted (split por usa_allocation_pct)
    - contracted_etp, planted_etp (según etp_type)
    - contracted_cop, planted_cop (según etp_type)

    Args:
        contract_codes: Lista de códigos de contrato a sincronizar.
                       Si es None, sincroniza TODOS los contratos.

    Returns:
        int: Número de contratos actualizados
    """
    engine = get_engine()

    # Construir filtro SQL
    where_clause = ""
    if contract_codes:
        codes_str = "', '".join(contract_codes)
        where_clause = f"WHERE cti.contract_code IN ('{codes_str}')"

    # Leer datos necesarios
    query = f"""
    SELECT 
        cti.contract_code,
        cti.trees_contract,
        cti.planted,
        ca.usa_allocation_pct,
        ca.etp_type
    FROM masterdatabase.contract_tree_information cti
    LEFT JOIN masterdatabase.contract_allocation ca ON cti.contract_code = ca.contract_code
    {where_clause}
    """

    df = pd.read_sql(query, engine)

    if df.empty:
        print("⚠️  No hay contratos para sincronizar")
        return 0

    # Asegurar tipos numéricos
    df["trees_contract"] = pd.to_numeric(df["trees_contract"], errors="coerce").fillna(0)
    df["planted"] = pd.to_numeric(df["planted"], errors="coerce").fillna(0)
    df["usa_allocation_pct"] = pd.to_numeric(df["usa_allocation_pct"], errors="coerce").fillna(0)
    df["etp_type"] = df["etp_type"].astype(str).str.strip()

    # Recalcular splits USA/Canada
    df["usa_trees_contracted"] = (df["trees_contract"] * df["usa_allocation_pct"]).round(0).astype(int)
    df["canada_trees_contracted"] = (df["trees_contract"] - df["usa_trees_contracted"]).astype(int)

    df["usa_trees_planted"] = (df["planted"] * df["usa_allocation_pct"]).round(0).astype(int)
    df["canada_trees_planted"] = (df["planted"] - df["usa_trees_planted"]).astype(int)

    # Recalcular splits COP/ETP según etp_type
    df["contracted_etp"] = 0
    df["contracted_cop"] = 0
    df["planted_etp"] = 0
    df["planted_cop"] = 0

    # ETP puro
    mask_etp = df["etp_type"] == "ETP"
    df.loc[mask_etp, "contracted_etp"] = df.loc[mask_etp, "trees_contract"]
    df.loc[mask_etp, "planted_etp"] = df.loc[mask_etp, "planted"]

    # COP puro
    mask_cop = df["etp_type"] == "COP"
    df.loc[mask_cop, "contracted_cop"] = df.loc[mask_cop, "trees_contract"]
    df.loc[mask_cop, "planted_cop"] = df.loc[mask_cop, "planted"]

    # Mix ETP/COP
    mask_mix = df["etp_type"] == "ETP/COP"
    df.loc[mask_mix, "contracted_etp"] = df.loc[mask_mix, "usa_trees_contracted"]
    df.loc[mask_mix, "contracted_cop"] = df.loc[mask_mix, "canada_trees_contracted"]
    df.loc[mask_mix, "planted_etp"] = df.loc[mask_mix, "usa_trees_planted"]
    df.loc[mask_mix, "planted_cop"] = df.loc[mask_mix, "canada_trees_planted"]

    # Actualizar en BD
    update_query = sqltext("""
        UPDATE masterdatabase.contract_allocation
        SET 
            usa_trees_contracted = :usa_c,
            canada_trees_contracted = :can_c,
            usa_trees_planted = :usa_p,
            contracted_etp = :etp_c,
            planted_etp = :etp_p,
            contracted_cop = :cop_c,
            planted_cop = :cop_p
        WHERE contract_code = :code
    """)

    count = 0
    with engine.begin() as conn:
        for _, row in df.iterrows():
            conn.execute(update_query, {
                'code': row['contract_code'],
                'usa_c': int(row['usa_trees_contracted']),
                'can_c': int(row['canada_trees_contracted']),
                'usa_p': int(row['usa_trees_planted']),
                'etp_c': int(row['contracted_etp']),
                'etp_p': int(row['planted_etp']),
                'cop_c': int(row['contracted_cop']),
                'cop_p': int(row['planted_cop'])
            })
            count += 1

    print(f"✅ Sincronizados {count} contratos en contract_allocation")
    return count


def sync_surviving_split(contract_codes: list[str] | None = None):
    """
    Recalcula surviving_etp y surviving_cop en contract_allocation
    usando survival_current y usa_allocation_pct.

    Esta es la función que ya existía como refresh_surviving_split(),
    pero ahora con la opción de filtrar por contratos específicos.

    Args:
        contract_codes: Lista de códigos de contrato a sincronizar.
                       Si es None, sincroniza TODOS los contratos.

    Returns:
        int: Número de contratos actualizados
    """
    engine = get_engine()

    where_clause = ""
    if contract_codes:
        codes_str = "', '".join(contract_codes)
        where_clause = f"AND ca.contract_code IN ('{codes_str}')"

    q = sqltext(f"""
        WITH sc AS (
            SELECT contract_code, current_surviving_trees
            FROM masterdatabase.survival_current
        )
        UPDATE masterdatabase.contract_allocation ca
        SET surviving_etp = CASE
                WHEN etp_type = 'ETP' THEN sc.current_surviving_trees
                WHEN etp_type = 'COP' THEN 0
                WHEN etp_type = 'ETP/COP' THEN CEIL(sc.current_surviving_trees * COALESCE(usa_allocation_pct,0))
                ELSE 0
            END,
            surviving_cop = CASE
                WHEN etp_type = 'ETP' THEN 0
                WHEN etp_type = 'COP' THEN sc.current_surviving_trees
                WHEN etp_type = 'ETP/COP' THEN sc.current_surviving_trees - CEIL(sc.current_surviving_trees * COALESCE(usa_allocation_pct,0))
                ELSE 0
            END
        FROM sc
        WHERE ca.contract_code = sc.contract_code {where_clause}
    """)

    with engine.begin() as conn:
        result = conn.execute(q)
        count = result.rowcount

    print(f"✅ Sincronizados {count} contratos (surviving split)")
    return count


def sync_contract_allocation_full(contract_codes: list[str] | None = None):
    """
    Ejecuta TODAS las sincronizaciones de contract_allocation para los contratos especificados.

    Esto incluye:
    1. Sync de contracted/planted desde CTI
    2. Sync de surviving split desde survival_current

    Args:
        contract_codes: Lista de códigos de contrato. Si es None, sincroniza TODOS.

    Returns:
        dict: Resumen de contratos sincronizados por cada operación
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