-- =========================================================
-- INVENTORY PIPELINE COMPLETE
-- WorldTreeSystem - Inventory Schema
-- Re-executable pipeline from staging to master_inventories
-- =========================================================

-- =========================================================
-- STEP 1: RECREATE FACT TABLE WITH FULL STRUCTURE
-- =========================================================

DROP TABLE IF EXISTS inventory.fact_contract_inventory_year CASCADE;

CREATE TABLE inventory.fact_contract_inventory_year (
    contract_code TEXT NOT NULL,
    planting_year INT,
    inventory_year INT NOT NULL,
    planting_age INT,
    expected_inventory_type TEXT,
    observed_inventory BOOLEAN,
    trees_observed BIGINT,
    survival NUMERIC,
    dbh_in_mean NUMERIC,
    merch_ht_ft_mean NUMERIC,
    doyle_bf_mean NUMERIC,
    source_inventory TEXT,
    out_of_program_year INT,
    created_at TIMESTAMP DEFAULT now(),
    PRIMARY KEY (contract_code, inventory_year)
);

-- =========================================================
-- STEP 2: REBUILD FACT FUNCTION (CORREGIDO)
-- =========================================================

CREATE OR REPLACE FUNCTION inventory.rebuild_fact()
RETURNS TABLE(
    rows_inserted BIGINT,
    execution_time_ms INTEGER
)
LANGUAGE plpgsql
AS $$
DECLARE
    start_time TIMESTAMP;
    end_time TIMESTAMP;
    row_count BIGINT;
BEGIN
    start_time := clock_timestamp();

    -- Clear fact table (TRUNCATE instead of DELETE)
    TRUNCATE TABLE inventory.fact_contract_inventory_year;

    -- Build fact from staging + CTI + dim
    WITH contract_years AS (
        SELECT
            c.contract_code,
            c.planting_year,
            c.out_of_program_year,
            d.planting_age,
            (c.planting_year + d.planting_age) AS inventory_year,
            d.inventory_type AS expected_inventory_type
        FROM masterdatabase.contract_tree_information c
        JOIN inventory.dim_inventory_type_by_age d
          ON d.planting_age BETWEEN 0 AND 11
    ),
    observed AS (
        SELECT
            contractcode AS contract_code,
            inventory_year,
            COUNT(*) AS trees_observed,
            CASE
                WHEN SUM(alive_tree + dead_tree) > 0
                THEN SUM(alive_tree) / SUM(alive_tree + dead_tree)
                ELSE NULL
            END AS survival,
            AVG(dbh_in) AS dbh_in_mean,
            AVG(merch_ht_ft) AS merch_ht_ft_mean,
            AVG(doyle_bf) AS doyle_bf_mean,
            STRING_AGG(DISTINCT source_table, ', ') AS source_inventory
        FROM inventory.stg_inventory_unified_v1
        GROUP BY contractcode, inventory_year
    )
    INSERT INTO inventory.fact_contract_inventory_year (
        contract_code,
        planting_year,
        inventory_year,
        planting_age,
        expected_inventory_type,
        observed_inventory,
        trees_observed,
        survival,
        dbh_in_mean,
        merch_ht_ft_mean,
        doyle_bf_mean,
        source_inventory,
        out_of_program_year
    )
    SELECT
        cy.contract_code,
        cy.planting_year,
        cy.inventory_year,
        cy.planting_age,
        cy.expected_inventory_type,
        (o.contract_code IS NOT NULL) AS observed_inventory,
        o.trees_observed,
        o.survival,
        o.dbh_in_mean,
        o.merch_ht_ft_mean,
        o.doyle_bf_mean,
        o.source_inventory,
        cy.out_of_program_year
    FROM contract_years cy
    LEFT JOIN observed o
      ON cy.contract_code = o.contract_code
     AND cy.inventory_year = o.inventory_year;

    GET DIAGNOSTICS row_count = ROW_COUNT;
    end_time := clock_timestamp();

    RETURN QUERY SELECT
        row_count,
        EXTRACT(MILLISECONDS FROM (end_time - start_time))::INTEGER;
END;
$$;

-- =========================================================
-- STEP 3: VW_INVENTORY_EVENTS
-- =========================================================

CREATE OR REPLACE VIEW inventory.vw_inventory_events AS
SELECT
    f.contract_code,
    f.inventory_year,
    f.survival,
    f.dbh_in_mean,
    f.merch_ht_ft_mean,
    f.doyle_bf_mean,
    ROW_NUMBER() OVER (
        PARTITION BY f.contract_code
        ORDER BY f.inventory_year
    ) AS inventory_event
FROM inventory.fact_contract_inventory_year f
JOIN masterdatabase.contract_tree_information c
  ON f.contract_code = c.contract_code
WHERE
    f.observed_inventory = TRUE
    AND c.status <> 'Out of Program';

-- =========================================================
-- STEP 4: INVENTORY_EXEC_MATRIX (VIEW WITH PIVOT)
-- =========================================================

CREATE OR REPLACE VIEW inventory.inventory_exec_matrix AS
SELECT
    contract_code,

    -- Inventory 1
    MAX(CASE WHEN inventory_event = 1 THEN inventory_year END) AS inv1_year,
    MAX(CASE WHEN inventory_event = 1 THEN FLOOR(survival * 100) / 100 END) AS inv1_survival,
    MAX(CASE WHEN inventory_event = 1 THEN FLOOR(dbh_in_mean * 100) / 100 END) AS inv1_dbh_in_mean,
    MAX(CASE WHEN inventory_event = 1 THEN FLOOR(merch_ht_ft_mean * 100) / 100 END) AS inv1_merch_ht_ft_mean,
    MAX(CASE WHEN inventory_event = 1 THEN FLOOR(doyle_bf_mean * 100) / 100 END) AS inv1_doyle_bf_mean,

    -- Inventory 2
    MAX(CASE WHEN inventory_event = 2 THEN inventory_year END) AS inv2_year,
    MAX(CASE WHEN inventory_event = 2 THEN FLOOR(survival * 100) / 100 END) AS inv2_survival,
    MAX(CASE WHEN inventory_event = 2 THEN FLOOR(dbh_in_mean * 100) / 100 END) AS inv2_dbh_in_mean,
    MAX(CASE WHEN inventory_event = 2 THEN FLOOR(merch_ht_ft_mean * 100) / 100 END) AS inv2_merch_ht_ft_mean,
    MAX(CASE WHEN inventory_event = 2 THEN FLOOR(doyle_bf_mean * 100) / 100 END) AS inv2_doyle_bf_mean,

    -- Inventory 3
    MAX(CASE WHEN inventory_event = 3 THEN inventory_year END) AS inv3_year,
    MAX(CASE WHEN inventory_event = 3 THEN FLOOR(survival * 100) / 100 END) AS inv3_survival,
    MAX(CASE WHEN inventory_event = 3 THEN FLOOR(dbh_in_mean * 100) / 100 END) AS inv3_dbh_in_mean,
    MAX(CASE WHEN inventory_event = 3 THEN FLOOR(merch_ht_ft_mean * 100) / 100 END) AS inv3_merch_ht_ft_mean,
    MAX(CASE WHEN inventory_event = 3 THEN FLOOR(doyle_bf_mean * 100) / 100 END) AS inv3_doyle_bf_mean

FROM inventory.vw_inventory_events
GROUP BY contract_code;

-- =========================================================
-- STEP 5: MASTER_INVENTORIES (FULL MASKING LOGIC)
-- =========================================================

CREATE OR REPLACE VIEW inventory.inventory_exec_matrix AS
WITH all_contracts AS (
    SELECT DISTINCT contract_code
    FROM masterdatabase.contract_tree_information
),
expected_years AS (
    SELECT
        c.contract_code,
        c.planting_year + 5 AS expected_inv1_year,
        c.planting_year + 8 AS expected_inv2_year,
        c.planting_year + 11 AS expected_inv3_year
    FROM masterdatabase.contract_tree_information c
)
SELECT
    ac.contract_code,

    -- Inventory 1: observed OR expected
    COALESCE(
        MAX(CASE WHEN e.inventory_event = 1 THEN e.inventory_year END),
        ey.expected_inv1_year
    ) AS inv1_year,
    MAX(CASE WHEN e.inventory_event = 1 THEN FLOOR(e.survival * 100) / 100 END) AS inv1_survival,
    MAX(CASE WHEN e.inventory_event = 1 THEN FLOOR(e.dbh_in_mean * 100) / 100 END) AS inv1_dbh_in_mean,
    MAX(CASE WHEN e.inventory_event = 1 THEN FLOOR(e.merch_ht_ft_mean * 100) / 100 END) AS inv1_merch_ht_ft_mean,
    MAX(CASE WHEN e.inventory_event = 1 THEN FLOOR(e.doyle_bf_mean * 100) / 100 END) AS inv1_doyle_bf_mean,

    -- Inventory 2: observed OR expected
    COALESCE(
        MAX(CASE WHEN e.inventory_event = 2 THEN e.inventory_year END),
        ey.expected_inv2_year
    ) AS inv2_year,
    MAX(CASE WHEN e.inventory_event = 2 THEN FLOOR(e.survival * 100) / 100 END) AS inv2_survival,
    MAX(CASE WHEN e.inventory_event = 2 THEN FLOOR(e.dbh_in_mean * 100) / 100 END) AS inv2_dbh_in_mean,
    MAX(CASE WHEN e.inventory_event = 2 THEN FLOOR(e.merch_ht_ft_mean * 100) / 100 END) AS inv2_merch_ht_ft_mean,
    MAX(CASE WHEN e.inventory_event = 2 THEN FLOOR(e.doyle_bf_mean * 100) / 100 END) AS inv2_doyle_bf_mean,

    -- Inventory 3: observed OR expected
    COALESCE(
        MAX(CASE WHEN e.inventory_event = 3 THEN e.inventory_year END),
        ey.expected_inv3_year
    ) AS inv3_year,
    MAX(CASE WHEN e.inventory_event = 3 THEN FLOOR(e.survival * 100) / 100 END) AS inv3_survival,
    MAX(CASE WHEN e.inventory_event = 3 THEN FLOOR(e.dbh_in_mean * 100) / 100 END) AS inv3_dbh_in_mean,
    MAX(CASE WHEN e.inventory_event = 3 THEN FLOOR(e.merch_ht_ft_mean * 100) / 100 END) AS inv3_merch_ht_ft_mean,
    MAX(CASE WHEN e.inventory_event = 3 THEN FLOOR(e.doyle_bf_mean * 100) / 100 END) AS inv3_doyle_bf_mean

FROM all_contracts ac
JOIN expected_years ey ON ac.contract_code = ey.contract_code
LEFT JOIN inventory.vw_inventory_events e ON ac.contract_code = e.contract_code
GROUP BY ac.contract_code, ey.expected_inv1_year, ey.expected_inv2_year, ey.expected_inv3_year;

-- Recrear master_inventories
CREATE OR REPLACE VIEW inventory.master_inventories AS
SELECT
    iem.contract_code AS "Contract Code",

    -- INVENTORY 1
    inv1_year AS "Inventory 1 Year",

    CASE
        WHEN inv1_survival IS NOT NULL THEN
            to_char(FLOOR(inv1_survival * 100) / 100, 'FM999999990.00')
        WHEN status = 'Out of Program' THEN
            'Out of Program'
        WHEN inv1_year > EXTRACT(YEAR FROM CURRENT_DATE) THEN
            'Pending ' || inv1_year
        WHEN inv1_year <= EXTRACT(YEAR FROM CURRENT_DATE)
             AND (inv1_survival IS NULL OR inv1_dbh_in_mean IS NULL
                  OR inv1_merch_ht_ft_mean IS NULL OR inv1_doyle_bf_mean IS NULL)
             AND (inv1_survival IS NOT NULL OR inv1_dbh_in_mean IS NOT NULL
                  OR inv1_merch_ht_ft_mean IS NOT NULL OR inv1_doyle_bf_mean IS NOT NULL) THEN
            'NA'
        ELSE NULL
    END AS "Survival 1",

    CASE
        WHEN inv1_dbh_in_mean IS NOT NULL THEN
            to_char(FLOOR(inv1_dbh_in_mean * 100) / 100, 'FM999999990.00')
        WHEN status = 'Out of Program' THEN
            'Out of Program'
        WHEN inv1_year > EXTRACT(YEAR FROM CURRENT_DATE) THEN
            'Pending ' || inv1_year
        WHEN inv1_year <= EXTRACT(YEAR FROM CURRENT_DATE)
             AND (inv1_survival IS NULL OR inv1_dbh_in_mean IS NULL
                  OR inv1_merch_ht_ft_mean IS NULL OR inv1_doyle_bf_mean IS NULL)
             AND (inv1_survival IS NOT NULL OR inv1_dbh_in_mean IS NOT NULL
                  OR inv1_merch_ht_ft_mean IS NOT NULL OR inv1_doyle_bf_mean IS NOT NULL) THEN
            'NA'
        ELSE NULL
    END AS "DBH Mean 1",

    CASE
        WHEN inv1_merch_ht_ft_mean IS NOT NULL THEN
            to_char(FLOOR(inv1_merch_ht_ft_mean * 100) / 100, 'FM999999990.00')
        WHEN status = 'Out of Program' THEN
            'Out of Program'
        WHEN inv1_year > EXTRACT(YEAR FROM CURRENT_DATE) THEN
            'Pending ' || inv1_year
        WHEN inv1_year <= EXTRACT(YEAR FROM CURRENT_DATE)
             AND (inv1_survival IS NULL OR inv1_dbh_in_mean IS NULL
                  OR inv1_merch_ht_ft_mean IS NULL OR inv1_doyle_bf_mean IS NULL)
             AND (inv1_survival IS NOT NULL OR inv1_dbh_in_mean IS NOT NULL
                  OR inv1_merch_ht_ft_mean IS NOT NULL OR inv1_doyle_bf_mean IS NOT NULL) THEN
            'NA'
        ELSE NULL
    END AS "Merch HT Mean 1",

    CASE
        WHEN inv1_doyle_bf_mean IS NOT NULL THEN
            to_char(FLOOR(inv1_doyle_bf_mean * 100) / 100, 'FM999999990.00')
        WHEN status = 'Out of Program' THEN
            'Out of Program'
        WHEN inv1_year > EXTRACT(YEAR FROM CURRENT_DATE) THEN
            'Pending ' || inv1_year
        WHEN inv1_year <= EXTRACT(YEAR FROM CURRENT_DATE)
             AND (inv1_survival IS NULL OR inv1_dbh_in_mean IS NULL
                  OR inv1_merch_ht_ft_mean IS NULL OR inv1_doyle_bf_mean IS NULL)
             AND (inv1_survival IS NOT NULL OR inv1_dbh_in_mean IS NOT NULL
                  OR inv1_merch_ht_ft_mean IS NOT NULL OR inv1_doyle_bf_mean IS NOT NULL) THEN
            'NA'
        ELSE NULL
    END AS "Doyle BF 1",

    -- INVENTORY 2
    inv2_year AS "Inventory 2 Year",

    CASE
        WHEN inv2_survival IS NOT NULL THEN
            to_char(FLOOR(inv2_survival * 100) / 100, 'FM999999990.00')
        WHEN status = 'Out of Program' THEN
            'Out of Program'
        WHEN inv2_year > EXTRACT(YEAR FROM CURRENT_DATE) THEN
            'Pending ' || inv2_year
        WHEN inv2_year <= EXTRACT(YEAR FROM CURRENT_DATE)
             AND (inv2_survival IS NULL OR inv2_dbh_in_mean IS NULL
                  OR inv2_merch_ht_ft_mean IS NULL OR inv2_doyle_bf_mean IS NULL)
             AND (inv2_survival IS NOT NULL OR inv2_dbh_in_mean IS NOT NULL
                  OR inv2_merch_ht_ft_mean IS NOT NULL OR inv2_doyle_bf_mean IS NOT NULL) THEN
            'NA'
        ELSE NULL
    END AS "Survival 2",

    CASE
        WHEN inv2_dbh_in_mean IS NOT NULL THEN
            to_char(FLOOR(inv2_dbh_in_mean * 100) / 100, 'FM999999990.00')
        WHEN status = 'Out of Program' THEN
            'Out of Program'
        WHEN inv2_year > EXTRACT(YEAR FROM CURRENT_DATE) THEN
            'Pending ' || inv2_year
        WHEN inv2_year <= EXTRACT(YEAR FROM CURRENT_DATE)
             AND (inv2_survival IS NULL OR inv2_dbh_in_mean IS NULL
                  OR inv2_merch_ht_ft_mean IS NULL OR inv2_doyle_bf_mean IS NULL)
             AND (inv2_survival IS NOT NULL OR inv2_dbh_in_mean IS NOT NULL
                  OR inv2_merch_ht_ft_mean IS NOT NULL OR inv2_doyle_bf_mean IS NOT NULL) THEN
            'NA'
        ELSE NULL
    END AS "DBH Mean 2",

    CASE
        WHEN inv2_merch_ht_ft_mean IS NOT NULL THEN
            to_char(FLOOR(inv2_merch_ht_ft_mean * 100) / 100, 'FM999999990.00')
        WHEN status = 'Out of Program' THEN
            'Out of Program'
        WHEN inv2_year > EXTRACT(YEAR FROM CURRENT_DATE) THEN
            'Pending ' || inv2_year
        WHEN inv2_year <= EXTRACT(YEAR FROM CURRENT_DATE)
             AND (inv2_survival IS NULL OR inv2_dbh_in_mean IS NULL
                  OR inv2_merch_ht_ft_mean IS NULL OR inv2_doyle_bf_mean IS NULL)
             AND (inv2_survival IS NOT NULL OR inv2_dbh_in_mean IS NOT NULL
                  OR inv2_merch_ht_ft_mean IS NOT NULL OR inv2_doyle_bf_mean IS NOT NULL) THEN
            'NA'
        ELSE NULL
    END AS "Merch HT Mean 2",

    CASE
        WHEN inv2_doyle_bf_mean IS NOT NULL THEN
            to_char(FLOOR(inv2_doyle_bf_mean * 100) / 100, 'FM999999990.00')
        WHEN status = 'Out of Program' THEN
            'Out of Program'
        WHEN inv2_year > EXTRACT(YEAR FROM CURRENT_DATE) THEN
            'Pending ' || inv2_year
        WHEN inv2_year <= EXTRACT(YEAR FROM CURRENT_DATE)
             AND (inv2_survival IS NULL OR inv2_dbh_in_mean IS NULL
                  OR inv2_merch_ht_ft_mean IS NULL OR inv2_doyle_bf_mean IS NULL)
             AND (inv2_survival IS NOT NULL OR inv2_dbh_in_mean IS NOT NULL
                  OR inv2_merch_ht_ft_mean IS NOT NULL OR inv2_doyle_bf_mean IS NOT NULL) THEN
            'NA'
        ELSE NULL
    END AS "Doyle BF 2",

    -- INVENTORY 3
    inv3_year AS "Inventory 3 Year",

    CASE
        WHEN inv3_survival IS NOT NULL THEN
            to_char(FLOOR(inv3_survival * 100) / 100, 'FM999999990.00')
        WHEN status = 'Out of Program' THEN
            'Out of Program'
        WHEN inv3_year > EXTRACT(YEAR FROM CURRENT_DATE) THEN
            'Pending ' || inv3_year
        WHEN inv3_year <= EXTRACT(YEAR FROM CURRENT_DATE)
             AND (inv3_survival IS NULL OR inv3_dbh_in_mean IS NULL
                  OR inv3_merch_ht_ft_mean IS NULL OR inv3_doyle_bf_mean IS NULL)
             AND (inv3_survival IS NOT NULL OR inv3_dbh_in_mean IS NOT NULL
                  OR inv3_merch_ht_ft_mean IS NOT NULL OR inv3_doyle_bf_mean IS NOT NULL) THEN
            'NA'
        ELSE NULL
    END AS "Survival 3",

    CASE
        WHEN inv3_dbh_in_mean IS NOT NULL THEN
            to_char(FLOOR(inv3_dbh_in_mean * 100) / 100, 'FM999999990.00')
        WHEN status = 'Out of Program' THEN
            'Out of Program'
        WHEN inv3_year > EXTRACT(YEAR FROM CURRENT_DATE) THEN
            'Pending ' || inv3_year
        WHEN inv3_year <= EXTRACT(YEAR FROM CURRENT_DATE)
             AND (inv3_survival IS NULL OR inv3_dbh_in_mean IS NULL
                  OR inv3_merch_ht_ft_mean IS NULL OR inv3_doyle_bf_mean IS NULL)
             AND (inv3_survival IS NOT NULL OR inv3_dbh_in_mean IS NOT NULL
                  OR inv3_merch_ht_ft_mean IS NOT NULL OR inv3_doyle_bf_mean IS NOT NULL) THEN
            'NA'
        ELSE NULL
    END AS "DBH Mean 3",

    CASE
        WHEN inv3_merch_ht_ft_mean IS NOT NULL THEN
            to_char(FLOOR(inv3_merch_ht_ft_mean * 100) / 100, 'FM999999990.00')
        WHEN status = 'Out of Program' THEN
            'Out of Program'
        WHEN inv3_year > EXTRACT(YEAR FROM CURRENT_DATE) THEN
            'Pending ' || inv3_year
        WHEN inv3_year <= EXTRACT(YEAR FROM CURRENT_DATE)
             AND (inv3_survival IS NULL OR inv3_dbh_in_mean IS NULL
                  OR inv3_merch_ht_ft_mean IS NULL OR inv3_doyle_bf_mean IS NULL)
             AND (inv3_survival IS NOT NULL OR inv3_dbh_in_mean IS NOT NULL
                  OR inv3_merch_ht_ft_mean IS NOT NULL OR inv3_doyle_bf_mean IS NOT NULL) THEN
            'NA'
        ELSE NULL
    END AS "Merch HT Mean 3",

    CASE
        WHEN inv3_doyle_bf_mean IS NOT NULL THEN
            to_char(FLOOR(inv3_doyle_bf_mean * 100) / 100, 'FM999999990.00')
        WHEN status = 'Out of Program' THEN
            'Out of Program'
        WHEN inv3_year > EXTRACT(YEAR FROM CURRENT_DATE) THEN
            'Pending ' || inv3_year
        WHEN inv3_year <= EXTRACT(YEAR FROM CURRENT_DATE)
             AND (inv3_survival IS NULL OR inv3_dbh_in_mean IS NULL
                  OR inv3_merch_ht_ft_mean IS NULL OR inv3_doyle_bf_mean IS NULL)
             AND (inv3_survival IS NOT NULL OR inv3_dbh_in_mean IS NOT NULL
                  OR inv3_merch_ht_ft_mean IS NOT NULL OR inv3_doyle_bf_mean IS NOT NULL) THEN
            'NA'
        ELSE NULL
    END AS "Doyle BF 3"

FROM inventory.inventory_exec_matrix iem
LEFT JOIN masterdatabase.contract_tree_information cti
  ON cti.contract_code = iem.contract_code;

-- =========================================================
-- ORCHESTRATOR: RUN ENTIRE PIPELINE
-- =========================================================

CREATE OR REPLACE FUNCTION inventory.rebuild_pipeline()
RETURNS TABLE(
    step TEXT,
    rows_affected BIGINT,
    execution_time_ms INTEGER
)
LANGUAGE plpgsql
AS $$
DECLARE
    result RECORD;
BEGIN
    -- Step 1: Rebuild fact
    FOR result IN SELECT * FROM inventory.rebuild_fact() LOOP
        RETURN QUERY SELECT
            '1. Rebuild Fact'::TEXT,
            result.rows_inserted,
            result.execution_time_ms;
    END LOOP;

    -- Step 2: Views auto-refresh (just verify counts)
    RETURN QUERY SELECT
        '2. vw_inventory_events'::TEXT,
        COUNT(*)::BIGINT,
        0::INTEGER
    FROM inventory.vw_inventory_events;

    RETURN QUERY SELECT
        '3. inventory_exec_matrix'::TEXT,
        COUNT(*)::BIGINT,
        0::INTEGER
    FROM inventory.inventory_exec_matrix;

    RETURN QUERY SELECT
        '4. master_inventories'::TEXT,
        COUNT(*)::BIGINT,
        0::INTEGER
    FROM inventory.master_inventories;
END;
$$;

-- =========================================================
-- EXECUTE FULL PIPELINE
-- =========================================================
SELECT * FROM inventory.rebuild_pipeline();