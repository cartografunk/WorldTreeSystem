def process_inventory_table(engine, table):
    df = pd.read_sql(f'SELECT * FROM public.{table}', engine)
    #print(f"\n📄 Columnas cargadas para {table}: {df.columns.tolist()}")
    if df.empty:
        return []

    df = df.copy()

    # Obtener columnas reales desde el esquema
    contract_col = get_column("contractcode", df)
    #print(f"✅ contract_col: {contract_col}")
    dbh_col = get_column("dbh_in", df)
    tht_col = get_column("tht_ft", df)
    mht_col = get_column("merch_ht_ft", df)
    dead_tree_col = get_column("dead_tree", df)
    alive_tree_col = get_column("alive_tree", df)
    doyle_col = get_column("doyle_bf", df)

    #print(f"✅ status_col: {status_col}")

    # Crear vista filtrada sin perder metadatos como CruiseDate
    filtered_df = df[
        df[dbh_col].notna() &
        df[tht_col].notna() &
        df[mht_col].notna() &
        df[doyle_col].notna() &
        df[dead_tree_col].notna() &
        df[alive_tree_col].notna() &
        df[doyle_col].notna()
    ]

    rows = []

    for contract_code, group in filtered_df.groupby(contract_col):

        total_alive = group["alive_tree"].sum()
        total_dead = group["dead_tree"].sum()
        total_trees = total_alive + total_dead
        survival = round((total_alive / total_trees) * 100, 2) if total_trees else 0.0
        mortality = round((total_dead / total_trees) * 100, 2) if total_trees else 0.0

        country_year = re.findall(r"inventory_([a-z]+)_(\d{4})", table)[0]
        country, year = country_year
        year = int(year)

        cruise_date = get_cruise_date(df, contract_code, engine, country, year)
        if cruise_date is None:
            cruise_date = "pending"  # default value when no date is available

        live = group  # ya no filtramos por árboles vivos

        row = {
            "contract_code": contract_code,
            "inventory_year": year,
            "region": country,
            "inventory_date": cruise_date,
            "total_trees": total_trees,
            "survival": f"{survival}%",
            "mortality": f"{mortality}%",
            "dbh_mean": round(safe_numeric(live[dbh_col]).mean(), 2),
            "dbh_std": round(safe_numeric(live[dbh_col]).std(), 2),
            "tht_mean": round(safe_numeric(live[tht_col]).mean(), 2),
            "tht_std": round(safe_numeric(live[tht_col]).std(), 2),
            "mht_mean": round(safe_numeric(live[mht_col]).mean(), 2),
            "mht_std": round(safe_numeric(live[mht_col]).std(), 2),
            "doyle_bf_mean": round(safe_numeric(live[doyle_col]).mean(), 2),
            "doyle_bf_std": round(safe_numeric(live[doyle_col]).std(), 2),
            "doyle_bf_total": round(safe_numeric(live[doyle_col]).sum(), 2),
        }

        rows.append(row)

    return rows
