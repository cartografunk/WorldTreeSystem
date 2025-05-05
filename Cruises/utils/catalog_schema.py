COLUMNS_CAT_FARMERS = [
    {
        "key": "contractcode",
        "sql_name": "contractcode",
        "aliases": ["contract_code", "Contract Code", "contractcode"],
        "dtype": "TEXT",
        "source": "cat_farmers"
    },
    {
        "key": "farmername",
        "sql_name": "farmername",
        "aliases": ["nombre del productor", "farmer name", "farmername"],
        "dtype": "TEXT",
        "source": "cat_farmers"
    },
    {
        "key": "planting_year",
        "sql_name": "planting_year",
        "aliases": ["Año de plantación", "planting year", "year"],
        "dtype": "INTEGER",
        "source": "cat_farmers"
    },
    {
        "key": "contracted_trees",
        "sql_name": "contracted_trees",
        "aliases": ["árboles contratados", "# árboles", "contracted trees"],
        "dtype": "INTEGER",
        "source": "cat_farmers"
    },
]

def rename_farmers_columns(df):
    rename_map = {}

    for col_def in COLUMNS_CAT_FARMERS:
        logical = col_def["key"]
        for alias in [col_def["sql_name"]] + col_def.get("aliases", []):
            if alias in df.columns:
                rename_map[alias] = logical
    return df.rename(columns=rename_map)

