# WorldTreeSystem/Cruises/utils/schema.py

from sqlalchemy import Text, Float, SmallInteger, Numeric, Date

# 1) Lista única de tu “esquema” con nombre interno, SQL y tipos
COLUMNS = [
    {
        "key": "contractcode",
        "sql_name": "Contract Code",
        "aliases": ["ContractCode", "contractcode", "contract_code"],
        "dtype": Text(),
    },
    {
        "key": "farmername",
        "sql_name": "FarmerName",
        "aliases": ["FarmerName", "farmername"],
        "dtype": Text(),
    },
    {
        "key": "cruisedate",
        "sql_name": "CruiseDate",
        "aliases": ["CruiseDate", "cruisedate"],
        "dtype": Date(),
    },
    {
        "key": "id",
        "sql_name": "id",
        "aliases": ["id"],
        "dtype": Text(),
    },
    {
        "key": "stand",
        "sql_name": "Stand#",
        "aliases": ["Stand #", "# Posición", "_posicion_"],
        "dtype": Float(),
    },
    {
        "key": "plot",
        "sql_name": "Plot#",
        "aliases": ["Plot #", "# Parcela", "_parcela", "plot"],
        "dtype": Float(),
    },
    {
        "key": "plot_coordinate",
        "sql_name": "PlotCoordinate",
        "aliases": ["Plot Coordinate", "Coordenadas de la Parcela", "Plot Cooridnate", "Plot Cooridinate", "coordenadas_de_la_parcela"],
        "dtype": Text(),
    },
    {
        "key": "tree_number",
        "sql_name": "Tree#",
        "aliases": ["Tree #", "# Árbol", "# Arbol", "_arbol", "tree", "tree_#", "arbol", "#_arbol", "tree_number", "_árbol", "árbol", "#_árbol"],
        "dtype": Float(),
    },
    {
        "key": "defect_ht_ft",
        "sql_name": "Defect HT(ft)",
        "aliases": ["Defect HT (ft)", "AT del Defecto (m)", "at_del_defecto_m"],
        "dtype": Numeric(),
    },
    {
        "key": "dbh_in",
        "sql_name": "DBH (in)",
        "aliases": ["DBH (in)", "DAP (cm)", "dap_cm", "dbh_in"],
        "dtype": Numeric(),
    },
    {
        "key": "tht_ft",
        "sql_name": "THT (ft)",
        "aliases": ["THT (ft)", "AT (m)", "at_m", "tht_ft"],
        "dtype": Numeric(),
    },
    {
        "key": "merch_ht_ft",
        "sql_name": "Merch. HT (ft)",
        "aliases": ["Merch. HT (ft)", "Alt. Com. (m)", "alt_com_m", "merch_ht_ft"],
        "dtype": Numeric(),
    },
    {
        "key": "short_note",
        "sql_name": "Short Note",
        "aliases": ["Short Note", "Nota Breve", "nota_breve"],
        "dtype": Text(),
    },
    {
        "key": "status_id",
        "sql_name": "status_id",
        "aliases": ["status_id", "EstadoID"],
        "dtype": SmallInteger(),
    },
    {
        "key": "species_id",
        "sql_name": "cat_species_id",
        "aliases": ["species_id", "Especie"],
        "dtype": SmallInteger(),
    },
    {
        "key": "defect_id",
        "sql_name": "cat_defect_id",
        "aliases": ["defect_id", "Defect"],
        "dtype": SmallInteger(),
    },
    {
        "key": "pests_id",
        "sql_name": "cat_pest_id",
        "aliases": ["pests_id", "Plagas"],
        "dtype": SmallInteger(),
    },
    {
        "key": "coppiced_id",
        "sql_name": "cat_coppiced_id",
        "aliases": ["coppiced_id", "Poda Basal"],
        "dtype": SmallInteger(),
    },
    {
        "key": "permanent_plot_id",
        "sql_name": "cat_permanent_plot_id",
        "aliases": ["permanent_plot_id", "Parcela Permanente"],
        "dtype": SmallInteger(),
    },
    {
        "key": "disease_id",
        "sql_name": "cat_disease_id",
        "aliases": ["disease_id", "Enfermedadas"],
        "dtype": SmallInteger(),
    },
    {
        "key": "doyle_bf",
        "sql_name": "doyle_bf",
        "aliases": ["doyle_bf"],
        "dtype": Numeric(),
    },
    {
        "key": "dead_tree",
        "sql_name": "dead_tree",
        "aliases": ["DeadTreeValue", "Valor_Muerto", "valor_muerto",
        "Muerto", "muerto", "Dead_Value"],
        "dtype": Float(),
    },
    {
        "key": "alive_tree",
        "sql_name": "alive_tree",
        "aliases": ["AliveTree", "Valor_Vivo", "valor_vivo",
        "Vivo", "vivo", "Alive_Value"],
        "dtype": Float(),
    },
]
