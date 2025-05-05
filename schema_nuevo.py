# Archivo actualizado
from sqlalchemy import Float, SmallInteger, Text, Date, Numeric

COLUMNS = [
    {
        "key": "contractcode",
        "sql_name": "Contract Code",
        "aliases": [
            "ContractCode",
            "contractcode",
            "contract_code"
        ],
        "dtype": "TEXT",
        "source": "metadata"
    },
    {
        "key": "farmername",
        "sql_name": "FarmerName",
        "aliases": [
            "FarmerName",
            "farmername"
        ],
        "dtype": "TEXT",
        "source": "metadata"
    },
    {
        "key": "cruisedate",
        "sql_name": "CruiseDate",
        "aliases": [
            "CruiseDate",
            "cruisedate"
        ],
        "dtype": "DATE",
        "source": "metadata"
    },
    {
        "key": "id",
        "sql_name": "id",
        "aliases": [
            "id"
        ],
        "dtype": "TEXT",
        "source": "calculated"
    },
    {
        "key": "stand",
        "sql_name": "Stand#",
        "aliases": [
            "Stand #",
            "# Posici\u00f3n",
            "_posicion_"
        ],
        "dtype": "FLOAT",
        "source": "input"
    },
    {
        "key": "plot",
        "sql_name": "Plot#",
        "aliases": [
            "Plot #",
            "# Parcela",
            "_parcela",
            "plot"
        ],
        "dtype": "FLOAT",
        "source": "input"
    },
    {
        "key": "plot_coordinate",
        "sql_name": "PlotCoordinate",
        "aliases": [
            "Plot Coordinate",
            "Coordenadas de la Parcela",
            "Plot Cooridnate",
            "Plot Cooridinate",
            "coordenadas_de_la_parcela"
        ],
        "dtype": "TEXT",
        "source": "input"
    },
    {
        "key": "tree_number",
        "sql_name": "Tree#",
        "aliases": [
            "Tree #",
            "# \u00c1rbol",
            "# Arbol",
            "_arbol",
            "tree",
            "tree_#",
            "arbol",
            "#_arbol",
            "tree_number",
            "_\u00e1rbol",
            "\u00e1rbol",
            "#_\u00e1rbol"
        ],
        "dtype": "FLOAT",
        "source": "input"
    },
    {
        "key": "defect_ht_ft",
        "sql_name": "Defect HT(ft)",
        "aliases": [
            "Defect HT (ft)",
            "AT del Defecto (m)",
            "at_del_defecto_m"
        ],
        "dtype": "NUMERIC",
        "source": "input"
    },
    {
        "key": "dbh_in",
        "sql_name": "DBH (in)",
        "aliases": [
            "DBH (in)",
            "DAP (cm)",
            "dap_cm",
            "dbh_in"
        ],
        "dtype": "NUMERIC",
        "source": "input"
    },
    {
        "key": "tht_ft",
        "sql_name": "THT (ft)",
        "aliases": [
            "THT (ft)",
            "AT (m)",
            "at_m",
            "tht_ft"
        ],
        "dtype": "NUMERIC",
        "source": "input"
    },
    {
        "key": "merch_ht_ft",
        "sql_name": "Merch. HT (ft)",
        "aliases": [
            "Merch. HT (ft)",
            "Alt. Com. (m)",
            "alt_com_m",
            "merch_ht_ft"
        ],
        "dtype": "NUMERIC",
        "source": "input"
    },
    {
        "key": "short_note",
        "sql_name": "Short Note",
        "aliases": [
            "Short Note",
            "Nota Breve",
            "nota_breve"
        ],
        "dtype": "TEXT",
        "source": "input"
    },
    {
        "key": "Status",
        "aliases": [
            "Status",
            "Condicion",
            "Estado",
            "Condici\u00f3n",
            "estado",
            "condici\u00f3n"
        ],
        "sql_name": "status_id",
        "source": "input"
    },
    {
        "key": "species_id",
        "sql_name": "cat_species_id",
        "aliases": [
            "species_id",
            "Especie"
        ],
        "dtype": "SMALLINT",
        "source": "calculated"
    },
    {
        "key": "defect_id",
        "sql_name": "cat_defect_id",
        "aliases": [
            "defect_id",
            "Defect"
        ],
        "dtype": "SMALLINT",
        "source": "calculated"
    },
    {
        "key": "pests_id",
        "sql_name": "cat_pest_id",
        "aliases": [
            "pests_id",
            "Plagas"
        ],
        "dtype": "SMALLINT",
        "source": "calculated"
    },
    {
        "key": "coppiced_id",
        "sql_name": "cat_coppiced_id",
        "aliases": [
            "coppiced_id",
            "Poda Basal"
        ],
        "dtype": "SMALLINT",
        "source": "calculated"
    },
    {
        "key": "permanent_plot_id",
        "sql_name": "cat_permanent_plot_id",
        "aliases": [
            "permanent_plot_id",
            "Parcela Permanente"
        ],
        "dtype": "SMALLINT",
        "source": "calculated"
    },
    {
        "key": "disease_id",
        "sql_name": "cat_disease_id",
        "aliases": [
            "disease_id",
            "Enfermedadas"
        ],
        "dtype": "SMALLINT",
        "source": "calculated"
    },
    {
        "key": "doyle_bf",
        "sql_name": "doyle_bf",
        "aliases": [
            "doyle_bf"
        ],
        "dtype": "NUMERIC",
        "source": "calculated"
    },
    {
        "key": "dead_tree",
        "sql_name": "dead_tree",
        "aliases": [
            "DeadTreeValue",
            "Valor_Muerto",
            "valor_muerto",
            "Muerto",
            "muerto",
            "Dead_Value"
        ],
        "dtype": "FLOAT",
        "source": "calculated"
    },
    {
        "key": "alive_tree",
        "sql_name": "alive_tree",
        "aliases": [
            "AliveTree",
            "Valor_Vivo",
            "valor_vivo",
            "Vivo",
            "vivo",
            "Alive_Value"
        ],
        "dtype": "FLOAT",
        "source": "calculated"
    }
]