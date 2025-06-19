# WorldTreeSystem/core/schema.py

COLUMNS = [
  {
    "key": "contractcode", "sql_name": "Contract Code",
    "aliases": ["ContractCode", "contractcode", "contract_code", "Contract Code", "Contract"],
    "dtype": "TEXT",
    "source": "metadata"
  },
  {
    "key": "farmername", "sql_name": "FarmerName",
    "aliases": ["FarmerName", "farmername", "Property"],
    "dtype": "TEXT",
    "source": "metadata"
  },
  {
    "key": "cruisedate", "sql_name": "CruiseDate",
    "aliases": ["CruiseDate", "cruisedate"],
    "dtype": "DATE",
    "source": "metadata"
  },
  {
    "key": "id", "sql_name": "id",
    "aliases": ["id"],
    "dtype": "TEXT",
    "source": "calculated"
  },
  {
    "key": "stand", "sql_name": "Stand#",
    "aliases": ["Stand #", "# Posición", "_posicion_", "StandID"],
    "dtype": "FLOAT",
    "source": "input"
  },
  {
    "key": "plot", "sql_name": "Plot#",
    "aliases": ["Plot #", "# Parcela", "_parcela", "plot", "Plot#"],
    "dtype": "TEXT",
    "source": "input"
  },
  {
    "key": "plot_coordinate", "sql_name": "PlotCoordinate",
    "aliases": ["Plot Coordinate", "Coordenadas de la Parcela", "Plot Cooridnate", "Plot Cooridinate", "coordenadas_de_la_parcela"],
    "dtype": "TEXT",
    "source": "input"
  },
  {
    "key": "tree_number", "sql_name": "Tree#",
    "aliases": ["Tree #", "# Árbol", "# Arbol", "_arbol", "tree", "tree_#", "arbol", "#_arbol", "tree_number", "_árbol", "árbol", "#_árbol", "TreeID"],
    "dtype": "FLOAT",
    "source": "input"
  },
  {
    "key": "defect_ht_ft", "sql_name": "Defect HT",
    "aliases": ["Defect HT (ft)", "AT del Defecto (m)", "at_del_defecto_m"],
    "dtype": "NUMERIC",
    "source": "input"
  },
  {
    "key": "dbh_in", "sql_name": "DBH",
    "aliases": ["DBH (in)", "DAP (cm)", "dap_cm", "dbh_in"],
    "dtype": "NUMERIC",
    "source": "input"
  },
  {
    "key": "tht_ft", "sql_name": "THT",
    "aliases": ["THT (ft)", "AT (m)", "at_m", "tht_ft", "THt (ft)", "THt (m)"],
    "dtype": "NUMERIC",
    "source": "input"
  },
  {
    "key": "merch_ht_ft", "sql_name": "Merch. HT",
    "aliases": ["Merch. HT (ft)", "Alt. Com. (m)", "alt_com_m", "merch_ht_ft", "MHt (ft)", "MHt (m)"],
    "dtype": "NUMERIC",
    "source": "input"
  },
  {
    "key": "short_note", "sql_name": "Short Note",
    "aliases": ["Short Note", "Nota Breve", "nota_breve"],
    "dtype": "TEXT",
    "source": "input"
  },
  {
    "key": "Status", "sql_name": "status",
    "aliases": ["Status", "Condicion", "Estado", "Condición", "estado", "condición", "condicion", "TreeStatus"],
    "dtype": "TEXT",
    "source": "input",
    "catalog_table": "cat_status",
    "catalog_field": "id"
  },
  {
    "key": "status_id", "sql_name": "status_id",
    "aliases": ["status_id"],
    "dtype": "SMALLINT",
    "source": "calculated"
  },
  {
    "key": "species_id", "sql_name": "cat_species_id",
    "aliases": ["species_id", "Especie"],
    "dtype": "SMALLINT",
    "source": "calculated"
  },
  {
    "key": "Species", "sql_name": "species",
    "aliases": ["Species", "Especie", "especie"],
    "source": "input",
    "catalog_table": "cat_species",
    "catalog_field": "id",
    "dtype": "TEXT"
  },
  {
    "key": "defect_id", "sql_name": "cat_defect_id",
    "aliases": ["defect_id", "Defect_id"],
    "dtype": "SMALLINT",
    "source": "calculated"
  },
  {
    "key": "pests_id", "sql_name": "cat_pest_id",
    "aliases": ["pests_id", "Plagas", "Pest_id", "Pests_id"],
    "dtype": "SMALLINT",
    "source": "calculated"
  },
  {
    "key": "coppiced_id", "sql_name": "cat_coppiced_id",
    "aliases": ["coppiced_id", "Poda Basal"],
    "dtype": "SMALLINT",
    "source": "calculated"
  },
  {
    "key": "permanent_plot_id", "sql_name": "cat_permanent_plot_id",
    "aliases": ["permanent_plot_id", "Parcela Permanente"],
    "dtype": "SMALLINT",
    "source": "calculated"
  },
  {
    "key": "disease_id", "sql_name": "cat_disease_id",
    "aliases": ["disease_id", "Enfermedadas", "Disease_id"],
    "dtype": "SMALLINT",
    "source": "calculated"
  },
  {
    "key": "doyle_bf", "sql_name": "doyle_bf",
    "aliases": ["doyle_bf"],
    "dtype": "NUMERIC",
    "source": "calculated"
  },
  {
    "key": "dead_tree", "sql_name": "dead_tree",
    "aliases": ["DeadTreeValue", "Valor_Muerto", "valor_muerto", "Muerto", "muerto", "Dead_Value"],
    "dtype": "FLOAT",
    "source": "calculated"
  },
  {
    "key": "alive_tree", "sql_name": "alive_tree",
    "aliases": ["AliveTree", "Valor_Vivo", "valor_vivo", "Vivo", "vivo", "Alive_Value"],
    "dtype": "FLOAT",
    "source": "calculated"
  },
    {
    "key": "Defect", "sql_name": "defect",
    "aliases": ["Defect", "Defecto", "defecto"],
    "source": "input",
    "catalog_table": "cat_defect",
    "catalog_field": "id"
  },
  {
    "key": "Pests", "sql_name": "pest",
    "aliases": ["Pests", "Plagas", "plagas"],
    "source": "input",
    "catalog_table": "cat_pest",
    "catalog_field": "id"
  },
  {
    "key": "Disease", "sql_name": "disease",
    "aliases": ["Disease", "Enfermedadas", "enfermedadas"],
    "source": "input",
    "catalog_table": "cat_disease",
    "catalog_field": "id"
  },
  {
    "key": "Coppiced", "sql_name": "coppiced",
    "aliases": ["Coppiced", "Poda Basal", "poda_basal"],
    "source": "input",
    "catalog_table": "cat_coppiced",
    "catalog_field": "id"
  },
  {
    "key": "Permanent Plot", "sql_name": "permanent_plot",
    "aliases": ["Permanent Plot", "Parcela Permanente", "parcela_permanente"],
    "source": "input",
    "catalog_table": "cat_permanent_plot",
    "catalog_field": "id"
  }
]
