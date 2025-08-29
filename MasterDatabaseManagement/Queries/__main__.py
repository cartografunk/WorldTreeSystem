from core.db import get_engine
from .active_contracts_query import fetch_active_contracts_query_df

if __name__ == "__main__":
    eng = get_engine()
    df = fetch_active_contracts_query_df(eng)
    print(df.head(10).to_string(index=False))
