from sqlalchemy import create_engine
import os

# Resolve the project root so the SQLite path works regardless of CWD (CLI or Airflow).
PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DB_PATH = os.path.join(PROJECT_ROOT, "data", "database.db")

# Single endpoint table for all taxi versions (baseline and LLM outputs).
CANONICAL_TABLE = "trips_canonical"


def load(df, table_name: str = "users"):
    """
    Load a DataFrame into the project SQLite database.

    Args:
        df: pandas DataFrame to persist.
        table_name: target table name in data/database.db.
    """
    engine = create_engine(f"sqlite:///{DB_PATH}")
    df.to_sql(table_name, con=engine, if_exists="replace", index=False)


def ensure_canonical_table_exists(engine=None):
    """Create the single canonical table if it does not exist."""
    from sqlalchemy import inspect
    from .canonical_schema import get_canonical_columns_with_metadata
    if engine is None:
        engine = create_engine(f"sqlite:///{DB_PATH}")
    if inspect(engine).has_table(CANONICAL_TABLE):
        return
    cols = get_canonical_columns_with_metadata() + ["source_version", "ingestion_method"]
    import pandas as pd
    dummy = {c: [] for c in cols}
    dummy["source_version"] = []
    dummy["ingestion_method"] = []
    pd.DataFrame(dummy).to_sql(CANONICAL_TABLE, con=engine, if_exists="replace", index=False)


def load_into_canonical(df, source_version: str, ingestion_method: str):
    """
    Append a normalized (canonical-schema) DataFrame into the single endpoint table.
    Adds source_version (e.g. 'v1', 'v2') and ingestion_method ('baseline' or 'llm').
    """
    from .canonical_schema import get_canonical_columns_with_metadata
    engine = create_engine(f"sqlite:///{DB_PATH}")
    cols = get_canonical_columns_with_metadata()
    if list(df.columns) != cols:
        import pandas as pd
        out = pd.DataFrame(index=df.index)
        for c in cols:
            out[c] = df[c] if c in df.columns else pd.NA
        df = out[cols]
    df["source_version"] = source_version
    df["ingestion_method"] = ingestion_method
    df.to_sql(CANONICAL_TABLE, con=engine, if_exists="append", index=False)