from sqlalchemy import create_engine
import os

# Resolve the project root so the SQLite path works regardless of CWD (CLI or Airflow).
PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DB_PATH = os.path.join(PROJECT_ROOT, "data", "database.db")


def load(df, table_name: str = "users"):
    """
    Load a DataFrame into the project SQLite database.

    Args:
        df: pandas DataFrame to persist.
        table_name: target table name in data/database.db.
    """
    engine = create_engine(f"sqlite:///{DB_PATH}")
    df.to_sql(table_name, con=engine, if_exists="replace", index=False)