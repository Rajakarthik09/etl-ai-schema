from sqlalchemy import create_engine

def load(df, table_name="users"):
    engine = create_engine("sqlite:///../data/database.db")
    df.to_sql(table_name, con=engine, if_exists="replace", index=False)