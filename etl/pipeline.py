from extract import extract
from transform import transform
from load import load

def run_pipeline(csv_path="../data/raw/users_v1.csv", table_name="users"):
    """Run ETL for users dataset (default) or any CSV with table_name for load."""
    df = extract(csv_path)
    df = transform(df)
    load(df, table_name=table_name)


def run_taxi_pipeline(version="v1", use_generated=False):
    """
    Run ETL on NYC taxi CSV version. version in ("v1", "v2", "v3").
    Loads to table yellow_trips_v1, yellow_trips_v2, yellow_trips_v3.
    If use_generated is True, use etl/transform_generated.py (AI output) instead of transform_taxi.py.
    """
    if use_generated:
        try:
            from transform_generated import transform as taxi_transform
        except ImportError:
            from transform_taxi import transform as taxi_transform
    else:
        from transform_taxi import transform as taxi_transform
    csv_path = f"../data/raw/yellow_base_{version}.csv"
    table_name = f"yellow_trips_{version}"
    df = extract(csv_path)
    df = taxi_transform(df)
    load(df, table_name=table_name)


if __name__ == "__main__":
    import sys
    if len(sys.argv) > 1 and sys.argv[1] == "taxi":
        ver = sys.argv[2] if len(sys.argv) > 2 else "v1"
        use_gen = "--generated" in sys.argv or "-g" in sys.argv
        run_taxi_pipeline(ver, use_generated=use_gen)
    else:
        run_pipeline()