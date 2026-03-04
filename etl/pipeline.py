from extract import extract
from load import load


def run_taxi_pipeline(version: str = "v1", use_generated: bool = False):
    """
    Run ETL on NYC taxi CSV version. Supported versions: \"v1\", \"v2\".

    Loads to table yellow_trips_v1 or yellow_trips_v2.
    If use_generated is True, use etl/transform_generated.py (AI output) instead of transform_taxi.py.
    """
    if version not in ("v1", "v2"):
        raise ValueError(f"Unsupported taxi version: {version!r}. Only 'v1' and 'v2' are supported.")
    if use_generated:
        try:
            import importlib
            import transform_generated as gen
            importlib.reload(gen)
            # Prefer a generic `transform` function if present; otherwise fall back
            # to a version-specific name such as `transform_v2`.
            fn = getattr(gen, "transform", None)
            if fn is None:
                candidate = f"transform_{version}"
                fn = getattr(gen, candidate, None)
            if fn is None:
                raise AttributeError(
                    f"transform_generated module has no 'transform' or 'transform_{version}' function"
                )
            taxi_transform = fn
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
    ver = sys.argv[2] if len(sys.argv) > 2 else "v1" if len(sys.argv) > 1 and sys.argv[1] == "taxi" else "v1"
    use_gen = "--generated" in sys.argv or "-g" in sys.argv
    run_taxi_pipeline(ver, use_generated=use_gen)