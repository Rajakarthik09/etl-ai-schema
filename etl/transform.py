def transform(df):
    df["full_name"] = df["first_name"] + " " + df["last_name"]
    return df