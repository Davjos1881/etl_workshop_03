from sqlalchemy import create_engine, text
import pandas as pd


def insert_ignore(df: pd.DataFrame, table_name: str, engine) -> None:
    temp_table = f"tmp_{table_name}"

    df.to_sql(temp_table, engine, if_exists="replace", index=False)

    cols = ", ".join(df.columns)

    insert_sql = f"""
        INSERT IGNORE INTO {table_name} ({cols})
        SELECT {cols} FROM {temp_table};
    """

    with engine.begin() as conn:
        conn.execute(text(insert_sql))
        conn.execute(text(f"DROP TABLE {temp_table}"))


def load_to_dw(dim_model: dict) -> None:

    dim_artist   = dim_model["dim_artist"]
    dim_genre    = dim_model["dim_genre"]
    dim_track    = dim_model["dim_track"]
    fact_streams = dim_model["fact_streams"]

    engine = create_engine(
        "mysql+pymysql://root:davidsp@localhost:3306/spotify_grammys_dw"
    )

    # Load dimensions first (fact has FK constraints on them)
    insert_ignore(dim_artist, "dim_artist", engine)
    insert_ignore(dim_genre,  "dim_genre",  engine)
    insert_ignore(dim_track,  "dim_track",  engine)

    # Anti-join on stream_key to avoid duplicates in fact
    # stream_key is the surrogate key generated in Python
    existing_keys_sql = "SELECT stream_key FROM fact_streams"

    try:
        existing_keys_df = pd.read_sql(existing_keys_sql, engine)
    except Exception:
        existing_keys_df = pd.DataFrame(columns=["stream_key"])

    if not existing_keys_df.empty:
        merged = fact_streams.merge(
            existing_keys_df.drop_duplicates(),
            on="stream_key",
            how="left",
            indicator=True
        )
        fact_new = merged[merged["_merge"] == "left_only"].drop(columns=["_merge"])
    else:
        fact_new = fact_streams.copy()

    print(
        f"fact_streams total={len(fact_streams)} "
        f"new rows={len(fact_new)} "
        f"duplicates omitted={len(fact_streams) - len(fact_new)}"
    )

    if not fact_new.empty:
        insert_ignore(fact_new, "fact_streams", engine)
    else:
        print("No new rows for fact_streams")

    print("Load to Data Warehouse completed successfully")