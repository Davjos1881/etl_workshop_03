from airflow.decorators import dag, task
from datetime import datetime
import pandas as pd

BASE_PATH   = "/opt/airflow"
SPOTIFY_RAW = f"{BASE_PATH}/data/raw/spotify_dataset.csv"
GRAMMYS_RAW = f"{BASE_PATH}/data/raw/the_grammy_awards.csv"


@dag(
    dag_id="etl_spotify_grammys",
    description="ETL pipeline: Spotify + Grammys → MySQL DW",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
    tags=["etl", "spotify", "grammys"],
    max_active_runs=1
)
def pipeline_etl_spotify_grammys():

    @task
    def extract_spotify() -> str:
        df = pd.read_csv(f"{BASE_PATH}/data/raw/spotify_dataset.csv")
        assert len(df) > 0, "Spotify dataset is empty"
        assert "track_id" in df.columns, "track_id column missing"
        print(f"[extract_spotify] Rows loaded: {len(df)}")
        return f"{BASE_PATH}/data/raw/spotify_dataset.csv"

    @task
    def extract_grammys() -> str:
        from sqlalchemy import create_engine, text
        engine = create_engine(
            "mysql+pymysql://root:@192.168.1.5:3306/grammys_db"
        )
        df = pd.read_sql(text("SELECT * FROM grammys;"), con=engine.connect())
        print(f"[extract_grammys] Rows loaded: {len(df)}")
        output = f"{BASE_PATH}/data/cleaned/grammys_raw.csv"
        df.to_csv(output, index=False)
        return output
    
    @task
    def clean_spotify(path: str) -> str:
        df = pd.read_csv(path)

        df = df.drop_duplicates()
        df = df.dropna(subset=["track_id", "track_name", "artists", "album_name"])
        df = df[df["time_signature"] != 0]

        for col in ["track_id", "track_name", "artists", "album_name", "track_genre"]:
            df[col] = df[col].str.strip()

        df["artists"]  = df["artists"].str.replace(";", ", ", regex=False)
        df["explicit"] = df["explicit"].astype(bool)

        key_map = {
            0: "C", 1: "C#", 2: "D",  3: "D#", 4: "E",  5: "F",
            6: "F#", 7: "G", 8: "G#", 9: "A",  10: "A#", 11: "B"
        }
        df["key"]  = df["key"].map(key_map)
        df["mode"] = df["mode"].map({0: "Minor", 1: "Major"})

        df["duration_s"] = (df["duration_ms"] / 1000).round(2)
        df = df.drop(columns=["duration_ms"])

        output = f"{BASE_PATH}/data/cleaned/spotify_clean.csv"
        df.to_csv(output, index=False)
        print(f"[clean_spotify] Final shape: {df.shape}")
        return output

    @task
    def clean_grammys(path: str) -> str:
        df = pd.read_csv(path)

        df = df.drop(columns=["img", "title", "published_at", "updated_at"])

        for col in ["category", "nominee", "artist", "workers"]:
            df[col] = df[col].str.strip()

        df["category"] = df["category"].str.title().str.replace(r"\s+", " ", regex=True)
        df["nominee"]  = df["nominee"].fillna("N/A")
        df["artist"]   = df["artist"].fillna("N/A")
        df["workers"]  = df["workers"].fillna("N/A")

        output = f"{BASE_PATH}/data/cleaned/grammys_clean.csv"
        df.to_csv(output, index=False)
        print(f"[clean_grammys] Final shape: {df.shape}")
        return output

    @task
    def transform(sp_path: str, gr_path: str) -> str:
        import re

        df_sp = pd.read_csv(sp_path)
        df_gr = pd.read_csv(gr_path)

        def normalize_artist(name):
            if pd.isnull(name) or name == "N/A":
                return "n/a"
            name = name.lower().strip()
            name = re.split(r"\sfeat\.?|\swith\s|\s&\s|\(", name)[0].strip()
            return name

        df_sp["primary_artist"]            = df_sp["artists"].str.split(",").str[0].str.strip()
        df_sp["primary_artist_normalized"] = df_sp["primary_artist"].apply(normalize_artist)
        df_sp["track_genre"]               = df_sp["track_genre"].str.replace("-", " ").str.title()
        df_sp = df_sp.rename(columns={"explicit": "is_explicit"})

        df_gr["artist_normalized"] = df_gr["artist"].apply(normalize_artist)
        grammy_agg = (
            df_gr[df_gr["artist_normalized"] != "n/a"]
            .groupby("artist_normalized")
            .agg(
                grammy_wins       =("winner", "count"),
                grammy_first_year =("year", "min"),
                grammy_last_year  =("year", "max"),
                grammy_categories =("category", lambda x: " | ".join(sorted(x.unique())))
            )
            .reset_index()
        )

        df_merged = df_sp.merge(
            grammy_agg,
            left_on="primary_artist_normalized",
            right_on="artist_normalized",
            how="left"
        )
        df_merged = df_merged.drop(columns=["artist_normalized"])
        df_merged["grammy_winner"]     = df_merged["grammy_wins"].notna()
        df_merged["grammy_wins"]       = df_merged["grammy_wins"].fillna(0).astype(int)
        df_merged["grammy_first_year"] = df_merged["grammy_first_year"].fillna(0).astype(int)
        df_merged["grammy_last_year"]  = df_merged["grammy_last_year"].fillna(0).astype(int)
        df_merged["grammy_categories"] = df_merged["grammy_categories"].fillna("N/A")

        output = f"{BASE_PATH}/data/cleaned/merged.csv"
        df_merged.to_csv(output, index=False)
        print(f"[transform] Merged shape: {df_merged.shape}")
        return output

    @task
    def load_dw(path: str) -> None:
        from sqlalchemy import create_engine, text

        df_merged = pd.read_csv(path)

        engine = create_engine(
            "mysql+pymysql://root:@192.168.1.5:3306/spotify_grammys_dw"
        )

        # ── dim_artist ────────────────────────────────────────
        dim_artist = (
            df_merged[[
                "primary_artist", "grammy_winner", "grammy_wins",
                "grammy_first_year", "grammy_last_year", "grammy_categories"
            ]]
            .drop_duplicates(subset=["primary_artist"])
            .reset_index(drop=True)
        )
        dim_artist.insert(0, "artist_key", range(1, len(dim_artist) + 1))
        dim_artist["grammy_first_year"] = dim_artist["grammy_first_year"].replace(0, None)
        dim_artist["grammy_last_year"]  = dim_artist["grammy_last_year"].replace(0, None)

        # ── dim_genre ─────────────────────────────────────────
        dim_genre = (
            df_merged[["track_genre"]]
            .drop_duplicates()
            .reset_index(drop=True)
        )
        dim_genre.insert(0, "genre_key", range(1, len(dim_genre) + 1))

        # ── dim_track ─────────────────────────────────────────
        dim_track_with_id = (
            df_merged[["track_id"]]
            .drop_duplicates()
            .reset_index(drop=True)
        )
        dim_track_with_id.insert(0, "track_key", range(1, len(dim_track_with_id) + 1))

        dim_track = (
            df_merged[[
                "track_id", "track_name", "album_name",
                "is_explicit", "duration_s", "key", "mode", "time_signature"
            ]]
            .drop_duplicates(subset=["track_id"])
            .reset_index(drop=True)
            .rename(columns={"key": "key_note"})
        )
        dim_track = dim_track.merge(dim_track_with_id, on="track_id", how="left")
        dim_track = dim_track.drop(columns=["track_id"])
        cols = ["track_key"] + [c for c in dim_track.columns if c != "track_key"]
        dim_track = dim_track[cols]

        # ── fact_streams ──────────────────────────────────────
        fact_streams = df_merged[[
            "track_id", "primary_artist", "track_genre",
            "popularity", "danceability", "energy", "loudness",
            "speechiness", "acousticness", "instrumentalness",
            "liveness", "valence", "tempo"
        ]].copy()

        fact_streams = fact_streams.merge(dim_track_with_id, on="track_id", how="left")
        fact_streams = fact_streams.merge(
            dim_artist[["artist_key", "primary_artist"]], on="primary_artist", how="left"
        )
        fact_streams = fact_streams.merge(
            dim_genre[["genre_key", "track_genre"]], on="track_genre", how="left"
        )
        fact_streams = fact_streams[[
            "track_key", "artist_key", "genre_key",
            "popularity", "danceability", "energy", "loudness",
            "speechiness", "acousticness", "instrumentalness",
            "liveness", "valence", "tempo"
        ]]
        fact_streams.insert(0, "stream_key", range(1, len(fact_streams) + 1))

        # ── Insert ignore ─────────────────────────────────────
        def insert_ignore(df, table_name):
            temp = f"tmp_{table_name}"
            df.to_sql(temp, engine, if_exists="replace", index=False)
            cols = ", ".join(df.columns)
            with engine.begin() as conn:
                conn.execute(text(f"INSERT IGNORE INTO {table_name} ({cols}) SELECT {cols} FROM {temp};"))
                conn.execute(text(f"DROP TABLE {temp}"))

        insert_ignore(dim_artist,   "dim_artist")
        insert_ignore(dim_genre,    "dim_genre")
        insert_ignore(dim_track,    "dim_track")
        insert_ignore(fact_streams, "fact_streams")

        print("[load_dw] Load to Data Warehouse completed successfully")

    # ── Task dependencies ─────────────────────────────────────────────────────
    sp_path = extract_spotify()
    gr_path = extract_grammys()

    sp_clean = clean_spotify(sp_path)
    gr_clean = clean_grammys(gr_path)

    merged = transform(sp_clean, gr_clean)
    load_dw(merged)


pipeline_etl_spotify_grammys()