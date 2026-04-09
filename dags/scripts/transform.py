import pandas as pd
import re


def normalize_artist(name: str) -> str:
    if pd.isnull(name) or name == "N/A":
        return "n/a"
    name = name.lower().strip()
    name = re.split(r"\sfeat\.?|\swith\s|\s&\s|\(", name)[0].strip()
    return name


def transform_spotify(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    print("\n" + "=" * 55)
    print("SPOTIFY — TRANSFORM")
    print("=" * 55)

    df["primary_artist"] = df["artists"].str.split(",").str[0].str.strip()
    df["primary_artist_normalized"] = df["primary_artist"].apply(normalize_artist)
    print(f"[1] primary_artist and primary_artist_normalized created")

    df["track_genre"] = df["track_genre"].str.replace("-", " ").str.title()
    print(f"[2] track_genre normalized")

    df = df.rename(columns={"explicit": "is_explicit"})
    print(f"[3] 'explicit' renamed to 'is_explicit'")

    print(f"\n[transform_spotify] Final shape: {df.shape}")
    return df


def transform_grammys(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    print("\n" + "=" * 55)
    print("GRAMMYS — TRANSFORM")
    print("=" * 55)

    df["artist_normalized"] = df["artist"].apply(normalize_artist)
    print(f"[1] artist_normalized created")

    grammy_agg = (
        df[df["artist_normalized"] != "n/a"]
        .groupby("artist_normalized")
        .agg(
            grammy_wins       =("winner", "count"),
            grammy_first_year =("year", "min"),
            grammy_last_year  =("year", "max"),
            grammy_categories =("category", lambda x: " | ".join(sorted(x.unique())))
        )
        .reset_index()
    )
    print(f"[2] Grammy info aggregated: {len(grammy_agg)} unique artists")

    print(f"\n[transform_grammys] Aggregated shape: {grammy_agg.shape}")
    return grammy_agg


def merge_datasets(df_sp: pd.DataFrame, df_gr_agg: pd.DataFrame) -> pd.DataFrame:
    print("\n" + "=" * 55)
    print("MERGE — Spotify (left) ← Grammys (aggregated)")
    print("=" * 55)

    df_merged = df_sp.merge(
        df_gr_agg,
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

    matched = df_merged["grammy_winner"].sum()
    total   = len(df_merged)
    print(f"\nTotal rows:             {total}")
    print(f"Rows with Grammy match: {matched} ({matched/total*100:.1f}%)")
    print(f"Rows without match:     {total - matched}")
    print(f"\n[merge] Final shape: {df_merged.shape}")
    return df_merged


def build_dimensional_model(df_merged: pd.DataFrame) -> dict:
    print("\n" + "=" * 55)
    print("DIMENSIONAL MODEL — Building dims and fact")
    print("=" * 55)

    # ── dim_artist ────────────────────────────────────────────
    dim_artist = (
        df_merged[[
            "primary_artist", "grammy_winner", "grammy_wins",
            "grammy_first_year", "grammy_last_year", "grammy_categories"
        ]]
        .drop_duplicates(subset=["primary_artist"])
        .reset_index(drop=True)
    )
    # Surrogate key generated in Python (no AUTO_INCREMENT in DDL)
    dim_artist.insert(0, "artist_key", range(1, len(dim_artist) + 1))
    # grammy_first/last_year: replace 0 with None (NULL in MySQL)
    dim_artist["grammy_first_year"] = dim_artist["grammy_first_year"].replace(0, None)
    dim_artist["grammy_last_year"]  = dim_artist["grammy_last_year"].replace(0, None)
    print(f"[dim_artist] Rows: {len(dim_artist)}")

    # ── dim_genre ─────────────────────────────────────────────
    dim_genre = (
        df_merged[["track_genre"]]
        .drop_duplicates()
        .reset_index(drop=True)
    )
    dim_genre.insert(0, "genre_key", range(1, len(dim_genre) + 1))
    print(f"[dim_genre]  Rows: {len(dim_genre)}")

    # ── dim_track ─────────────────────────────────────────────
    dim_track = (
        df_merged[[
            "track_id", "track_name", "album_name",
            "is_explicit", "duration_s", "key", "mode", "time_signature"
        ]]
        .drop_duplicates(subset=["track_id"])
        .reset_index(drop=True)
        .rename(columns={
            "key": "key_note"
        })
        .drop(columns=["track_id"])
    )
    dim_track.insert(0, "track_key", range(1, len(dim_track) + 1))
    print(f"[dim_track]  Rows: {len(dim_track)}")

    # ── fact_streams ──────────────────────────────────────────
    # Join to recover surrogate keys for each dimension
    # We need track_id and primary_artist temporarily for joining
    fact_base = df_merged[[
        "track_id", "primary_artist", "track_genre",
        "popularity", "danceability", "energy", "loudness",
        "speechiness", "acousticness", "instrumentalness",
        "liveness", "valence", "tempo"
    ]].copy()

    # Recover track_key: re-attach track_id to dim_track temporarily
    dim_track_with_id = (
        df_merged[["track_id", "primary_artist"]]
        .drop_duplicates(subset=["track_id"])
        .reset_index(drop=True)
    )
    dim_track_with_id.insert(0, "track_key", range(1, len(dim_track_with_id) + 1))

    fact_base = fact_base.merge(
        dim_track_with_id[["track_key", "track_id"]],
        on="track_id", how="left"
    )
    fact_base = fact_base.merge(
        dim_artist[["artist_key", "primary_artist"]],
        on="primary_artist", how="left"
    )
    fact_base = fact_base.merge(
        dim_genre[["genre_key", "track_genre"]],
        on="track_genre", how="left"
    )

    fact_streams = fact_base[[
        "track_key", "artist_key", "genre_key",
        "popularity", "danceability", "energy", "loudness",
        "speechiness", "acousticness", "instrumentalness",
        "liveness", "valence", "tempo"
    ]].copy()

    # Surrogate key for fact
    fact_streams.insert(0, "stream_key", range(1, len(fact_streams) + 1))
    print(f"[fact_streams] Rows: {len(fact_streams)}")

    return {
        "dim_artist":   dim_artist,
        "dim_genre":    dim_genre,
        "dim_track":    dim_track,
        "fact_streams": fact_streams
    }


def transform_data(
    df_sp: pd.DataFrame,
    df_gr: pd.DataFrame,
) -> tuple:

    df_sp_t   = transform_spotify(df_sp)
    df_gr_t   = transform_grammys(df_gr)
    df_merged = merge_datasets(df_sp_t, df_gr_t)
    dim_model = build_dimensional_model(df_merged)

    return df_sp_t, df_gr_t, df_merged, dim_model