import pandas as pd

def clean_spotify(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    initial_rows = len(df)
 
    # 1. Drop fully duplicated rows
    df = df.drop_duplicates()
    print(f"[clean_spotify] Fully duplicated rows removed: {initial_rows - len(df)}")
 
    # 2. Drop rows with nulls in core identity columns
    core_cols = ["track_id", "track_name", "artists", "album_name"]
    before = len(df)
    df = df.dropna(subset=core_cols)
    print(f"[clean_spotify] Rows dropped (null in core columns): {before - len(df)}")
 
    # 3. Duplicate track_ids: same song in multiple genres — keep all
    #    This is intentional in the dataset structure.
    #    The genre column will be used as a dimension in the data model.
    dup_ids = df["track_id"].duplicated(keep=False).sum()
    print(f"[clean_spotify] Rows with duplicate track_id (multi-genre): {dup_ids} — kept intentionally")
 
    # 4. Flag unusual time_signature = 0
    before = len(df)
    invalid_ts = df["time_signature"] == 0
    df = df[~invalid_ts]
    print(f"[clean_spotify] Rows removed (time_signature = 0): {before - len(df)}")
 
    # 5. Strip whitespace from string columns
    str_cols = ["track_id", "track_name", "artists", "album_name", "track_genre"]
    for col in str_cols:
        df[col] = df[col].str.strip()
 
    # 6. Normalize artists: replace semicolon separator with comma+space
    #    Raw format uses ';' to separate multiple artists
    df["artists"] = df["artists"].str.replace(";", ", ", regex=False)
 
    # 7. Cast explicit to bool (already bool, but ensure consistency after any reload)
    df["explicit"] = df["explicit"].astype(bool)
 
    # 8. Map key (int 0–11) to pitch class name
    key_map = {
        0: "C", 1: "C#", 2: "D", 3: "D#", 4: "E", 5: "F",
        6: "F#", 7: "G", 8: "G#", 9: "A", 10: "A#", 11: "B"
    }
    df["key"] = df["key"].map(key_map)
 
    # 9. Map mode (0/1) to label
    df["mode"] = df["mode"].map({0: "Minor", 1: "Major"})
 
    # 10. Convert duration from ms to seconds for readability
    df["duration_s"] = (df["duration_ms"] / 1000).round(2)
    df = df.drop(columns=["duration_ms"])
 
    print(f"\n[clean_spotify] Final shape: {df.shape}")
    print(f"[clean_spotify] Columns: {df.columns.tolist()}")

    return df