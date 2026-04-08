import pandas as pd


def extract_spotify(path: str) -> pd.DataFrame:
    """Read Spotify dataset from CSV."""
    try:
        df = pd.read_csv(path, index_col=0, encoding="utf-8")
    except UnicodeDecodeError:
        df = pd.read_csv(path, index_col=0, encoding="latin-1")

    print(f"[extract_spotify] Rows loaded: {len(df)}")
    return df