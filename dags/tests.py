from scripts.extract_sp import extract_spotify
from scripts.extract_gr import extract_grammys_db, get_mysql_engine


# ── Test Spotify ─────────────────────────────
print("\n--- Testing Spotify Extract ---")

df_spotify = extract_spotify("data/raw/spotify_dataset.csv")
print(df_spotify.head())


# ── Test Grammys ─────────────────────────────
print("\n--- Testing Grammys Extract ---")

engine = get_mysql_engine(
    user="root",
    password="davidsp",
    host="localhost",
    port=3306,
    db="grammys_db"
)

df_grammys = extract_grammys_db(engine)
print(df_grammys.head())