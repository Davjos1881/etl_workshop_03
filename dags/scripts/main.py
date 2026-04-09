import warnings
warnings.filterwarnings('ignore')
from pathlib import Path
from log import log_progress
from extract_sp import extract_spotify
from extract_gr import extract_grammys_db
from clean_sp import clean_spotify
from clean_gr import clean_grammys
from transform import transform_data
from load import load_to_dw
#from load_drive import load_to_drive

base_path = Path(r"C:\Users\santa\Desktop\ETL_cositas\workshop_02")

# Raw
spotify_raw  = base_path / "data" / "raw" / "spotify_dataset.csv"
grammys_raw  = base_path / "data" / "raw" / "the_grammy_awards.csv"

# Processed
spotify_clean       = base_path / "data" / "cleaned" / "spotify_clean.csv"
grammys_clean       = base_path / "data" / "cleaned" / "grammys_clean.csv"
spotify_transformed = base_path / "data" / "transformed" / "spotify_transformed.csv"
grammys_transformed = base_path / "data" / "transformed" / "grammys_transformed.csv"
merged_file         = base_path / "data" / "transformed" / "merged.csv"

# Dimensional model
dims_path = base_path / "data" / "dims"

# Logs
log_file = base_path / "logs" / "log_file.txt"


def main():

    log_progress("ETL process started", log_file)

    log_progress("Extract: Spotify CSV", log_file)
    df_sp_raw = extract_spotify(spotify_raw)

    log_progress("Extract: Grammys DB", log_file)
    df_gr_raw = extract_grammys_db(grammys_raw)

    log_progress("Clean: Spotify", log_file)
    df_sp_clean = clean_spotify(df_sp_raw)
    df_sp_clean.to_csv(spotify_clean, index=False)
    log_progress("Spotify clean data saved", log_file)

    log_progress("Clean: Grammys", log_file)
    df_gr_clean = clean_grammys(df_gr_raw)
    df_gr_clean.to_csv(grammys_clean, index=False)
    log_progress("Grammys clean data saved", log_file)

    log_progress("Transform and merge", log_file)
    df_sp_t, df_gr_t, df_merged, dim_model = transform_data(df_sp_clean, df_gr_clean)
    df_sp_t.to_csv(spotify_transformed, index=False)
    df_gr_t.to_csv(grammys_transformed, index=False)
    df_merged.to_csv(merged_file, index=False)
    for name, table in dim_model.items():
        table.to_csv(dims_path / f"{name}.csv", index=False)
    log_progress("Transform and merge saved", log_file)

    log_progress("Load: Data Warehouse", log_file)
    load_to_dw(dim_model)
    log_progress("Load: Data Warehouse completed", log_file)

    #log_progress("Load: Google Drive", log_file)
    #load_to_drive(merged_file)
    #log_progress("Load: Google Drive completed", log_file)

    log_progress("ETL process finished successfully", log_file)


if __name__ == "__main__":
    main()