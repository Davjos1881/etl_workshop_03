import pandas as pd

def clean_grammys(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
 
    # 1. Drop columns not useful for analysis
    #    img: image URL, irrelevant for ETL/analytics
    #    title: ceremony name, fully derivable from 'year'
    #    published_at, updated_at. These are massive updates to the website's database
    #    It also doesn't reflect anything about the ceremony itself
    df = df.drop(columns=["img", "title", "published_at", "updated_at"])
    print(f"[clean_grammys] Dropped 4 non-analytical columns: img, title, published_at, updated_at")
 
    # 2. Strip whitespace from all string columns
    str_cols = ["category", "nominee", "artist", "workers"]
    for col in str_cols:
        df[col] = df[col].str.strip()
 
    # 3. Normalize category: title-case + strip redundant spaces
    df["category"] = df["category"].str.title().str.replace(r"\s+", " ", regex=True)
 
    # 4. Handle nominee nulls
    #    6 rows with no specific nominee (like 'Remixer of the Year')
    #    Fill with 'N/A' to avoid propagating nulls downstream
    before = df["nominee"].isnull().sum()
    df["nominee"] = df["nominee"].fillna("N/A")
    print(f"[clean_grammys] nominee nulls filled with 'N/A': {before}")
 
    # 5. Handle artist nulls
    #    around 1840 rows where artist is null because the relevant credit
    #    is in the 'workers' column (songwriter/producer awards).
    #    Fill with 'N/A' to preserve rows where they are valid winners.
    before = df["artist"].isnull().sum()
    df["artist"] = df["artist"].fillna("N/A")
    print(f"[clean_grammys] artist nulls filled with 'N/A': {before}")
 
    # 6. Handle workers nulls
    #    Null when the award is for a specific artist not a behind-the-scenes role.
    before = df["workers"].isnull().sum()
    df["workers"] = df["workers"].fillna("N/A")
    print(f"[clean_grammys] workers nulls filled with 'N/A': {before}")
 
    # 7. winner column
    #    All values are supossed to be True since dataset contains only Grammy winners. But maybe some rows have nulls or False due to data issues
    print(f"[clean_grammys] 'winner' column: all True (only winners in dataset — expected)")
 
    # 8. Validate year range (sanity check)
    invalid_years = df[(df["year"] < 1958) | (df["year"] > 2030)]
    if not invalid_years.empty:
        print(f"[clean_grammys] WARNING: {len(invalid_years)} rows with unexpected year values")
    else:
        print(f"[clean_grammys] Year range OK: {df['year'].min()} – {df['year'].max()}")
 
    print(f"\n[clean_grammys] Final shape: {df.shape}")
    print(f"[clean_grammys] Columns: {df.columns.tolist()}")
    return df