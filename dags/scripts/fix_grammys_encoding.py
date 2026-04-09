import pandas as pd
from sqlalchemy import create_engine

# Read CSV handling encoding issues
df = pd.read_csv(
    r"C:\Users\santa\Desktop\ETL_cositas\workshop_02\data\raw\the_grammy_awards.csv",
    encoding="latin-1",
    on_bad_lines="skip"
)

# Clean special characters from all text columns
for col in df.select_dtypes(include="object").columns:
    df[col] = df[col].apply(
        lambda x: x.encode("latin-1").decode("utf-8", errors="replace") if isinstance(x, str) else x
    )

# Connect to MySQL — replace with your credentials
engine = create_engine("mysql+pymysql://root:davidsp@localhost:3306/grammys_db")

# Load directly into MySQL
df.to_sql(
    name="grammy_awards",
    con=engine,
    if_exists="replace",
    index=False
)

print(f"{len(df)} rows loaded successfully")