import pandas as pd

df = pd.read_csv("C:\\Users\\santa\\Desktop\\ETL_cositas\\workshop_02\\data\\raw\\spotify_dataset.csv", encoding="latin-1")
df.to_csv("C:\\Users\\santa\\Desktop\\ETL_cositas\\workshop_02\\data\\grammys_fixed\\grammys_fixed.csv", index=False, encoding="utf-8")