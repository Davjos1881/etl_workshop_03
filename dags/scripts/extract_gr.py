import pandas as pd
from sqlalchemy import create_engine, text


def get_mysql_engine(user: str, password: str, host: str, port: int, db: str):
    url = f"mysql+pymysql://{user}:{password}@{host}:{port}/{db}"
    return create_engine(url)


def extract_grammys_db(engine) -> pd.DataFrame:
    engine = get_mysql_engine(
        user="root",  
        password="davidsp",  
        host="localhost",        
        port=3306,               
        db="grammys_db"          
    )

    query = "SELECT * FROM grammys;"  

    with engine.connect() as conn:
        df = pd.read_sql(text(query), con=conn)

    print(f"[extract_grammys_db] Rows loaded: {len(df)}")
    return df