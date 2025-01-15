import requests
import pandas as pd
from sqlalchemy import create_engine

# extract data from api
def extract()-> dict:
    API_URL = "http://universities.hipolabs.com/search?country=United+States"
    data=requests.get(API_URL).json()
    return data

# transform the raw data


def transform(data:dict) -> pd.DataFrame:
    '''Transforms the dataset into desired structure and filters'''
    df=pd.DataFrame(data)
    print(f"Total Number of universities from API {len(data)}")
    df=df[df["name"].str.contains("California")]
    print(f"Number of universities in california {len(df)}")
    df['domains'] = [','.join(map(str,l)) for l in df['domains']]
    df['web_pages'] = [','.join(map(str,l)) for l in df['web_pages']]
    df = df.reset_index(drop=True)
    return df[["domains","country","web_pages","name"]]

def load(df: pd.DataFrame)-> None:
  '''Loads data into PostgresSQL database'''
  engine=create_engine("postgresql+psycopg2://postgres:python@localhost:5432/mydatabase") #"postgresql+psycopg2://postgres:python@localhost:5432/mydatabase"
  # Create the SQLAlchemy engine
  #engine = create_engine(DATABASE_URI)
  try:
    #Load the DataFrame into PostgreSQL -> create a new table or replace if exists
    df.to_sql("universities", engine, if_exists="replace", index=False)
    print("Data successfully loaded into PostgresSQL!")
  except Exception as e:
    print(f"An error occured while loading data:{e}")
  finally:
    engine.dispose()

#Main ETL pipeline
if __name__ == "__main__":
  data=extract()
  transformed_data=transform(data)
  load(transformed_data)