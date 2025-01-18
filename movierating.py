import pandas as pd
from sqlalchemy import create_engine

#Extracting data
df = pd.read_csv("movie_ratings.csv",index_col=0)

#transform data
df=df.dropna()
#print(df.duplicated().sum())
df=df.drop_duplicates()
df['movie'] = df['movie'].str.strip()


#load the data into PostgresSQL database
def load(df: pd.DataFrame)->None:
    engine=create_engine("postgresql+psycopg2://postgres:python@localhost:5432/mydatabase")
    try:
        df.to_sql("Movie_ratings",engine, if_exists='replace',index=False)
        print("Data successfully loaded into PostgresSQL!")
    except Exception as e:
        print(f"An error occured while loading the {e}")
    finally:
        engine.dispose() 
           
load(df)      


engine = create_engine("postgresql+psycopg2://postgres:python@localhost:5432/mydatabase")
def get_highest_rated_movie():
    try:
        with engine.connect() as conn:
            # Query to find the highest rated movie
            query = """
                SELECT * 
                FROM "Movie_ratings"
                ORDER BY rating DESC 
                LIMIT 1;
            """
            # Execute query and load the result into a DataFrame
            highest_rated_movie = pd.read_sql(query, conn)
            print("Highest rated movie data:")
            print(highest_rated_movie)
    except Exception as e:
        print(f"An error occurred: {e}")
    finally:
        engine.dispose()

get_highest_rated_movie()
    