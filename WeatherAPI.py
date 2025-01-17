import pandas as pd
import requests
from sqlalchemy import create_engine

#extract data from API
def extract()-> dict:
    API_url= "https://api.weatherapi.com/v1/current.json?key=29515dc18dcd49f495115106251701&q=Nepal"
    try:
        data=requests.get(API_url).json()
        return data
    except Exception as e:
        print(f"Error fetching data: {e}")
        return {}
#Transform the Raw data
def transform(data:dict)->pd.DataFrame:
    extracted_data={
        "Location":data['location']['name'],
        "region": data['location']['region'],
        "country": data['location']['country'],
        "lat": data['location']['lat'],
        "lon": data['location']['lon'],
        "temp_c": data['current']['temp_c'],
        "temp_f": data['current']['temp_f'],
        "wind_mph": data['current']['wind_mph'],
        "wind_kph": data['current']['wind_kph'],
        "Local_Time": data['location']['localtime']
    }
        
    df=pd.DataFrame(extracted_data, index=[0])
    print(df)
    return df
    

#Load data into PostgresSQL database
def load(df:pd.DataFrame)->None:
    engine=create_engine("postgresql+psycopg2://postgres:python@localhost:5432/mydatabase") 
    try:
        df.to_sql("weather_project",engine, if_exists='replace',index=False)
        print("Data successfully loaded into PostgresSQL!")
    except Exception as e:
        print(f"An error occured while loading the data{e}")
    finally:
        engine.dispose()
        
        
#Main ETL pipeline
if __name__== "__main__":
    data=extract()
    transformed_data=transform(data)
    load(transformed_data)