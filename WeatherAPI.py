import json
import requests
import pandas as pd
import boto3
import os
from datetime import datetime

#extract data from API
def extract()-> dict:
    API_url= "https://api.weatherapi.com/v1/current.json?key={API_KEY}&q=Nepal"
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
    
    def lambda_handler(event, context):
    data = extract()

    if not data:
        return {
            "statusCode": 500,
            "body": "Weather data extraction failed"
        }

    transformed_df = transform(data)
    load(data, transformed_df)

    return {
        "statusCode": 200,
        "body": "Serverless weather data pipeline executed successfully"
    }
    

#Load data into S3

def load(raw_data: dict, df: pd.DataFrame) -> None:
    timestamp = datetime.utcnow()
    date_path = timestamp.strftime("%Y/%m/%d/%H%M%S")

    # Store raw json
    raw_key = f"raw/weather_{date_path}.json"
    s3.put_object(
        Bucket=BUCKET_NAME,
        Key=raw_key,
        Body=json.dumps(raw_data),
        ContentType="application/json"
    )

    # Store transformed CSV 
    processed_key = f"processed/weather_{date_path}.csv"
    csv_data = df.to_csv(index=False)

    s3.put_object(
        Bucket=BUCKET_NAME,
        Key=processed_key,
        Body=csv_data,
        ContentType="text/csv"
    )

    print("Raw and transformed weather data successfully stored in S3")
        
