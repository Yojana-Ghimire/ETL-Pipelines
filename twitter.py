import tweepy
import psycopg2
from psycopg2 import Error

# Twitter API credentials
api_key = 'Wy3B5pg1OwdmCe1pS33Hf0HKD'
api_secret_key = 'MlqqxOZ7yibM1i0HlaIAsepi5iY6kBVuw9Mz3RyWdOptR3p8FW'
access_token = '1880790631477616641-DLnnvwNTuEBSI0jVZ24FMqHMVICizp'
access_secret_token = 'uH0S0APRYkDMIBAcqWYgu7b7GS77WdesrPEAFLldfpwL9'

# Authenticate to Twitter
auth = tweepy.OAuthHandler(api_key, api_secret_key)
auth.set_access_token(access_token, access_secret_token)

# Create API object
api = tweepy.API(auth, wait_on_rate_limit=True)

# Twitter user ID (can be replaced with any valid user ID)
user_id = "23083404"  # Example user ID
twitter_user = api.get_user(user_id=user_id)

# Prepare the data to insert into the database
data = {
    "screen_name": twitter_user.screen_name,
    "followers_count": twitter_user.followers_count,
    "friends_count": twitter_user.friends_count,
    "description": twitter_user.description,
    "location": twitter_user.location
}

# Create PostgreSQL connection
try:
    conn = psycopg2.connect(
        host="localhost",  # Database server (e.g., localhost)
        database="mydatabase",  # Replace with your PostgreSQL database name
        user="postgres",  # Replace with your PostgreSQL username
        password="python"  # Replace with your PostgreSQL password
    )
    conn.autocommit = True
    cursor = conn.cursor()
    
    # Create table if it does not exist
    try:
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS twitter_user (
                screen_name VARCHAR(255),
                followers_count INT,
                friends_count INT,
                description TEXT,
                location VARCHAR(255)
            )
        """)
    except psycopg2.Error as e:
        print("Error creating table:", e)

    # Insert data into the table
    try:
        cursor.execute("""
            INSERT INTO twitter_user(screen_name, followers_count, friends_count, description, location)
            VALUES (%s, %s, %s, %s, %s)
        """, (data["screen_name"], data["followers_count"], data["friends_count"], data["description"], data["location"]))
    except psycopg2.Error as e:
        print("Error inserting data:", e)

    print("Data inserted successfully!")

except psycopg2.Error as e:
    print("Error connecting to PostgreSQL:", e)

finally:
    if conn:
        cursor.close()
        conn.close()
        print("PostgreSQL connection closed")
