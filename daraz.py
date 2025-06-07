from selenium import webdriver
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.chrome.options import Options
from bs4 import BeautifulSoup

import pandas as pd
import psycopg2
from sqlalchemy import create_engine
import re
import time

options = Options()
options.add_argument("--disable-gpu")
options.add_argument("--headless")  # Optional: Run in headless mode
driver = webdriver.Chrome(options=options)

url = "https://pages.daraz.com.np/wow/gcp/route/daraz/np/upr/router?hybrid=1&data_prefetch=true&prefetch_replace=1&at_iframe=1&wh_pid=%2Flazada%2Fchannel%2Fnp%2Fflashsale%2FeBQX2YfTXs&hide_h5_title=true&lzd_navbar_hidden=true&disable_pull_refresh=true&skuIds=169302458%2C150268528%2C179779770%2C129562539%2C172989829%2C164028435%2C164079325&spm=a2a0e.tm80335409.FlashSale.d_shopMore"
driver.get(url)

# Scroll and load all products
try:
    last_height = driver.execute_script("return document.body.scrollHeight")
    while True:
        # Scroll to the bottom
        driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
        time.sleep(2)  # Wait for the page to load
        new_height = driver.execute_script("return document.body.scrollHeight")
        if new_height == last_height:
            break
        last_height = new_height

    # Wait for the elements to load
    WebDriverWait(driver, 20).until(
        EC.presence_of_all_elements_located((By.CLASS_NAME, 'flash-unit'))
    )
    # Scrape the page source
    html = driver.page_source
    soup = BeautifulSoup(html, 'html.parser')
    product_containers = soup.find_all('div', class_='flash-unit')
    print(f"Found {len(product_containers)} product containers.")
except Exception as e:
    print(f"An error occurred: {e}")
finally:
    driver.quit()

# Transform the data
products = []

# Extract product information
for idx, container in enumerate(product_containers, start=1):
    product_name = container.find('div', class_='sale-title')
    original_price = container.find('span', class_='origin-price-value')
    discounted_price = container.find('div', class_='sale-price')
    discount = container.find('span', 'discount')

    products.append({
        "Name": product_name.text.strip() if product_name else 'N/A',
        "Original_Price": re.sub(r'Rs\.?\s*', '', original_price.text).replace(',', '').strip() if original_price else 'N/A',
        "Discounted_Price": re.sub(r'Rs\.?\s*', '', discounted_price.text).replace(',', '').strip() if discounted_price else 'N/A',
        "Discount": discount.text.replace('-', '').replace('%', '').strip() if discount else 'N/A'
    })

# Load data into a DataFrame
df = pd.DataFrame(products)

# Display raw data for verification
print(f"Extracted {len(products)} products.")
print("Raw Data:")
print(df.head(10))

# Database connection and loading
engine = create_engine("postgresql+psycopg2://postgres:python@localhost:5432/mydatabase")
df.to_sql('daraz_products', engine, if_exists='replace', index=False)
print("Data has been successfully loaded into PostgreSQL!")

# Extract data from the database
query = "SELECT * FROM daraz_products;"
df_extracted = pd.read_sql(query, engine)

# Save the extracted data to a CSV file
csv_file_path = "products_data.csv"
df_extracted.to_csv(csv_file_path, index=False)
print(f"Data has been saved to {csv_file_path}")

# Display extracted data for verification
print("Extracted Data:")
print(df_extracted.head(10))