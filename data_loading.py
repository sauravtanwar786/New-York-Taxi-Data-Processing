# loading the processed data into SQLITE DATABASE

import sqlite3
import pandas as pd
import os

# Define the database file
db_file = 'taxi_data.db'

# Connect to SQLite database
conn = sqlite3.connect(db_file)
cursor = conn.cursor()

       
# Create table schema
create_table_query = '''
CREATE TABLE IF NOT EXISTS trips (
    VendorID INTEGER,
    tpep_pickup_datetime DATETIME NOT NULL,
    tpep_dropoff_datetime DATETIME NOT NULL,
    passenger_count INTEGER,
    PULocationID FLOAT,
    DOLocationID FLOAT,
    trip_distance FLOAT,
    RatecodeID INTEGER,
    store_and_fwd_flag FLOAT,
    extra FLOAT,
    mta_tax FLOAT,
    tolls_amount FLOAT,
    ehail_fee FLOAT,
    trip_type FLOAT,
    improvement_surcharge FLOAT,
    payment_type FLOAT,
    Airport_fee FLOAT,
    fare_amount FLOAT,
    tip_amount FLOAT,
    total_amount FLOAT,
    congestion_surcharge FLOAT,
    trip_duration FLOAT,
    average_speed FLOAT
);
'''
cursor.execute(create_table_query)

# Define the directory containing the cleaned data
cleaned_directory = 'cleaned_data'

# Function to load data into SQLite
def load_data_to_sqlite(file_path, conn):
    try:
        df = pd.read_parquet(file_path)
        df.to_sql('trips', conn, if_exists='append', index=False)
        print(f"Data from {file_path} loaded into SQLite.")
    except Exception as e:
        print(f"Error loading {file_path} into SQLite: {e}")

# Load all cleaned data files into SQLite
for filename in os.listdir(cleaned_directory):
    if filename.endswith('.parquet'):
        file_path = os.path.join(cleaned_directory, filename)
        load_data_to_sqlite(file_path, conn)

# Commit and close connection
conn.commit()
conn.close()

print("All data loaded into SQLite database.")