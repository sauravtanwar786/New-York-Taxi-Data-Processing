import sys
import pandas as pd
import numpy as np
import os


# Define new column names mapping
new_column_names = {
    'lpep_pickup_datetime': 'tpep_pickup_datetime',
    'lpep_dropoff_datetime': 'tpep_dropoff_datetime'
}

# Define the directory paths
data_directory = 'data'
cleaned_directory = 'cleaned_data'
os.makedirs(cleaned_directory, exist_ok=True)

# Function to rename columns in Parquet files
def rename_columns(file_path, new_column_names):
    df = pd.read_parquet(file_path)
    df.rename(columns=new_column_names, inplace=True)
    df.to_parquet(file_path, index=False)
    print(f"Columns after renaming in {file_path}: {df.columns}")

# Rename columns in files
for filename in os.listdir(data_directory):
    if filename.endswith('.parquet') and 'green' in filename:
        file_path = os.path.join(data_directory, filename)
        rename_columns(file_path, new_column_names)

# List of critical columns
critical_columns = [
    'tpep_pickup_datetime', 'tpep_dropoff_datetime', 'passenger_count',
    'trip_distance', 'fare_amount', 'tip_amount', 'total_amount', 'congestion_surcharge'
]

# Function to remove corrupt data from the dataset
def remove_corrupt_data(file_path):
    try:
        df = pd.read_parquet(file_path)
        df_cleaned = df.dropna(subset=critical_columns)
        cleaned_path = os.path.join(cleaned_directory, os.path.basename(file_path))
        df_cleaned.to_parquet(cleaned_path)
        print(f"Processed and cleaned {file_path}")
        return df_cleaned
    except Exception as e:
        print(f"Error processing the file {file_path}: {e}")
        return pd.DataFrame()

# Remove corrupt data from files
for file in os.listdir(data_directory):
    file_path = os.path.join(data_directory, file)
    if file_path.endswith('.parquet'):
        remove_corrupt_data(file_path)

# Function to calculate trip duration and average speed
def calculate_trip_duration(df):
    df['tpep_pickup_datetime'] = pd.to_datetime(df['tpep_pickup_datetime'])
    df['tpep_dropoff_datetime'] = pd.to_datetime(df['tpep_dropoff_datetime'])
    df['trip_duration'] = (df['tpep_dropoff_datetime'] - df['tpep_pickup_datetime']).dt.total_seconds() / 60.0
    df['average_speed'] = df['trip_distance'] / (df['trip_duration'] / 60.0)
    return df

# Calculate trip duration and average speed
for filename in os.listdir(cleaned_directory):
    file_path = os.path.join(cleaned_directory, filename)
    df = pd.read_parquet(file_path)
    df = calculate_trip_duration(df)
    df.to_parquet(file_path, index=False)
    print(f"Processed and updated {filename}")

# Function to aggregate data by day
def aggregate_data_individually(data_directory, start_year, end_year):
    trip_counts = {}
    for filename in os.listdir(data_directory):
        file_path = os.path.join(data_directory, filename)
        df = pd.read_parquet(file_path)
        df['tpep_pickup_datetime'] = pd.to_datetime(df['tpep_pickup_datetime'])
        # Include end_year in the filter
        df = df[(df['tpep_pickup_datetime'].dt.year >= start_year) & (df['tpep_pickup_datetime'].dt.year <= end_year)]
        df['date'] = df['tpep_pickup_datetime'].dt.date
        daily_summary = df.groupby('date').agg(
            total_trips=('VendorID', 'count'),
            total_fare=('fare_amount', 'sum')
        ).reset_index()
        for index, row in daily_summary.iterrows():
            date = row['date']
            if date not in trip_counts:
                trip_counts[date] = {'total_trips': 0, 'total_fare': 0}
            trip_counts[date]['total_trips'] += row['total_trips']
            trip_counts[date]['total_fare'] += row['total_fare']
    aggregated_df = pd.DataFrame([
        {'date': date, 'total_trips': data['total_trips'], 'average_fare': data['total_fare'] / data['total_trips']}
        for date, data in trip_counts.items()
    ])
    return aggregated_df

# Specify the date range
start_year = 2019
end_year = 2024

# Aggregate the data
aggregated_data = aggregate_data_individually(cleaned_directory, start_year, end_year)

# Display the aggregated data
print(aggregated_data)