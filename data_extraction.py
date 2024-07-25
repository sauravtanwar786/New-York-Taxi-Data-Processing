import os
import requests
import csv
import numpy as np
import time
import argparse


def download_data_from_url(BASE_URL, color, year, month, data_directory):
    """
      Download a parquet file from a URL and save it to the specified directory.
    """

    url = BASE_URL.format(color=color, year=year, month=month)

    # Define the directory for the specific color
    color_directory = os.path.join(data_directory, f"{color}_taxi")
    os.makedirs(color_directory, exist_ok=True)

    #Define the file path
    data_path = os.path.join(color_directory, f"{color}_tripdata_{year}-{month:02d}.parquet")

    # Attempt to download the file with retries
    for i in range(3):
        try:        
            response=requests.get(url)      
            response.raise_for_status()
            
            with open(data_path, 'wb') as fp:
                fp.write(response.content)
            print("Data successfully downloaded") 
        except requests.exceptions.ConnectionError as e:
            if i < 2:
                time.sleep(5) 
                print("Error occured while fetching data from url...retrying...")
            else:
                raise(e) 


if __name__ == "__main__":

    parser = argparse.ArgumentParser(description="Download taxi trip data")

    parser.add_argument('--start-year', type=int, required=True, 
                            help='Start year for data download (e.g., 2019)')
    parser.add_argument('--end-year', type=int, required=True, 
                            help='End year for data download (e.g., 2024)')
    parser.add_argument('--start-month', type=int, required=True, 
                            help='Start month for data download (06)')
    parser.add_argument('--end-month', type=int, required=True, 
                            help='End month for data download (05)')

    args = parser.parse_args()

    start_year = args.start_year
    end_year = args.end_year
    start_month = args.start_month
    end_month = args.end_month

    print(f"Start Year: {start_year}")
    print(f"Start Month{start_month}")
    print(f"End Year: {end_year}")
    print(f"End Month: {end_month}")

    data_directory = "data/raw"
    os.makedirs(data_directory, exist_ok=True)

    BASE_URL = "https://d37ci6vzurychx.cloudfront.net/trip-data/{color}_tripdata_{year}-{month:02d}.parquet"
    colors=["yellow","green"]
    for year in range(start_year, end_year+1):
        if year == start_year:
           start_month=start_month
        else:
            start_month=1
        
        if year==end_year:
            end_month=end_month
        else:
            end_month=12
        for month in range(start_month, end_month+1):
            for color in colors:
                download_data_from_url(BASE_URL,color,year,month,data_directory)
