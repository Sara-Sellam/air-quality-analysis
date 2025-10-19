import pandas as pd
from openaq_collector import OpenAQCollector
from datetime import datetime
import os

def read_api_key(file_path):
    """Read API key from file"""
    try:
        with open(file_path, "r") as f:
            return f.read().strip()
    except FileNotFoundError:
        print(f"API key file not found: {file_path}")
        return None



def main():
    # Configuration
    API_KEY_PATH = "/Users/sara/Documents/Projects/project_data_collections/open_aq_collector/credentials/openaq_key.txt"
    CITY = "SANTIAGO DE COMPOSTELA"#"VIGO" #"SANTIAGO DE COMPOSTELA" #"A Coruña" 
    COUNTRY_CODE = "ES" 
    START_DATE = "2023-08-01"
    END_DATE = "2023-10-30"
    MEASUREMENTS_LIMIT = 1000

    # Read API key
    api_key = read_api_key(API_KEY_PATH)
    if not api_key:
        print("Cannot proceed without API key")
        return

    # Create collector instance
    collector = OpenAQCollector(api_key)
    
    try:
        print(f"Collecting air quality data for {CITY}, {COUNTRY_CODE}")
        print(f"Date range: {START_DATE} to {END_DATE}")
        print(f"Limit: {MEASUREMENTS_LIMIT} measurements")

        # Get air quality data
        air_data_df = collector.get_city_air_quality_data(
            CITY, 
            COUNTRY_CODE, 
            START_DATE, 
            END_DATE,
            MEASUREMENTS_LIMIT
        )

        # Create descriptive filename
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        city_clean = CITY.replace(' ', '_').replace('ñ', 'n').replace('á', 'a').lower()
        filename = f"/Users/sara/Documents/Projects/project_data_collections/open_aq_collector/data_output/openaq_{city_clean}_{COUNTRY_CODE}_{START_DATE}_to_{END_DATE}_limit_{MEASUREMENTS_LIMIT}_{timestamp}.csv"
        
        
        # Save with UTF-8 encoding for special characters
        air_data_df.to_csv(filename, index=False, encoding='utf-8-sig')

        # Display results
        if not air_data_df.empty:
            print(f"SUCCESS: Data saved to: {filename}")
            print(f"Dataset shape: {air_data_df.shape}")
            
            print("\nData sample:")
            print(air_data_df.head(10).to_markdown(index=False))
            print(f"\nTotal records: {len(air_data_df)}")
            
            # Show basic stats if value column exists
            if 'value' in air_data_df.columns:
                print(f"Value statistics:")
                print(f"   Mean: {air_data_df['value'].mean():.2f}")
                print(f"   Max: {air_data_df['value'].max():.2f}")
                print(f"   Min: {air_data_df['value'].min():.2f}")
            
     
        else:
            print("No data found for the specified parameters")
            
    except Exception as e:
        print(f"Error during data collection: {e}")
    finally:
        collector.close()
        print("Data collection completed")

if __name__ == "__main__":
    main()