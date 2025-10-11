import pandas as pd
from openaq_collector import OpenAQCollector
from datetime import datetime
def read_api_key(file_path):
    """Read API key from file"""
    try:
        with open(file_path, "r") as f:
            return f.read().strip()
    except FileNotFoundError:
        print(f"API key file not found: {file_path}")
        return None

def main():

    # Example of use 
    #CITY = "A Coruña"
    #START_DATE = "2024-01-01"
    #END_DATE = "2024-12-31"
    #MEASUREMENTS_LIMIT = 100  
    # You're asking for: "Give me up to 100 air quality measurements for A Coruña between Jan 1, 2024 and Dec 31, 2024"

    # Configuration
    API_KEY_PATH = "/Users/sara/Documents/Projects/project_data_collections/open_aq_collector/credentials/openaq_key.txt"
    CITY = "A Coruña" 
    COUNTRY_CODE = "ES" 
    START_DATE = "2024-01-01"
    END_DATE = "2024-12-31"
    MEASUREMENTS_LIMIT = 100




    # Read API key
    api_key = read_api_key(API_KEY_PATH)
    if not api_key:
        return

    # Create collector instance
    collector = OpenAQCollector(api_key)
    
    try:
        # Get air quality data
        air_data_df = collector.get_city_air_quality_data(
            CITY, 
            COUNTRY_CODE, 
            START_DATE, 
            END_DATE,
            MEASUREMENTS_LIMIT
        )

        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"openaq_{CITY.replace(' ', '_').lower()}_{COUNTRY_CODE}_{START_DATE}_to_{END_DATE}_limit_{MEASUREMENTS_LIMIT}_file_created_on_{timestamp}.csv"
        air_data_df.to_csv(filename, index=False)

        # Display results
        if not air_data_df.empty:
            print("\nData sample:")
            print(air_data_df.head(10).to_markdown(index=False))
            print(f"\nTotal records: {len(air_data_df)}")
            
            # Show some basic stats
        else:
            print("No data found for the specified parameters")
            
    finally:
        collector.close()

if __name__ == "__main__":
    main()

