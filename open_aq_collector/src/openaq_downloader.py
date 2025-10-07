import requests
import pandas as pd

def get_data_from_api(url, limit, api_key):
    headers = {"X-API-Key": api_key} 
    
    try:
        response = requests.get(url, headers=headers)
        response.raise_for_status()

        data = response.json()
        
        # Extract results list into a pandas DataFrame
        df = pd.json_normalize(data.get("results", []))
        return df
    
    except Exception as e:
        print(f"Error fetching API data: {e}")
        return pd.DataFrame()

with open("/Users/sara/Documents/Projects/project_data_collections/data_APIs/openaq_key.txt", "r") as f:
    api_key = f.read().strip()



if __name__ == "__main__":
    # Read API key from text file
    with open("openaq_key.txt", "r") as f:
        api_key = f.read().strip()
    

    # 67 is spain
    city = "67"  
    limit = 10
    url = f"https://api.openaq.org/v3/countries/{city}"

    df = get_data_from_api(url,limit, api_key)
    
    if not df.empty:
        print(f" Loaded {len(df)} records for {city}.")
        print(df.head())
        print("shape",df.shape)
        print(df.head(10))
        print(df["parameters"][0][10])
        df_exploded = df.explode("parameters")
        df_flat = pd.json_normalize(df_exploded["parameters"])
        print(df_flat.head())
        
    else:
        print("No data returned.")


 
    headers = {"X-API-Key": api_key}

    locations_id="67"
    params = {
    "limit": 100,
    }
    url_locations = f"https://api.openaq.org/v3/locations/{locations_id}/latest"
  

    new_response = requests.get(url_locations, headers=headers,params=params)
    new_response.raise_for_status()

    new_data = new_response.json()

    df_measurements = pd.json_normalize(new_data.get("results", []))
    print(df_measurements.head())
    print("Number of rows:", len(df_measurements))
    sensorsId_list=df_measurements["sensorsId"].tolist()
    print("sensorsId_list",sensorsId_list)
    print("coordinates",df_measurements["sensorsId"])

    

    for sensors_id in sensorsId_list:
        params_sensors = {
        "limit": 1000,
        "date_from": "2024-01-01T00:00:00Z",
        "date_to": "2024-12-31T23:59:59Z"
        }
        
        #2162376 santiago de compostela

        url_sensors= f"https://api.openaq.org/v3/sensors/{sensors_id}/measurements"
        sensor_response = requests.get(url_sensors, headers=headers,params=params_sensors)
        sensor_response.raise_for_status()
        sensors_data = sensor_response.json()
        df_sensor_measurment= pd.json_normalize(sensors_data.get("results", []))



        #print("data for ",sensors_id)
        #print("parameter name ", df_sensor_measurment["parameter.name"][0])
        #print(df_sensor_measurment.head())
        df_sensor_measurment['coverage.datetimeFrom.utc'] = pd.to_datetime(
        df_sensor_measurment['coverage.datetimeFrom.utc']
        )
        print(df_sensor_measurment["coverage.datetimeFrom.utc"][0])
        print(pd.to_datetime("2018-01-01T00:00:00Z"))
        print("coordinates",df_sensor_measurment["coordinates"][0])
        print(df_sensor_measurment[df_sensor_measurment["coverage.datetimeFrom.utc"]>pd.to_datetime("2017-01-01T00:00:00Z")])
        #print("column_list",df_sensor_measurment.columns.tolist())
    
    
    url_santiago_locations=f"https://api.openaq.org/v3/locations/2162376"
    sensor_santiago_response = requests.get(url_santiago_locations, headers=headers,params=params)
    sensor_santiago_response.raise_for_status()
    sensors_santiago_data = sensor_santiago_response.json()
    df_santiago_sensor_measurment= pd.json_normalize(sensors_santiago_data.get("results", []))
    
    
