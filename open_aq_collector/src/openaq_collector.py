import openaq
import pandas as pd
from typing import List
import time
from datetime import datetime

class OpenAQCollector:
    def __init__(self, api_key: str):
        self.client = openaq.OpenAQ(api_key=api_key)
        self.api_key = api_key
    
    def get_available_cities(self, country_code: str, limit : int=100) -> List[str]:
        """Get list of all available cities in a country"""
        try:
            locations_response = self.client.locations.list(iso=country_code, limit=limit)
            cities = set()
            
            for location in locations_response.results:
                if hasattr(location, 'city') and getattr(location, 'city'):
                    city_name = getattr(location, 'city')
                    if city_name and city_name not in cities:
                        cities.add(city_name)
            
            return sorted(list(cities))
        except Exception as e:
            print(f"Error fetching cities: {e}")
            return []
    
    def get_city_air_quality_data(self, city: str, country_code: str, 
                                date_from: str, date_to: str, 
                                limit: int = 100,
                                verbose: bool = False) -> pd.DataFrame:
        """Get air quality data for a city and date range"""
        
        all_measurements_data = []
        
        print(f"Getting data for {city}, {country_code} from {date_from} to {date_to}")

        try:
            # Get all locations in the country
            locations_response = self.client.locations.list(iso=country_code, limit=limit)
            all_country_locations = locations_response.results
            all_country_locations_df = pd.DataFrame(all_country_locations)
            if verbose:
                timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
                filename = f"all_country_locations_{timestamp}.csv"
                all_country_locations_df.to_csv(filename, index=False)
    
                print("Try for recent data")
            # Filter locations by city name
            locations = []
            sensor_locations=[]
            available_cities=[]
            for loc in all_country_locations:
                loc_city = getattr(loc, 'locality', '') 
                loc_name = getattr(loc, 'name', '') # this retrieves the name of the locations of the sensor 
                available_cities.append(loc_city)
                
                if city in str(loc_city):
                    locations.append(loc)
                    sensor_locations.append(loc_name)

            if not locations:
                print(f"No locations found for '{city}' in '{country_code}'")
               
                if available_cities:
                    print(f"Available cities is {country_code} are: ")
                    print(available_cities)
                return pd.DataFrame()

            assert len(locations)== len(sensor_locations)
            print(f"Found {len(locations)} locations for '{city}'")
            print(f"Locations are '{sensor_locations}'")

            # Get sensor details
            sensor_details = []

            for location in locations:
                location_id = getattr(location, 'id', None)
                location_name = getattr(location, 'name', 'Unknown')
                # Extract coordinates from location
                coordinates = getattr(location, 'coordinates', {})
                latitude = getattr(coordinates, 'latitude', None) if hasattr(coordinates, 'latitude') else None
                longitude = getattr(coordinates, 'longitude', None) if hasattr(coordinates, 'longitude') else None
                
                print(location_name," has ",len(getattr(location, 'sensors', [])))
                for i, sensor in enumerate(getattr(location, 'sensors', [])):
                    parameter_obj = getattr(sensor, 'parameter', None)
                    param_name = getattr(parameter_obj, 'name', None) if parameter_obj else None
                    if verbose:
                        print("measured",param_name, "station id ",getattr(sensor, 'id', None))
                    if param_name:
                        sensor_details.append({
                            'sensor_id': getattr(sensor, 'id', None),
                            'parameter': param_name,
                            'location_id': location_id,
                            'location_name': location_name,
                            'latitude': latitude,
                            'longitude': longitude
                        })
                    if (i + 1) % 5 == 0 and (i + 1) < len(getattr(location, 'sensors', [])):
                        print("Taking a brief pause...")
                        time.sleep(5)  # Sleep for 1 second after every 5 locations

            unique_sensor_ids = {d['sensor_id'] for d in sensor_details if d['sensor_id']}
            print(f"Found {len(unique_sensor_ids)} sensors")

            # Get measurements for each sensor
            sensor_map = {d['sensor_id']: d for d in sensor_details if d['sensor_id']}

            for i, sensor_id in enumerate(unique_sensor_ids):
                details = sensor_map[sensor_id]
                if verbose:
                    print(f"Processing sensor {i + 1} of {len(unique_sensor_ids)}: {details['location_name']} (ID: {sensor_id})")
                if (i + 1) % 5 == 0 and (i + 1) < len(unique_sensor_ids):
                    print("Taking a brief pause...")
                    time.sleep(5)  # Sleep for 1 second after every 5 sensors

                page = 1
                total_retrieved = 0
                MAX_PAGES = 1000 # Safety limit for very large queries
                empty_pages=0
                while page <= MAX_PAGES:
                    try:
                        measurements_response = self.client.measurements.list(
                            sensors_id=sensor_id,  
                            datetime_from=date_from,
                            datetime_to=date_to,
                            limit=limit, # max 1000
                            page=page    # Increment page number
                        )
                        
                       
                        current_page_count = len(measurements_response.results)
                        total_retrieved += current_page_count
                        
                        print(f"   -> Page {page}: Fetched {current_page_count} records.")
                        if current_page_count==0:
                            empty_pages+=1
                        if (empty_pages >3):
                            recent_data=self.client.locations.latest(sensor_id)
                            all_recent_data = recent_data.results
                            all_recent_data_df = pd.DataFrame(all_recent_data)
                            if verbose:
                                all_recent_data_df.to_csv(f"all_recent_data_{city}_{sensor_id}_.csv", index=False)
               
                            break

                        #if not measurements_response.results:
                        #    # No more results, break the inner loop
                        #    break
                        
                        for measurement_obj in measurements_response.results:
                            
                            if hasattr(measurement_obj, 'dict'):
                                measurement = measurement_obj.dict()  
                            else:
                                measurement = vars(measurement_obj).copy()
                            
                            # Extract datetime
                            datetime_local_str = getattr(measurement_obj, 'datetime_local', None)
                            
                            measurement['datetime_local'] = datetime_local_str
                            
                            # Add metadata including coordinates
                            measurement['location_id'] = details['location_id']
                            measurement['location_name'] = details['location_name']
                            measurement['sensor_id'] = sensor_id
                            measurement['latitude'] = details['latitude']
                            measurement['longitude'] = details['longitude']
                            
                            all_measurements_data.append(measurement)
                        
                        page += 1
                        # Small pause after each page request
                        time.sleep(1) 

                    except Exception as e:
                        print(f"Error fetching measurements for sensor {sensor_id} on page {page}: {e}")
                        break
                
                if verbose:
                    print(f"Processed sensor {sensor_id} with a total of {total_retrieved} records.")

            print(f"Retrieved {len(all_measurements_data)} total measurements")
            
            # Create and clean dataframe
            if not all_measurements_data:
                print("No measurements found")
                return pd.DataFrame()
                
            measurements_df = pd.DataFrame(all_measurements_data)
            #timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            #filename = f"measurements_{timestamp}.csv"
            #measurements_df.to_csv(filename, index=False)
            #print(f"Data saved to {filename}")


            print(measurements_df.head())


            print(measurements_df.columns)


            # Convert datetime
            measurements_df['datetime_from'] = measurements_df['period'].apply(
                lambda dt_obj: dt_obj.datetime_from.local if hasattr(dt_obj, 'datetime_from') else None
            )
            measurements_df['datetime_from'] = pd.to_datetime(measurements_df['datetime_from'])

           
            measurements_df['datetime_to'] = measurements_df['period'].apply(
                lambda dt_obj: dt_obj.datetime_to.local if hasattr(dt_obj, 'datetime_to') else None
            )
            measurements_df['datetime_to'] = pd.to_datetime(measurements_df['datetime_to'])


            measurements_df['param_id'] = measurements_df['parameter'].apply(
                lambda dt_obj: dt_obj.id  if hasattr(dt_obj, 'id') else None
            )
            
            measurements_df['param_name'] = measurements_df['parameter'].apply(
                lambda dt_obj: dt_obj.name if hasattr(dt_obj, 'name') else None
            )
            
            measurements_df['unit'] = measurements_df['parameter'].apply(
                lambda dt_obj: dt_obj.units if hasattr(dt_obj, 'units') else None
            )

            measurements_df['expected_count'] = measurements_df['summary'].apply(
            lambda x: x.expected_count if hasattr(x, 'expected_count') else None
            )
            measurements_df['observed_count'] = measurements_df['summary'].apply(
            lambda x: x.observed_count if hasattr(x, 'observed_count') else None
            )
            measurements_df['percent_complete'] = measurements_df['summary'].apply(
            lambda x: x.percent_complete if hasattr(x, 'percent_complete') else None
            )
            measurements_df['percent_coverage'] = measurements_df['summary'].apply(
            lambda x: x.percent_coverage if hasattr(x, 'percent_coverage') else None
            )
            measurements_df['data_gap'] = measurements_df['expected_count'] - measurements_df['observed_count']
            
            # Select final columns - added latitude and longitude
            final_cols = ['datetime_from','datetime_to', 'location_id',"location_name",'sensor_id', 'param_id', 'param_name' , 'value', 'unit', 'latitude', 'longitude']
            
            final_df = measurements_df[final_cols]
            
            return final_df

        except Exception as e:
            print(f"Error: {e}")
            return pd.DataFrame()
    
    def close(self):
        """Close the OpenAQ client"""
        if self.client:
            self.client.close()