import json
import uuid
from datetime import datetime, timedelta
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry


def transform(input_object):
    transformed_objects = []
    base_time = datetime.strptime(input_object['FECHA'], "%Y-%m-%dT%H:%M:%S")
    
    # Handle H1 to H23 normally
    for hour in range(1, 24):
        new_time = base_time + timedelta(hours=hour)
        energy_value = input_object[f'H{hour}']
        transformed_objects.append({
            "id": str(uuid.uuid4()),
            "group": input_object['GRUPO'],
            "company": input_object['EMPRESA'],
            "central": input_object['CENTRAL'],
            "date": new_time.isoformat(),
            "energy": energy_value
        })
    
    # Handle H24 for the next day
    next_day_time = base_time + timedelta(days=1)
    energy_value = input_object['H24']
    transformed_objects.append({
        "id": str(uuid.uuid4()),
        "group": input_object['GRUPO'],
        "company": input_object['EMPRESA'],
        "central": input_object['CENTRAL'],
        "date": next_day_time.isoformat(),
        "energy": energy_value
    })
    
    return transformed_objects

def fetch_data_from_api(url, retries=3, backoff_factor=0.3, timeout=10):
    session = requests.Session()

    # Retry strategy
    retry_strategy = Retry(
        total=retries,
        backoff_factor=backoff_factor,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["HEAD", "GET", "OPTIONS"]  # Updated parameter name
    )

    # Apply retry strategy
    adapter = HTTPAdapter(max_retries=retry_strategy)
    session.mount("https://", adapter)
    session.mount("http://", adapter)

    try:
        response = session.get(url, timeout=timeout)
        response.raise_for_status()
        return response.json()
    except requests.exceptions.RequestException as e:
        print(f"An error occurred: {e}")
        return None

def main():
    current_date = '05/17/2024'
    api_url = f'https://apps.oc.org.do/wsOCWebsiteChart/Service.asmx/GetPostDespachoJSon?Fecha={current_date}'
    
    # Fetch data from API
    data = fetch_data_from_api(api_url)
    if data:
        input_data = data["GetPostDespacho"]
        
        # Transform the data
        all_transformed_data = []
        for item in input_data:
            all_transformed_data.extend(transform(item))
        
        # Save the transformed data to a file in UTF-8 encoding
        with open('transformed_data.json', 'w', encoding='utf-8') as outfile:
            json.dump(all_transformed_data, outfile, indent=4, ensure_ascii=False)
        
        print("Data transformation complete. Output written to transformed_data.json.")
    else:
        print("Failed to fetch data from API.")

if __name__ == "__main__":
    main()
