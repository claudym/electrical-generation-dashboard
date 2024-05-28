import json
import uuid
from datetime import datetime, timedelta
import requests

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

def fetch_data_from_api(url):
    response = requests.get(url)
    response.raise_for_status()  # Raise an error for bad status codes
    return response.json()

def main():
    current_date = '05/17/2024'
    api_url = f'https://apps.oc.org.do/wsOCWebsiteChart/Service.asmx/GetPostDespachoJSon?Fecha={current_date}'
    
    # Fetch data from API
    data = fetch_data_from_api(api_url)
    input_data = data["GetPostDespacho"]
    
    all_transformed_data = []
    for item in input_data:
        all_transformed_data.extend(transform(item))
    
    with open('transformed_data.json', 'w', encoding='utf-8') as outfile:
        json.dump(all_transformed_data, outfile, indent=4, ensure_ascii=False)
    
    print("Data transformation complete. Output written to transformed_data.json.")

if __name__ == "__main__":
    main()
