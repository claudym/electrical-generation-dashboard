import json
import uuid
from datetime import datetime, timedelta
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from decimal import Decimal

NAMESPACE_UUID = uuid.NAMESPACE_URL

def generate_uuid(group, company, plant, datetime_str):
    unique_string = f"{group}-{company}-{plant}-{datetime_str}"
    return str(uuid.uuid5(NAMESPACE_UUID, unique_string))

def transform(input_object):
    transformed_objects = {}
    base_time = datetime.strptime(input_object['FECHA'], "%Y-%m-%dT%H:%M:%S")
    
    for hour in range(1, 24):
        new_time = base_time + timedelta(hours=hour)
        datetime_str = new_time.strftime("%Y-%m-%dT%H:%M:%S")
        energy_value = Decimal(str(input_object[f'H{hour}']))
        item_id = generate_uuid(input_object['GRUPO'], input_object['EMPRESA'], input_object['CENTRAL'], datetime_str)
        transformed_objects[item_id] = {
            "id": item_id,
            "group": input_object['GRUPO'],
            "group_plant": f"{input_object['GRUPO']}-{input_object['CENTRAL']}",
            "company": input_object['EMPRESA'],
            "plant": input_object['CENTRAL'],
            "datetime": new_time.isoformat(),
            "energy": energy_value
        }
    
    # Handle H24 for the next day
    next_day_time = base_time + timedelta(days=1)
    datetime_str = next_day_time.strftime("%Y-%m-%dT%H:%M:%S")
    energy_value = Decimal(str(input_object['H24']))
    item_id = generate_uuid(input_object['GRUPO'], input_object['EMPRESA'], input_object['CENTRAL'], datetime_str)
    transformed_objects[item_id] = {
        "id": item_id,
        "group": input_object['GRUPO'],
        "group_plant": f"{input_object['GRUPO']}-{input_object['CENTRAL']}",
        "company": input_object['EMPRESA'],
        "plant": input_object['CENTRAL'],
        "datetime": next_day_time.isoformat(),
        "energy": energy_value
    }
    
    return transformed_objects

def fetch_data_from_api(url, retries=3, backoff_factor=0.3, timeout=10):
    session = requests.Session()

    retry_strategy = Retry(
        total=retries,
        backoff_factor=backoff_factor,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["HEAD", "GET", "OPTIONS"]
    )

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

def decimal_serializer(obj):
    if isinstance(obj, Decimal):
        return str(obj)
    raise TypeError("Type not serializable")

def main():
    start_date = '2020-01-09'
    end_date = '2020-01-09'
    
    start_date_obj = datetime.strptime(start_date, '%Y-%m-%d')
    end_date_obj = datetime.strptime(end_date, '%Y-%m-%d')
    
    current_date_obj = start_date_obj
    all_transformed_data = []

    while current_date_obj <= end_date_obj:
        current_date_str = current_date_obj.strftime('%m/%d/%Y')
        api_url = f'https://apps.oc.org.do/wsOCWebsiteChart/Service.asmx/GetPostDespachoJSon?Fecha={current_date_str}'
        
        data = fetch_data_from_api(api_url)
        if data:
            input_data = data["GetPostDespacho"]
            all_transformed_items = {}

            for item in input_data:
                transformed_items = transform(item)
                all_transformed_items.update(transformed_items)
            
            all_transformed_data.extend(all_transformed_items.values())
        
        current_date_obj += timedelta(days=1)
    
    with open('transformed_data_sequential.json', 'w', encoding='utf-8') as outfile:
        json.dump(all_transformed_data, outfile, indent=4, ensure_ascii=False, default=decimal_serializer)
    
    print("Data transformation and output to JSON file complete.")

if __name__ == "__main__":
    main()