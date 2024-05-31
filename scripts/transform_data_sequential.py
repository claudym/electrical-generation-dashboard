import json
import uuid
from datetime import datetime, timedelta
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import boto3
from decimal import Decimal


TABLE_NAME = 'basic_electrical_generation_data_dom_rep'
dynamodb = boto3.resource('dynamodb')
table = dynamodb.Table(TABLE_NAME)

NAMESPACE_UUID = uuid.NAMESPACE_URL

def generate_uuid(group, company, plant, datetime_str):
    unique_string = f"{group}-{company}-{plant}-{datetime_str}"
    return str(uuid.uuid5(NAMESPACE_UUID, unique_string))

def transform(input_object):
    transformed_objects = []
    base_time = datetime.strptime(input_object['FECHA'], "%Y-%m-%dT%H:%M:%S")
    
    for hour in range(1, 24):
        new_time = base_time + timedelta(hours=hour)
        datetime_str = new_time.strftime("%Y-%m-%dT%H:%M:%S")
        energy_value = Decimal(str(input_object[f'H{hour}']))
        item_id = generate_uuid(input_object['GRUPO'], input_object['EMPRESA'], input_object['CENTRAL'], datetime_str)
        transformed_objects.append({
            "id": item_id,
            "group": input_object['GRUPO'],
            "group_plant": f"{input_object['GRUPO']}-{input_object['CENTRAL']}",
            "company": input_object['EMPRESA'],
            "plant": input_object['CENTRAL'],
            "datetime": new_time.isoformat(),
            "energy": energy_value
        })
    
    # Handle H24 for the next day
    next_day_time = base_time + timedelta(days=1)
    datetime_str = new_time.strftime("%Y-%m-%dT%H:%M:%S")
    energy_value = Decimal(str(input_object['H24']))
    item_id = generate_uuid(input_object['GRUPO'], input_object['EMPRESA'], input_object['CENTRAL'], datetime_str)
    transformed_objects.append({
        "id": item_id,
        "group": input_object['GRUPO'],
        "group_plant": f"{input_object['GRUPO']}-{input_object['CENTRAL']}",
        "company": input_object['EMPRESA'],
        "plant": input_object['CENTRAL'],
        "datetime": next_day_time.isoformat(),
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

def batch_write_items(table, items, batch_size=25):
    unique_items = {}
    for item in items:
        key = item['id']
        unique_items[key] = item

    items = list(unique_items.values())

    for i in range(0, len(items), batch_size):
        batch = items[i:i+batch_size]
        with table.batch_writer() as batch_writer:
            for item in batch:
                batch_writer.put_item(Item=item)

def main():
    start_date = '2020-01-01'
    end_date = '2020-01-02'
    
    start_date_obj = datetime.strptime(start_date, '%Y-%m-%d')
    end_date_obj = datetime.strptime(end_date, '%Y-%m-%d')
    
    current_date_obj = start_date_obj

    while current_date_obj <= end_date_obj:
        current_date_str = current_date_obj.strftime('%m/%d/%Y')
        api_url = f'https://apps.oc.org.do/wsOCWebsiteChart/Service.asmx/GetPostDespachoJSon?Fecha={current_date_str}'
        
        data = fetch_data_from_api(api_url)
        if data:
            input_data = data["GetPostDespacho"]
            all_transformed_items = []

            for item in input_data:
                transformed_items = transform(item)
                all_transformed_items.extend(transformed_items)
            
            batch_write_items(table, all_transformed_items)
        
        current_date_obj += timedelta(days=1)
    
    print("Data transformation and upload complete.")

if __name__ == "__main__":
    main()
