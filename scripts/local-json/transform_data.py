import json
import uuid
from datetime import datetime, timedelta
import aiohttp
from decimal import Decimal
import asyncio
from tenacity import retry, wait_exponential, stop_after_attempt, retry_if_exception_type
import aiohttp.client_exceptions
from concurrent.futures import ProcessPoolExecutor

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

@retry(wait=wait_exponential(min=1, max=10), stop=stop_after_attempt(5), retry=retry_if_exception_type(aiohttp.ClientError))
async def fetch_data_from_api(url, session):
    try:
        async with session.get(url) as response:
            response.raise_for_status()
            return await response.json()
    except aiohttp.ClientError as e:
        print(f"An error occurred: {e}")
        raise

async def process_date(http_session, current_date_str, executor):
    api_url = f'https://apps.oc.org.do/wsOCWebsiteChart/Service.asmx/GetPostDespachoJSon?Fecha={current_date_str}'
    data = await fetch_data_from_api(api_url, http_session)
    if data:
        input_data = data["GetPostDespacho"]
        loop = asyncio.get_running_loop()
        transformed_items_list = await loop.run_in_executor(executor, transform_parallel, input_data)
        all_transformed_items = {k: v for d in transformed_items_list for k, v in d.items()}
        
        return all_transformed_items

def transform_parallel(input_data):
    all_transformed = []
    for item in input_data:
        transformed = transform(item)
        all_transformed.append(transformed)
    return all_transformed

def decimal_serializer(obj):
    if isinstance(obj, Decimal):
        return str(obj)
    raise TypeError("Type not serializable")

async def main():
    start_date = '2013-02-12'
    end_date = '2013-02-12'
    
    start_date_obj = datetime.strptime(start_date, '%Y-%m-%d')
    end_date_obj = datetime.strptime(end_date, '%Y-%m-%d')
    
    current_date_obj = start_date_obj
    tasks = []

    with ProcessPoolExecutor() as executor:
        async with aiohttp.ClientSession() as http_session:
            while current_date_obj <= end_date_obj:
                current_date_str = current_date_obj.strftime('%m/%d/%Y')
                tasks.append(process_date(http_session, current_date_str, executor))
                current_date_obj += timedelta(days=1)
            
            all_results = await asyncio.gather(*tasks)
    
    all_transformed_data = {k: v for result in all_results for k, v in result.items()}
    
    with open('transformed_data.json', 'w', encoding='utf-8') as outfile:
        json.dump(all_transformed_data, outfile, indent=4, ensure_ascii=False, default=decimal_serializer)
    
    print("Data transformation and output to JSON file complete.")

if __name__ == "__main__":
    asyncio.run(main())
