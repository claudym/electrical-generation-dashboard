import json
import uuid
from datetime import datetime, timedelta
import aiohttp
import aioboto3
from decimal import Decimal
import asyncio
from tenacity import retry, wait_exponential, stop_after_attempt, retry_if_exception_type
import aiohttp.client_exceptions
from concurrent.futures import ProcessPoolExecutor


TABLE_NAME = 'basic_electrical_generation_data_dom_rep'
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
    datetime_str = next_day_time.strftime("%Y-%m-%dT%H:%M:%S")
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

@retry(wait=wait_exponential(min=1, max=10), stop=stop_after_attempt(5), retry=retry_if_exception_type(aiohttp.ClientError))
async def fetch_data_from_api(url, session):
    try:
        async with session.get(url) as response:
            response.raise_for_status()
            return await response.json()
    except aiohttp.ClientError as e:
        print(f"An error occurred: {e}")
        raise

async def batch_write_items(table, items, batch_size=25):
    unique_items = {}
    for item in items:
        key = item['id']
        unique_items[key] = item

    items = list(unique_items.values())

    async with table.batch_writer() as batch:
        for i in range(0, len(items), batch_size):
            for item in items[i:i + batch_size]:
                await batch.put_item(Item=item)

async def process_date(http_session, current_date_str, dynamodb, executor):
    api_url = f'https://apps.oc.org.do/wsOCWebsiteChart/Service.asmx/GetPostDespachoJSon?Fecha={current_date_str}'
    data = await fetch_data_from_api(api_url, http_session)
    if data:
        input_data = data["GetPostDespacho"]
        loop = asyncio.get_running_loop()
        transformed_items_list = await loop.run_in_executor(executor, transform_parallel, input_data)
        all_transformed_items = [item for sublist in transformed_items_list for item in sublist]
        
        table = await dynamodb.Table(TABLE_NAME)
        await batch_write_items(table, all_transformed_items)

def transform_parallel(input_data):
    return [transform(item) for item in input_data]

async def main():
    start_date = '2020-01-07'
    end_date = '2020-01-08'
    
    start_date_obj = datetime.strptime(start_date, '%Y-%m-%d')
    end_date_obj = datetime.strptime(end_date, '%Y-%m-%d')
    
    current_date_obj = start_date_obj
    tasks = []

    session = aioboto3.Session()

    with ProcessPoolExecutor() as executor:
        async with aiohttp.ClientSession() as http_session:
            async with session.resource('dynamodb') as dynamodb:
                while current_date_obj <= end_date_obj:
                    current_date_str = current_date_obj.strftime('%m/%d/%Y')
                    tasks.append(process_date(http_session, current_date_str, dynamodb, executor))
                    current_date_obj += timedelta(days=1)
            
                await asyncio.gather(*tasks)
    
    print("Data transformation and upload complete.")

if __name__ == "__main__":
    asyncio.run(main())
