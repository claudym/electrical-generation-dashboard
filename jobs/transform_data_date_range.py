import sys
import os
import zipfile

# Add the directory where Glue downloads the dependencies to the Python path
dependencies_path = '/tmp'

# Ensure the path exists
if not os.path.exists(dependencies_path):
    os.makedirs(dependencies_path)

# Unzip the dependencies
zip_file_path = os.path.join(dependencies_path, 'transform_data_date_range_deps.zip')

with zipfile.ZipFile(zip_file_path, 'r') as zip_ref:
    zip_ref.extractall(dependencies_path)

# Add the extracted dependencies to the Python path
sys.path.insert(0, dependencies_path)

import json
import uuid
from datetime import datetime, timedelta
from decimal import Decimal
import aiohttp
import aioboto3
import asyncio
from tenacity import retry, wait_exponential, stop_after_attempt, retry_if_exception_type
from concurrent.futures import ProcessPoolExecutor
import aiohttp.client_exceptions

# Initialize the Glue context and libraries
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

# Glue context setup
args = getResolvedOptions(sys.argv, ['JOB_NAME', 'START_DATE', 'END_DATE'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

TABLE_NAME = 'basic_electrical_generation_data_dom_rep'
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

async def batch_write_items(table, items, batch_size=25):
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
        all_transformed_items = {k: v for d in transformed_items_list for k, v in d.items()}
        
        table = await dynamodb.Table(TABLE_NAME)
        await batch_write_items(table, list(all_transformed_items.values()))

def transform_parallel(input_data):
    all_transformed = []
    for item in input_data:
        transformed = transform(item)
        all_transformed.append(transformed)
    return all_transformed

async def main():
    start_date = args['START_DATE']
    end_date = args['END_DATE']
    
    start_date_obj = datetime.strptime(start_date, '%Y-%m-%d')
    end_date_obj = datetime.strptime(end_date, '%Y-%m-%d')
    
    current_date_obj = start_date_obj
    tasks = []

    session = aioboto3.Session()

    with ProcessPoolExecutor() as executor:
        async with aiohttp.ClientSession() as http_session:
            async with session.resource('dynamodb', region_name='us-east-1') as dynamodb:
                while current_date_obj <= end_date_obj:
                    current_date_str = current_date_obj.strftime('%m/%d/%Y')
                    tasks.append(process_date(http_session, current_date_str, dynamodb, executor))
                    current_date_obj += timedelta(days=1)
            
                await asyncio.gather(*tasks)
    print("Data transformation and upload complete.")

if __name__ == "__main__":
    asyncio.run(main())
    job.commit()
