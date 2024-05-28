import json
import uuid
from datetime import datetime, timedelta
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import time
import boto3
from awsglue.utils import getResolvedOptions
import sys

def get_secret(secret_name):
    secrets_client = boto3.client('secretsmanager')
    try:
        response = secrets_client.get_secret_value(SecretId=secret_name)
        secret = json.loads(response['SecretString'])
        return secret
    except Exception as e:
        print(f"Error retrieving secret: {e}")
        return None

def transform(input_object):
    transformed_objects = []
    base_time = datetime.strptime(input_object['FECHA'], "%Y-%m-%dT%H:%M:%S")

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

def save_to_dynamodb(dynamodb, table_name, data):
    table = dynamodb.Table(table_name)
    with table.batch_writer() as batch:
        for item in data:
            batch.put_item(Item=item)

def main():
    # Parse arguments
    args = getResolvedOptions(sys.argv, ['start_date', 'end_date', 'dynamodb_table', 'secret_name'])
    start_date = args['start_date']
    end_date = args['end_date']
    dynamodb_table = args['dynamodb_table']
    secret_name = args['secret_name']
    
    # Retrieve DynamoDB credentials from AWS Secrets Manager
    secret = get_secret(secret_name)
    if not secret:
        print("Failed to retrieve secret. Exiting.")
        return

    # Initialize DynamoDB client with credentials from Secrets Manager
    dynamodb = boto3.resource('dynamodb', 
                              aws_access_key_id=secret['aws_access_key_id'], 
                              aws_secret_access_key=secret['aws_secret_access_key'], 
                              region_name=secret['region'])

    start_date_obj = datetime.strptime(start_date, '%Y-%m-%d')
    end_date_obj = datetime.strptime(end_date, '%Y-%m-%d')
    
    current_date_obj = start_date_obj

    all_transformed_data = []

    processing_start_time = time.process_time()

    while current_date_obj <= end_date_obj:
        current_date_str = current_date_obj.strftime('%m/%d/%Y')
        api_url = f'https://apps.oc.org.do/wsOCWebsiteChart/Service.asmx/GetPostDespachoJSon?Fecha={current_date_str}'
        
        data = fetch_data_from_api(api_url)
        if data:
            input_data = data["GetPostDespacho"]
            
            for item in input_data:
                all_transformed_data.extend(transform(item))
        
        current_date_obj += timedelta(days=1)

    processing_end_time = time.process_time()
    processing_time = processing_end_time - processing_start_time

    # Save the transformed data to DynamoDB
    save_to_dynamodb(dynamodb, dynamodb_table, all_transformed_data)

    print("Data transformation complete. Data saved to DynamoDB.")
    print(f"Data processing time: {processing_time:.2f} seconds.")

if __name__ == "__main__":
    main()
