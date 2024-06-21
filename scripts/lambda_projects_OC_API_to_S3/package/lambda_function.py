import json
import requests
import boto3
from datetime import datetime, timedelta

s3 = boto3.client('s3')

def lambda_handler(event, context):
    # Define el rango de fechas
    start_date = datetime(2024, 5, 1)
    end_date = datetime(2024, 5, 31)
    current_date = start_date

    while current_date <= end_date:
        # URL de la API con la fecha deseada
        fecha = current_date.strftime("%m/%d/%Y")
        url = f"https://apps.oc.org.do/wsOCWebsiteChart/Service.asmx/GetPostDespachoJSon?Fecha={fecha}"
        
        response = requests.get(url)
        data = response.json()
        
        # Nombre del archivo en S3
        file_name = f"data_{current_date.strftime('%Y-%m-%d')}.json"
        
        # Guardar datos en un bucket de S3
        s3.put_object(
            Bucket='getapidatatos3-06-18-2024',
            Key=file_name,
            Body=json.dumps(data),
            ContentType='application/json'
        )
        
        # Incrementar la fecha
        current_date += timedelta(days=1)
    
    return {
        'statusCode': 200,
        'body': json.dumps('Data fetched and stored in S3 for all dates')
    }
