import json
import requests
import boto3
import csv
from datetime import datetime, timedelta
from io import StringIO

s3 = boto3.client('s3')

def lambda_handler(event, context):
    # Define el rango de fechas
    start_date = datetime(2024, 5, 1)
    end_date = datetime(2024, 5, 10)
    current_date = start_date

    # Diccionario para almacenar los datos organizados
    data_dict = {}

    while current_date <= end_date:
        # URL de la API con la fecha deseada
        fecha = current_date.strftime("%m/%d/%Y")
        url = f"https://apps.oc.org.do/wsOCWebsiteChart/Service.asmx/GetPostDespachoJSon?Fecha={fecha}"
        
        try:
            response = requests.get(url)
            response.raise_for_status()
            data = response.json()
        except requests.RequestException as e:
            print(f"Request failed: {e}")
            current_date += timedelta(days=1)
            continue
        
        # Organizar los datos en el diccionario
        for entry in data['GetPostDespacho']:
            central = entry['CENTRAL']
            fecha = entry['FECHA'].split('T')[0]  # Tomar solo la parte de la fecha
            
            for hour in range(1, 25):
                hour_key = f'H{hour}'
                
                # Ajustar la hora 24 a 00:00:00 del siguiente día
                if hour == 24:
                    datetime_key = f"{(datetime.strptime(fecha, '%Y-%m-%d') + timedelta(days=1)).strftime('%Y-%m-%d')} 00:00:00"
                else:
                    datetime_key = f"{fecha} {hour:02d}:00:00"
                
                if datetime_key not in data_dict:
                    data_dict[datetime_key] = {}
                
                data_dict[datetime_key][central] = entry[hour_key]
        
        # Incrementar la fecha
        current_date += timedelta(days=1)
    
    # Crear un archivo CSV en memoria
    csv_buffer = StringIO()
    csv_writer = csv.writer(csv_buffer)
    
    # Escribir la cabecera
    all_centrales = set()
    for values in data_dict.values():
        all_centrales.update(values.keys())
    
    header = ['Fecha y Hora'] + sorted(all_centrales)
    csv_writer.writerow(header)
    
    # Escribir los datos
    for datetime_key, values in sorted(data_dict.items()):
        row = [datetime_key] + [values.get(central, '') for central in header[1:]]
        csv_writer.writerow(row)
    
    # Subir el archivo CSV a S3
    s3.put_object(
        Bucket='getapidatatos3-06-18-2024',
        Key='organized_data.csv',
        Body=csv_buffer.getvalue(),
        ContentType='text/csv'
    )
    
    return {
        'statusCode': 200,
        'body': json.dumps('Data fetched, organized, and stored in S3 as CSV')
    }