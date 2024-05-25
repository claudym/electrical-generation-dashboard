import json
import uuid
from datetime import datetime, timedelta

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

def main():
    input_data = [
        {
            "GRUPOS": "1 - Térmica",
            "INDICE": 1,
            "GRUPO": "Térmica",
            "EMPRESA": "AES ANDRES",
            "CENTRAL": "AES ANDRÉS GN",
            "FECHA": "2024-05-17T00:00:00",
            "H1": 249.78,
            "H2": 256.0,
            "H3": 256.0,
            "H4": 276.0,
            "H5": 276.0,
            "H6": 276.0,
            "H7": 287.0,
            "H8": 269.47,
            "H9": 276.05,
            "H10": 263.44,
            "H11": 249.23,
            "H12": 233.39,
            "H13": 242.14,
            "H14": 245.32,
            "H15": 225.86,
            "H16": 249.14,
            "H17": 230.48,
            "H18": 238.19,
            "H19": 238.28,
            "H20": 274.28,
            "H21": 264.39,
            "H22": 262.57,
            "H23": 257.41,
            "H24": 261.0
        }
        # Add more input objects as needed
    ]
    
    all_transformed_data = []
    for item in input_data:
        all_transformed_data.extend(transform(item))
    
    with open('transformed_data.json', 'w', encoding='utf-8') as outfile:
        json.dump(all_transformed_data, outfile, indent=4, ensure_ascii=False)
    
    print("Data transformation complete. Output written to transformed_data.json.")

if __name__ == "__main__":
    main()
