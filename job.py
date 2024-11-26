from firebase_admin import credentials, db
from dotenv import load_dotenv
import pandas as pd
import firebase_admin
import os
import sys

load_dotenv()

if not firebase_admin._apps:
    cred = credentials.Certificate("test-94202-firebase-adminsdk-4zyte-b7ecf8bf93.json")
    firebase_admin.initialize_app(cred, {
        'databaseURL': 'https://test-94202-default-rtdb.firebaseio.com/'
    })


def flatten_data(data):
    records = []
    for key, value in data.items():
        if isinstance(value, dict):
            record = {"id": key}
            record.update(value)
            records.append(record)
    return records


def get_metal_data_from_firebase():
    ref = db.reference('/')
    metal_data = ref.get()
    print("Fetched metal data:", metal_data)

    if metal_data:
        flat_data = flatten_data(metal_data)
        df_metal = pd.DataFrame(flat_data)
        if 'Date' in df_metal.columns:
            df_metal['Date'] = pd.to_datetime(df_metal['Date'], errors='coerce').dt.date
        return df_metal
    else:
        print("Metal data not found")
        return pd.DataFrame()


metal_df = get_metal_data_from_firebase()
print("Metal DataFrame:")
print(metal_df.head())


def save_data(date, metal):
    date = pd.to_datetime(date).date()

    if metal in ['Gold', 'Palladium', 'Platinum']:
        df_new = metal_df[['Date', f'{metal} AM Fix', f'{metal} PM Fix']]
        df_new = df_new.loc[(metal_df['Date'] == date)]
    elif metal == 'Silver':
        df_new = metal_df[['Date', f'{metal} Fix']]
        df_new = df_new.loc[(metal_df['Date'] == date)]
    elif metal in ['Iridium', 'Rhodium', 'Ruthenium']:
        df_new = metal_df[['Date', f'{metal}']]
        df_new = df_new.loc[(metal_df['Date'] == date)]

    if not df_new.empty:
        json_file_path = f"./path/to/my_dir/raw/{metal}/metal_data_{date}.json"

        os.makedirs(os.path.dirname(json_file_path), exist_ok=True)

        df_new.to_json(json_file_path, orient='records', date_format='iso')

        print(f"saved to {json_file_path}")
    else:
        print("no data was saved")


if __name__ == "__main__":
    args = sys.argv[1:]
    if len(args) == 2:
        date, metals = args
        save_data(date, metals)
    else:
        print("Usage: python script.py <date> <metal>")
