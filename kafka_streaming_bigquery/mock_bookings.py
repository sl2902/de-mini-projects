import numpy as np
from datetime import datetime, timedelta, date
import pandas as pd
import json
import string
from pathlib import Path

sample_data = {
"booking_id": "B333",
"customer_id": "C111",
"flight_id": "F666",
"booking_time": "2023-01-01 23:23:23",
"origin": "LAX",
"destination": "JFK",
"price": 100
}

num_examples = 1000
booking_ids = ["B" + str(1000 + _ + 1) for _ in range(num_examples)]
customer_ids = ["C" + str(1000 + _ + 1) for _ in range(num_examples)]
flight_ids = ["F" + str(1000 + _ + 1) for _ in range(num_examples)]
prices = [np.random.randint(1000, 9999) for _ in range(num_examples)]
origin_codes = [np.random.choice([string.ascii_uppercase[i: i+3] 
                                  if len(string.ascii_uppercase[i: i+3]) == 3 else string.ascii_uppercase[i: i+3] + np.random.choice(list(string.ascii_uppercase))
                                  for i in range(0, 26, 3)]) for _ in range(num_examples)]
reversed_letters = "".join(reversed(string.ascii_uppercase))
destination_codes = [np.random.choice([reversed_letters[i: i+3] 
                                       if len(string.ascii_uppercase[i: i+3]) == 3 else string.ascii_uppercase[i: i+3] + np.random.choice(list(string.ascii_uppercase))
                                       for i in range(0, 26, 3)]) for _ in range(num_examples)]

def generate_random_timestamp(
        start_date: str,
        end_date: str,
        granularity: str = 'minutes', 
        size:int = num_examples
):
    max_minutes = (datetime.strptime(end_date, "%Y-%m-%d %H:%M:%S") - datetime.strptime(start_date, "%Y-%m-%d %H:%M:%S")).total_seconds()//60
    start_date = datetime.strptime(start_date, "%Y-%m-%d %H:%M:%S")
    rand_dates = [
                str(start_date + timedelta(minutes=np.random.randint(0, max_minutes)))
                for _ in range(size)
    ]

    return rand_dates

def date_obj(obj):
    if isinstance(obj, date):
        return obj.isoformat()
    raise TypeError("Type not serializable")

def generate_json():
    data = {}
    data["booking_id"] = booking_ids
    data["customer_id"] = customer_ids
    data["flight_id"] = flight_ids
    data["booking_time"] = timestamps
    data["origin"] = origin_codes
    data["destination"] = destination_codes
    data["price"] = prices
    df = pd.DataFrame(data)
    # df.to_json("mock.json", orient="records", force_ascii=False, default_handler=date_obj)
    with open(f"{path}/mock_booking_data.json", 'w') as f:
        for row in df.itertuples(index=False):
            json_data = {
                "booking_id": row.booking_id,
                "customer_id": row.customer_id,
                "flight_id": row.flight_id,
                "booking_time": row.booking_time,
                "origin": row.origin,
                "destination": row.destination,
                "price": row.price
            }
            json_str = json.dumps(json_data, ensure_ascii=False)
            f.write(json_str + "\n")

start_date = "2023-01-01 00:00:00"
end_date = "2023-01-31 23:59:59"
timestamps = sorted(generate_random_timestamp(start_date, end_date))
path = Path("data")
path.mkdir(exists_ok=True)
generate_json()

# with open('mock_data.json', 'r') as f:
#     for line in f:
#         print(json.loads(line))
