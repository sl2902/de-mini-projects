import numpy as np
from datetime import datetime, timedelta, date
import pandas as pd
import json
import string
from pathlib import Path

sample_data = {
"payment_id": "C111",
"booking_id": "B333",
"payment_time": "2023-01-01 23:23:23",
"payment_method": "Credit Card",
"pyment_status": "Completed"
}

num_examples = 1000
payment_ids = ["P" + str(1000 + _ + 1) for _ in range(num_examples)]
booking_ids = ["B" + str(1000 + _ + 1) for _ in range(num_examples)]
payment_types = [np.random.choice(["Credit Card", "Debit Card", "UPI", "Net Banking"]) for _ in range(num_examples)]
payment_statues = [np.random.choice(["Initiated", "In progress", "Completed", "Failed"]) for _ in range(num_examples)]


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

def read_bookings(filepath: str):
    book_dates = {}
    with open(filepath, "r") as f:
        for line in f:
            data = json.loads(line)
            book_dates[data["booking_id"]] =  data["booking_time"]
    return book_dates


def generate_json():
    data = {}
    data["payment_id"] = payment_ids
    data["booking_id"] = booking_ids
    data["payment_time"] = timestamps
    data["payment_method"] = payment_types
    data["payment_status"] = payment_statues
    df = pd.DataFrame(data)
    # df.to_json("mock.json", orient="records", force_ascii=False, default_handler=date_obj)
    book_dates = read_bookings("mock_booking_data.json")
    with open(f"{path}/mock_payment_data.json", 'w') as f:
        for row in df.itertuples(index=False):
            json_data = {
                "payment_id": row.payment_id,
                "booking_id":row.booking_id,
                "payment_time": datetime.strftime(datetime.strptime(book_dates.get(row.booking_id, "1970-01-01 23:59:59"), "%Y-%m-%d %H:%M:%S") + timedelta(minutes=np.random.randint(10, 15)), "%Y-%m-%d %H:%M:%S"),
                "payment_method": row.payment_method,
                "payment_status": row.payment_status
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
