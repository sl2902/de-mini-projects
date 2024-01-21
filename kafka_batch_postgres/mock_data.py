import numpy as np
from datetime import datetime, timedelta
import pandas as pd

sample_data = {
"id": "12345",
"name": "product a",
"category": "category a",
"price": 12.23,
"last_update": "2023-08-23T12:01:05Z"
}

num_examples = 1000
total_cost = 100
ids = [np.random.randint(10000, 99999) for _ in range(num_examples)]
products = ["product " + str(_ + 1) for _ in range(10)]
category = ["category " + str(_ + 1) for _ in range(5)]

def generate_random_timestamp(
        start_date: str,
        end_date: str,
        granularity: str = 'minutes', 
        size:int = num_examples
):
    max_minutes = (datetime.strptime(end_date, "%Y-%m-%d %H:%M:%S") - datetime.strptime(start_date, "%Y-%m-%d %H:%M:%S")).total_seconds() // 60
    start_date = datetime.strptime(start_date, "%Y-%m-%d %H:%M:%S")
    rand_dates = [
                start_date + timedelta(seconds=np.random.randint(0, max_minutes))
                for _ in range(size)
    ]

    return rand_dates

start_date = "2023-01-01 00:00:00"
end_date = "2023-01-01 23:59:59"
timestamps = sorted(generate_random_timestamp(start_date, end_date))
# for dt in sorted(generate_random_timestamp(start_date, end_date)):
#     print(datetime.strftime(dt, "%Y-%m-%d %H:%M:%S"))

names = [np.random.choice(products) for _ in range(num_examples)]
categories = [np.random.choice(category) for _ in range(num_examples)]
costs = [np.round(total_cost * np.random.random(), 2) for idx in range(num_examples)]
# df = pd.DataFrame(np.hstack((ad_ids, timestamps, clicks, views, costs)), 
#              columns=["ad_id", "timestamp", "clicks", "views", "cost"])

dataset = {}
dataset["id"] = ids
dataset["last_update"] = timestamps
dataset["name"] = names
dataset["category"] = categories
dataset["price"] = costs
df = pd.DataFrame(dataset)

df.to_csv("mock_data.csv", index=False)

