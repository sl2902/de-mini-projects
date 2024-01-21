import numpy as np
from datetime import datetime, timedelta, date
import pandas as pd
from sample_diseases import *
import json
from pathlib import Path

sample_data = {
"patient_id": "P12345",
"hospital_name": "H500",
"diagnosis_code": "c100",
"disease": "Asthma",
"diagnosis_date": "2023-08-23",
"treatment": "treatment",
"doctor": "dr. a"
}

num_examples = 100
ids = ["P" + str(1200 + _ + 1) for _ in range(num_examples)]
hospitals = ["H" + str(500 + _ + 1) for _ in range(10)]
diagnosis_codes = [code for code in sample_diseases.keys()]

def generate_random_timestamp(
        start_date: str,
        end_date: str,
        granularity: str = 'minutes', 
        size:int = num_examples
):
    max_days = (datetime.strptime(end_date, "%Y-%m-%d") - datetime.strptime(start_date, "%Y-%m-%d")).days
    start_date = datetime.strptime(start_date, "%Y-%m-%d").date()
    rand_dates = [
                str(start_date + timedelta(days=np.random.randint(0, max_days)))
                for _ in range(size)
    ]

    return rand_dates

def date_obj(obj):
    if isinstance(obj, date):
        return obj.isoformat()
    raise TypeError("Type not serializable")

def generate_json():
    categories = [np.random.choice(diagnosis_codes) for _ in range(num_examples)]
    # hospital_name = [np.random.choice(hospitals) for _ in range(num_examples)]
    # disease = [sample_diseases[code][0] for code in categories]
    # treatment = [sample_diseases[code][1] for code in categories]
    # doctor = [sample_diseases[code][1] for code in categories]
    data = {}
    data["patient_id"] = ids
    data["hospital_id"] = [np.random.choice(hospitals) for _ in range(num_examples)]
    data["diagnosis_code"] =  [np.random.choice(diagnosis_codes) for _ in range(num_examples)]
    data["disease"] = [sample_diseases[code][0] for code in data["diagnosis_code"]]
    data["diagnosis_date"] = timestamps
    data["treatment"] = [sample_diseases[code][1] for code in data["diagnosis_code"]]
    data["doctor"] = [sample_diseases[code][2] for code in data["diagnosis_code"]]
    df = pd.DataFrame(data)
    # df.to_json("mock.json", orient="records", force_ascii=False, default_handler=date_obj)
    with open(f"{path}/mock_data.json", 'w') as f:
        for row in df.itertuples(index=False):
            json_data = {
                "patient_id": row.patient_id,
                "hospital_id": row.hospital_id,
                "diagnosis_code": row.diagnosis_code,
                "disease": row.disease,
                "diagnosis_date": row.diagnosis_date,
                "treatment": row.treatment,
                "doctor": row.doctor
            }
            json_str = json.dumps(json_data, ensure_ascii=False)
            f.write(json_str + "\n")

start_date = "2023-01-01"
end_date = "2023-12-31"
timestamps = sorted(generate_random_timestamp(start_date, end_date))
path = Path("data")
path.mkdir(exists_ok=True)
generate_json()

# with open('mock_data.json', 'r') as f:
#     for line in f:
#         print(json.loads(line))
