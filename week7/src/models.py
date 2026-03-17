import json
from dataclasses import dataclass
import dataclasses

import pandas as pd

# CREATE TABLE processed_events (
#     PULocationID INTEGER,
#     DOLocationID INTEGER,
#     lpep_pickup_datetime TIMESTAMP,
#     lpep_dropoff_datetime TIMESTAMP,
#     passenger_count INTEGER,
#     trip_distance DOUBLE PRECISION,
#     total_amount DOUBLE PRECISION,
#     tip_amount DOUBLE PRECISION
# );

@dataclass
class Ride:
    PULocationID: int
    DOLocationID: int
    lpep_pickup_datetime: str
    lpep_dropoff_datetime: str
    passenger_count: int
    trip_distance: float
    total_amount: float
    tip_amount: float


def ride_from_row(row):
    return Ride(
        PULocationID=int(row['PULocationID']),
        DOLocationID=int(row['DOLocationID']),
        lpep_pickup_datetime=str(row['lpep_pickup_datetime']),
        lpep_dropoff_datetime=str(row['lpep_dropoff_datetime']),
        passenger_count=1 if pd.isna(row['passenger_count']) else int(row['passenger_count']),
        trip_distance=float(row['trip_distance']),
        total_amount=float(row['total_amount']),
        tip_amount=float(row['tip_amount'])
    )

def ride_serializer(ride):
    ride_dict = dataclasses.asdict(ride)
    json_str = json.dumps(ride_dict)
    return json_str.encode('utf-8')


def ride_deserializer(data):
    json_str = data.decode('utf-8')
    ride_dict = json.loads(json_str)
    return Ride(**ride_dict)
