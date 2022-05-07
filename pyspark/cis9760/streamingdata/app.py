import json
import boto3
import random
import datetime
from time import sleep

kinesis = boto3.client('kinesis', "us-east-2")
def getReferrer():
    data = {}
    now = datetime.datetime.now()
    str_now = now.isoformat()
    data['EVENT_TIME'] = str_now
    data['TICKER'] = random.choice(['AAPL', 'AMZN', 'MSFT', 'INTC', 'TBV'])
    price = random.random() * 100
    data['PRICE'] = round(price, 2)
    return data

while True:
    data = json.dumps(getReferrer())+"\n"
    print(data)
    kinesis.put_record(
                StreamName="kinesis-datastream-exercise",
                Data=data,
                PartitionKey="partitionkey")
    sleep(1)
