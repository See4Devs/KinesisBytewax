from flask import Flask
import boto3
import re
import ast
from bytewax import Dataflow, run
app = Flask(__name__)

#------------------
# Bytewax 
#------------------
def records_input(listOfAllRecords):
    for item in listOfAllRecords:
        yield 1, item

def is_greaterThan(item):
     return float(item["total_amount"]) > 20

def is_moreThanTwoPassenger(item):
    return float(item["passenger_count"]) > 1

#Data pipeline flow
flow = Dataflow()
#filter trips greater than 20$
flow.filter(is_greaterThan)
#filter trips that has more thant 2 passengers
flow.filter(is_moreThanTwoPassenger)
flow.capture()

@app.route('/')
def hello_geek():
    return '<h1>Hello from Flask & Docker</h2>'

@app.route('/bytewax')
async def Run_Bytewax():
    #------------------
    # Reading Data from Kinesis then processing data through bytewax data flow
    #
    #------------------
    stream_name = 'bytewax-cab-trip'
    try:
            kinesis_client =  boto3.client('kinesis',
                                                                aws_access_key_id="YOUR ACCESS KEY",
                                                                aws_secret_access_key="YOU SECRET KEY",
                                                                region_name='us-east-1')
            #------------------
            # Get the shard ID.
            # In our example we defined 3 shards on aws kinesis
            #------------------
            response = kinesis_client.describe_stream(StreamName=stream_name)
            #only using 1 shard, you can improve the code to use 3 shards or more, by interating over the respons[StreamDescription][Shards] you get the available shards
            shard_id = response['StreamDescription']['Shards'][0]['ShardId']

            #---------------------------------------------------------------------------------------------
            # Get the shard iterator.
            # ShardIteratorType=AT_SEQUENCE_NUMBER|AFTER_SEQUENCE_NUMBER|TRIM_HORIZON|LATEST|AT_TIMESTAMP
            #---------------------------------------------------------------------------------------------
            response = kinesis_client.get_shard_iterator(
                StreamName=stream_name,
                ShardId=shard_id,
                ShardIteratorType='TRIM_HORIZON'
            )
            shard_iterator = response['ShardIterator']

            #-----------------------------------------------------------------
            # Get the records.
            # Run continuously
            #-----------------------------------------------------------------
            record_count = 0
            numberOfProcessedItems=0
            while True:
                listOfBatchRecords=[]
                response = kinesis_client.get_records(
                    ShardIterator=shard_iterator,
                    Limit=100
                )
                shard_iterator = response['NextShardIterator']
                records = response['Records']
                if len(records)>0:
                    for item in records:
                        newRecord= item["Data"].decode("utf8").replace("'", '"')
                        dictLine=ast.literal_eval(newRecord)
                        record_count+=1
                        listOfBatchRecords.append(dictLine)
                        print(record_count)
                #record_count += len(records)
               
                for epoch, item in run(flow, records_input(listOfBatchRecords)):
                    numberOfProcessedItems+=1
                    print(item, numberOfProcessedItems)
            return 'Hello, Docker!'
    except Exception as e:
        raise


if __name__ == "__main__":
    app.run(debug=True)
