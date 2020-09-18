import pandas as pd
from datetime import datetime
import boto3
from io import StringIO

from boto3.dynamodb.conditions import Key

def lambda_handler(event, context):
    NyTimesUrl = "https://raw.githubusercontent.com/nytimes/covid-19-data/master/us.csv"
    JhUrl = "https://raw.githubusercontent.com/datasets/covid-19/master/data/time-series-19-covid-combined.csv"
    
    cleanDf = clean(NyTimesUrl, JhUrl)

    # convert cleanDf into csv and store in s3
    bucket = 'aws-covid-alex' # already created on S3
    csv_buffer = StringIO()
    cleanDf.to_csv(csv_buffer)
    s3_resource = boto3.resource('s3')
    key_name = 'covid_data.csv'
    s3_resource.Object(bucket, key_name).put(Body=csv_buffer.getvalue())
        
    # insert cleanDf into dynamodb
    s3 = boto3.client('s3')
    dyndb = boto3.client('dynamodb', region_name='us-east-1')
    confile = s3.get_object(Bucket=bucket, Key=key_name)
    recList = confile['Body'].read().split('\n')
    firstrecord = True
    csv_reader = csv.reader(recList, delimiter=',', quotechar='"')
    for row in csv_reader:
        if (firstrecord):
            firstrecord=False
            continue
        Date = row[0].replace(',','').replace('/','').replace(' ','').replace(':','') if row[0] else 0
        Cases = row[1].replace(',','') if row[1] else 0
        Deaths = row[2].replace(',','') if row[2] else 0
        Recovered = row[3].replace(',','') if row[2] else 0
        response = dyndb.put_item(
            TableName='US_covid',
            Item={
            'Date': {'N': str(Date)},
            'Cases': {'N': str(Cases)},
            'Deaths': {'N': str(Deaths)},
            'Recovered': {'N': str(Recovered)}
            }
        )
    return {
        "statusCode": 200,
    }
    
def clean(src1, src2):
    # create a dataframe for NYtimes US Covid data
    df = pd.read_csv(src1, error_bad_lines=False)
    df = df.set_index("date")

    # create a dataframe for John Hopkins data
    dl = pd.read_csv(src2, error_bad_lines=False)

    # delete non-us data from dl dataframe
    droppable_us = []
    for i in range(len(dl)):
        if dl["Country/Region"][i] != "US":
            droppable_us.append(i)
    dl = dl.drop(droppable_us, axis=0)


    # inner join dataframes by date
    dl.rename(columns = {"Date":"date"}, inplace=True)
    dl = dl.set_index("date")
    result = df.merge(dl, how="inner", on=["date"])

    # remove all columns besides cases, deaths, recovered
    result = result.drop(["Country/Region", "Province/State", "Lat", "Long", "Confirmed", "Deaths"], axis=1)

    # add capitalization to the column names and reset index for dataframe
    result.reset_index(inplace=True)
    result.rename(columns = {"cases":"Cases", "deaths":"Deaths", "date":"Date"}, inplace=True)


    # change the date col from string to datetime
    for i in range(len(result["Date"])):
        result["Date"][i] = datetime.strptime(result["Date"][i], "%Y-%m-%d")
        
    return result