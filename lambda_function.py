import pandas as pd
from datetime import datetime
import boto3

from boto3.dynamodb.conditions import Key

def lambda_handler(event, context):
    NyTimesUrl = "https://raw.githubusercontent.com/nytimes/covid-19-data/master/us.csv"
    JhUrl = "https://raw.githubusercontent.com/datasets/covid-19/master/data/time-series-19-covid-combined.csv"
    
    cleanDf = clean(NyTimesUrl, JhUrl)

    # initialize database
    client = boto3.resource('dynamodb', region_name='us-east-1')
    table = client.Table('US_covid')
    
    #PROBLEM
    for i in range(len(cleanDf["Date"])):
        dfdate = cleanDf["Date"][i].strftime('%Y-%m-%d')
        response = table.get_item(Key="Date":dfdate)
        if dfdate not in response:
            table.put_item(Item={
                "Date":dfdate,
                "Cases":cleanDf["Cases"][i],
                "Deaths":cleanDf["Deaths"][i],
                "Recovered":cleanDf["Recovered"][i]
            })
    
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