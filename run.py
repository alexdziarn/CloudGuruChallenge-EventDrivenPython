import pandas as pd
from datetime import datetime
def lambda_handler():
    # create a dataframe for NYtimes US Covid data
    NyTimesUrl = "https://raw.githubusercontent.com/nytimes/covid-19-data/master/us.csv"
    df = pd.read_csv(NyTimesUrl, error_bad_lines=False)
    df = df.set_index("date")

    # create a dataframe for John Hopkins data
    JhUrl = "https://raw.githubusercontent.com/datasets/covid-19/master/data/time-series-19-covid-combined.csv"
    dl = pd.read_csv(JhUrl, error_bad_lines=False)

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

lambda_handler()