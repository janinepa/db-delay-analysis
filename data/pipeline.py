import pandas as pd

import os
import dotenv

import http.client
import requests

import gzip
import shutil

import sqlite3
from sqlalchemy import create_engine

from datetime import datetime

import json
from sklearn.neighbors import BallTree
from sklearn.metrics import DistanceMetric
import ast

import numpy as np

# Data engineering script to pull, massage, and store data
# Output: local datasets in /data directory (as raw csv or raw json and processed SQLite databases)

engine = create_engine('sqlite:///amse.db', echo=False)


def parse_date(date_str, raw=False):
    if date_str == 'nan':
        return float('nan')
    date = date_str[:6]
    time = date_str[6:10]

    year = int(date[:2]) + 2000
    month = int(date[2:4])
    day = int(date[4:6])
    hour = int(time[:2])
    minute = int(time[2:4])

    d = datetime(year, month, day, hour, minute, second=0)

    if raw:
        return d.timestamp()
    else:
        return d.strftime("%d.%m.%Y, %H:%M")


def get_data_from_db(table):
    engine = create_engine('sqlite:///amse.db', echo=False)

    with engine.connect() as conn, conn.begin():
        data = pd.read_sql_table(table, conn)

    return data

###################################
### Datasource 1: DB timetables ###
###################################


def get_trainstations(headers):
    # connect to API
    conn = http.client.HTTPSConnection("apis.deutschebahn.com")

    # get all Stations
    conn.request(
        "GET", "/db-api-marketplace/apis/timetables/v1/station/*", headers=headers)

    res = conn.getresponse()

    if (res.status != 200):
        print("HTTP Error", res.status, res.reason)
    else:
        data = res.read()
        train_stations = pd.read_xml(data)

        # Filter for main stations
        substrings = ["Hbf"]
        mask = train_stations["name"].str.contains("|".join(substrings))
        train_stations = train_stations[mask]

        return train_stations


def get_timetables(headers, train_stations):
    train_stations_list = list(train_stations.eva)

    conn = http.client.HTTPSConnection("apis.deutschebahn.com")
    timetable_table = pd.DataFrame()

    # get timetable data from stations
    for i in train_stations_list:
        conn.request(
            "GET", "/db-api-marketplace/apis/timetables/v1/fchg/{}".format(i), headers=headers)

        res = conn.getresponse()

        if (res.status != 200):
            print("HTTP Error", res.status, res.reason)
        else:
            data = res.read()
            # filter for departure data
            if 'dp' not in data.decode("utf-8"):
                print('dp not in data')
            else:
                timetables = pd.read_xml(
                    data.decode("utf-8"), xpath=".//s//dp")

                # filter for changed time and planned time
                if 'ct' in timetables.columns:
                    timetables[['ct']] = timetables[['ct']].astype(str)
                    timetables['ct'] = timetables['ct'].apply(parse_date)
                else:
                    timetables['ct'] = float('nan')

                if 'pt' in timetables.columns:
                    timetables[['pt']] = timetables[['pt']].astype(str)
                    timetables['pt'] = timetables['pt'].apply(parse_date)
                else:
                    timetables['pt'] = float('nan')

                timetables['eva'] = i

                timetable = pd.DataFrame(timetables[['eva', 'ct', 'pt']])
                timetable = timetable.dropna(subset=['pt'])

                timetable_table = pd.concat([timetable_table, timetable])

    return timetable_table

#################################
##### Datasource 2: Weather #####
#################################


def get_weather_station():
    # get all stations
    url = "https://bulk.meteostat.net/v2/stations/lite.json.gz"
    name = "weather_stations"

    res = requests.get(url)

    if (res.status_code != 200):
        print("HTTP Error", res.status_code, res.reason)
    else:
        with open("./temp/"+name+".json.gz", "wb") as file:
            file.write(res.content)

        with gzip.open("./temp/"+name+".json.gz", 'rb') as f_in:
            with open("./temp/"+name+".json", 'wb') as f_out:
                shutil.copyfileobj(f_in, f_out)

        data = pd.read_json("./temp/"+name+".json")
        data.to_csv("./temp/"+name+".csv")
        weather_stations = pd.read_csv("./temp/"+name+".csv")
        weather_stations = weather_stations[weather_stations['country'] == 'DE']

    return weather_stations


def get_match_table_update(weather_stations, geo_data):
    weather_stations['location'] = weather_stations['location'].apply(
        ast.literal_eval)
    weather_stations['longitude'] = weather_stations['location'].apply(
        lambda x: x['longitude'])
    weather_stations['latitude'] = weather_stations['location'].apply(
        lambda x: x['latitude'])

    # from sklearn.neighbors import BallTree, DistanceMetric
    w = weather_stations[['id', 'longitude',	'latitude', 'name']]

    #  DF1
    coords = np.radians(w[['latitude', 'longitude']])
    dist = DistanceMetric.get_metric('haversine')
    tree = BallTree(coords, metric=dist)

    # DF2
    coords = np.radians(geo_data[['latitude', 'longitude']])
    distances, indices = tree.query(coords, k=1)
    geo_data['name_train'] = w['name'].iloc[indices.flatten()].values
    geo_data['longitude_matched'] = w['longitude'].iloc[indices.flatten()
                                                        ].values
    geo_data['latitude_matched'] = w['latitude'].iloc[indices.flatten()].values
    geo_data['id'] = w['id'].iloc[indices.flatten()].values

    geo_data['Distance'] = distances.flatten()
    return geo_data


def get_train_station_geo_data(headers, train_stations):

    geo_data_dataframe = pd.DataFrame()

    for i in list(train_stations.eva):
        print(i)
        conn = http.client.HTTPSConnection("apis.deutschebahn.com")

        url = "/db-api-marketplace/apis/station-data/v2/stations?eva={}".format(
            i)

        # get all Stations
        conn.request(
            "GET", url, headers=headers)

        res = conn.getresponse()

        geo_data = pd.DataFrame()

        if (res.status != 200):
            print("HTTP Error", res.status, res.reason)
        else:
            data = res.read()
            parsed_data = json.loads(data)
            df = pd.DataFrame(parsed_data['result'])
            df['geographicCoordinates'] = df['evaNumbers'].apply(
                lambda x: x[0]['geographicCoordinates'])
            geo_data['name'] = df['name']
            geo_data['evaNumbers'] = df['evaNumbers'].apply(
                lambda x: x[0]['number'])

            geo_data['latitude'] = df['geographicCoordinates'].apply(
                lambda x: x['coordinates'][1])
            geo_data['longitude'] = df['geographicCoordinates'].apply(
                lambda x: x['coordinates'][0])
            geo_data_dataframe = pd.concat([geo_data_dataframe, geo_data])

    return geo_data_dataframe


def get_weather_data(match_table):
    weather_dataframe = pd.DataFrame()
    for i in list(match_table.id):
        print(i)

        url = "https://bulk.meteostat.net/v2/hourly/{}.csv.gz".format(i)
        # url = "https://bulk.meteostat.net/v2/daily/{}.csv.gz".format(i)

        name = i

        res = requests.get(url)

        if (res.status_code != 200):
            print("HTTP Error", res.status_code, res.reason)
        else:
            with open("./temp/"+name+".csv.gz", "wb") as file:
                file.write(res.content)

            with gzip.open("./temp/"+name+".csv.gz", 'rb') as f_in:
                with open("./temp/"+name+".csv", 'wb') as f_out:
                    shutil.copyfileobj(f_in, f_out)

            colnames = ['date', 'hour', 'temp', 'dwpt', 'rhum', 'prcp',
                        'snow', 'wdir', 'wspd', 'wpgt', 'pres', 'tsun', 'coco']
            # colnames = ['date', 'tavg', 'tmin', 'tmax', 'prcp', 'snow',
            #            'wdir', 'wspd', 'wpgt', 'pres', 'tsun']
            weather = pd.read_csv("./temp/"+name+".csv",
                                  names=colnames, header=None)
            weather['station'] = i
            weather = weather[weather.date > '2023-01-01']
            weather_dataframe = pd.concat([weather_dataframe, weather])

    return weather_dataframe


def load(data, table, db):
    conn = sqlite3.connect(db)
    data.to_sql(table, conn, if_exists='replace', index=False)
    conn.close()


if __name__ == "__main__":
    # get DB API key
    dotenv.load_dotenv()
    APIKey = os.getenv('APIKey')
    ClientID = os.getenv('ClientID')
    headers = {
        'DB-Client-Id': ClientID,
        'DB-Api-Key': APIKey,
        'accept': "application/xml"
    }

    train_stations = get_trainstations(headers)
    load(train_stations, 'train_stations', 'amse.sqlite')

    now = datetime.now()
    name = 'timetables'+now.strftime("%m-%d-%H")
    time_tables = get_timetables(headers, train_stations)
    time_tables.to_csv(name+'.csv')
    load(time_tables, name, 'amse.sqlite')

    headers = {
        'DB-Client-Id': ClientID,
        'DB-Api-Key': APIKey,
        'accept': "application/json"
    }
    #geo_data = get_train_station_geo_data(headers, train_stations)
    #load(geo_data, 'geo_data_train_stations', 'amse.sqlite')

    #weather_stations = get_weather_station()
    #load(weather_stations, 'weather_stations', 'amse.sqlite')

    #match_table = get_match_table_update(weather_stations, geo_data)
    #load(match_table, 'match_table', 'amse.sqlite')

    #weather_data = get_weather_data(match_table)
    #load(weather_data, 'weather', 'amse.sqlite')
