# Import Meteostat library and dependencies
from datetime import datetime
import matplotlib.pyplot as plt
from meteostat import Point, Daily, Stations
import os
import pandas as pd

import ast


def check_data(row):
    global data, _start_date, _end_date
    vancouver = Point(row['latitude'], row['longitude'])
    if len(data.index) == 0:
        data = Daily(vancouver, _start_date, _end_date)
        data = data.fetch()


def get_metostat(row):
    global data, _start_date, _end_date
    lat, lon, province = row['lat'], row['long'], row['province']

    _start_date = datetime.strptime(start_date, '%Y-%m-%d')
    _end_date = datetime.strptime(end_date, '%Y-%m-%d')
    vancouver = Point(lat, lon)
    data = Daily(vancouver, _start_date, _end_date)
    data = data.fetch()

    if len(data.index) == 0:
        stations = Stations()
        stations = stations.nearby(lat, lon)
        stations = stations.fetch(20)

        stations.apply(check_data, axis=1)
        # Print DataFrame


    data['province'] = province
    print(province)
    print(data)
    data.to_parquet(f's3/{province}_metostat.gzip', compression='gzip')
    print(f"SUCCESS !! SAVE {province}_metostat.gzip file ...\n\n")


if __name__ == "__main__":
    data, _start_date, _end_date = None, None, None
    with open('data/all_date.txt', encoding='utf-8') as f:
        data = f.read()
    d = ast.literal_eval(data)

    print("Data type after reconstruction : ", type(d))
    dict_all_date = dict(sorted(d.items()))
    start_date = list(dict_all_date.keys())[0]
    end_date = list(dict_all_date.keys())[-1]
    del d

    directory = 'station_location'
    for root, dirs, files in os.walk(directory):
        for file in files:
            if file.endswith('.xlsx'):
                print(file)
                filepath = os.path.join(root, file)
                df = pd.DataFrame(pd.read_excel(filepath))
                df.apply(get_metostat, axis=1)
