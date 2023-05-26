import pandas as pd
import os
from datetime import datetime
import matplotlib.pyplot as plt


if __name__ == "__main__":
    list_week = []
    list_year = []
    list_month = []
    directory = 's3'
    for root, dirs, files in os.walk(directory):
        for file in files:
            if file.endswith('.parquet') and file.find('_iwis') != -1:
                print(file)
                filepath = os.path.join(root, file)
                df = pd.read_parquet(filepath)

                dam = pd.read_parquet("s3/dam_rid.parquet")
                dam['date'] = pd.to_datetime(dam['date'], format='%Y-%m-%d')
                df2 = df.merge(dam, on='date', how='left')
                del df, dam

                metostats_data = pd.read_parquet(f"s3/{file.split('_iwis.')[0]}_metostat.parquet")
                metostats_data['date'] = pd.to_datetime(metostats_data['date'], format='%Y-%m-%d')
                df2 = df2.merge(metostats_data, on='date', how='left')
                del metostats_data

                print(df2.columns)

                df2 = df2.drop(['WQI_INFO', 'province_x', 'province_y', 'region', 'stationID', 'name', 'owner', 'date', 'snow', 'wpgt', 'tsun'], axis=1)
                df2 = df2.corr()['WQI'].sort_values()
                df2 = df2.dropna()
                df2 = df2.drop(labels=['WQI'])
                print(df2)
                df2.tail(5).plot(kind="barh", figsize=(10, 10))
                plt.title(file)
                plt.savefig(f"plots/corr/{file.split('.')[0]}.png")
                plt.show()

                # df_month = df.groupby(df['date'].dt.strftime('%m'))['WQI'].mean()
                # df_month.index.sort_values()
                # df_month = df_month.to_frame().reset_index()
                # print(df_month)


