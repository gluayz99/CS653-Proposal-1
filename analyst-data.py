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

                # # df = df.groupby('date') \
                # #     .agg({'WQI': 'mean'}) \
                # #     .reset_index()
                # # df.set_index('date', inplace=True)
                df_month = df.groupby(df['date'].dt.strftime('%m'))['WQI'].mean()
                df_month.index.sort_values()
                print(df_month)
                df_month.plot(y=['WQI'], figsize=(9,6))
                plt.title(file)
                plt.savefig(f"plots/month/0-{file.split('.')[0]}_by_month.png")
                plt.show()

                df_month = df_month.sort_values()
                df_month.plot(y=['WQI'], figsize=(9, 6))
                plt.title(file)
                plt.savefig(f"plots/month/1-{file.split('.')[0]}_by_month_sorted.png")
                plt.show()

                df_year = df.groupby(df['date'].dt.strftime('%Y'))['WQI'].mean()
                df_year.index.sort_values()
                print(df_year)
                df_year.plot(y=['WQI'], figsize=(9, 6))
                plt.title(file)
                plt.savefig(f"plots/year/0-{file.split('.')[0]}_by_year.png")
                plt.show()

                df_year = df_year.sort_values()
                df_year.plot(y=['WQI'], figsize=(9, 6))
                plt.title(file)
                plt.savefig(f"plots/year/1-{file.split('.')[0]}_by_year_sorted.png")
                plt.show()

                df_week = df.groupby(df['date'].dt.strftime('%W'))['WQI'].mean()
                df_week.index.sort_values()
                print(df_week)
                df_week.plot(y=['WQI'], figsize=(9, 6))
                plt.title(file)
                plt.savefig(f"plots/week/0-{file.split('.')[0]}_by_week.png")
                plt.show()

                df_week = df_week.sort_values()
                df_week.plot(y=['WQI'], figsize=(9, 6))
                plt.title(file)
                plt.savefig(f"plots/week/1-{file.split('.')[0]}_by_week_sorted.png")
                plt.show()
