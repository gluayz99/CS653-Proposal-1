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

                df_month = df.groupby(df['date'].dt.strftime('%m'))['WQI'].mean()
                df_month.index.sort_values()

                print(type(df_month))


