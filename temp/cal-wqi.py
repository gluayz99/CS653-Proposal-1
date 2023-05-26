import pandas as pd
import os


def cal_wqi(do):
    do = round(do, 1)
    if do >= 0.0 and do <= 4.0:
        return (15.25 * do) + 0.1667
    elif do >= 4.1 and do <= 6.0:
        return (5 * do + 41)
    elif do >= 6.1 and do <= 8.4:
        return (12.083 * do) - 1.5
    elif do >= 8.5 and do <= 8.9:
        return (-78 * do) + 755.2
    elif do >= 9.0 and do <= 11.2:
        return (-13.043 * do) + 177.09
    elif do >= 11.3:
        return (-7.561 * do) + 115.68

    return None


def add_more_cols(row):
    row['WQI'] = int(cal_wqi(row['DO']))

    if row['WQI'] >= 91:
        row['WQI_INFO'] = 'ดีมาก'
    elif row['WQI'] >= 71:
        row['WQI_INFO'] = 'ดี'
    elif row['WQI'] >= 61:
        row['WQI_INFO'] = 'พอใช้'
    elif row['WQI'] >= 31:
        row['WQI_INFO'] = 'เสื่อมโทรม'
    elif row['WQI'] >= 0:
        row['WQI_INFO'] = 'เสื่อมโทรมมาก'

    return row


if __name__ == "__main__":
    directory = 's3'
    for root, dirs, files in os.walk(directory):
        for file in files:
            if file.endswith('.gzip') and file.find('_iwis') != -1:
                print(file)
                filepath = os.path.join(root, file)
                data = pd.read_parquet(filepath)
                data = data.apply(add_more_cols, axis=1)
                print(data.columns)

                data.to_parquet(filepath, compression='gzip')
                print(f"SUCCESS !! SAVE {filepath} file ...\n\n")
