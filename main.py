import pandas as pd
import os
from datetime import datetime


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


def convert_str_date(row):
    dict_month_th = {
        'ม.ค.': '01',
        'ก.พ.': '02',
        'มี.ค.': '03',
        'เม.ย.': '04',
        'พ.ค.': '05',
        'มิ.ย.': '06',
        'ก.ค.': '07',
        'ส.ค.': '08',
        'ก.ย.': '09',
        'ต.ค.': '10',
        'พ.ย.': '11',
        'ธ.ค.': '12',
    }

    date_str = row['date']

    item = date_str.split(' ')
    day = int(item[0])
    month = item[1]
    year = int(month[-4:]) - 543
    month = dict_month_th[month[:-4]]

    return f"{day:02d}{month}{year}"


def check_num(row):
    if not isinstance(row['pH'], (int, float)):
        row['pH'] = None

    if not isinstance(row['DO'], (int, float)):
        row['DO'] = None

    if not isinstance(row['EC'], (int, float)):
        row['EC'] = None

    if not isinstance(row['Temp'], (int, float)):
        row['Temp'] = None

    if not isinstance(row['Turbidity'], (int, float)):
        row['Turbidity'] = None

    if not isinstance(row['Salinity'], (int, float)):
        row['Salinity'] = None

    return row


def get_date(row):
    global all_date

    date = row['date'].strftime('%Y-%m-%d')
    if date not in all_date:
        all_date[date] = None


if __name__ == "__main__":

    dict_province_name = {
        'ชัยนาท': 'chainat',
        'ดาวคะนอง': 'dowkhanong',
        'นครสวรรค์': 'nakornsawan',
        'บางไทร': 'bangsi',
        'ปากเกร็ด': 'pakket',
        'ป่าโมก': 'pamok',
        'สมุทรปราการ': 'samutprakarn',
        'สำแล': 'samray',
        'สิงห์บุรี': 'singburi',
        'อยุธยา': 'ayuttaya',
        'อ่างทอง': 'angthong'
    }

    directory = 'data'
    total_rows = 0
    total_files = 0
    dict_province = {}
    all_date = {}
    for root, dirs, files in os.walk(directory):
        total_files += len(files)
        frames = []
        for file in files:
            if file.endswith('.xls'):
                print(file)
                filepath = os.path.join(root, file)
                df = pd.DataFrame(pd.read_excel(filepath))
                df.columns = df.columns.str.replace('วันที่-เวลา', 'date')
                df.columns = df.columns.str.replace('หมายเลข', 'stationID')

                df['date'] = df.apply(convert_str_date, axis=1)
                df['date'] = pd.to_datetime(df["date"], format='%d%m%Y')

                df.apply(get_date, axis=1)

                frames.append(df)
                total_rows += len(df.index)

        if frames and files:
            province = files[0].split('-')[0][:-2]
            merge_df = pd.concat(frames)
            # print(merge_df.columns)
            merge_df = merge_df.apply(check_num, axis=1)

            merge_df.sort_values(by=['date'], inplace=True)
            merge_df.fillna(method='ffill', inplace=True)
            merge_df.fillna(method='bfill', inplace=True)
            merge_df = merge_df[merge_df['DO'] != 0]
            merge_df['province'] = province

            merge_df = merge_df.apply(add_more_cols, axis=1)
            merge_df = merge_df[merge_df['WQI'] >= 0.0]
            merge_df = merge_df[merge_df['WQI'] <= 100.0]
            print(merge_df)
            print(merge_df.groupby(merge_df.date.dt.year)['pH'].sum())

            # with pd.option_context('display.max_rows', None, 'display.max_columns', None):
            #     print(merge_df.describe())
            merge_df.to_parquet(f's3/{dict_province_name[province]}_iwis.parquet')
            print(f"SUCCESS !! SAVE {dict_province_name[province]}_iwis.parquet file ...\n\n")
            dict_province[province] = len(merge_df)

    print(f"total files: {total_files}, total rows: {total_rows}")

    # for akey in dict_province:
    #     print(akey, dict_province[akey])

    print("ALL DATE: ", len(all_date.keys()))
    with open('data/all_date.txt', 'w', encoding='utf-8') as data:
        data.write(str(all_date))
