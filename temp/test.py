import pandas as pd


def cal_wqi(do):

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
    if not row['DO']:
        print(row)
    try:
        int(cal_wqi(row['DO']))
    except:
        print(row)

data = pd.read_parquet("s3/ดาวคะนอง_iwis.gzip")
data.apply(add_more_cols, axis=1)