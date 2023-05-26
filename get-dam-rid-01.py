import pandas as pd
import requests
import ast
from tqdm import tqdm

if __name__ == "__main__":

    with open('data/all_date.txt', encoding='utf-8') as f:
        data = f.read()
    d = ast.literal_eval(data)

    print("Data type after reconstruction : ", type(d))
    dict_all_date = dict(sorted(d.items()))
    del d

    export_dict = {
        'date': [],
        'region': []
    }

    pbar = tqdm(dict_all_date, total=len(dict_all_date.keys()), desc="PROGRESSING: ")
    for adate in pbar:
        url = f'https://app.rid.go.th/reservoir/api/dam/public/{adate}'
        response = requests.get(url)

        if response.status_code == 200:
            list_response = response.json()['data']
            for region in list_response:
                if region['region'] != 'ภาคใต้':
                    for dam in region['dam']:
                        export_dict['date'].append(adate)
                        export_dict['region'].append(region['region'])
                        for akey in dam:
                            if akey != 'id' and akey not in export_dict:
                                export_dict[akey] = [dam[akey]]
                            elif akey != 'id':
                                export_dict[akey].append(dam[akey])

        else:
            print("Unable to get response with Code : %d " % (response.status_code))

    df = pd.DataFrame.from_dict(export_dict)
    df.to_parquet(f's3/dam_rid.parquet')
    print(f"SUCCESS !! SAVE dam_rid.gz file ...\n\n")

