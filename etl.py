import os
import argparse

import pandas as pd

'''
read into df
drop columns
sort
append key field
group by key
apply: create drug name, results dictionary
apply: create column, first row dictionary
apply: combine dictionaries
apply: append dictionary to list
extend lists from all files
create df from list of dictionaries
convert NAN to blank
print df to Excel
'''

class Extract:
    def __init__(self, dir, sheet_name):
        self.dir = dir
        self.sheet_name = sheet_name
        self.data = []

        self.extract()

    def read_excel(self, path, sheet_name):
        file = pd.ExcelFile(path)
        df = pd.read_excel(file, sheet_name=sheet_name)
        return df

    def extract(self):
        for root, dirs, files in os.walk(self.dir, topdown=False):
            for file in files:
                path = os.path.join(root, file)
                df = self.read_excel(path, self.sheet_name)
                self.data.append(df)


class Transform:
    def __init__(self, data):
        self.transformed_data = []
        self.sort_columns = ['MRN', 'ORDER', 'ISO. COMM']
        self.keep_columns = ['ORDER', 'LAST', 'FIRST', 'MRN', 'CDATE', 'WARD', 'SOURCE', 'SITE', 'TEST NAME', 'ORG', 'ISO. COMM', 'Drug Name', 'Drug Result']

        self.transform(data)

    def drop_columns(self, df):
        return df[df.columns.intersection(self.keep_columns)]

    def sort(self, df: pd.DataFrame):
        df.sort_values(by=self.sort_columns, inplace=True)
        df.reset_index(drop=True, inplace=True)

    def append_key_column(self, df):
        df['key'] = df['ORDER'].astype(str) + '-' + df['MRN'].astype(str) + '-' + df['ISO. COMM'].astype(str)

    def flatten_record(self, df):
        base_record = df.iloc[:, :11].to_dict(orient='records')[0]
        drug_record = dict(zip(df['Drug Name'], df['Drug Result']))
        record = {**base_record, **drug_record}
        return record

    def transform(self, data):
        for df in data:
            tmp_df = self.drop_columns(df)
            self.sort(tmp_df)
            self.append_key_column(tmp_df)
            tmp_series = tmp_df.groupby('key').apply(self.flatten_record)
            tmp_series.reset_index(drop=True, inplace=True)
            tmp_df = pd.DataFrame(tmp_series)
            tmp_df.columns = ['records']
            self.transformed_data.extend(tmp_df['records'].tolist())


def main(args):
    extract = Extract(args.data_dir, args.sheet_name)
    transfrom = Transform(extract.data)
    records = transfrom.transformed_data

    df = pd.DataFrame(records)
    df.to_excel(args.output_name, index=False)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='transform data')
    parser.add_argument('--data_dir', type=str, required=True, help='directory with data')
    parser.add_argument('--sheet_name', type=str, default='data', required=False, help='name of sheet with data')
    parser.add_argument('--output_name', type=str, default='output.xlsx', required=False, help='name of output file')

    args = parser.parse_args()

    main(args)
