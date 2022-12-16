import os
import argparse

import pandas as pd


class Extract:
    def __init__(self, dir, sheet_name):
        self.dir = dir
        self.sheet_name = sheet_name
        self.sort_columns = ['MRN', 'ORDER', 'ISO. COMM']
        self.keep_columns = ['ORDER', 'LAST', 'FIRST', 'MRN', 'CDATE', 'WARD', 'SOURCE', 'SITE', 'TEST NAME', 'ORG', 'ISO. COMM', 'Drug Name', 'Drug Result']
        self.drug_column = 'Drug Name'
        self.drug_names = []
        self.data = []

        self.extract()

    def read_excel(self, path, sheet_name):
        file = pd.ExcelFile(path)
        df = pd.read_excel(file, sheet_name=sheet_name)
        return df

    def get_unique_names(self, df: pd.DataFrame):
        self.unique_names.extend(df[self.unique_column].unique().tolist)

    def drop_columns(self, df):
        return df[df.columns.intersection(self.keep_columns)]

    def sort(self, df: pd.DataFrame):
        df.sort_values(by=self.sort_columns, inplace=True)

    def extract(self):
        for root, dirs, files in os.walk(self.dir, topdown=False):
            for file in files:
                path = os.path.join(root, file)
                df = self.read_excel(path, self.sheet_name)
                self.drug_names.extend(df[self.drug_column].tolist())
                df = self.drop_columns(df)
                self.sort(df)
                df.reset_index(drop=True, inplace=True)
                self.data.append(df)


class Transform:
    def __init__(self, df_list):
        self.df_list = df_list

        self.transform()

    def append_key_column(self, df):
        df['key'] = df['ORDER'].astype(str) + '-' + df['MRN'].astype(str) + '-' + df['ISO. COMM'].astype(str)

    def transform(self):
        for df in self.df_list:
            self.append_key_column(df)

def flatten_record(df):
    base_record = df.to_dict(orient='records')[0]
    drug_record = dict(zip(df['Drug Name'], df['Drug Result']))
    record = {**base_record, **drug_record}
    return record

def main(args):
    column_names = ['ORDER', 'LAST', 'FIRST', 'MRN', 'CDATE', 'WARD', 'SOURCE', 'SITE', 'TEST NAME', 'ORG', 'ISO. COMM']

    extract = Extract(args.data_dir, args.sheet_name)
    transform = Transform(extract.data)

    drug_names = pd.unique(extract.drug_names).tolist()

    import pdb
    pdb.set_trace()



    # output_df = pd.DataFrame(columns=column_names)
    

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='transform data')
    parser.add_argument('--data_dir', type=str, required=True, help='directory with data')
    parser.add_argument('--sheet_name', type=str, default='data', required=False, help='name of sheet with data')
    parser.add_argument('--output_name', type=str, default='output.xlsx', required=False, help='name of output file')

    args = parser.parse_args()

    main(args)
