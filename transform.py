import os
import argparse

import pandas as pd


class Extract:
    def __init__(self, dir, sheet_name):
        self.dir = dir
        self.sheet_name = sheet_name
        self.sort_columns = ['MRN', 'ORDER', 'ISO. COMM']
        self.keep_columns = ['ORDER', 'LAST', 'FIRST', 'MRN', 'CDATE', 'WARD', 'SOURCE', 'SITE', 'TEST NAME', 'ORG', 'ISO. COMM', 'Drug Name', 'Drug Result']
        self.data = []

        self.extract()

    def read_excel(self, path, sheet_name):
        file = pd.ExcelFile(path)
        df = pd.read_excel(file, sheet_name=sheet_name)
        return df

    def drop_columns(self, df):
        return df[df.columns.intersection(self.keep_columns)]

    def sort(self, df: pd.DataFrame):
        df.sort_values(by=self.sort_columns, inplace=True)

    def extract(self):
        for root, dirs, files in os.walk(self.dir, topdown=False):
            for file in files:
                path = os.path.join(root, file)
                df = self.read_excel(path, self.sheet_name)
                df = self.drop_columns(df)
                self.sort(df)
                self.data.append(df)


def get_unique_names(df_list: list[pd.DataFrame], column_name: str):
    names = []
    for df in df_list:
        names.extend(df[column_name].tolist())
    names = pd.unique(names).tolist()
    return names

def main(args):
    column_names = ['ORDER', 'LAST', 'FIRST', 'MRN', 'CDATE', 'WARD', 'SOURCE', 'SITE', 'TEST NAME', 'ORG', 'ISO. COMM']

    extract = Extract(args.data_dir, args.sheet_name)


    # output_df = pd.DataFrame(columns=column_names)
    

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='transform data')
    parser.add_argument('--data_dir', type=str, required=True, help='directory with data')
    parser.add_argument('--sheet_name', type=str, default='data', required=False, help='name of sheet with data')
    parser.add_argument('--output_name', type=str, default='output.xlsx', required=False, help='name of output file')

    args = parser.parse_args()

    main(args)
