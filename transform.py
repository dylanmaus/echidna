import os
import argparse

import pandas as pd


def read_excel(path, sheet_name):
    file = pd.ExcelFile(path)
    df = pd.read_excel(file, sheet_name=sheet_name)
    return df

def get_all_files(dir):
    file_paths = []
    for root, dirs, files in os.walk(dir, topdown=False):
        for file in files:
            file_paths.append(os.path.join(root, file))
    return file_paths

def get_unique_names(df_list: list[pd.DataFrame], column_name: str):
    names = []
    for df in df_list:
        names.extend(df[column_name].tolist())
    names = pd.unique(names).tolist()
    return names

def main(args):
    column_names = ['ORDER', 'LAST', 'FIRST', 'MRN', 'CDATE', 'WARD', 'SOURCE', 'SITE', 'TEST NAME', 'ORG', 'ISO. COMM', 'Drug Name', 'Drug Result']

    paths = get_all_files(args.data_dir)
    
    df_list = [read_excel(path, args.sheet_name) for path in paths]

    column_names.extend(get_unique_names(df_list))

    output_df = pd.DataFrame(columns=column_names)
    



if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='transform data')
    parser.add_argument('--data_dir', type=str, required=True, help='directory with data')
    parser.add_argument('--sheet_name', type=str, default='data', required=False, help='name of sheet with data')
    parser.add_argument('--output_name', type=str, default='output.xlsx', required=False, help='name of output file')

    args = parser.parse_args()

    main(args)
