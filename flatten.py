import argparse
import pandas as pd


def read_excel(path):
    file = pd.ExcelFile(path)
    df = pd.read_excel(file)
    return df

def col_list(base_cols, k):
    cols = []
    for i in range(1, k+1):
        for c in base_cols:
            cols.append(f'{c}-{str(i)}')
    return cols

def flatten_record(df):
    # list of dictionaries
    v = df.iloc[:, 1:].to_dict(orient='records')
    # list of lists
    v = [list(x.values()) for x in v]
    # list
    v = [a for b in v for a in b]

    # fields
    h = df.columns[1:].to_list()
    h = col_list(h, len(v))

    # flattened record
    r = dict(zip(h, v))

    # include key field
    df.reset_index(inplace=True)
    k = {'key': df['key'][0]}

    return {**k, **r}

def main(args):
    df1 = read_excel(args.d1)
    df2 = read_excel(args.d2)

    # remove rows if key not in df1
    unqique_keys = df1['key'].unique()
    df2 = df2[df2['key'].isin(unqique_keys)]

    flat = df2.groupby('key').apply(flatten_record)
    flat.reset_index(drop=True, inplace=True)
    tmp_df = pd.DataFrame(flat)
    tmp_df.columns = ['records']
    flat_df = pd.DataFrame(tmp_df['records'].tolist())

    result = pd.merge(df1, flat_df, how='left', on=['key'])

    result.to_excel('flattened.xlsx', index=False)

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='transform data')
    parser.add_argument('--d1', type=str, required=True, help='data')
    parser.add_argument('--d2', type=str, required=True, help='data')

    args = parser.parse_args()

    main(args)
