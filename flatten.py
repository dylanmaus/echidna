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

def main(args):
    df1 = read_excel(args.d1)
    df2 = read_excel(args.d2)

    # remove rows if key not in df1
    unqique_keys = df1['key'].unique()
    df2 = df2[df2['key'].isin(unqique_keys)]

    # get max row count from df2
    max_rows = max(df2.groupby('key').apply(lambda x: x.shape[0]))
    extra_cols = df2.columns[1:].to_list()
    f = col_list(extra_cols, max_rows)


    import pdb
    pdb.set_trace()

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='transform data')
    parser.add_argument('--d1', type=str, required=True, help='data')
    parser.add_argument('--d2', type=str, required=True, help='data')

    args = parser.parse_args()

    main(args)
