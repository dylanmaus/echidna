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
    k = {'CSN': df['CSN'][0]}

    return {**k, **r}

def sd(df):
    output = []

    df.reset_index(inplace=True)
    t = df['Index_cx'][0]
    k = df['CSN'][0]
    n = df['Rx'][0]

    # sum lower dates
    l = df[df['Start'] < t]
    if l.shape[0] > 0:
        l_sum = l['DOT'].sum()
        a = {'CSN': k, 'Rx': n, 'Start': 0, 'DOT': l_sum}
        output.append(a)

    # sum greater dates
    g = df[df['Start'] >= t]
    if g.shape[0] > 0:
        g_sum = g['DOT'].sum()
        b = {'CSN': k, 'Rx': n, 'Start': 1, 'DOT': g_sum}
        output.append(b)

    return output

def sum_duration(df):
    tmp = df.groupby('Rx').apply(sd)
    tmp.reset_index(drop=True, inplace=True)
    tmp_df = pd.DataFrame(tmp)
    tmp_df.columns = ['records']

    # list of lists
    tmp_list_of_list = tmp_df['records'].tolist()

    # list
    tmp_list = []
    for i in tmp_list_of_list:
        for j in i:
            tmp_list.append(j)

    return tmp_list

def main(args):
    df1 = read_excel(args.d1)
    df2 = read_excel(args.d2)

    # remove rows if CSN not in df1
    unqique_keys = df1['CSN'].unique()
    df2 = df2[df2['CSN'].isin(unqique_keys)]

    # sum over duration
    df2 = df2.groupby('CSN').apply(sum_duration)
    df2.reset_index(drop=True, inplace=True)
    df2 = pd.DataFrame(df2)
    df2.columns = ['records']
    df2 = df2['records'].tolist()
    tmp_list = []
    for i in df2:
        for j in i:
            tmp_list.append(j)
    df2 = pd.DataFrame(tmp_list)
    # df2.to_excel('sum_over_duration.xlsx', index=False)

    flat = df2.groupby('CSN').apply(flatten_record)
    flat.reset_index(drop=True, inplace=True)
    tmp_df = pd.DataFrame(flat)
    tmp_df.columns = ['records']
    flat_df = pd.DataFrame(tmp_df['records'].tolist())

    result = pd.merge(df1, flat_df, how='left', on=['CSN'])

    result.to_excel('flattened.xlsx', index=False)

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='transform data')
    parser.add_argument('--d1', type=str, required=True, help='data')
    parser.add_argument('--d2', type=str, required=True, help='data')

    args = parser.parse_args()

    main(args)
