import argparse
import pandas as pd


def excel_to_df(path: str) -> pd.DataFrame:
    file = pd.ExcelFile(path)
    df = pd.read_excel(file)

    return df


def unstack_abx(mssa_dot: pd.DataFrame, first_or_last: str) -> pd.DataFrame:
    result = mssa_dot.groupby(["PAT_ENC_CSN_ID", "ABX_Category"])[first_or_last].min().reset_index(name=first_or_last)
    result.set_index(["PAT_ENC_CSN_ID", "ABX_Category"], inplace=True)
    result = result.unstack(level=-1).rename_axis(None)
    result.reset_index(level=0, drop=False, inplace=True)
    result.columns = ["PAT_ENC_CSN_ID"] + [f"{x[1]}_{first_or_last[0]}A" for x in result.columns.tolist()[1:]]

    return result


def main(args):
    final_result_dates = excel_to_df(args.f)
    mssa_dot = excel_to_df(args.g)
    print(mssa_dot.head(10))

    first_admin = unstack_abx(mssa_dot=mssa_dot, first_or_last="First_Admin")
    # print(first_admin.head())
    last_admin = unstack_abx(mssa_dot=mssa_dot, first_or_last="Last_Admin")
    # print(last_admin.head())

    result = pd.merge(first_admin, last_admin)
    result = result[[result.columns[0]] + sorted(result.columns[1:])]
    print(result.head())


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="transform data")
    parser.add_argument("--f", type=str, required=True, help="path to file with final result dates")
    parser.add_argument("--g", type=str, required=True, help="path to file with mssa dot data")

    args = parser.parse_args()

    main(args)
