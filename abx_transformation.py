import argparse
import pandas as pd
import numpy as np


def excel_to_df(path: str) -> pd.DataFrame:
    file = pd.ExcelFile(path)
    df = pd.read_excel(file)

    return df


def unstack_abx(mssa_dot: pd.DataFrame, first_or_last: str) -> pd.DataFrame:
    if first_or_last == "First_Admin":
        result = mssa_dot.groupby(["PAT_ENC_CSN_ID", "category"])[first_or_last].min().reset_index(name=first_or_last)
    elif first_or_last == "Last_Admin":
        result = mssa_dot.groupby(["PAT_ENC_CSN_ID", "category"])[first_or_last].max().reset_index(name=first_or_last)

    result.set_index(["PAT_ENC_CSN_ID", "category"], inplace=True)
    result = result.unstack(level=-1).rename_axis(None)
    result.reset_index(level=0, drop=False, inplace=True)
    result.columns = ["PAT_ENC_CSN_ID"] + [f"{x[1]}_{first_or_last[0]}A" for x in result.columns.tolist()[1:]]

    return result


def main(args):
    # final_result_dates = excel_to_df(args.f)
    mssa_dot = excel_to_df(args.g)
    mssa_dot["First_Admin"] = pd.to_datetime(mssa_dot["First_Admin"])
    mssa_dot["First_Admin"] = mssa_dot["First_Admin"].dt.date
    mssa_dot["First_Admin"] = pd.to_datetime(mssa_dot["First_Admin"])
    mssa_dot["Last_Admin"] = pd.to_datetime(mssa_dot["Last_Admin"])
    mssa_dot["Last_Admin"] = mssa_dot["Last_Admin"].dt.date
    mssa_dot["Last_Admin"] = pd.to_datetime(mssa_dot["Last_Admin"])
    print(mssa_dot.head(14))

    mssa_dot["delta"] = (
        mssa_dot.groupby(["PAT_ENC_CSN_ID", "ABX_Category"], as_index=False)
        .apply(lambda x: x["First_Admin"].dt.day - x["Last_Admin"].dt.day.shift(1), include_groups=False)
        .reset_index(drop=True)
    )
    mssa_dot.replace({"delta": {1: 0}}, inplace=True)
    mssa_dot.fillna({"delta": 1}, inplace=True)
    mssa_dot.loc[mssa_dot["delta"] > 1, "delta"] = 1
    mssa_dot.replace(0, np.nan, inplace=True)

    mssa_dot["course"] = mssa_dot.groupby(["PAT_ENC_CSN_ID", "ABX_Category", "delta"]).cumcount()

    mssa_dot.ffill(inplace=True)

    mssa_dot["course"] += 1
    mssa_dot["course"] = mssa_dot["course"].astype(int)
    mssa_dot["category"] = mssa_dot["ABX_Category"] + "_" + mssa_dot["course"].astype(str)

    # print(mssa_dot.head(15))

    first_admin = unstack_abx(mssa_dot=mssa_dot, first_or_last="First_Admin")
    last_admin = unstack_abx(mssa_dot=mssa_dot, first_or_last="Last_Admin")

    result = pd.merge(first_admin, last_admin)
    result = result[[result.columns[0]] + sorted(result.columns[1:])]

    print(result.head(15))

    result.to_excel("output.xlsx", index=False)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="transform data")
    parser.add_argument("--f", type=str, required=True, help="path to file with final result dates")
    parser.add_argument("--g", type=str, required=True, help="path to file with mssa dot data")

    args = parser.parse_args()

    main(args)
