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
    # read in files to DataFrames
    final_result_dates = excel_to_df(args.f)
    mssa_dot = excel_to_df(args.g)
    print(mssa_dot.head(final_result_dates.shape[0]))
    print(mssa_dot.head(mssa_dot.shape[0]))

    # date columns
    final_result_dates["Final_Result_Date"] = pd.to_datetime(final_result_dates["Final_Result_Date"])
    mssa_dot["First_Admin"] = pd.to_datetime(mssa_dot["First_Admin"])
    mssa_dot["Last_Admin"] = pd.to_datetime(mssa_dot["Last_Admin"])

    # remove times for easier readability
    mssa_dot["First_Admin"] = mssa_dot["First_Admin"].dt.date
    mssa_dot["First_Admin"] = pd.to_datetime(mssa_dot["First_Admin"])
    mssa_dot["Last_Admin"] = mssa_dot["Last_Admin"].dt.date
    mssa_dot["Last_Admin"] = pd.to_datetime(mssa_dot["Last_Admin"])

    # keep earliest date per CSN in final result dates
    earliest_final_result_dates = final_result_dates.loc[final_result_dates.groupby("CSN")["Final_Result_Date"].idxmin()]

    # join mssa data and final result dates
    earliest_final_result_dates.rename(columns={"CSN": "PAT_ENC_CSN_ID"}, inplace=True)
    mssa_dot = mssa_dot.merge(earliest_final_result_dates)

    # remove admins that entirely occur before the final result date
    mssa_dot = mssa_dot[mssa_dot["Last_Admin"] >= mssa_dot["Final_Result_Date"]]
    mssa_dot.reset_index(inplace=True, drop=True)

    # calculate deltas between rows for each abx group to find boundaries between abx courses
    mssa_dot.sort_values(by=["PAT_ENC_CSN_ID", "ABX_Category", "First_Admin"], ascending=True, inplace=True)
    mssa_dot["abx_delta"] = (
        mssa_dot.groupby(["PAT_ENC_CSN_ID", "ABX_Category"], as_index=False)
        .apply(lambda x: x["First_Admin"].dt.day - x["Last_Admin"].dt.day.shift(1), include_groups=False)
        .reset_index(drop=True)
    )

    # assign each admin to an abx course
    mssa_dot.replace({"abx_delta": {1: 0}}, inplace=True)
    mssa_dot.fillna({"abx_delta": 1}, inplace=True)
    mssa_dot.loc[mssa_dot["abx_delta"] > 1, "abx_delta"] = 1
    mssa_dot.replace(0, np.nan, inplace=True)
    mssa_dot["abx_course"] = mssa_dot.groupby(["PAT_ENC_CSN_ID", "ABX_Category", "abx_delta"]).cumcount()
    mssa_dot.ffill(inplace=True)
    mssa_dot["abx_course"] += 1
    mssa_dot["abx_course"] = mssa_dot["abx_course"].astype(int)

    # calculate deltas between rows ignoring the specific abx to find boundaries between courses of any abx
    mssa_dot.sort_values(by=["PAT_ENC_CSN_ID", "First_Admin"], ascending=True, inplace=True)
    print(mssa_dot.head(mssa_dot.shape[0]))

    mssa_dot["any_delta"] = (
        mssa_dot.groupby(["PAT_ENC_CSN_ID"], as_index=False)
        .apply(lambda x: x["First_Admin"].dt.day - x["Last_Admin"].dt.day.shift(1), include_groups=False)
        .reset_index(drop=True)
    )
    print("post any delta")
    print(mssa_dot.head(mssa_dot.shape[0]))

    # assign each admin to a course ignoring the specific abx
    mssa_dot.replace({"any_delta": {1: 0}}, inplace=True)
    mssa_dot.fillna({"any_delta": 1}, inplace=True)
    mssa_dot.loc[mssa_dot["any_delta"] > 1, "any_delta"] = 1
    mssa_dot.replace(0, np.nan, inplace=True)
    mssa_dot["any_course"] = mssa_dot.groupby(["PAT_ENC_CSN_ID", "any_delta"]).cumcount()
    mssa_dot.ffill(inplace=True)
    mssa_dot["any_course"] += 1
    mssa_dot["any_course"] = mssa_dot["any_course"].astype(int)

    # label each abx abx course
    mssa_dot["category"] = mssa_dot["ABX_Category"] + "_" + mssa_dot["abx_course"].astype(str)
    print(mssa_dot.head(mssa_dot.shape[0]))

    # compute total days of therapy for each drug
    abx_dot = (
        mssa_dot.groupby(["PAT_ENC_CSN_ID", "ABX_Category", "abx_course"])
        .apply(lambda x: x["Last_Admin"].max() - x["First_Admin"].min(), include_groups=False)
        .reset_index(name="dot")
    )
    abx_dot["dot"] += pd.Timedelta("1 days")
    print(abx_dot.head(mssa_dot.shape[0]))

    # compute total days of therapy for any drug
    any_dot = (
        mssa_dot.groupby(["PAT_ENC_CSN_ID", "any_course"])
        .apply(lambda x: x["Last_Admin"].max() - x["First_Admin"].min(), include_groups=False)
        .reset_index(name="dot")
    )
    any_dot["dot"] += pd.Timedelta("1 days")
    print(any_dot.head(any_dot.shape[0]))

    # display each CSNs data on a single row
    first_admin = unstack_abx(mssa_dot=mssa_dot, first_or_last="First_Admin")
    last_admin = unstack_abx(mssa_dot=mssa_dot, first_or_last="Last_Admin")
    result = pd.merge(first_admin, last_admin)
    result = result[[result.columns[0]] + sorted(result.columns[1:])]

    print(result.head(15))

    # create excel file
    result.to_excel("output.xlsx", index=False)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="transform data")
    parser.add_argument("--f", type=str, required=True, help="path to file with final result dates")
    parser.add_argument("--g", type=str, required=True, help="path to file with mssa dot data")

    args = parser.parse_args()

    main(args)
