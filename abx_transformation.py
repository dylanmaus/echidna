import argparse
import pandas as pd


def excel_to_df(path: str) -> pd.DataFrame:
    file = pd.ExcelFile(path)
    df = pd.read_excel(file)

    return df


def unstack_abx(mssa_dot: pd.DataFrame, first_or_last: str) -> pd.DataFrame:
    if first_or_last == "First_Admin":
        result = mssa_dot.groupby(["PAT_ENC_CSN_ID", "ABX_Category"])[first_or_last].min().reset_index(name=first_or_last)
    elif first_or_last == "Last_Admin":
        result = mssa_dot.groupby(["PAT_ENC_CSN_ID", "ABX_Category"])[first_or_last].max().reset_index(name=first_or_last)

    result.set_index(["PAT_ENC_CSN_ID", "ABX_Category"], inplace=True)
    result = result.unstack(level=-1).rename_axis(None)
    result.reset_index(level=0, drop=False, inplace=True)
    result.columns = ["PAT_ENC_CSN_ID"] + [f"{x[1]}_{first_or_last[0]}A" for x in result.columns.tolist()[1:]]

    return result



def main(args):
    # final_result_dates = excel_to_df(args.f)
    mssa_dot = excel_to_df(args.g)
    mssa_dot["First_Admin"] = pd.to_datetime(mssa_dot["First_Admin"])
    mssa_dot["Last_Admin"] = pd.to_datetime(mssa_dot["Last_Admin"])
    mssa_dot.sort_values(by=["PAT_ENC_CSN_ID", "ABX_Category", "First_Admin"], inplace=True, ascending=True)
    mssa_dot["delta"] = mssa_dot["First_Admin"] - mssa_dot["Last_Admin"].shift(1)
    print(mssa_dot.head(10))

    first_admin = unstack_abx(mssa_dot=mssa_dot, first_or_last="First_Admin")
    last_admin = unstack_abx(mssa_dot=mssa_dot, first_or_last="Last_Admin")

    result = pd.merge(first_admin, last_admin)
    result = result[[result.columns[0]] + sorted(result.columns[1:])]
    result.replace({pd.NaT: None}, inplace=True)

    print(result.head())

    result.to_excel("output.xlsx", index=False)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="transform data")
    parser.add_argument("--f", type=str, required=True, help="path to file with final result dates")
    parser.add_argument("--g", type=str, required=True, help="path to file with mssa dot data")

    args = parser.parse_args()

    main(args)
