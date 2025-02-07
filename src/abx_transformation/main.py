from ..util import excel_to_df
import argparse
import pandas as pd


def main(args):
    final_result_dates = excel_to_df(args.f)
    print(final_result_dates.head())
    mssa_dot = excel_to_df(args.g)
    # print(mssa_dot.head())

    mssa_dot.groupby(["PAT_ENC_CSN_ID", "ABX_Category"])


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="transform data")
    parser.add_argument("--f", type=str, required=True, help="path to file with final result dates")
    parser.add_argument("--g", type=str, required=True, help="path to file with mssa dot data")

    args = parser.parse_args()

    main(args)
