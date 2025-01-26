from util import excel_to_df
import argparse


def main(args):
    df = excel_to_df(args.file)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='transform data')
    parser.add_argument('--file', type=str, required=True, help='path to data file')

    args = parser.parse_args()

    main(args)
