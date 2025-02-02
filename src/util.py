import pandas as pd
import json


def excel_to_df(path: str) -> pd.DataFrame:
    file = pd.ExcelFile(path)
    df = pd.read_excel(file)
    return df


def read_json(path: str) -> None:
    with open(path, 'r') as f:
        data = json.load(f)
    return data
