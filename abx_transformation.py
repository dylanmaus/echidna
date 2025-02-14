import argparse
from datetime import timedelta

import pandas as pd


class Graph:
    """Create graph in adjacency list form"""

    def __init__(self):
        self.nodes: dict = {}

    def add_node(self, name: str) -> None:
        if name not in self.nodes:
            self.nodes[name] = []

    def add_neighbor(self, name: str, neighbor: str) -> None:
        if neighbor not in self.nodes[name]:
            self.nodes[name].append(neighbor)


def build_graph(data: list[tuple]) -> Graph:
    graph = Graph()

    for s, d in data:
        graph.add_node(s)
        graph.add_node(d)
        graph.add_neighbor(s, d)
        graph.add_neighbor(d, s)

    return graph


def dfs_iterative(graph: Graph) -> list:
    explored = []
    components = []

    for node in list(graph.nodes):
        if node in explored:
            continue

        stack = []
        stack.append(node)

        subgraph = []

        while True:
            if len(stack) == 0:
                break

            next_node = stack.pop()

            if next_node in explored:
                continue

            subgraph.append(next_node)
            explored.append(next_node)

            for neighbor in graph.nodes[next_node]:
                if neighbor in explored:
                    continue
                stack.append(neighbor)

        components.append(subgraph)

    return components


def expand_dates(df: pd.DataFrame):
    start_dates = df["First_Admin"].dt.date.tolist()
    end_dates = df["Last_Admin"].dt.date.tolist()
    pairs = list(zip(start_dates, end_dates))

    # add connecting date pairs
    for date in start_dates:
        next_day = date + timedelta(days=1)
        if next_day in end_dates:
            pairs.append((date, next_day))
        previous_day = date - timedelta(days=1)
        if previous_day in end_dates:
            pairs.append((previous_day, date))

    expanded = []
    for start_date, end_date in pairs:
        if start_date == end_date:
            expanded.append((start_date, end_date))
        else:
            current_date = start_date
            while True:
                if current_date == end_date:
                    break
                next_date = current_date + timedelta(days=1)
                expanded.append((current_date, next_date))
                current_date = next_date

    # remove duplicates
    return list(set(expanded))


def assign_courses(df: pd.DataFrame):
    expanded_dates = expand_dates(df=df)

    graph = build_graph(data=expanded_dates)
    components = dfs_iterative(graph=graph)

    components.sort(key=lambda x: min(x))

    output = []
    for i, component in enumerate(components):
        first_admin = min(component)
        last_admin = max(component)
        dot = last_admin - first_admin + timedelta(days=1)
        output.append({"First_Admin": first_admin, "Last_Admin": last_admin, "Course": i + 1, "DOT": dot})

    return pd.DataFrame(data=output)


def excel_to_df(path: str) -> pd.DataFrame:
    file = pd.ExcelFile(path)
    df = pd.read_excel(file)

    return df


def unstack_abx(df: pd.DataFrame, first_or_last: str) -> pd.DataFrame:
    result = df.set_index(["PAT_ENC_CSN_ID", "Category"])
    result = result[[first_or_last]]
    result = result.unstack(level=-1).rename_axis(None)
    result.reset_index(level=0, drop=False, inplace=True)
    result.columns = ["PAT_ENC_CSN_ID"] + [f"{x[1]}_{first_or_last[0]}A" for x in result.columns.tolist()[1:]]

    return result


def main(args):
    # read in files to DataFrames
    final_result_dates = excel_to_df(args.f)
    mssa_dot = excel_to_df(args.g)

    # date columns
    final_result_dates["Final_Result_Date"] = pd.to_datetime(final_result_dates["Final_Result_Date"])
    mssa_dot["First_Admin"] = pd.to_datetime(mssa_dot["First_Admin"])
    mssa_dot["Last_Admin"] = pd.to_datetime(mssa_dot["Last_Admin"])

    # keep earliest date per CSN in final result dates
    earliest_final_result_dates = final_result_dates.loc[final_result_dates.groupby("CSN")["Final_Result_Date"].idxmin()]

    # join mssa data and final result dates
    earliest_final_result_dates.rename(columns={"CSN": "PAT_ENC_CSN_ID"}, inplace=True)
    mssa_dot = mssa_dot.merge(earliest_final_result_dates)

    # remove admins that entirely occur before the final result date
    mssa_dot = mssa_dot[mssa_dot["Last_Admin"] >= mssa_dot["Final_Result_Date"]]
    mssa_dot.reset_index(inplace=True, drop=True)

    # remove times for easier readability
    mssa_dot["First_Admin"] = mssa_dot["First_Admin"].dt.date
    mssa_dot["First_Admin"] = pd.to_datetime(mssa_dot["First_Admin"])
    mssa_dot["Last_Admin"] = mssa_dot["Last_Admin"].dt.date
    mssa_dot["Last_Admin"] = pd.to_datetime(mssa_dot["Last_Admin"])
    mssa_dot["Final_Result_Date"] = mssa_dot["Final_Result_Date"].dt.date
    mssa_dot["Final_Result_Date"] = pd.to_datetime(mssa_dot["Final_Result_Date"])

    print(mssa_dot.head(mssa_dot.shape[0]))

    # group each admin of abx into courses
    abx_courses = mssa_dot.groupby(["PAT_ENC_CSN_ID", "ABX_Category"]).apply(assign_courses, include_groups=False).reset_index()
    abx_courses["DOT"] = abx_courses["DOT"].dt.days

    # label each course of abx
    abx_courses["Category"] = abx_courses["ABX_Category"] + "_" + abx_courses["Course"].astype(str)

    # find total days of therapy for each drug
    abx_dot = abx_courses.groupby(["PAT_ENC_CSN_ID", "ABX_Category"])["DOT"].sum().reset_index(name="ABX_DOT")
    print(abx_dot.head())

    # group contiguous admins of any drug by course
    any_abx_courses = mssa_dot.groupby(["PAT_ENC_CSN_ID"]).apply(assign_courses, include_groups=False).reset_index()
    any_abx_courses["DOT"] = any_abx_courses["DOT"].dt.days

    # find total days of therapy
    total_dot = any_abx_courses.groupby("PAT_ENC_CSN_ID")["DOT"].sum().reset_index(name="Total_DOT")
    print(total_dot.head())

    # display each PTs data on a single row
    first_admin = unstack_abx(df=abx_courses, first_or_last="First_Admin")
    last_admin = unstack_abx(df=abx_courses, first_or_last="Last_Admin")
    result = pd.merge(first_admin, last_admin)
    result = result[[result.columns[0]] + sorted(result.columns[1:])]
    print(result.head())

    # create excel file
    result.to_excel("output.xlsx", index=False)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="transform data")
    parser.add_argument("--f", type=str, required=True, help="path to file with final result dates")
    parser.add_argument("--g", type=str, required=True, help="path to file with mssa dot data")

    args = parser.parse_args()

    main(args)
