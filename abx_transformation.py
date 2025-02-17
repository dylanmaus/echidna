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

    # create intermediate date pairs
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
    result = df.set_index(["CSN", "Category"])
    result = result[[first_or_last]]
    result = result.unstack(level=-1).rename_axis(None)
    result.reset_index(level=0, drop=False, inplace=True)
    result.columns = ["CSN"] + [f"{x[1]}_{first_or_last[0]}A" for x in result.columns.tolist()[1:]]

    return result


def assign_abx_group(df: pd.DataFrame) -> None:
    group_zero = ["Cefazolin", "Nafcillin", "Oxacillin"]

    df["Group"] = 1
    df.loc[df["ABX_Category"].isin(group_zero), "Group"] = 0


def main(args):
    # read in files to DataFrames
    mssa_fin = excel_to_df(args.f)
    mssa_dot = excel_to_df(args.g)
    mssa_dem = excel_to_df(args.h)

    # convert date columns
    mssa_fin["Final_Result_Date"] = pd.to_datetime(mssa_fin["Final_Result_Date"])
    mssa_dot["First_Admin"] = pd.to_datetime(mssa_dot["First_Admin"])
    mssa_dot["Last_Admin"] = pd.to_datetime(mssa_dot["Last_Admin"])

    # keep earliest date per CSN in final result dates
    earliest_mssa_fin = mssa_fin.loc[mssa_fin.groupby("CSN")["Final_Result_Date"].idxmin()]

    # join mssa dot and final result dates
    mssa_dot = mssa_dot.merge(earliest_mssa_fin, on="CSN")

    # remove ampicillin, amoxicillin, penicillin
    abx_ignore = ["Ampicillin", "Amoxicillin", "Penicillin"]
    mssa_dot = mssa_dot[~mssa_dot["ABX_Category"].isin(abx_ignore)]

    # remove admins that entirely occur before the final result date
    mssa_dot = mssa_dot[mssa_dot["Last_Admin"] >= mssa_dot["Final_Result_Date"]]
    # truncate admin window to begin at final result date
    mssa_dot.loc[mssa_dot["First_Admin"] < mssa_dot["Final_Result_Date"], "First_Admin"] = mssa_dot["Final_Result_Date"]
    mssa_dot.reset_index(inplace=True, drop=True)

    # remove times for easier readability
    # mssa_dot["First_Admin"] = mssa_dot["First_Admin"].dt.date
    # mssa_dot["First_Admin"] = pd.to_datetime(mssa_dot["First_Admin"])
    # mssa_dot["Last_Admin"] = mssa_dot["Last_Admin"].dt.date
    # mssa_dot["Last_Admin"] = pd.to_datetime(mssa_dot["Last_Admin"])
    # mssa_dot["Final_Result_Date"] = mssa_dot["Final_Result_Date"].dt.date
    # mssa_dot["Final_Result_Date"] = pd.to_datetime(mssa_dot["Final_Result_Date"])

    print(mssa_dot.head(mssa_dot.shape[0]))

    # find last admin date
    last_admin = mssa_dot.groupby("CSN")["Last_Admin"].max().reset_index()

    # group each admin of abx into courses
    abx_courses = mssa_dot.groupby(["CSN", "ABX_Category"]).apply(assign_courses, include_groups=False).reset_index()
    abx_courses["DOT"] = abx_courses["DOT"].dt.days

    # find total days of therapy for each drug
    abx_dot = abx_courses.groupby(["CSN", "ABX_Category"])["DOT"].sum().reset_index(name="ABX_DOT")

    # find drug with longest days of therapy
    max_abx_dot = (
        abx_dot.groupby("CSN")[[x for x in abx_dot.columns.tolist()]].apply(lambda x: x[x["ABX_DOT"] == x["ABX_DOT"].max()]).reset_index(drop=True)
    )

    # assign group based on antibiotic
    assign_abx_group(max_abx_dot)

    # group contiguous admins of any drug by course
    any_abx_courses = mssa_dot.groupby(["CSN"]).apply(assign_courses, include_groups=False).reset_index()
    any_abx_courses["DOT"] = any_abx_courses["DOT"].dt.days

    # find total days of therapy
    total_dot = any_abx_courses.groupby("CSN")["DOT"].sum().reset_index(name="Total_DOT")

    # unstack
    unstack_abx_dot = abx_dot.set_index(["CSN", "ABX_Category"])
    unstack_abx_dot = unstack_abx_dot.unstack(level=-1).rename_axis(None)
    unstack_abx_dot.reset_index(level=0, drop=False, inplace=True)
    unstack_abx_dot.columns = ["CSN"] + [x[1] for x in unstack_abx_dot.columns.tolist()[1:]]

    # merge intermediary results into final result
    result = pd.merge(unstack_abx_dot, max_abx_dot[["CSN", "Group"]], on="CSN")
    result = pd.merge(result, total_dot, on="CSN")
    result = pd.merge(result, last_admin, on="CSN")
    result = pd.merge(mssa_dem, result, on="CSN")
    print(result.head(result.shape[0]))

    # create excel file
    result.to_excel("output.xlsx", index=False)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="transform data")
    parser.add_argument("--f", type=str, required=True, help="path to file with final result dates")
    parser.add_argument("--g", type=str, required=True, help="path to file with mssa dot data")
    parser.add_argument("--h", type=str, required=True, help="path to file with mssa dem data")

    args = parser.parse_args()

    main(args)
