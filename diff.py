from datetime import date
import pandas as pd
import numpy as np


def str_to_time(time):
    time = time.split(":")
    inttime = [int(t) for t in time]
    return (
        60 * 60 * inttime[0]
        + 60 * inttime[1]
        + inttime[2] / (10 ** (len(time[-1]) - 2))
    ) * 1000


def compare_time_first(s1: str, s2: str):
    if s1 == s2:
        return s1
    if s1 == "":
        return s1
    if s2 == "":
        return s2
    if int(s1[0]) < int(s2[0]):
        return s1
    elif int(s1[0]) > int(s2[0]):
        return s2
    else:
        return s1[0] + compare_time_first(s1[1:], s2[1:])


def is_first(s1, s2):
    if s1 == compare_time_first(s1, s2) and s1 != s2:
        return True
    else:
        return False


def sort_time(times):
    if len(times) <= 1:
        return times

    pivot = times[len(times) // 2]  # 选择中间元素作为基准
    left = [x for x in times if is_first(x, pivot)]
    middle = [x for x in times if x == pivot]
    right = [x for x in times if is_first(pivot, x)]

    return sort_time(left) + middle + sort_time(right)


def get_second_first(times):
    scecond_first = []
    for t in times:
        if len(scecond_first) == 0:
            scecond_first.append(t)
        if t[:8] != scecond_first[-1][:8]:
            scecond_first.append(t)
        else:
            s1 = t[8:]
            s2 = scecond_first[-1][8:]
            if is_first(s2, s1):
                continue
            else:
                scecond_first[-1] = t
    return scecond_first


def get_common_second(times):
    times_set = [set([t[:8] for t in tim]) for tim in times]
    commn_sec = times_set[0].intersection(*times_set[1:])
    com_times = [[t for t in tim if t[:8] in commn_sec] for tim in times]
    return com_times


def get_diff(df: pd.DataFrame, sours: list[str]) -> pd.DataFrame:
    diffs = []
    change_diffs = []
    change_tele_diff = (
        df[f"{sours[0]}_time"].apply(str_to_time).values
        - df["change_time"].apply(str_to_time).values
    )
    change_diffs.append(change_tele_diff)
    for sour in sours[1:]:
        sourc_diff = (
            df[f"{sours[0]}_time"].apply(str_to_time).values
            - df[f"{sour}_time"].apply(str_to_time).values
        )
        change_diff = (
            df[f"{sour}_time"].apply(str_to_time).values
            - df["change_time"].apply(str_to_time).values
        )
        diffs.append(sourc_diff)
        change_diffs.append(change_diff)
    return diffs, change_diffs


def get_test_data(filename):
    def trans_col_name(cols, sorce):
        return [
            (
                f"{sorce}_{col}"
                if col not in ["future", "change_time", "lastprice"]
                else col
            )
            for col in cols
        ]

    file = f"./data/{filename}_result.csv"
    au_data = pd.read_csv(
        file,
        names=["time", "change_time", "future", "lastprice", "source"],
        encoding="gbk",
    ).dropna()
    can_merg = False
    sours = []
    for source, group in au_data.groupby(by="source"):
        group.columns = trans_col_name(group.columns, source)
        sours.append(source)
        if not can_merg:
            merge_df = group
            can_merg = True
            continue
        merge_df = pd.merge(
            merge_df, group, how="right", on=["change_time", "future", "lastprice"]
        ).dropna()
    merge_df.to_csv(f"./data/{filename}_test.csv")
    product_group = list(merge_df.groupby(by="future"))
    for product, group in product_group:
        group.reset_index(drop=True).to_csv(f"./data/{filename}_{product}.csv")
    diffs, change_diffs = get_diff(merge_df, sours)
    print(f"------总时差——{filename}------")
    mean_diff = []
    std_diff = []
    mean_change = []
    std_change = []
    for i in range(len(sours)):
        if i > 0:
            print(
                f"{sours[i]}源和{sours[0]}源的平均时差： {np.mean(diffs[i-1]):.2f} ms, 标准差为： {np.std(diffs[i-1]):.2f}"
            )
            mean_diff.append(np.mean(diffs[i - 1]))
            std_diff.append(np.std(diffs[i - 1]))
        print(
            f"{sours[i]}源和交易所平均时差： {np.mean(change_diffs[i]):.2f} ms, 标准差为： {np.std(change_diffs[i]):.2f} ms"
        )
        print()
        mean_change.append(np.mean(change_diffs[i]))
        std_change.append(np.std(change_diffs[i]))
    return (
        mean_diff,
        std_diff,
        mean_change,
        std_change,
    )


def sorce_diff(filename, product, source):
    file = f"./data/{filename}_{product}.csv"
    dat = pd.read_csv(
        file,
        header=0,
        encoding="gbk",
    )
    diffs, change_diffs = get_diff(dat, source)
    mean_diff = []
    std_diff = []
    mean_change = []
    std_change = []
    print(f"------品种 {product}------")
    for i in range(len(source)):
        if i > 0:
            print(
                f"{source[i]}源和{source[0]}源的平均时差： {np.mean(diffs[i-1]):.2f} ms, 标准差为： {np.std(diffs[i-1]):.2f}"
            )
            mean_diff.append(np.mean(diffs[i - 1]))
            std_diff.append(np.std(diffs[i - 1]))
        print(
            f"{source[i]}源和交易所平均时差： {np.mean(change_diffs[i]):.2f} ms, 标准差为： {np.std(change_diffs[i]):.2f} ms"
        )
        print()
        mean_change.append(np.mean(change_diffs[i]))
        std_change.append(np.std(change_diffs[i]))
    return (
        mean_diff,
        std_diff,
        mean_change,
        std_change,
    )


def product_diff(filename, products):
    pro_tims = []
    for product in products:
        file = f"./data/{filename}_{product}.csv"
        dat = pd.read_csv(
            file,
            header=0,
            encoding="gbk",
        )
        product_tim = dat["change_time"].values
        pro_tims.append(product_tim)

    com_tims = get_common_second(pro_tims)
    times = [get_second_first(tim) for tim in com_tims]
    print(f"------{len(products)}个品种 ------")
    mean_diff = []
    std_diff = []
    for i in range(len(products)):
        diff = []
        for j in range(len(times[i])):
            diff.append(str_to_time(times[i][j]) - str_to_time(times[0][j]))
        print(
            f"{products[i]}和{products[0]}的平均时差： {np.mean(diff):.2f} ms, 标准差为： {np.std(diff):.2f}"
        )
        mean_diff.append(np.mean(diff))
        std_diff.append(np.std(diff))
    return (
        mean_diff,
        std_diff,
    )


if __name__ == "__main__":
    subid = ["au2406", "ag2406", "sc2406"]
    source = ["tele1", "unicom1", "gigabit", "10_gigabit"]
    print(
        f"-----测试的行情源有{len(source)}个，分别是simnow的电信源2和移动源，测试了{len(source)}个品种。-----"
    )
    print()
    today = date.today()
    to_date = f"{today.year}-{today.month:>02d}-{today.day:>02d}"
    sourc_diffs = []
    get_test_data(to_date)
    for por in subid:
        sourc_diff = sorce_diff(to_date, por, source)
        sourc_diffs.append(sourc_diff)
    mean_pro, std_pro = product_diff(to_date, subid)
    data1 = {
        "futrue": subid,
        f"{subid[0]}_diff": mean_pro,
    }
    sors = [so + "(ms)" for so in source]
    for i in range(len(sors)):
        data1[sors[i]] = [s[2][i] for s in sourc_diffs]
    df1 = pd.DataFrame(data1)
    print("-----汇总-----")
    print(df1)
