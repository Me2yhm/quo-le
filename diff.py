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


def get_common_second(times1, times2):
    commn_sec = set([t[:8] for t in times1]) & set([t[:8] for t in times2])
    times1 = [t for t in times1 if t[:8] in commn_sec]
    times2 = [t for t in times2 if t[:8] in commn_sec]
    return times1, times2


def get_test_data(filename):
    file = f"./data/{filename}_result.csv"
    au_data = pd.read_csv(
        file,
        names=["time", "change_time", "future", "lastprice", "source"],
        encoding="gbk",
    ).dropna()
    tele, move = au_data.groupby(by="source")
    tele_diff = tele[1]
    move_diff = move[1]
    merge_df = tele_diff.merge(
        move_diff, how="right", on=["change_time", "future", "lastprice"]
    ).dropna()
    merge_df.to_csv(f"./data/{filename}_test.csv")
    product_group = list(merge_df.groupby(by="future"))
    for product, group in product_group:
        group.reset_index(drop=True).to_csv(f"./data/{filename}_{product}.csv")
    sourc_diff = (
        merge_df["time_x"].apply(str_to_time).values
        - merge_df["time_y"].apply(str_to_time).values
    )
    change_tele_diff = (
        merge_df["time_x"].apply(str_to_time).values
        - merge_df["change_time"].apply(str_to_time).values
    )
    change_move_diff = (
        merge_df["time_y"].apply(str_to_time).values
        - merge_df["change_time"].apply(str_to_time).values
    )
    print(f"------总时差——{filename}------")
    print(
        f"电信源2和移动源的平均时差： {np.mean(sourc_diff):.2f} ms, 标准差为： {np.std(sourc_diff):.2f}"
    )
    print(
        f"电信源2和交易所平均时差： {np.mean(change_tele_diff):.2f} ms, 移动源和交易所平均时差： {np.mean(change_move_diff):.2f} ms"
    )
    return (
        np.mean(sourc_diff),
        np.std(sourc_diff),
        np.mean(change_tele_diff),
        np.mean(change_move_diff),
    )


def sorce_diff(filename, product):
    file = f"./data/{filename}_{product}.csv"
    dat = pd.read_csv(
        file,
        header=0,
        encoding="gbk",
    )
    sourc_diff = (
        dat["time_x"].apply(str_to_time).values
        - dat["time_y"].apply(str_to_time).values
    )
    change_tele_diff = (
        dat["time_x"].apply(str_to_time).values
        - dat["change_time"].apply(str_to_time).values
    )
    change_move_diff = (
        dat["time_y"].apply(str_to_time).values
        - dat["change_time"].apply(str_to_time).values
    )
    print(f"------品种{product}------")
    print(
        f"电信源2和移动源的平均时差： {np.mean(sourc_diff):.2f} ms, 标准差为： {np.std(sourc_diff):.2f}"
    )
    print(
        f"电信源2和交易所平均时差： {np.mean(change_tele_diff):.2f} ms, 移动源和交易所平均时差： {np.mean(change_move_diff):.2f} ms"
    )
    return (
        np.mean(sourc_diff),
        np.std(sourc_diff),
        np.mean(change_tele_diff),
        np.mean(change_move_diff),
    )


def product_diff(filename, product1, product2):
    file1 = f"./data/{filename}_{product1}.csv"
    file2 = f"./data/{filename}_{product2}.csv"
    dat1 = pd.read_csv(
        file1,
        header=0,
        encoding="gbk",
    )
    dat2 = pd.read_csv(
        file2,
        header=0,
        encoding="gbk",
    )
    product1_tim = dat1["change_time"].values
    product2_tim = dat2["change_time"].values
    product1_tim, product2_tim = get_common_second(product1_tim, product2_tim)
    product1_tim = get_second_first(product1_tim)
    product2_tim = get_second_first(product2_tim)
    diff = []
    for i in range(len(product1_tim)):
        diff.append(str_to_time(product1_tim[i]) - str_to_time(product2_tim[i]))
    print(f"------品种 {product1,product2}------")
    print(
        f"{product1}和{product1}的平均时差： {np.mean(diff):.2f} ms, 标准差为： {np.std(diff):.2f}"
    )
    return (
        np.mean(diff),
        np.std(diff),
    )


if __name__ == "__main__":
    print(
        "-----测试的行情源有两个，分别是simnow的电信源2和移动源，测试了三个品种。-----"
    )
    print()
    date = "2024-04-24"
    get_test_data(date)
    au_m, au_std, au_te, au_mo = sorce_diff(date, "au2406")
    sc_m, sc_std, sc_te, sc_mo = sorce_diff(date, "sc2406")
    ag_m, ag_std, ag_te, ag_mo = sorce_diff(date, "ag2406")
    au_ag_mean, au_ag_std = product_diff(date, "au2406", "ag2406")
    sc_ag_mean, sc_ag_std = product_diff(date, "sc2406", "ag2406")
    ag_ag_mean, ag_ag_std = product_diff(date, "ag2406", "ag2406")
    df = pd.DataFrame(
        {
            "futrue": ["au2406", "sc2406", "ag2406"],
            "mean (ms)": [au_m, sc_m, ag_m],
            "std (ms)": [au_std, sc_std, ag_std],
            "source_ct (ms)": [au_te, sc_te, ag_te],
            "sourc_cmcc (ms)": [au_mo, sc_mo, ag_mo],
            "ag_diff (ms)": [au_ag_mean, sc_ag_mean, ag_ag_mean],
        }
    )
    print("-----汇总-----")
    print(df)
