import pandas as pd
import numpy as np
from functools import reduce
from multiprocessing import Pool, cpu_count
import time


def chunkify(df, num_chunks):
    chunk_size = len(df) // num_chunks + 1
    return [df.iloc[i:i+chunk_size] for i in range(0, len(df), chunk_size)]


def mapper(chunk):
    chunk_clean = chunk[chunk['category_id'].notna()]

    # Группируем по category_id и вычисляем сумму и количество
    grouped = chunk_clean.groupby('category_id')['price'].agg(['sum', 'count'])

    # Преобразуем в словарь с кортежами (sum, count)
    local_stats = {cat: (row['sum'], row['count']) for cat, row in grouped.iterrows()}

    return local_stats


def reducer(dict1, dict2):
    result = dict1.copy()

    for cat, (sum_prices, count) in dict2.items():
        if cat in result:
            # Суммируем суммы и количества
            result[cat] = (result[cat][0] + sum_prices, result[cat][1] + count)
        else:
            result[cat] = (sum_prices, count)

    return result


def finalize(aggregated_dict):
    return {cat: sum_prices / count for cat, (sum_prices, count) in aggregated_dict.items()}


def mapreduce_parallel(df, num_processes=None):
    if num_processes is None:
        num_processes = cpu_count()

    chunks = chunkify(df, num_processes)

    with Pool(num_processes) as pool:
        mapped = pool.map(mapper, chunks)

    aggregated = reduce(reducer, mapped)
    result = finalize(aggregated)

    return result


def mapreduce_sequential(df, num_chunks=8):
    chunks = chunkify(df, num_chunks)

    mapped = [mapper(chunk) for chunk in chunks]

    aggregated = reduce(reducer, mapped)
    result = finalize(aggregated)

    return result


def sequential_baseline(df):
    df_clean = df[df['category_id'].notna()]
    return df_clean.groupby('category_id')['price'].mean().to_dict()


N_RUNS = 10
CSV_PATH = "../data/kz.csv"
# CSV_PATH = "../data/synthetic.csv"


def main():
    df = pd.read_csv(CSV_PATH)
    print(f"Загружено {len(df)} строк\n")

    times_baseline = []
    for i in range(N_RUNS):
        t_start = time.time()
        result_baseline = sequential_baseline(df)
        times_baseline.append(time.time() - t_start)
    avg_time_baseline = sum(times_baseline) / N_RUNS
    print(f"Baseline (pandas groupby): {avg_time_baseline:.4f}s")
    print(f"Категорий: {len(result_baseline)}")
    del times_baseline
    del result_baseline

    times_seq = []
    for i in range(N_RUNS):
        t_start = time.time()
        result_seq = mapreduce_sequential(df)
        times_seq.append(time.time() - t_start)
    avg_time_seq = sum(times_seq) / N_RUNS
    print(f"Sequential MapReduce: {avg_time_seq:.4f}s ({avg_time_seq / avg_time_baseline:.2f}x)")
    del times_seq
    del result_seq

    times_par = []
    for i in range(N_RUNS):
        t_start = time.time()
        result_par = mapreduce_parallel(df, num_processes=8)
        times_par.append(time.time() - t_start)
    avg_time_par = sum(times_par) / N_RUNS
    print(f"Parallel MapReduce: {avg_time_par:.4f}s ({avg_time_par / avg_time_baseline:.2f}x)")
    # del times_par
    # del result_par

    # all_match = all(
    #     abs(result_baseline[cat] - result_seq[cat]) < 0.01 and
    #     abs(result_baseline[cat] - result_par[cat]) < 0.01
    #     for cat in result_baseline
    # )
    # print(f"Результаты совпадают: {all_match}")


if __name__ == '__main__':
    main()
