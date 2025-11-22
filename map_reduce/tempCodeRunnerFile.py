
def mapreduce_sequential(df, num_chunks=8):
    chunks = chunkify(df, num_chunks)

    mapped = [mapper(chunk) for chunk in chunks]

    aggregated = reduce(reducer, mapped)
    result = finalize(aggregated)

    return result


def sequential_baseline(df):
    df_clean = df[df['category_id'].notna()]
    return df_clean.groupby('category