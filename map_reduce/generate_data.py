import pandas as pd
import numpy as np

NUM_ROWS = 500_000_000
NUM_CATEGORIES = 500
OUTPUT_PATH = "../data/synthetic.csv"

np.random.seed(42)

category_ids = np.random.randint(1, NUM_CATEGORIES + 1, NUM_ROWS)
prices = np.random.uniform(10, 1000, NUM_ROWS).round(2)

df = pd.DataFrame({
    'category_id': category_ids,
    'price': prices
})

df.to_csv(OUTPUT_PATH, index=False)
print(f"Сгенерировано {NUM_ROWS} строк в {OUTPUT_PATH}")
