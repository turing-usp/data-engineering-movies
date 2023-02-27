import pandas as pd

for idx, df in enumerate(pd.read_csv('movie.json', chunksize=38000, delimiter='\t')):
    df.to_csv(f"parte{idx:02}.csv")
