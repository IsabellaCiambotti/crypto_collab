import pandas as pd
import preprocess

data = pd.read_parquet("bf_btceur_trades.parquet")
df = pd.DataFrame(data)

# Creates a new dataframe for the min and max per every second

df_minmax = df.resample('T')['price'].agg(['min', 'max'])

