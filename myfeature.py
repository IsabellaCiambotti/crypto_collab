import pandas as pd
import numpy as np

pd.read_parquet("bf_btceur_trades.parquet")
df = pd.DataFrame(data)

#exponential weighted moving average 

df['ewma_com05'] = df['price'].ewm(com=0.5).mean()
df['ewma_com1'] = df['price'].ewm(com=1).mean()

