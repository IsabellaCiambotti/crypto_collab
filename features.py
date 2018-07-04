import pandas as pd
import numpy as np

data = pd.read_parquet("bf_btceur_trades.parquet")
df = pd.DataFrame(data)

# Simple Moving Averages with differing windows
df["SMA5"] = df["price"].rolling(window=5).mean()
df["SMA10"] = df["price"].rolling(window=10).mean()
df["SMA25"] = df["price"].rolling(window=25).mean()
df["SMA50"] = df["price"].rolling(window=50).mean()
df["SMA100"] = df["price"].rolling(window=100).mean()

# Exponentially weighted mean
df['ewma_com05'] = df['price'].ewm(com=0.5).mean()
df['ewma_com1'] = df['price'].ewm(com=1).mean()

# Fast Fourier Transform
df["fast_fourier"] = np.fft.fft(df["price"])

df_minmax = df.resample('T')['price'].agg(['min', 'max'])

