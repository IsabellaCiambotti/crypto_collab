from datetime import datetime
import pandas as pd
import os

def read_data(filepath):
    """
    Read parquet file containing crypto trade data at <filepath>.

    :param filepath: path to parquet file
    :returns: (date, price, amount, sell Pandas dataframe)
    """
    trades = pd.read_parquet(filepath, engine='fastparquet')
    # trade_features = trades[['date', 'price', 'amount', 'sell']
    return trades

def add_SMA(df, n):
    df['MA' + str(n)] = df['price'].rolling(window=n).mean()
    # dataframe['MA50'] = df['price'].rolling(window=50).mean()
    # dataframe['MA100'] = df['price'].rolling(window=100).mean()

def add_bollinger_bands(df, n):
    """
    
    :param df: pandas.DataFrame
    :param n: 
    :return: pandas.DataFrame
    """
    MA = pd.Series(df['price'].rolling(n, min_periods=n).mean())
    MSD = pd.Series(df['price'].rolling(n, min_periods=n).std())
    b1 = 4 * MSD / MA
    B1 = pd.Series(b1, name='BollingerB_' + str(n))
    df = df.join(B1)
    b2 = (df['price'] - MA + 2 * MSD) / (4 * MSD)
    B2 = pd.Series(b2, name='Bollinger%b_' + str(n))
    df = df.join(B2)
    return df

def write_processed(data, loc=None):
    """
    Write processed features for a given exchange and coin pair
    to a parquet file named 'trades_features.parquet'.
    :param data: dataframe
    :param loc: alternative filepath in which to save
    """
    filename = f'trades_features.parquet'
    path = os.getcwd() if loc is None else loc
    filepath = os.path.join(path, filename)
    data.to_parquet(filepath)

def add_indices(data):
    data = read_data('bf_btceur_trades.parquet')
    add_SMA(data, 15) ##add 15-day SME
    add_SMA(data, 50) ##add 50-day SME
    add_SMA(data, 100) ##add 100-day SME
    # add_bollinger_bands(data, 5)
    return(data)

if __name__ == "__main__": ## runs when command "python [file]"" is called
    data = read_data('bf_btceur_trades.parquet')
    add_indices(data)
    write_processed(data)

