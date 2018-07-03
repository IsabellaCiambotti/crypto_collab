from datetime import datetime
import pandas as pd
import numpy as np
import os


def convert_dates(date):
    """
    Converts time since the Unix epoch in milliseconds to a datetime object.
    """
    return datetime.fromtimestamp(date / 1000)


def read_trade_data(filepath):
    """
    Read csv containing crypto trade data at <filepath>, convert time fields
    and select relevant columns.

    :param filepath: path to csv file
    :returns: (exchange, symbol, preprocessed Pandas dataframe)
    """
    trades = pd.read_csv(filepath)
    # convert dates from unix timestamps
    trades['date'] = trades['date'].apply(convert_dates)
    trades.set_index('date', inplace=True)
    trades.sort_index(ascending=True, inplace=True)
    # check data relates to a single exchange and coin pair
    assert trades['exchange'].nunique() == 1, 'Multiple exchanges present'
    assert trades['symbol'].nunique() == 1, 'Multiple symbols present'
    # select relevant columns
    trade_features = trades[['price', 'amount', 'sell']]
    exchange = trades['exchange'].iloc[0]
    symbol = trades['symbol'].iloc[0] #if it's passed the checks above^
    return exchange, symbol, trade_features


def write_processed(exchange, symbol, data, loc=None):
    """
    Write processed features for a given exchange and coin pair
    to a parquet file named '<exchange>_<symbol>_trades.parquet'.

    :param exchange: str exchange name
    :param symbol: str symbol name
    :param data: dataframe
    :param loc: alternative filepath in which to save
    """
    filename = f'{exchange}_{symbol}_trades.parquet'
    path = os.getcwd() if loc is None else loc
    filepath = os.path.join(path, filename)
    data.to_parquet(filepath)


def featurize(filename):
    """
    Takes a parquet file and converts it to a pandas DataFrame to 
    be outputted.

    :param filename: str parquet file to be read and processed
    """
    trade_data = pd.read_parquet(filename) 
    trade_df = pd.DataFrame(trade_data)
    trade_df["date"] = trade_df["MTS"].apply(lambda x: datetime.fromtimestamp(x / 1000))
    
    # Simple Moving Averages with differing windows
    trade_df["SMA5"] = trade_df["price"].rolling(window=5).mean()
    trade_df["SMA10"] = trade_df["price"].rolling(window=10).mean()
    trade_df["SMA25"] = trade_df["price"].rolling(window=25).mean()
    trade_df["SMA50"] = trade_df["price"].rolling(window=50).mean()
    trade_df["SMA100"] = trade_df["price"].rolling(window=100).mean()

    # Exponentially weighted mean
    trade_df["ewm_com05"] = trade_df["price"].ewm(com=0.5).mean()
    trade_df["ewm_com1"] = trade_df["price"].ewm(com=1).mean()

    # Fast Fourier Transform
    trade_df["fast_fourier"] = np.fft.fft(trade_df["price"]) 
    return trade_df

if __name__ == "__main__":
    exchange, symbol, data = read_trade_data('Bitfinex_BTCEUR_trades_'
                                             '2018_02_02.csv')
    write_processed(exchange, symbol, data)

