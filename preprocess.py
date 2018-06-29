from datetime import datetime
import pandas as pd
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


if __name__ == "__main__":
    exchange, symbol, data = read_trade_data('Bitfinex_BTCEUR_trades_'
                                             '2018_02_02.csv')
    write_processed(exchange, symbol, data)

