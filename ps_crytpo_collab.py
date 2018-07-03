
# coding: utf-8

# In[ ]:


# Parker Steinberg crypto_collab


# In[128]:


import preprocess as pp
import pandas as pd
import numpy as np


# In[129]:


exchange, symbol, trade_features = pp.read_trade_data('Bitfinex_BTCEUR_trades_2018_02_02.csv')
# print(exchange)
# print(symbol)


# In[130]:


pp.write_processed(exchange, symbol, trade_features)


# In[131]:


df = pd.read_parquet('bf_btceur_trades.parquet')


# In[132]:


# print(df["price"])
# df["variation"] = df["price"][5]-df["price"][0]
# df.head()


# In[133]:


print(df.loc["2018-02-02 02:00:08"])


# In[134]:


# Feature: variation (max price - min price) over a 10 min interval 
ts = df.index
ts[0]

# create interval object that is a list of data points 
# int_begin = ts[0]
# interval = ts[0:10]
# print(interval.time)


# loop through interval to to find min and max
def min_int(interval):
    my_min = interval[0]
    for i in interval:
        if i < my_min:
            my_min = i
    return my_min

def max_int(interval):
    my_max = interval[0]
    for i in interval:
        if i > my_max:
            my_max = i
    return my_max


# In[139]:


from tqdm import tqdm_notebook as tqdm
pbar=tqdm(range(len(price)-10))
price = df["price"]
for i in range(len(price)-10):
#     my_range = price[i:i+10]
#     max_price = max(price[i:i+10])
#     min_price = min(price[i:i+10])
#     variation = max_price - min_price
    df["variation"].iloc[i] = max(price[i:i+10]) - min(price[i:i+10])
    pbar.update()
    
df.head(30)

