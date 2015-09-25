import pandas as pd
import numpy as np
import matplotlib.pyplot as plt

#set some options
pd.set_option('display.mpl_style', 'default') 

# read data
df = pd.read_csv('RU-UA_data_raw.csv')

# convert dates
df['Date'] = pd.to_datetime(df['Date'])

# create prices table
prices = df.pivot(index='Date', columns='SEDOL', values='Price')
# cut for only date we are interested in
prices = prices['20131101':]

# create companies table
companies = df.drop('Date',1)
companies = df.drop('Price',1)

prices.to_csv('RU-UA_prices.csv')