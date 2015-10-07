import pandas as pd
import pandas.io.sql as psql
import numpy as np
import MySQLdb as mdb
import matplotlib.pyplot as plt
import dateutil.parser
from event_days import peakdetect
import pylab

# matplotlib.style.use('ggplot')
# Connect to the MySQL instance
db_host = '127.0.0.1'
db_port = 3306
db_user = 'root'
db_pass = ''
db_name = 'security_master'
con = mdb.connect(host=db_host, port=db_port, user=db_user, passwd=db_pass, db=db_name)

# Select all of the historic Google adjusted close data

symbol = "'0002.HK'"

sql = """SELECT dp.price_date, dp.adj_close_price
         FROM symbol AS sym
         INNER JOIN daily_price AS dp
         ON dp.symbol_id = sym.id
         WHERE sym.ticker = %s
         ORDER BY dp.price_date ASC;""" % symbol

# # Create a pandas dataframe from the SQL query
# df = psql.read_sql(sql, con=con, index_col='price_date')    

df = pd.read_csv('~/Downloads/table.csv',parse_dates={'Date'},index_col='Date')
column_str = ['open_price', 'high_price', 'low_price', 'close_price', 'volume', 'adj_close_price']
df.columns = column_str

# Calculate return and log returns
df['pct_change'] = df.adj_close_price.pct_change()
df['log_ret'] = np.log(df.adj_close_price) - np.log(df.adj_close_price.shift(1))
df = df[np.isfinite(df['log_ret'])]
# print df

# create returns DF and only keep data from 2015
ret = df['log_ret']
ret = ret[ret.index > dateutil.parser.parse("2014-12-31")]
print ret

# calculate standard deviations and summarize values
index = range(1,7)
col = ["Num", "% of total"]
summary = pd.DataFrame(index = index, columns = col)
total = len(ret.index)

for i in index:
	summary.loc[i] = [len(ret[np.abs(ret-ret.mean())>=(i*ret.std())]), float(len(ret[np.abs(ret-ret.mean())>=(i*ret.std())]))/total * 100]
print summary
print "Total data points: %d" % total

#keep only the ones that are outside +2 to -2 standard deviations
s = 2
sigmas = ret[np.abs(ret-ret.mean())>=(s*ret.std())]
print sigmas
# get the dates and then subtract 1 business day from them
dates = sigmas.index - pd.tseries.offsets.BDay(1)
output = df[df.index > dateutil.parser.parse("2014-12-31")]

# #plot
# fig = output.adj_close_price.plot()
# ymin, ymax = fig.get_ylim()
# fig.vlines(x=dates, ymin=ymin, ymax=ymax, color='r')

# plt.show()

y = output.adj_close_price
x = output.index
threshold = len(ret[np.abs(ret-ret.mean())>=(3*ret.std())])

_max, _min = peakdetect(y, x, threshold)
print _max
print _min
xm = [p[0] for p in _max]
ym = [p[1] for p in _max]
xn = [p[0] for p in _min]
yn = [p[1] for p in _min]

plot = pylab.plot(x, y)
pylab.hold(True)
pylab.plot(xm, ym, 'ro',markersize=10)
pylab.plot(xn, yn, 'go',markersize=10)

pylab.show()
