from eventDays import peakdetect
from collections import defaultdict
import pandas as pd
import pandas.io.sql as psql
import numpy as np
import MySQLdb as mdb
import pprint
import pylab as pl
import matplotlib.dates as mdates

maxima = defaultdict(int)
minima = defaultdict(int)

db_host = '127.0.0.1'
db_port = 3306
db_user = 'root'
db_pass = ''
db_name = 'security_master'
con = mdb.connect(host=db_host, port=db_port, user=db_user, passwd=db_pass, db=db_name)

def obtain_list_of_db_tickers():
	"""Obtains a list of the ticker symbols in the database that have some data."""

	sql = """SELECT id, ticker from 
			(select symbol.id, ticker, max(daily_price.price_date) as end from symbol 
			join daily_price on (symbol.id = daily_price.symbol_id)
			where symbol.id in(select symbol_id from daily_price) and ticker like '%.hk%' 
		group by daily_price.symbol_id) as table1
		where end > '2015-10-01'
	"""
	with con: 
		cur = con.cursor()
		# get only symbols with some data traded in hong kong
		cur.execute(sql)
		data = cur.fetchall()
		return [(d[0], d[1]) for d in data]

def get_prices(ticker):
	"""get prices from 12/31/2014 to most recent for specified ticker"""
	sql = """SELECT dp.price_date, dp.adj_close_price
		 FROM symbol AS sym
		 INNER JOIN daily_price AS dp
		 ON dp.symbol_id = sym.id
		 WHERE sym.ticker = '%s'  and dp.price_date > '2000-12-31'
		 ORDER BY dp.price_date ASC;""" % ticker

	df = psql.read_sql(sql, con=con, index_col='price_date') 

	return df

def calc_returns(df):
		# Calculate return and log returns
	df['pct_change'] = df.adj_close_price.pct_change()
	df['log_ret'] = np.log(df.adj_close_price) - np.log(df.adj_close_price.shift(1))
	df = df[np.isfinite(df['log_ret'])]

	return df

def sigma(df, s):
	# calculate standard deviations and summarize values
	ret = df['log_ret']

	index = range(1,7)
	col = ["Num", "% of total"]
	summary = pd.DataFrame(index = index, columns = col)
	total = len(ret.index)

	for i in index:
		summary.loc[i] = [len(ret[np.abs(ret-ret.mean())>=(i*ret.std())]), float(len(ret[np.abs(ret-ret.mean())>=(i*ret.std())]))/total * 100]
	# print summary
	# print "Total data points: %d" % total

	#keep only the ones that are outside +2 to -2 standard deviations

	sigmas = ret[np.abs(ret-ret.mean())>=(s*ret.std())]

	# get the dates and then subtract 1 business day from them
	dates = sigmas.index - pd.tseries.offsets.BDay(1)

	return sigmas

def extrema(df,s):
	y = df.adj_close_price
	x = df.index
	threshold = len(df.log_ret[np.abs(df.log_ret-df.log_ret.mean())>=(s*df.log_ret.std())])

	_max, _min = peakdetect(y, x, threshold)

	xm = pd.DatetimeIndex([p[0] for p in _max])
	ym = [p[1] for p in _max]
	xn = pd.DatetimeIndex([p[0] for p in _min])
	yn = [p[1] for p in _min]

	for i in xm:
		minima[i.date()] +=1

	for j in xn:
		maxima[j.date()] +=1

def plot(maxima, minima):
	X = np.arange(len(maxima))
	pl.bar(X, maxima.values(), align='center', width=0.5)
	pl.xticks(X, maxima.keys())
	pl.gca().xaxis.set_major_formatter(mdates.DateFormatter('%m'))
	ymax = max(maxima.values()) + 1
	pl.ylim(0, ymax)
	pl.show()


if __name__ == "__main__":
	pp = pprint.PrettyPrinter(indent=2)
	s = 2
	i = 0

	tickers = obtain_list_of_db_tickers()

	for t in tickers:
		print "%d %s" %(i,t[1])
		df = get_prices(t[1])
		df = calc_returns(df)
		sigmas = sigma(df,s)
		extrema(df,s)
		i+=1
	print "Total Tickers: %d" % len(tickers)
	pp.pprint(dict(maxima))
	print
	print
	pp.pprint(dict(minima))

	print sum(maxima.values())/len(tickers)
	print sum(minima.values())/len(tickers)
	plot(maxima,minima)

