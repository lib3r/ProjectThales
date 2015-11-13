import pandas as pd
import pandas.io.sql as psql
import numpy as np
import MySQLdb as mdb
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import dateutil.parser
from event_days import peakdetect
import pylab

import matplotlib.image as mpimg
import matplotlib.cm as cm
from PIL import Image
from matplotlib import rcParams
rcParams.update({'figure.autolayout': True})

def mysql():

	# Connect to the MySQL instance
	db_host = '127.0.0.1'
	db_port = 3306
	db_user = 'root'
	db_pass = ''
	db_name = 'security_master'
	con = mdb.connect(host=db_host, port=db_port, user=db_user, passwd=db_pass, db=db_name)

	# Select all of the historic Google adjusted close data

	symbol = "'0321.HK'"

	sql = """SELECT dp.price_date, dp.adj_close_price
	         FROM symbol AS sym
	         INNER JOIN daily_price AS dp
	         ON dp.symbol_id = sym.id
	         WHERE sym.ticker = %s
	         ORDER BY dp.price_date ASC;""" % symbol

	# Create a pandas dataframe from the SQL query
	df = psql.read_sql(sql, con=con, index_col='price_date')

	return df 

def importDF():
	
	df = pd.read_csv('~/Downloads/table.csv',parse_dates={'Date'},index_col='Date')
	column_str = ['open_price', 'high_price', 'low_price', 'close_price', 'volume', 'adj_close_price']
	df.columns = column_str

	return df

def calculateReturns(df):

	# Calculate return and log returns
	df['pct_change'] = df.adj_close_price.pct_change()
	df['log_ret'] = np.log(df.adj_close_price) - np.log(df.adj_close_price.shift(1))
	df = df[np.isfinite(df['log_ret'])]

	# create returns DF and only keep data from 2015
	ret = df['log_ret']
	# print ret
	return ret

def sigmas(ret, s):
	# calculate standard deviations and summarize values
	index = range(1,7)
	col = ["Num", "% of total"]
	summary = pd.DataFrame(index = index, columns = col)
	total = len(ret.index)

	for i in index:
		summary.loc[i] = [len(ret[np.abs(ret-ret.mean())>=(i*ret.std())]), float(len(ret[np.abs(ret-ret.mean())>=(i*ret.std())]))/total * 100]
	print summary
	# print "Total data points: %d" % total

	#keep only the ones that are outside +2 to -2 standard deviations

	sigmas = ret[np.abs(ret-ret.mean())>=(s*ret.std())]

	# get the dates and then subtract 1 business day from them
	dates = sigmas.index - pd.tseries.offsets.BDay(1)
	return sigmas

def plot(x,y,xm,ym,xn,yn):
	# #plot
	# fig = output.adj_close_price.plot()
	# ymin, ymax = fig.get_ylim()
	# fig.vlines(x=dates, ymin=ymin, ymax=ymax, color='r')

	# plt.show()

	plot = pylab.plot(x, y)
	pylab.hold(True)

	pylab.plot(xm, ym, 'ro',markersize=10)
	pylab.plot(xn, yn, 'go',markersize=10)

	# pylab.title('Shanghai Composite Index',fontsize=32)
	# pylab.xlabel('2015', fontsize=24)
	# pylab.ylabel('Price (in CNY)', fontsize=24)
	# pylab.gca().tight_layout()
	# pylab.gca().xaxis.set_major_formatter(mdates.DateFormatter('%b'))
	# pylab.gca().xaxis.set_tick_params(labelsize=20)
	# pylab.gca().yaxis.set_tick_params(labelsize=20)


	pylab.savefig('pic.png')

	# figprops = dict(figsize=(10,10), dpi=300)
	# fig1 = pylab.figure(**figprops)

	# adjustprops = dict()
	# image=Image.open('pic.png').convert("L")
	# arr=np.asarray(image)
	# pylab.figimage(arr,cmap=cm.Greys_r)
	# pylab.savefig('pic_grayed.png')
	# pylab.savefig('grayed.pdf',papertype='a4',orientation='portrait')

	pylab.show()

if __name__ == "__main__":

	df = importDF()
	ret = calculateReturns(df)
	print len(df)

	date = "2011-12-31"
	output = df[df.index > dateutil.parser.parse(date)]
	ret = ret[ret.index > dateutil.parser.parse(date)]
	
	s = 2
	sigmas(ret,s)
	
	y = output.adj_close_price
	x = output.index

	# threshold = len(ret[np.abs(ret-ret.mean())>=(s*ret.std())])
	threshold = 12
	_max, _min = peakdetect(y, x, threshold)
	print threshold

	xm = [p[0] for p in _max]
	ym = [p[1] for p in _max]
	xn = [p[0] for p in _min]
	yn = [p[1] for p in _min]

	extrema = pd.DataFrame.from_records([xm, xn]).transpose()
	extrema.columns = ['Maxima', 'Minima']
	print extrema

	plot(x,y,xm,ym,xn,yn)


