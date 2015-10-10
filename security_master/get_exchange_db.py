import requests
import pandas as pd
from bs4 import BeautifulSoup
import MySQLdb as mdb
import time

def getPageData(url):
	success = False
	#Keep trying until status code is 200
	while not success:
		response = requests.get(url)
		try:
			response.raise_for_status()
			success = True
		except requests.exceptions.HTTPError as e:
			#Wait before trying again
			print 'retrying ' + url
			time.sleep(10)
	return response.text

def getCountryCurrencies():
	currencypage = 'http://www.science.co.il/International/Currency-codes.asp'
	pageinfo = getPageData(currencypage)
	soup = BeautifulSoup(pageinfo, 'lxml')

	results = soup.findAll('tr',attrs={'class':['c0','c1']})
	countrycodes = {}
	for result in results:
		info = result.contents
		country, code = info[0], info[3]
		country = country.get_text()
		code = code.get_text()
		#I know this is bad practice, but hard-coding this for simplicity and consistency
		if country == 'U.K.':
			country = 'United Kingdom'
		if country == 'USA':
			country = 'United States of America'
		if country == 'Korea-South':
			country = 'South Korea'
		if country == 'Czech Rep.':
			country = 'Czech Republic'
		countrycodes[country] = code
	return countrycodes

def getExchangeInfo():
	info = pd.read_excel('yahooexchange.xlsx')
	exchanges = info['Market, or Index'].tolist()
	exchangecountry = info['Country'].tolist()
	exchangesuffix = info['Suffix'].tolist()
	exchangesymbol = info['Abbreviation'].tolist()
	exchangeinfo = zip(exchangecountry, exchangesuffix, exchangesymbol)
	exchangedict = dict(zip(exchanges, exchangeinfo))
	print exchangedict
	return exchangedict

def insertExchanges(countrycodes, exchangedict):
	# Connect to the MySQL instance
	db_host = '127.0.0.1'
	db_port = 3306
	db_user = 'root'
	db_pass = ''
	db_name = 'security_master'
	con = mdb.connect(host=db_host, port=db_port, user=db_user, passwd=db_pass, db=db_name)

	column_str = "abbrev, name, country, currency, suffix, created_date, last_updated_date"
	for exchange in exchangedict:
		now = datetime.datetime.utcnow()
		insert_str = exchangedict[exchange][2]+', '+exchange+', '+exchangedict[exchange][0]+', '+countrycodes[exchangedict[exchange][0]]+', '+exchangedict[exchange][1]+', '+now+', '+now
		cursor = con.cursor()
		final_str = "INSERT INTO symbol (%s) VALUES (%s)" % (column_str, insert_str)
		print final_str



if __name__=='__main__':
	countrycodes = getCountryCurrencies()
	exchangedict = getExchangeInfo()
	print countrycodes
	print exchangedict
	insertExchanges(countrycodes, exchangedict)