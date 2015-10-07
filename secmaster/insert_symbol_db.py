import requests
from bs4 import BeautifulSoup
import time
import re
import MySQLdb as mdb
import datetime
from math import ceil

# get the rest of the data
#http://d.yimg.com/autoc.finance.yahoo.com/autoc?query=USFR&region=1&lang=en&callback=YAHOO.Finance.SymbolSuggest.ssCallback


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

def getSectors():
	yahoolink = 'http://biz.yahoo.com/p/'
	pagedata = getPageData(yahoolink)
	soup = BeautifulSoup(pagedata, 'lxml')

	#Get all sectors
	sectorlinks = {}
	sectorresults = soup.findAll('td', attrs={'bgcolor': 'ffffee'})
	for result in sectorresults:
		resa = result.a
		sectorname = resa.font.contents[0]
		sectorlink = resa['href']
		sectorlinks[sectorname] = sectorlink
	return sectorlinks

#Get all industries, similar to getting all sectors
def getIndustries(sectorlinks):
	yahoolink = 'http://biz.yahoo.com/p/'
	industrylinks = {}
	for sector in sectorlinks.keys():
		print yahoolink+sectorlinks[sector]
		time.sleep(2)
		sectorpage = getPageData(yahoolink+sectorlinks[sector])
		sectorsoup = BeautifulSoup(sectorpage, 'lxml')
		industryresults = sectorsoup.findAll('td', attrs={'bgcolor': 'ffffee'})
		for result in industryresults:
			#First row of table is part of results, but we don't want it
			resulta = result.a
			#Since it doesn't have a link, we can filter it out this way
			if resulta:
				industryname = resulta.font.contents[0]
				industrylink = resulta['href']
				industrylinks[industryname] = (industrylink, sector)
	return industrylinks

def getCompanies(indlink, industry, sectorname):
	yahoolink = 'http://biz.yahoo.com/p/'
	now = datetime.datetime.utcnow()
	companylist = []
	print yahoolink+indlink
	time.sleep(5)
	industrypage = getPageData(yahoolink+indlink)
	print 'got page, now souping'
	industrysoup = BeautifulSoup(industrypage, 'lxml')
	industryresults = industrysoup.findAll('td', attrs={'bgcolor': 'ffffee'})
	print 'getting companies'
	for result in industryresults:
		#First two rows are not wanted, so filter out using <b> tag
		resultb = result.b
		if not resultb and result.font:
			companycontent = result.font.contents
			#Has company name, open paren, symbol link, close paren
			#Some companies will have length 2 (symbol is a string, meaning page does not exist for prices)
			if len(companycontent) == 4:
				companyname = str(companycontent[0])
				#Get rid of tags
				companyname = re.sub('<[^<]+?>', '', companyname)
				#Company names have newline characters, making them definitively weird
				companyname = re.sub('\n', ' ', companyname)
				company = str(companycontent[2])
				#Find URL
				companylink = re.findall('http[s]?://(?:[a-zA-Z]|[0-9]|[$-_@.&+]|[!*\(\),]|(?:%[0-9a-fA-F][0-9a-fA-F]))+', company)
				#Result is a list, so just grab first (and only) element
				companylink = companylink[0]
				#Do the same thing for company symbol as name
				companysymbol = re.sub('<[^<]+?>', '', company)
				companysymbol = re.sub('\n', ' ', companysymbol)
				companylist.append((companysymbol, 'stock', companyname, sectorname, industry, companylink, now, now))
	return companylist

def insertSymbols(companies):
	# Connect to the MySQL instance
	db_host = '127.0.0.1'
	db_port = 3306
	db_user = 'root'
	db_pass = ''
	db_name = 'security_master'
	con = mdb.connect(host=db_host, port=db_port, user=db_user, passwd=db_pass, db=db_name)

	# Create the insert strings
	column_str = "ticker, instrument, name, sector, industry, companylink, created_date, last_updated_date"
	insert_str = ("%s, " * 8)[:-2]
	final_str = "INSERT INTO symbol (%s) VALUES (%s)" % (column_str, insert_str)
	print final_str, len(companies)

	with con:
		cur = con.cursor()
		for i in range(0, int(ceil(len(companies) / 100.0))):
			cur.executemany(final_str, companies[i*100:(i+1)*100-1])

if __name__ == "__main__":
	sectorlinks = getSectors()
	industrylinks = getIndustries(sectorlinks)
	for industry in industrylinks.keys():
		industrylink = industrylinks[industry][0]
		sectorname = industrylinks[industry][1]
		companylist = getCompanies(industrylink, industry, sectorname)
		insertSymbols(companylist)
