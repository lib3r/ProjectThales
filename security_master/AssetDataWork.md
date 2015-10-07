Security Master Database Work
---
The goal of the security master is to hold all neccesary information about assets. Right now we get all data (which is daily prices) from Yahoo Finance and we hold them in a mysql db.

I have included scripts 3 scripts that you will see:

1. `create_secmaster.sql` - creates the security master database.
	* Run first to create the empty database in mysql
	* It holds all information neccesary for our equities (stocks) datatbase 
	* We will eventually use this for James' work on company metadata

2. `parse_wiki_spx_ticker.py` - gets tickers from wikipedia and inserts them into the secmaster
	* This only inserts tickers, after we get our tickers we will get prices.
	* One of the first improvements we need to make is to extend this to capture not just all US companies, but also as many global companies as we can get.

3. `insert_price_db.py` - gets data from yahoo finance given tickers already in the db and inserts into a mysql db
	* we want daily prices from 2011 (12/31/2010) to today for as many companies as we can get
	* it will historically add these companies into the db given the tickers. If data for a company already exists it takes only what is missing.
	* we want to extend this and potentially make it more efficient/scalable

	 

