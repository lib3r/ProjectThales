import requests
from bs4 import BeautifulSoup
import time
import datetime
import MySQLdb as mdb

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
            time.sleep(5)
    return response.text

def insertData(exchangeid, ticker, name, page):
    # Connect to the MySQL instance
    db_host = '127.0.0.1'
    db_port = 3306
    db_user = 'root'
    db_pass = ''
    db_name = 'security_master'
    con = mdb.connect(host=db_host, port=db_port, user=db_user, passwd=db_pass, db=db_name)

    now = datetime.datetime.utcnow()
    column_str = "exchange_id, ticker, instrument, name, companylink, created_date, last_updated_date"
    insert_str = str(exchangeid) + ', "' + ticker + '", "stock", "' + name + '", "' + page + '", "' + str(now) + '", "' + str(now) + '"'
    final_str = "INSERT INTO symbol (%s) VALUES (%s)" % (column_str, insert_str)
    print final_str

    with con:
        cur = con.cursor()
        cur.execute(final_str)

if __name__ == '__main__':
    google_url = 'http://www.google.com/finance?q=SHA%3A'
    #Shanghai goes from 600000 to 900957
    #Shenzhen goes from 000001 to 300489
    for i in xrange(602024, 900958):
        stockpage = google_url+str(i)
        pagedata = getPageData(stockpage)
        soup = BeautifulSoup(pagedata, 'lxml')
        title = soup.findAll('title')
        titlecontents = title[0].text
        if titlecontents.startswith('SHA:'):
            print 'SHA:' + str(i) + ' is not valid company'
            continue
        firstcolon = titlecontents.find(':')
        companyname = titlecontents[:firstcolon]
        companyticker = 'SHA.'+str(i)
        exchangeid = '15'
        insertData(exchangeid, companyticker, companyname, stockpage)
        time.sleep(2)
