from urllib import urlopen
import json
 
#Latest available quote for the GBPUSD pair (delayed data)
def googleQuoteGBPUSD():
    url = 'http://www.google.com/finance/info?q=CURRENCY:GBPUSD'
    doc = urlopen(url)
    content = doc.read()
    quote = json.loads(content[3:])
    quote = float(quote[0][u'l'])
    return quote
 
#Latest available quote from Google Finance
def googleQuote(ticker):
    url = '%s%s' % ('http://www.google.com/finance/info?q=', ticker)
    doc = urlopen(url)
    content = doc.read()
    #The unicode codex (here ISO-8859-1) needs sometimes to be specified
    data = json.loads(content[3:].decode('ISO-8859-1'))
    quote = float(str(data[0][u'l']).replace(',',''))
    #Google also specifies the currency if it is not USD
    currency = data[0][u'l_cur']
    GBPUSD = googleQuoteGBPUSD()
    #Verifies that the currency is GBp
    if currency[0:3] == 'GBX':
        quote = quote * GBPUSD / 100
    else:
        print ticker, 'not quoted in GBP'
    return quote
 
if __name__ == "__main__":
    #The exchange is specified for the ticker
    arg = input("Enter Ticker <Exchange:Ticker>: ")
    # ticker = 'LON:BLT'
    arg = "'"+arg+"'"
    print arg
    print googleQuote(arg)