from urllib import urlopen
import json
 
def googleQuote(ticker):
    url = '%s%s' % ('http://www.google.com/finance/info?q=', ticker)
    doc = urlopen(url)
    content = doc.read()
    quote = json.loads(content[3:])
    quote = float(quote[0][u'l'])
    return quote
 
if __name__ == "__main__":
    ticker = 'GOOG'
    print googleQuote(ticker)