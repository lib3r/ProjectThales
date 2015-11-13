import os
import datetime as dt
import time
import io

import numpy as np
import requests

URL = "http://data.gdeltproject.org/gkg/" # The GKG data directory
PATH = "/Users/enidmagari/Documents/Work/GFT/Thales/data/GDELT_Events/gkg/" # The local directory to store the data

# Specify the start and end date for your data
start_date = dt.datetime(2014, 06, 13)
end_date = dt.datetime.today()
date = start_date

# For each date in between, download the corresponding file
while date <= end_date:
    filename = date.strftime("%Y%m%d") + ".gkg.csv.zip"
    req = requests.get(URL + filename)
    dl = io.open(PATH + filename, "wb")
    for chunk in req.iter_content(chunk_size=1024):
        if chunk:
            dl.write(chunk)
    print("%s!"%filename)
    dl.close()
    time.sleep(2) # Be nice and don't overload the server.
    date += dt.timedelta(days=1)