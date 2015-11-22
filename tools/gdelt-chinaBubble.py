import pandas as pd
import numpy as np
# import matplotlib.pyplot as plt
import csv


with open("/Users/enidmagari/Documents/Work/GFT/data/GDELT_Events/dict/CSV.header.dailyupdates.txt", 'rb') as f:
	headers = f.readline().strip().split('\t')
# print headers
# filename = "/Users/enidmagari/Documents/Work/GFT/Thales/data/GDELT_Events/gkg/20150824.gkg.csv"
filename = "/Users/enidmagari/Documents/Work/GFT/data/GDELT_Events/events/20150826.export.CSV"
df = pd.read_csv(filename,  sep='\t', names=headers)

test = df[(df.Actor1CountryCode=="CHN") | ( df.Actor2CountryCode=="CHN")]
test = df[(df['Actor1Type1Code'].str.contains("BUS")==True) | (df['Actor2Type1Code'].str.contains("BUS")==True)]

test = test.sort(['NumArticles','NumMentions','NumSources'], ascending=[False,False,False])
print test[['NumArticles','SOURCEURL']].values