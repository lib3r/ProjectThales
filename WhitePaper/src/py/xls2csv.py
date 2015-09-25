#! /usr/bin/env python

import xlrd
import csv

book = xlrd.open_workbook('RU-UA_data_raw.xlsx')

# Assuming the fist sheet is of interest 
sheet = book.sheet_by_index(0)

# Many options here to control how quotes are handled, etc.
csvWriter = csv.writer(open('RU-UA_data_raw.csv', 'w'), delimiter=',') 

for i in range(sheet.nrows):
    csvWriter.writerow(sheet.row_values(i))