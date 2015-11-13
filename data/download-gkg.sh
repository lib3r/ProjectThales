#!/bin/sh

for year in `seq 2013 2015`
do
  for month in `seq 1 12`
  do
    mmzero=0$month
    mm=${mmzero: -2}
    curl -O 'http://data.gdeltproject.org/gkg/'$year$mm'gkg.csv.zip'
    unzip $year$mm'.zip'
    rm $year$mm'.zip'
    gzip $year$mm.csv
    sleep 3
  done
done