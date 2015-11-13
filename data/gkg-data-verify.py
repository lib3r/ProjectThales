import csv
import glob, os


os.chdir("/Users/enidmagari/Documents/Work/GFT/Thales/data/GDELT_Events/gkg")
i = 0
for file in glob.glob("*.csv"):
	with open(file) as f:
	    reader = csv.reader(f, delimiter='\t', skipinitialspace=True)
	    first_row = next(reader)
	    num_cols = len(first_row)
	    i+=1
	    print "%d\t%s: %d" % (i,f.name,num_cols)