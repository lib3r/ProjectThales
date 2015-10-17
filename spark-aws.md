Spark Setup
===========

We will walk through setting up Spark on AWS

Before
------

1.	Create an Amazon EC2 key pair for yourself. This can be done by logging into your Amazon Web Services account through the AWS console, clicking Key Pairs on the left sidebar, and creating and downloading a key.

2.	Make sure that you set the permissions for the private key file to 600 (i.e. only you can read and write it) so that ssh will work.

3.	Whenever you want to use the spark-ec2 script, set the environment variables AWS\_ACCESS\_KEY\_ID and AWS\_SECRET\_ACCESS\_KEY to your Amazon EC2 access key ID and secret access key.

	I would suggest putting this in your `~/.bash_profile` for easy access

	```bash
	export AWS_ACCESS_KEY_ID='YOURACCESSKEYID'
	export AWS_SECRET_ACCESS_KEY='YOURSECRETACCESSKEY'
	```

4.	If you already have key access and forgot it or if you just lost your keys these can be obtained from the AWS homepage by clicking Account > Security Credentials > Access Credentials.

Spark-EC2
---------

Now navigate to you spark directory and go to the `ec2/` directory. This is where our scripts are for working with Spark on EC2. If installed with ```homebrew``` then navigate to ```/usr/local/Cellar/apache-spark/<version>/libexec/ec2/```

Here I will use ```gdelt-cluster``` as my cluster-name. You can name it whatever.

1.	to launch a new spark clusters

	```bash
	./spark-ec2 -k $AWS_KEY_PAIR -i $AWS_KEY_FILE -s 4 launch gdelt-cluster --instance-type=m4.large --region=us-west-2 --zone=us-west-2b --copy-aws-credentials
	```

2.	to login in to said cluster

	```bash
	./spark-ec2 -k $AWS_KEY_PAIR -i $AWS_KEY_FILE login gdelt-cluster --region=us-west-2 --zone=us-west-2b
	```

3.	Stop existing cluster

	```bash
	./spark-ec2 --region=us-west-2 stop gdelt-cluster
	```

4.	Start existing cluster

	```bash
	./spark-ec2 -i $AWS_KEY_FILE --region=us-west-2 start gdelt-cluster
	```

5.	copy spark-jar from local to aws master

	```bash
	scp -i $AWS_KEY_FILE target/scala-2.11/spark-csv_2.11-1.2.0.jar
	root@ec2-52-24-163-189.us-west-2.compute.amazonaws.com:/root/spark/lib
	```

6.	start a spark shell on spark-ec2

	To start the shell (with the ability to read in csv files) go to ```~/spark/bin/``` and normally you run :
	```bash
	./spark-shell --packages com.databricks:spark-csv_2.10:1.2.0
	```

	Here we are including the jar we created before and adding the `spark-csv` package to help us handle CSV files

	```bash
	./spark-shell --jars ../lib/spark-sql-gdelt_2.10-0.1-SNAPSHOT.jar --packages com.databricks:spark-csv_2.11:1.2.0
	```

7. Debug any potential errors

	One error I received when I first ran the ```spark-shell``` was:
	```
	INFO DataNucleus.Datastore: The class "org.apache.hadoop.hive.metastore.model.MResourceUri" is tagged as "embedded-only" so does not have its own datastore table.
	java.lang.RuntimeException: java.lang.RuntimeException: The root scratch dir: /tmp/hive on HDFS should be writable. Current permissions are: rwx--x--x
	```

	I fixed this by going to ```/root/ephemeral-hdfs/bin``` and then running
	```bash
	./hadoop fs -chmod 777 /tmp/hive
	```

8. Destroy Spark cluster
	```bash
	./spark-ec2 destroy gdelt-cluster
	```

S3 on Spark-ec2 Cluster
---

1. Getting GDELT data into S3
	* First we need to set up a way to send data to S3. Once you're logged into your EC2 shell, run:
	```bash
	curl https://raw.githubusercontent.com/timkay/aws/master/aws -o aws
	chmod +x aws
	perl aws --install
	```
	* Now that we can talk with S3, let's download some GDELT data using a python script ```download_gkg.py```:
	```python
	import os
	import datetime as dt
	import time
	import io

	import numpy as np
	import requests

	URL = "http://data.gdeltproject.org/gkg/" # The GKG data directory
	PATH = "~/data/GDELT_Events/gkg/" # The local directory to store the data

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
		```
		* Once we have all the data files, upload them to the bucket
		```bash
		s3put <bucket-name>/<folder>/<filename1>.csv
		s3put <bucket-name>/<folder>/<filename2>.csv
		```

2. Access data on spark cluster  
	* On your Spark cluster you need to go to your ```.bash_profile``` file and add this line so you can access your data from S3
	```bash
	export AWS_ACCESS_KEY_ID=<YOURKEY>
	export AWS_SECRET_ACCESS_KEY=<YOURSECRETKEY>
	```
	* Then you can access your data on S3 like
	```scala
	val column_num = 3 // specify the number of columns
	val x = sc.textFile("s3n://<bucket-name>/<folder>/*") // we can just specify all the files.

	x.take(column_num) // to make sure we read it correctly
	x.saveAsTextFile("s3n://<bucket-name>/<new or old folder>/")
	```

Zeppelin
---
We will build Zeppelin locally then use it to process data on our spark cluster

1. Make sure you have the proper security settings in your master worker of the EC2 cluster
	* EC2 > Network & Security > Select Master > Inbound
	* create a custom TCP rule where the port range is ``7077`` and the source is your public ip. You can use ``0.0.0.0/0`` but it is a security concern.
2. In your browser go to ``<master public DNS>:8080`` and at the top you will see ``spark://<master public DNS>:7077``, leave it for now, we will get back to it
3. Download apache Zeppelin
	```bash
	git clone git@github.com:apache/incubator-zeppelin.git
	```

4. Build for you cluster
	```bash
	mvn clean package -Pspark-1.5 -Dhadoop.version=2.2.0 -Phadoop-2.2 -DskipTests
	```

5. Start the notebook, once started go to [localhost:8080](http://localhost:8080/)
	```bash
	bin/zeppelin-daemon.sh start
	```

6. In master property, put (in the place of ``local[*]``) your master hostname with ``spark://`` at the beginning, and the port at the end.
7. Now your'e ready!

Zeppelin on EC2
-----------------

1.	install maven on ec2 with Yum

	```bash
	sudo wget http://repos.fedorapeople.org/repos/dchen/apache-maven/epel-apache-maven.repo -O /etc/yum.repos.d/epel-apache-maven.repo
	sudo sed -i s/\$releasever/6/g /etc/yum.repos.d/epel-apache-maven.repo
	sudo yum install -y apache-maven
	mvn --version
	```

2.	install npm on ec2 with Yum

	```bash
	sudo yum install nodejs npm --enablerepo=epel
	```

3.	install zeppelin on spark-ec2

	```bash
	mvn clean install -DskipTests -Dspark.version=1.5.0 -Dhadoop.version=1.0.4
	```

Verification
------------

1.	check how many lines in all the files in a folder

	```bash
	wc -l `find ./ -type f`
	```
	or
	```bash
	find . -name '*.CSV' | xargs wc -l
	```


2.	word count difference

	-	Total GDELT dataset

		```
		161004790 - spark
		159951604 - bash
		```

	-	Euromaidan GDELT subset

		```
		1828676
		1828675
		```
