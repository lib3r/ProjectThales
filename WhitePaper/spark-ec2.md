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

Now navigate to you spark directory and go to the `ec2/` directory. This is where our scripts are for working with Spark on EC2.

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

	Here we are including the jar we created before and adding the `spark-csv` package to help us handle CSV files

	```bash
	./spark-shell --jars ../lib/spark-sql-gdelt_2.10-0.1-SNAPSHOT.jar --packages com.databricks:spark-csv_2.11:1.2.0
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
