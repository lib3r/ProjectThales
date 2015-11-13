RDS Set Up
---

1. Connect to RDS

2. Data dump of mysql 
	```bash
	mysqldump -u root security_master > backup.sql
	```

3. Connect to RDS
	```bash
	mysql -h sec-master.canroa1ethiw.us-west-2.rds.amazonaws.com -P 3306 -u evmagari -p
	```
