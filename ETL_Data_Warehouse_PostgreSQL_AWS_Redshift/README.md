Project: Data Warehouse
Project to help Sparkify, a streaming startup to move their processes and data onto the cloud. This move has been required to accommodate their user base and song database growth. 

Project Description
Model ETL pipeline that extracts company data from S3, stage in Redshift, and transforms data into a set of dimensional tables and fact table for their analytics team to continue finding insights into what songs their users are listening to. Project will be done using Python.

The ETL pipeline consists of the following steps:
1)Programmatically create a Redshift Cluster
2)Create IAM role for 'ready only' access to S3 bucket. 
3)Estabilsh connection with redshift cluster. 
4)Create staging tables
5)Load data from S3 Bucket
6)Create fact and dimensional tables
7)Transform dimension 'dimtime'
8)Transfer data from staging tables to fact and dimensional tables

Running the files
Files should be openned thru command line and the following order:

1) For windows terminal (cmd):

1.1 Execute in the command line: "python create_tables.py" 
This program will create all the required tables on the Postgresql database.

1.2 Execute in the command line: "etl.py" (This program will iterate and extract each file from the directories, transform and load to the database tables)

Output of program execution should follow the example below:
root@55acxxxxxxxx:/home/workspace# python create_tables.py
root@55acxxxxxxxx:/home/workspace# python etl.py
root@55acxxxxxxxx:/home/workspace# 

