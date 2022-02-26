<h3>README</h3>

<h4>Project: Data Warehouse</h4>
<p>Project to help Sparkify, a streaming startup to move their processes and data onto the cloud. This move has been required to accommodate their user base and song database growth. </p>

<h4>Project Description</h4>
<p>Model ETL pipeline that extracts company data from S3, stage in Redshift, and transforms data into a set of dimensional tables and fact table for their analytics team to continue finding insights into what songs their users are listening to. Project will be done using Python.</p>

<h4>ETL pipeline steps:</h4>
<p>1)Programmatically create a Redshift Cluster</p>
<p>2)Create IAM role for 'ready only' access to S3 bucket.</p> 
<p>3)Estabilsh connection with redshift cluster.</p>
<p>4)Create staging tables.</p>
<p>5)Load data from S3 Bucket.</p>
<p>6)Create fact and dimensional tables.</p>
<p>7)Transform dimension 'dimtime'.</p>
<p>8)Transfer data from staging tables to fact and dimensional tables.</p>

<h4>Files execution</h4>
<p>Files should be openned thru command line and the following order:</p>
<p>1) For windows terminal (cmd):</p>	

<p>1.1 Execute in the command line: "python create_tables.py"</p> 
This program will create all the required tables on the Postgresql database.
<p></p>
<p>1.2 Execute in the command line: "etl.py"</p> 
This program will iterate and extract each file from the directories, transform and load to the database tables.
<p></p>
<p>
Output of program execution should follow the example below:
root@55acxxxxxxxx:/home/workspace# python create_tables.py
root@55acxxxxxxxx:/home/workspace# python etl.py
root@55acxxxxxxxx:/home/workspace# </p>

