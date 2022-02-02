README

Project: Data Modeling with Postgres
This project plans to help analytics team with the collection songs and user activity on their new music streaming app.

Project Description
Project for data modeling with Postgres and build an ETL pipeline using Python. 
The main feature is a ETL pipeline that transfers data from files in two local directories into these tables in Postgres using Python and SQL.

Running the files
Files should be openned thru command line and the following order:

1) For windows terminal:
    
    1.1 type on the command line: "python create_tables.py" 
    This program create all the required tables on the Postgresql database.
    
    1.2 type on the command line: "etl.py" (This program iterate and extrac each file from the directories, transform and load to the     database tables)

Examples of the program execution:

root@d18a7dd42548:/home/workspace# python create_tables.py
root@d18a7dd42548:/home/workspace# python etl.py

At the end of execution the following commands should apper, indicating a successful execution.

28/30 files processed.
29/30 files processed.
30/30 files processed.
root@d18a7dd42548:/home/workspace#
