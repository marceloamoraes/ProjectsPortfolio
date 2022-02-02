<h3>README</h3>

<h4> Project: Data Modeling with Postgres</h4>
<p>Project to help analytics team with the collection songs and user activity on their new music streaming app.</p>

<h4> Project Description </h4>
<p>Project for data modeling with Postgres and build an ETL pipeline using Python.</p> 
<p>The main feature is a ETL pipeline that transfers data from files in two local directories into these tables in Postgres using Python and SQL.</p>

<h4>Running the files</h4>
<p>Files should be openned thru command line and the following order:</p>

<p>1) For windows terminal:
    
    1.1 type on the command line: "python create_tables.py" 
    This program create all the required tables on the Postgresql database.
    
    1.2 type on the command line: "etl.py" (This program iterate and extrac each file from the directories, transform and load to the     database tables)
</p>

<h4>Examples of the program execution:</h4>

<p>root@d18a7dd42548:/home/workspace# python create_tables.py</p>
<p>root@d18a7dd42548:/home/workspace# python etl.py</p>

<p>
At the end of execution the following commands should apper, indicating a successful execution.</p>
<p>28/30 files processed.</p>
<p>29/30 files processed.</p>
<p>30/30 files processed.</p>
<p>root@d18a7dd42548:/home/workspace#</p>

