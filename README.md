# reindexing_python_Script

Postgres Index Maintenance Script
Written in Python

Features:
Major Features Include:
* Identifies all indexes which are required to be reindex based on criteria like index-to-table ratio
* Filter out indexes which cannot be reindexed because of dependencies like foreign key
* Reindex in non-blocking fashion. (CONCURRENTLY)
* Optionaly  generate DDL scripts for reindexing job.
* Ability to perform maintenance without any additional scripts (Execute statements directly on database)


DEPENDANCIES:
This python program uses following python modules

psycopg2 -- for postgres connectivity 
argparse -- for argument parsing.

If this modules are missing in your OS, you may have to install them

On CentOS
--------
sudo yum install python-argparse.noarch 
sudo yum install python-psycopg2.x86_64

In addition to this it uses sys module also.

V2: Features
Redesigned and rewritten legacy queries to have 20 times performance gain.
New Dictionary to maintain the dependancy information.
Indexes with Primary keys will be addressed  
