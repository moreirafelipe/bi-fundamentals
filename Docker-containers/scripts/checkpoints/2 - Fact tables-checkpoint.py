#Fact tables Notebook
#Load libraries and set general variables

# Import the libraries
from pyspark.sql import SparkSession
from datetime import datetime
import time

#Include jar to avoid error: java.sql.SQLException: No suitable driver
import os
os.environ['PYSPARK_SUBMIT_ARGS'] = '--jars file:////home/jovyan/work/postgresql-42.2.14.jar pyspark-shell'

#Create SparkSession
scSpark = SparkSession\
        .builder\
        .appName("fact_tables") \
        .getOrCreate()

# create properties
properties={"user": "postgres", "password": "postgres", "driver":"org.postgresql.Driver"}
url_source = 'jdbc:postgresql://source-db-container:5432/dvdrental'
url_target = 'jdbc:postgresql://dest-db-container:5432/dvdrental_staging'
url_dwh_target = 'jdbc:postgresql://dest-db-container:5432/dvdrental_dwh'

#Fact Tables Types
#Transactional

