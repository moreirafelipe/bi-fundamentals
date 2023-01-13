"""
This script manages queries on dvd rental database tables by PySpark library in order to describe the Dvd Rentals's customer table schemma as a sample for
bi fundamentals course (https://github.corp.globant.com/big-data-studio/bi-fundamentals/blob/master/Course-Material/3-DWH/DWH_Index.md). 
"""

from pyspark.sql import SparkSession
from pyspark.sql import SQLContext

from datetime import datetime

# https://stackoverflow.com/questions/51772350/how-to-specify-driver-class-path-when-using-pyspark-within-a-jupyter-notebook

#Create SparkSession
scSpark = SparkSession.builder.appName("dvdrental ingestion").config('spark.driver.extraClassPath',
            "postgresql-42.2.14.jar").getOrCreate()

df = scSpark.read.format("jdbc").option("url", "jdbc:postgresql://source-db-container:5432/dvdrental").option("dbtable", "actor").option("user", "postgres").option("password", "postgres").option("driver", "org.postgresql.Driver").load()

df.printSchema()

# create properties
properties={"user": "postgres", "password": "postgres"}
url_source = 'jdbc:postgresql://source-db-container:5432/dvdrental'
url_target = 'jdbc:postgresql://dest-db-container:5432/dvdrental_staging'

jdbc_customer = scSpark.read.jdbc(url_source,"public.customer",properties=properties)

# let's check the schema
jdbc_customer.printSchema()