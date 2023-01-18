"""
This script manages the connection with hthe client database in order to recover the Fact tables for checkpoint activity topics of
bi fundamentals course (https://github.corp.globant.com/big-data-studio/bi-fundamentals/blob/master/Course-Material/3-DWH/DWH_Index.md). 
"""

#Fact tables Notebook
#Load libraries and set general variables

# Import the libraries
from pyspark.sql import SparkSession
from datetime import datetime
import asyncio
import asyncpg
import time

#Include jar to avoid error: java.sql.SQLException: No suitable driver
import os
os.environ['PYSPARK_SUBMIT_ARGS'] = '--jars file:////home/jovyan/work/postgresql-42.2.14.jar pyspark-shell'

#Create SparkSession
scSpark = SparkSession.builder.appName("fact_tables").getOrCreate()

#Implement native Python package for database connection
async def run():
        conn = await asyncpg.connect(user='postgres', password='password', database='dvdrental', host='127.0.0.1')
        values = await conn.fetch(
                'SELECT * FROM customers',
                10,
        )
        await conn.close()

loop = asyncio.new_event_loop()
asyncio.set_event_loop(loop)

# create properties
url_source = 'jdbc:postgresql://source-db-container:5432/dvdrental'
url_target = 'jdbc:postgresql://dest-db-container:5432/dvdrental_staging'
url_dwh_target = 'jdbc:postgresql://dest-db-container:5432/dvdrental_dwh'

#Fact Tables Types
#Transactional