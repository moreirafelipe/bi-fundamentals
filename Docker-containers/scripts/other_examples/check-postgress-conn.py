"""
This script checks connection with dvd rental postgres database by PySpark library in order stabilish the lintegration layer and cover the topics of
bi fundamentals course (https://github.corp.globant.com/big-data-studio/bi-fundamentals/blob/master/Course-Material/3-DWH/DWH_Index.md). 
"""

from pyspark.sql import SparkSession
from pyspark.sql import SQLContext

from datetime import datetime

# https://stackoverflow.com/questions/51772350/how-to-specify-driver-class-path-when-using-pyspark-within-a-jupyter-notebook

#Create SparkSession
scSpark = SparkSession.builder.appName("dvdrental ingestion").config('spark.driver.extraClassPath',
            "postgresql-42.2.14.jar").getOrCreate()

df = scSpark.read.format("jdbc").option("url", "jdbc:postgresql://source-db-container:5432/dvdrental").option("dbtable", "customer").option("user", "postgres").option("password", "postgres").option("driver", "org.postgresql.Driver").load()

df.printSchema()

jardrv = "~/drivers/postgresql-42.2.14.jar"
spark = SparkSession.builder.config('spark.driver.extraClassPath', jardrv).getOrCreate()
url = 'jdbc:postgresql://source-db-container:5432/dvdrental'
properties = {'user': 'postgres', 'password': 'postgres'}
df = spark.read.jdbc(url=url, table='actor', properties=properties)
df.show()

print (type(df))

display(df.groupBy('actor_id'))

# Crea una tabla nueva
url_target = 'jdbc:postgresql://dest-db-container:5432/dvdrental_dwh'
properties = {'user': 'postgres', 'password': 'postgres'}

df.filter(df.first_name == "Bob").write.mode('overwrite').jdbc(url=url_target, table="public.actor2",properties=properties)

# Probar SERIAL columns
jardrv = "~/drivers/postgresql-42.2.14.jar"
spark = SparkSession.builder.config('spark.driver.extraClassPath', jardrv).getOrCreate()
url = 'jdbc:postgresql://source-db-container:5432/dvdrental'
properties = {'user': 'postgres', 'password': 'postgres'}
df_customer = spark.read.jdbc(url=url, table='customer', properties=properties)
df_customer.show()

# MEte registros a una tabla existente
url_target = 'jdbc:postgresql://dest-db-container:5432/dvdrental_dwh'
properties = {'user': 'postgres', 'password': 'postgres'}

#quitar una columna
df_customer= df_customer.drop("address_id")

df_customer.filter(df_customer.store_id == "1").write.mode('append').jdbc(url=url_target, table="datalake_raw.customer_dim",properties=properties)

# MEte registros a una tabla existente en STaging para ELT
url_target = 'jdbc:postgresql://dest-db-container:5432/dvdrental_staging'
properties = {'user': 'postgres', 'password': 'postgres'}

#quitar una columna
df_customer= df_customer.drop("address_id")

df_customer.filter(
df_customer.store_id == "1").write.mode('append').jdbc(url=url_target,
table="datalake_raw.customer_dim",properties=properties
)

spark.version