"""
This script manages DML queries on dvd rental database tables by PySpark library in order to cover the topics of
bi-fundamentals course (https://github.corp.globant.com/big-data-studio/bi-fundamentals/blob/master/Course-Material/3-DWH/DWH_Index.md). 
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import lit
from pyspark.sql import Row
from pyspark.sql.types import *

#Include jar to avoid error: java.sql.SQLException: No suitable driver
import os
os.environ['PYSPARK_SUBMIT_ARGS'] = '--jars file:////home/jovyan/work/postgresql-42.2.14.jar pyspark-shell'

#Create SparkSession
spark = SparkSession.builder.appName("scd2_demo").getOrCreate()

# create properties
properties={"user": "postgres", "password": "postgres", "driver":"org.apostgresql.Driver"}
#properties={"user": "postgres", "password": "postgres", "driver":"org.apostgresql.Driver"}
url_source = 'jdbc:postgresql://source-db-container:5432/dvdrental'
url_target = 'jdbc:postgresql://dest-db-container:5432/dvdrental_staging'
url_dwh_target = 'jdbc:postgresql://dest-db-container:5432/dvdrental_dwh'

# ############## generate current_scd2 dataset ############## #
# Create a list of rows
rows = [
    (1, "John", "Smith", "G", "123 Main Street", "Springville", "VT", "01234-5678", 289374, "2014-01-01", "9999-12-31", True),
    (2, "Susan", "Jones", "L", "987 Central Avenue", "Central City", "MO", "49257-2657", 862447, "2015-03-23", "2018-11-17", False),
    (3, "Susan", "Harris", "L", "987 Central Avenue", "Central City", "MO", "49257-2657", 862447, "2018-11-18", "9999-12-31", True),
    (4, "William", "Chase", "X", "57895 Sharp Way", "Oldtown", "CA", "98554-1285", 31568, "2018-12-07", "9999-12-31", True)
]

# Define the schema for the DataFrame
schema = StructType([
    StructField("customer_dim_key", LongType(), True),
    StructField("first_name", StringType(), True),
    StructField("last_name", StringType(), True),
    StructField("middle_initial", StringType(), True),
    StructField("address", StringType(), True),
    StructField("city", StringType(), True),
    StructField("state", StringType(), True),
    StructField("zip_code", StringType(), True),
    StructField("customer_number", LongType(), True),
    StructField("eff_start_date", StringType(), True),
    StructField("eff_end_date", StringType(), True),
    StructField("is_current", BooleanType(), True)
])

# Create the DataFrame
df_current_scd2 = spark.createDataFrame(rows, schema)

# Show the DataFrame
df_current_scd2.orderBy("customer_dim_key").show(10, False)

# Create a temporary view
df_current_scd2.createOrReplaceTempView("current_scd2")
# ############## review dataset ############## #
df_current_scd2.orderBy("customer_dim_key")
df_current_scd2.show(10, False)

df_customer_data = spark.createDataFrame([
    (289374, "John", "Smith", "G", "456 Derry Court", "Springville", "VT", "01234-5678"),
    (932574, "Lisa", "Cohen", "S", "69846 Mason Road", "Atlanta", "GA", "26584-3591"),
    (862447, "Susan", "Harris", "L", "987 Central Avenue", "Central City", "MO", "49257-2657"),
    (31568, "William", "Chase", "X", "57895 Sharp Way", "Oldtown", "CA", "98554-1285")
], ["customer_number", "first_name", "last_name", "middle_initial", "address", "city", "state", "zip_code"])

df_customer_data.createOrReplaceTempView("customer_data")

df_customer_data.show(10, False)

# ############## create new current recs dataaset ############## #
df_new_curr_recs = spark.sql(
    """
    SELECT   current_scd2.customer_dim_key,
            customer_data.customer_number,
            customer_data.first_name,
            customer_data.last_name,
            customer_data.middle_initial,
            customer_data.address,
            customer_data.city,
            customer_data.state,
            customer_data.zip_code,
            DATE(FROM_UTC_TIMESTAMP(CURRENT_TIMESTAMP, 'EST'))
                AS eff_start_date,
            DATE('9999-12-31') AS eff_end_date,
            BOOLEAN(1) AS is_current
    FROM     customer_data customer_data
            INNER JOIN current_scd2 current_scd2
                ON current_scd2.customer_number = customer_data.customer_number
                AND current_scd2.is_current = True
    WHERE    NVL(customer_data.first_name, '') <> NVL(current_scd2.first_name, '')
            OR NVL(customer_data.last_name, '') <> NVL(current_scd2.last_name, '')
            OR NVL(customer_data.middle_initial, '') <> NVL(current_scd2.middle_initial, '')
            OR NVL(customer_data.address, '') <> NVL(current_scd2.address, '')
            OR NVL(customer_data.city, '') <> NVL(current_scd2.city, '')
            OR NVL(customer_data.state, '') <> NVL(current_scd2.state, '')
            OR NVL(customer_data.zip_code, '') <> NVL(current_scd2.zip_code, '')
    """
)

df_new_curr_recs.createOrReplaceTempView("new_curr_recs")
# ############## review dataset ############## #

df_new_curr_recs.show(10, False)

# ########### isolate keys of records to be modified ########### #
df_modfied_keys = df_new_curr_recs.select("customer_dim_key")

df_modfied_keys.createOrReplaceTempView("modfied_keys")
df_modfied_keys.show()

# ############## create new hist recs dataaset ############## #
df_new_hist_recs = spark.sql(
    """
    SELECT   current_scd2.customer_dim_key,
            current_scd2.customer_number,
            current_scd2.first_name,
            current_scd2.last_name,
            current_scd2.middle_initial,
            current_scd2.address,
            current_scd2.city,
            current_scd2.state,
            current_scd2.zip_code,
            current_scd2.eff_start_date,
            DATE_SUB(
                DATE(FROM_UTC_TIMESTAMP(CURRENT_TIMESTAMP, 'EST')), 1
            ) AS eff_end_date,
            BOOLEAN(0) AS is_current
    FROM     current_scd2 current_scd2
            INNER JOIN modfied_keys modfied_keys
                ON modfied_keys.customer_dim_key = current_scd2.customer_dim_key
    WHERE    current_scd2.is_current = True
    """
)
#df_new_hist_recs.coalesce(1).write.mode("overwrite").parquet(v_s3_path + "/new_hist_recs/")
df_new_hist_recs.createOrReplaceTempView("new_hist_recs")
# ############## review dataset ############## #
#df_new_hist_recs = spark.read.parquet(v_s3_path + "/new_hist_recs/*").orderBy("customer_number")
df_new_hist_recs.show(10, False)

# ############## create unaffected recs dataset ############## #
df_unaffected_recs = spark.sql(
    """
    SELECT   current_scd2.customer_dim_key,
            current_scd2.customer_number,
            current_scd2.first_name,
            current_scd2.last_name,
            current_scd2.middle_initial,
            current_scd2.address,
            current_scd2.city,
            current_scd2.state,
            current_scd2.zip_code,
            current_scd2.eff_start_date,
            current_scd2.eff_end_date,
            current_scd2.is_current
    FROM     current_scd2 current_scd2
            LEFT OUTER JOIN modfied_keys modfied_keys
                ON modfied_keys.customer_dim_key = current_scd2.customer_dim_key
    WHERE    modfied_keys.customer_dim_key IS NULL
    """
)
#df_unaffected_recs.coalesce(1).write.mode("overwrite").parquet(v_s3_path + "/unaffected_recs/")
df_unaffected_recs.createOrReplaceTempView("unaffected_recs")
# ############## review dataset ############## #
#df_unaffected_recs = spark.read.parquet(v_s3_path + "/unaffected_recs/*").oorderBy("customer_number").rderBy("customer_number")
df_unaffected_recs.orderBy("customer_number").show(10, False)

# ############## create new recs dataset ############## #
df_new_cust = spark.sql(
    """
    SELECT   customer_data.customer_number,
            customer_data.first_name,
            customer_data.last_name,
            customer_data.middle_initial,
            customer_data.address,
            customer_data.city,
            customer_data.state,
            customer_data.zip_code,
            DATE(FROM_UTC_TIMESTAMP(CURRENT_TIMESTAMP, 'EST'))
                AS eff_start_date,
            DATE('9999-12-31') AS eff_end_date,
            BOOLEAN(1) AS is_current
    FROM     customer_data customer_data
            LEFT OUTER JOIN current_scd2 current_scd2
                ON current_scd2.customer_number = customer_data.customer_number
    WHERE    current_scd2.customer_number IS NULL
    """
)
#df_new_cust.coalesce(1).write.mode("overwrite").parquet(v_s3_path + "/new_cust/")
df_new_cust.createOrReplaceTempView("new_cust")
# ############## review dataset ############## #
#df_new_cust = spark.read.parquet(v_s3_path + "/new_cust/*").orderBy("customer_number")
df_new_cust.orderBy("customer_number").show(10, False)

v_max_key = spark.sql(
    "SELECT STRING(MAX(customer_dim_key)) FROM current_scd2"
).collect()[0][0]

print(v_max_key)

hd_new_scd2 = """
 WITH a_cte
 AS   (
        SELECT     new_cust.first_name, new_cust.last_name,
                   new_cust.middle_initial, new_cust.address,
                   new_cust.city, new_cust.state, new_cust.zip_code,
                   new_cust.customer_number, new_cust.eff_start_date,
                   new_cust.eff_end_date, new_cust.is_current
        FROM       new_cust new_cust
        UNION ALL
        SELECT     new_curr_recs.first_name, new_curr_recs.last_name,
                   new_curr_recs.middle_initial, new_curr_recs.address,
                   new_curr_recs.city, new_curr_recs.state, new_curr_recs.zip_code,
                   new_curr_recs.customer_number, new_curr_recs.eff_start_date,
                   new_curr_recs.eff_end_date, new_curr_recs.is_current
        FROM       new_curr_recs new_curr_recs
      )
  ,   b_cte
  AS  (
        SELECT  ROW_NUMBER() OVER(ORDER BY a_cte.eff_start_date)
                    + BIGINT('{v_max_key}') AS customer_dim_key,
                a_cte.first_name, a_cte.last_name,
                a_cte.middle_initial, a_cte.address,
                a_cte.city, a_cte.state, a_cte.zip_code,
                a_cte.customer_number, a_cte.eff_start_date,
                a_cte.eff_end_date, a_cte.is_current
        FROM    a_cte a_cte
      )
  SELECT  customer_dim_key, first_name, last_name,
          middle_initial, address,
          city, state, zip_code,
          customer_number, eff_start_date,
          eff_end_date, is_current
  FROM    b_cte
  UNION ALL
  SELECT  customer_dim_key, first_name,  last_name,
          middle_initial, address,
          city, state, zip_code,
          customer_number, eff_start_date,
          eff_end_date, is_current
  FROM    unaffected_recs
  UNION ALL
  SELECT  customer_dim_key, first_name,  last_name,
          middle_initial, address,
          city, state, zip_code,
          customer_number, eff_start_date,
          eff_end_date, is_current
  FROM    new_hist_recs
"""
df_new_scd2 = spark.sql(hd_new_scd2.replace("{v_max_key}", v_max_key))
# ############## review dataset ############## #
#df_new_scd2.coalesce(1).write.mode("overwrite").parquet(v_s3_path + "/new_scd2/")
#df_new_scd2 = spark.read.parquet(v_s3_path + "/new_scd2/*").orderBy("customer_dim_key")
df_new_scd2.orderBy("customer_dim_key").show(10, False)