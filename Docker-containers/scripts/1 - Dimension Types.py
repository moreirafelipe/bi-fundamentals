# Dimension Types Notebook
## Load libraries and set general variables

# Import the libraries
from pyspark.sql import SparkSession
from datetime import datetime
from pyspark.sql.window import Window
from pyspark.sql.functions import row_number, coalesce, lit, col, current_timestamp, from_utc_timestamp, to_date
import time

from pyspark.sql.functions import col, current_timestamp

#Include jar to avoid error: java.sql.SQLException: No suitable driver
import os
os.environ['PYSPARK_SUBMIT_ARGS'] = '--jars file:////home/jovyan/work/postgresql-42.2.14.jar pyspark-shell'

#Create SparkSession
scSpark = SparkSession\
        .builder\
        .appName("dimension_types") \
        .getOrCreate()

# Other option to create the SparkSession
jardrv = "~/drivers/postgresql-42.2.14.jar"

#Create SparkSession
scSpark2 = SparkSession\
        .builder\
        .appName("dimension_types")\
        .config('spark.driver.extraClassPath',
                jardrv)\
        .getOrCreate()

        # create properties
properties={"user": "postgres", "password": "postgres", "driver":"org.postgresql.Driver"}
url_source = 'jdbc:postgresql://source-db-container:5432/dvdrental'
url_target = 'jdbc:postgresql://dest-db-container:5432/dvdrental_staging'
url_dwh_target = 'jdbc:postgresql://dest-db-container:5432/dvdrental_dwh'

## Customer Dimension: Natural vs Surrogated Key

"""
A **natural key** a single column or set of columns that uniquely identifies a single record in a table, where the key columns are made up of real data.  When I say “real data” I mean data that has meaning and occurs naturally in the world of data.  A natural key is a column value that has a relationship with the rest of the column values in a given data record.   Here are some examples of natural keys values: Social Security Number, ISBN, and TaxId.

A **surrogate** keys  don’t have a natural relationship with the rest of the columns in a table.  The surrogate key is just a value that is generated and then stored with the rest of the columns in a record.  The key value is typically generated at run time right before the record is inserted into a table.   It is sometimes also referred to as a dumb key, because there is no meaning associated with the value.  Surrogate keys are commonly a numeric number. 
 
The advantage of natural keys is that they exist already, you don't need to introduce a new "unnatural" value to your data schema. However, the disadvantage of natural keys is that because they have business meaning they are effectively coupled to your business: you may need to rework your key when your business requirements change.

"""

# **Let's read the customer table**

jdbc_customer = scSpark.read \
    .jdbc(url_source, 
         "public.customer",
         properties=properties)

# let's check the schema
jdbc_customer.printSchema()

# We can create temp table for use spark sql
jdbc_customer.createOrReplaceTempView("source_customer")

#Let's include a new column for data load time

# Let's use sql in the SparkSession
customer_target = scSpark.sql("select *,now() as load_date  from source_customer")

#Write info into staging area

# Let's do it 4 times to duplicate data
for i in range (0,3):
    customer_target.write \
        .mode('append') \
        .jdbc(url=url_target, table="public.stg_dim_customer",properties=properties)
    
    #wait 3 sec for generating diff load_date
    time.sleep(3)

#As the Natural Key starts in 1, let's create a table with a Surrogate Key

""" 
-- Run this in DB to create a new table with a fake surrogate key in dvdrental_staging DB
CREATE TABLE AS public.stg_dim_customer_surrogate AS
SELECT row_number () over (order by load_date , customer_id ) as surrogate_key,
customer_id, store_id, first_name, last_name, email, address_id, activebool, create_date, last_update, active, load_date
FROM public.stg_dim_customer;

select *
  from public.stg_dim_customer_surrogate
 order by customer_id

"""

### Role Playing Dimension: Date and Time
"""
A single physical dimension can be referenced multiple times in a fact table, with each reference linking to a logically distinct role for the dimension. . This is most commonly seen in dimensions such as Time and Customer. We can see that also in city/province/country dimensions.
"""

#Create Date Dimension in DWH

"""
create table public.dim_date AS
SELECT	
	to_char(datum,'YYYYMMDD') as Date,
	extract(year from datum) AS Year,
	extract(month from datum) AS Month,
	-- Localized month name
	to_char(datum, 'TMMonth') AS MonthName,
	extract(day from datum) AS Day,
	extract(doy from datum) AS DayOfYear,
	-- Localized weekday
	to_char(datum, 'TMDay') AS WeekdayName,
	-- ISO calendar week
	extract(week from datum) AS CalendarWeek,
	to_char(datum, 'dd. mm. yyyy') AS FormattedDate,
	'Q' || to_char(datum, 'Q') AS Quartal,
	to_char(datum, 'yyyy/"Q"Q') AS YearQuartal,
	to_char(datum, 'yyyy/mm') AS YearMonth,
	-- ISO calendar year and week
	to_char(datum, 'iyyy/IW') AS YearCalendarWeek,
	-- Weekend
	CASE WHEN extract(isodow from datum) in (6, 7) THEN 'Weekend' ELSE 'Weekday' END AS Weekend,
	-- Fixed holidays 
        -- for America
        CASE WHEN to_char(datum, 'MMDD') IN ('0101', '0704', '1225', '1226')
		THEN 'Holiday' ELSE 'No holiday' END
		AS AmericanHoliday,
        -- for Austria
	CASE WHEN to_char(datum, 'MMDD') IN 
		('0101', '0106', '0501', '0815', '1101', '1208', '1225', '1226') 
		THEN 'Holiday' ELSE 'No holiday' END 
		AS AustrianHoliday,
        -- for Canada
        CASE WHEN to_char(datum, 'MMDD') IN ('0101', '0701', '1225', '1226')
		THEN 'Holiday' ELSE 'No holiday' END 
		AS CanadianHoliday,
	-- Some periods of the year, adjust for your organisation and country
	CASE WHEN to_char(datum, 'MMDD') BETWEEN '0701' AND '0831' THEN 'Summer break'
	     WHEN to_char(datum, 'MMDD') BETWEEN '1115' AND '1225' THEN 'Christmas season'
	     WHEN to_char(datum, 'MMDD') > '1225' OR to_char(datum, 'MMDD') <= '0106' THEN 'Winter break'
		ELSE 'Normal' END
		AS Period,
	-- ISO start and end of the week of this date
	datum + (1 - extract(isodow from datum))::integer AS CWStart,
	datum + (7 - extract(isodow from datum))::integer AS CWEnd,
	-- Start and end of the month of this date
	datum + (1 - extract(day from datum))::integer AS MonthStart,
	(datum + (1 - extract(day from datum))::integer + '1 month'::interval)::date - '1 day'::interval AS MonthEnd
FROM (	
	SELECT '2000-01-01'::DATE + sequence.day AS datum
	FROM generate_series(0,7652) AS sequence(day)
	GROUP BY sequence.day
     ) DQ
order by 1
"""

#Create Time Dimension in DWH

""" 

create table public.dim_time AS
select to_char(minute, 'hh24:mi') AS TimeOfDay,
	-- Hour of the day (0 - 23)
	extract(hour from minute) as Hour, 
	-- Extract and format quarter hours
	to_char(minute - (extract(minute from minute)::integer % 15 || 'minutes')::interval, 'hh24:mi') ||
	' – ' ||
	to_char(minute - (extract(minute from minute)::integer % 15 || 'minutes')::interval + '14 minutes'::interval, 'hh24:mi')
		as QuarterHour,
	-- Minute of the day (0 - 1439)
	extract(hour from minute)*60 + extract(minute from minute) as minute,
	-- Names of day periods
	case when to_char(minute, 'hh24:mi') between '06:00' and '08:29'
		then 'Morning'
	     when to_char(minute, 'hh24:mi') between '08:30' and '11:59'
		then 'AM'
	     when to_char(minute, 'hh24:mi') between '12:00' and '17:59'
		then 'PM'
	     when to_char(minute, 'hh24:mi') between '18:00' and '22:29'
		then 'Evening'
	     else 'Night'
	end as DaytimeName,
	-- Indicator of day or night
	case when to_char(minute, 'hh24:mi') between '07:00' and '19:59' then 'Day'
	     else 'Night'
	end AS DayNight
from (SELECT '0:00'::time + (sequence.minute || ' minutes')::interval AS minute
	FROM generate_series(0,1439) AS sequence(minute)
	GROUP BY sequence.minute
     ) DQ
order by 1
"""

### Junk Dimension 

""" 


To understand **junk dimensions** let's define **cardinality**. This is the number of different values in a table. A low-cardinality shows *few distinct values* (many rows for each value), and a high-cardinality means a *lot of different values* (few rows for each value)

A **junk dimension** combines *several low-cardinality flags and attributes into a single dimension table* rather than modeling them as separate dimensions. There are good reasons to create this combined dimension, including reducing the size of the fact table and making the dimensional model easier to work with. 

**Centipede fact tables** also result when designers embed numerous foreign keys to individual low-cardinality dimension tables rather than creating a junk dimension.
"""

#Load `category`, `language` y `film`

# read category
jdbc_category = scSpark.read \
    .jdbc(url_source, 
         "public.category",
         properties=properties)

# Create temp table for using spark sql
jdbc_category.createOrReplaceTempView("source_category")
category_target = scSpark.sql("select *,now() as load_date  from source_category")

# write category
for i in range (1,4):
    category_target.write \
            .mode('append') \
            .jdbc(url=url_target, table="public.stg_dim_category",properties=properties)
    
    #wait 2 sec for enerating diff load_date
    time.sleep(2)

# read language
jdbc_language = scSpark.read \
    .jdbc(url_source, 
         "public.language",
         properties=properties)

# Create temp table for using spark sql
jdbc_language.createOrReplaceTempView("source_language")
language_target = scSpark.sql("select *,now() as load_date  from source_language")

# write category
for i in range (1,3):
    language_target.write \
            .mode('append') \
            .jdbc(url=url_target, table="public.stg_dim_language",properties=properties)
    
    #wait 2 sec for enerating diff load_date
    time.sleep(2)


# read film
jdbc_film = scSpark.read \
    .jdbc(url_source, 
         "public.film",
         properties=properties)

# Create temp table for using spark sql
jdbc_film.createOrReplaceTempView("source_film")
film_target = scSpark.sql("select *,now() as load_date  from source_film")

# write category
for i in range (0,3):
    film_target.write \
            .mode('append') \
            .jdbc(url=url_target, table="public.stg_dim_film",properties=properties)
    
    #wait 2 sec for enerating diff load_date
    time.sleep(2)

# read film_category
jdbc_film_category = scSpark.read \
    .jdbc(url_source, 
         "public.film_category",
         properties=properties)

# Create temp table for using spark sql
jdbc_film_category.createOrReplaceTempView("source_film_category")
film_category_target = scSpark.sql("select *,now() as load_date  from source_film_category")

# write category
for i in range (0,3):
    film_category_target.write \
            .mode('append') \
            .jdbc(url=url_target, table="public.stg_dim_film_category",properties=properties)
    
    #wait 2 sec for enerating diff load_date
    time.sleep(2)
#read source film as a dataframe
df_film = scSpark.read.table("source_film")

#read source language as a dataframe
df_language = scSpark.read.table("source_language")

#read spirce_film_category as a dataframe
df_film_category = scSpark.read.table("source_film_category")

#read source_category as a dataframe
df_category = scSpark.read.table("source_category")

#join the dataframes
df_joined = df_film.join(df_language, df_film["language_id"] == df_language["language_id"], "inner") \
  .join(df_film_category, df_film["film_id"] == df_film_category["film_id"], "inner") \
    .join(df_category, df_film_category["category_id"] == df_category["category_id"], "inner") \
      .select("film_id", "title", "description", "name as category_name", "l.name as language_name", "rental_duration", "rental_rate", "length", "replacement_cost", "rating", "release_year", "special_features", "fulltext", current_timestamp().alias("load_date"))

df_joined.write \
          .mode('append') \
          .jdbc(url=url_target, table="public.stg_dim_junk_film",properties=properties)

### Conformed Dimension

""" 
**Conformed dimensions** are dimensions that are shared by multiple stars. They are used to compare the measures from each star schema.

Conformed dimensions are those dimensions which have been designed in such a way that the dimension can be used across many fact tables in different subject areas of the warehouse. It is imperative that the designer plan for these dimensions as they will provide reporting consistency across subject areas and reduce the development costs of those subject areas via reuse of existing dimensions. The date dimension is an excellent example of a conformed dimension. Most warehouses only have a single date dimension used throughout the warehouse.
"""

#**Create customer tables**
df_tweets = scSpark.read.format('csv').options(header= 'true').load('tweets.csv')

# Create temp table for using spark sql
df_tweets.createOrReplaceTempView("df_tweets")

# Modify fields
tweets_target = df_tweets.select(col("tweet_id").cast("string").alias("tweet_id"), "sentiment", "sentiment_confidence", 
                                 "negativereason_confidence", "name", "retweet_count", "tweet_coord", 
                                 "tweet_created", "user_timezone", current_timestamp().alias("load_date"))

# write to table
tweets_target.write \
            .mode('append') \
            .jdbc(url=url_target, table="public.fact_tweets",properties=properties)


# read_csv for conformed_customer
df_conformed_customer = scSpark.read.format('csv').options(header= 'true').load('conformed_customer.csv')

# write to table
df_conformed_customer.write \
            .mode('append') \
            .jdbc(url=url_target, table="public.conformed_customer",properties=properties)

""" 
This query returns the number of tweets by sentiment that every user with rentals has made in Twitter
"""

""" 
-- run this query in DB dvdrental_staging 


with rental_customer as 
  (select row_number() over (partition by customer_id  order by load_date desc) as load_number,
    *
    from public.stg_dim_customer sdc     
    ) 
select cc.rental_customer_id , cc.twitter_id ,
       rc.first_name, rc.last_name, sentiment, count(1)
  from public.conformed_customer cc 
  join rental_customer rc
    on cast (cc.rental_customer_id as int)= rc.customer_id
  join public.fact_tweets ft 
    on trim(ft."name") = trim(cc.twitter_id )
  where rc.load_number = 1
 group by cc.customer_conformed_id , cc.rental_customer_id , cc.twitter_id ,
       rc.first_name, rc.last_name, sentiment
 order by cc.twitter_id ,ft.sentiment

"""

### Slow Changing Dimension

""" 
A Slowly Changing Dimension (SCD) is a dimension that stores and manages both current and historical data over time in a data warehouse. 

There are three types of SCDs and you can use 

#### What are the three types of SCDs?

The three types of SCDs are:

##### Type 1 SCDs - Overwriting

In a Type 1 SCD the new data overwrites the existing data. Thus the existing data is lost as it is not stored anywhere else. This is the default type of dimension you create. You do not need to specify any additional information to create a Type 1 SCD.

##### Type 2 SCDs - Creating another dimension record

A Type 2 SCD retains the full history of values. When the value of a chosen attribute changes, the current record is closed. A new record is created with the changed data values and this new record becomes the current record. Each record contains the effective time and expiration time to identify the time period between which the record was active.

##### Type 3 SCDs - Creating a current value field

A Type 3 SCD stores two versions of values for certain selected level attributes. Each record stores the previous value and the current value of the selected attribute. When the value of any of the selected attributes changes, the current value is stored as the old value and the new value becomes the current value. """

# Let's create an SCD2 dimension
# Time to load for the first time

# as we are merge address and customer, I need to load address to staging db

# read address
jdbc_address = scSpark.read \
    .jdbc(url_source, 
         "public.address",
         properties=properties)

# Create temp table for using spark sql
jdbc_address.createOrReplaceTempView("source_address")
address_target = scSpark.sql("select *,now() as load_date  from source_address")

# write category
for i in range (1,4):
    address_target.write \
            .mode('append') \
            .jdbc(url=url_target, table="public.stg_dim_address",properties=properties)
    
    #wait 2 sec for enerating diff load_date
    time.sleep(2)

### get recent_staging_records #####

# read customer from  staging DB
jdbc_staging_customer = scSpark.read \
    .jdbc(url_target, 
         "public.stg_dim_customer",
         properties=properties)

jdbc_staging_customer.createOrReplaceTempView("stg_dim_customer")

# read address from  staging DB
jdbc_staging_address = scSpark.read \
    .jdbc(url_target, 
         "public.stg_dim_address",
         properties=properties)

jdbc_staging_address.createOrReplaceTempView("stg_dim_address")


w_customer = Window.partitionBy("customer_id").orderBy(col("load_date").desc())
w_address = Window.partitionBy("address_id").orderBy(col("load_date").desc())

recent_customer_staging_records = jdbc_staging_customer.withColumn("row_num", row_number().over(w_customer))
recent_address_staging_records = jdbc_staging_address.withColumn("row_num", row_number().over(w_address))

df_first_customer_records = recent_customer_staging_records.filter("row_num = 1").join(recent_address_staging_records.filter("row_num = 1"), "address_id")\
    .select(row_number().over(Window.orderBy("customer_id")).alias("customer_dim_id"), "customer_id", "first_name", "last_name", "email", "activebool", "address", "address2", "district", "postal_code", "phone", "create_date", "last_update", "active",coalesce(col("last_update").cast("date"), col("create_date")).alias("valid_from"),lit("9999-12-31").alias("valid_to"),lit(1).alias("dim_active"))

df_first_customer_records.write \
            .mode('overwrite') \
            .jdbc(url=url_dwh_target, table="public.dim_customer",properties=properties)

df_first_customer_records.createOrReplaceTempView("recent_staging_records")
# ############## review dataset ############## #

df_first_customer_records.orderBy("customer_dim_id").show(5, False)


##### Time to play with dimension SCD type 2

""" 
- Change one record and insert another one
select *
  from public.customer
  where customer_id = 5;
  
-- insert into Dvdrental DB
update public.customer
   set last_name = 'Brown-Vazquez'
 where customer_id = 5

-- create new customer
 insert into public.customer (store_id ,first_name ,last_name ,email ,address_id , activebool ,active )
 values (1,'Luka','Doncic','luka@gmail.com',23,true,1)
"""

#Now we need to load again the information to staging DB

# Check we have the update in the source DB
customer_target.filter(customer_target.customer_id == 5).show()

# Let's write the changes into Staging DB

customer_target.write \
        .mode('append') \
        .jdbc(url=url_target, table="public.stg_dim_customer",properties=properties)


""" 
-- run query in Staging DB
SELECT customer_id, store_id, first_name, last_name, email, address_id, activebool, create_date, last_update, active, load_date
FROM public.stg_dim_customer
where customer_id in (5,600)
"""
#let's check we have the new records in STG
#df_first_customer_records.filter(df_first_customer_records.customer_id == 600).show()
df_first_customer_records.filter(df_first_customer_records.customer_id == 5).show()

""" 
 #### Steps for update a SCD2
 
 Once the new record is in STG we need to:
 
1. Create new current records for existing customers - (new Elizabeth Brown-Vazquez)
2. Find previous current records to expire - (PK for Elizabeth Brown-Vazquez)
3. Expire previous current records - (old Elizabeth Brown-Vazquez)
4. Isolate unaffected records - (all the other customers)
5. Create records for new customers - (new Luca Doncic)
6. Combine the datasets for new SCD2 - Insert into dimension table
"""

# read SCD2 
jdbc_customer_dim = scSpark.read \
    .jdbc(url_dwh_target, 
         "public.dim_customer",
         properties=properties)

# Create temp table for using spark sql
jdbc_customer_dim.createOrReplaceTempView("current_scd2")

jdbc_customer_dim.show(5)

# ############## 1. Create new current records for existing customers ############## #
# I should have one new record for Elizabeth Brown-Vazquez, customer_id 5

#Select only the rows where the values in the first_name, last_name, email, address, address2, district, postal_code and phone columns are different between recent_staging_records and current_scd2
df_new_curr_recs = recent_customer_staging_records.join(current_scd2, on=["customer_id"], how="inner")\
    .filter(col("dim_active") == 1)\
    .filter(
        (col("s.first_name") != col("t.first_name")) |
        (col("s.last_name") != col("t.last_name")) |
        (col("s.email") != col("t.email")) |
        (col("s.address") != col("t.address")) |
        (col("s.address2") != col("t.address2")) |
        (col("s.district") != col("t.district")) |
        (col("s.postal_code") != col("t.postal_code")) |
        (col("s.phone") != col("t.phone"))
    )\
    .select("t.customer_dim_id", "s.customer_id", "s.first_name", "s.last_name", "s.email", "s.activebool", "s.address", "s.address2", "s.district", "s.postal_code", "s.phone", "s.create_date", "s.last_update", "s.active", when(col("s.last_update").isNotNull(), col("s.last_update").cast("date")).otherwise(col("s.create_date")).alias("valid_from"), lit("9999-12-31").alias("valid_to"), lit(1).alias("dim_active"))

df_new_curr_recs.createOrReplaceTempView("new_curr_recs")
#review dataset
df_new_curr_recs.orderBy("customer_id").show(3, False)

# ########### 2. Find previous current records to expire  ########
# ########### isolate keys of records to be modified ########### #
df_modfied_keys = df_new_curr_recs.select("customer_dim_id")
df_modfied_keys.createOrReplaceTempView("modfied_keys")
df_modfied_keys.show()

# ############## 3. Expire previous current records  ############
# ############## create new hist recs dataset ############## #
# we should have the modified register as expired

# Get the columns we need from the current_scd2 dataframe
df_new_hist_recs = current_scd2.select(
    "customer_dim_id", "customer_id", "first_name", "last_name",
    "email", "activebool", "address", "address2", "district",
    "postal_code", "phone", "create_date", "last_update", "active",
    to_date(col("valid_from"),"MM-dd-yyyy").alias("valid_from"),
    to_date(from_utc_timestamp(current_timestamp(), "EST"), "MM-dd-yyyy").alias("valid_to"),
    lit("0").alias("dim_active")  # expire the records
)

# Join with the modified_keys dataframe on the customer_dim_id column
df_new_hist_recs = df_new_hist_recs.join(
    modified_keys, on="customer_dim_id", how="inner"
)

# Filter out records where dim_active is not 1
df_new_hist_recs = df_new_hist_recs.filter(col("dim_active") == 1)

df_new_hist_recs.show(3, False)

# ############## 4. Isolate unaffected records  ################
# ############## create unaffected recs dataset ############## #
# row for customer_id 5 should not appear

# Get the columns we need from the current_scd2 dataframe
df_unaffected_recs = current_scd2.select(
    "customer_dim_id", "customer_id", "first_name", "last_name",
    "email", "activebool", "address", "address2", "district",
    "postal_code", "phone", "create_date", "last_update", "active",
    "valid_from", "valid_to", "dim_active"
)

# Left join with the modified_keys dataframe on the customer_dim_id column
df_unaffected_recs = df_unaffected_recs.join(
    modified_keys, on="customer_dim_id", how="left"
)

# Filter out records where customer_dim_id is not null
df_unaffected_recs = df_unaffected_recs.filter(col("customer_dim_id").isNull())

df_unaffected_recs.orderBy("customer_dim_id").show(6, False)
################ 5. Create records for new customers ######
# ############## create new recs dataset ############## #
# We should have the new record for Luka Doncic
# Get the columns we need from the recent_staging_records dataframe
df_new_cust = recent_staging_records.select(
    "customer_id", "first_name", "last_name", "email", "activebool",
    "address", "address2", "district", "postal_code", "phone",
    "create_date", "last_update", "active"
)

# Left join with the current_scd2 dataframe on the customer_id column
df_new_cust = df_new_cust.join(
    current_scd2, on="customer_id", how="left"
)

# Filter out records where customer_id is not null
df_new_cust = df_new_cust.filter(col("customer_id").isNull())

# Add valid_from and valid_to columns
df_new_cust = df_new_cust.withColumn("valid_from", 
    coalesce(to_date(col("last_update"), "MM-dd-yyyy"), to_date(col("create_date"), "MM-dd-yyyy"))
)

df_new_cust = df_new_cust.withColumn("valid_to", lit("9999-12-31"))

df_new_cust = df_new_cust.withColumn("dim_active", lit(1))

df_new_cust.show(3, False)

# Get max surrogate key
v_max_key = scSpark.sql(
    "SELECT STRING(MAX(customer_dim_id)) FROM current_scd2"
).collect()[0][0]

print (v_max_key)

################ 6. Combine the datasets for new SCD2 #########
# ############## create new scd2 dataset ############## #

# Load new_cust and new_curr_recs dataframes
df_new_cust = scSpark.read.format("csv").load("path/to/new_cust.csv")
df_new_curr_recs = scSpark.read.format("csv").load("path/to/new_curr_recs.csv")

# Union the two dataframes
df_a_cte = df_new_cust.union(df_new_curr_recs)

# Add a customer_dim_id column using row_number and v_max_key
df_b_cte = df_a_cte.select("*", F.row_number().over(F.orderBy("valid_from")).cast("bigint").alias("customer_dim_id"))
df_b_cte = df_b_cte.withColumn("customer_dim_id", F.col("customer_dim_id") + v_max_key)

# Load unaffected_recs and new_hist_recs dataframes
df_unaffected_recs = scSpark.read.format("csv").load("path/to/unaffected_recs.csv")
df_new_hist_recs = scSpark.read.format("csv").load("path/to/new_hist_recs.csv")

# Union the three dataframes
df_new_scd2 = df_b_cte.union(df_unaffected_recs).union(df_new_hist_recs)

# Order the resulting dataframe by customer_dim_id in descending order
df_new_scd2 = df_new_scd2.orderBy("customer_dim_id", ascending=False)

# Show the first 3 rows of the resulting dataframe
df_new_scd2.show(3, False)

# Create a backup
df_backup_scd2 = df_new_scd2

#Check backup  
df_backup_scd2.select(['customer_dim_id']).orderBy("customer_dim_id",ascending=False).show(3, False)

#test write temporal_scd2 table with backup

df_backup_scd2.write \
              .mode("overwrite") \
              .jdbc(url=url_dwh_target, table = "public.temporal_scd2", properties=properties)

# read temporal table
df_temporal_scd2 = scSpark.read \
    .jdbc(url_dwh_target, 
         "public.temporal_scd2",
         properties=properties)

#write dimension with the backup info
df_temporal_scd2.write \
            .mode('overwrite') \
            .jdbc(url=url_dwh_target, table="public.dim_customer",properties=properties)

#### You're done!