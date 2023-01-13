# Dimension Types Notebook
## Load libraries and set general variables

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

scSpark.sql("SELECT  fc.film_id, f.title, f.description, c.name as category_name , l.name as language_name, \
                     f.rental_duration, f.rental_rate, f.length, f.replacement_cost, f.rating, \
                     f.release_year, f.special_features, f.fulltext, now() as load_date \
               FROM source_film f \
               JOIN source_language l \
                 ON f.language_id = l.language_id \
               JOIN source_film_category fc \
                 ON fc.film_id = f.film_id \
               JOIN source_category c \
                 ON c.category_id = fc.category_id") \
            .write \
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
tweets_target = scSpark.sql(" SELECT CAST(tweet_id AS STRING) tweet_id , sentiment, sentiment_confidence,\
                               negativereason_confidence, name, retweet_count, \
                               tweet_coord, tweet_created, user_timezone, now() as load_date  from df_tweets")

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


first_customer_records = """
WITH recent_customer_staging_records AS(
SELECT ROW_NUMBER () OVER (PARTITION BY customer_id  ORDER BY load_date DESC) AS row_num,
       customer_id, first_name, last_name, email,  activebool, 
       create_date, last_update, active, load_date, address_id 
FROM stg_dim_customer
),
recent_address_staging_records as(
SELECT ROW_NUMBER() OVER (PARTITION BY address_id ORDER BY load_date DESC) AS row_num,
       address, address2, district, postal_code, phone,address_id 
FROM stg_dim_address
)
SELECT ROW_NUMBER() OVER(ORDER BY customer_id ) AS customer_dim_id,customer_id, first_name, last_name, email, activebool,               
       address, address2, district, postal_code, phone,
       create_date, last_update, active ,
       COALESCE (DATE(last_update) , create_date) AS valid_from,
       DATE('9999-12-31') AS valid_to,
       1 AS dim_active
FROM recent_customer_staging_records c 
JOIN recent_address_staging_records a
  ON c.address_id = a.address_id
WHERE a.row_num = 1
  AND c.row_num = 1
"""

df_first_customer_records = scSpark.sql(first_customer_records)

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


hd_new_curr_recs = """
 SELECT t.customer_dim_id, s.customer_id, s.first_name ,s.last_name, s.email, s.activebool,               
        s.address, s.address2, s.district, s.postal_code, s.phone,
        s.create_date, s.last_update, s.active ,
        COALESCE (DATE(s.last_update) , DATE(s.create_date)) AS valid_from,
        DATE('9999-12-31') AS valid_to,
        1 AS dim_active        
 FROM     recent_staging_records s
          INNER JOIN current_scd2 t
              ON t.customer_id = s.customer_id
              AND CAST(t.dim_active AS STRING) = '1'
 WHERE   NVL(s.first_name, '') <> NVL(t.first_name, '')
        OR NVL(s.last_name, '') <> NVL(t.last_name, '')
        OR NVL(s.email, '') <> NVL(t.email, '')          
        OR NVL(s.address, '') <> NVL(t.address, '')
        OR NVL(s.address2, '') <> NVL(t.address2, '')
        OR NVL(s.district, '') <> NVL(t.district, '')
        OR NVL(s.postal_code, '') <> NVL(t.postal_code, '')
        OR NVL(s.phone, '') <> NVL(t.phone, '')
"""
df_new_curr_recs = scSpark.sql(hd_new_curr_recs)

df_new_curr_recs.createOrReplaceTempView("new_curr_recs")
# ############## review dataset ############## #

df_new_curr_recs.orderBy("customer_id").show(3, False)

# ########### 2. Find previous current records to expire  ########
# ########### isolate keys of records to be modified ########### #
df_modfied_keys = df_new_curr_recs.select("customer_dim_id")
df_modfied_keys.createOrReplaceTempView("modfied_keys")
df_modfied_keys.show()

# ############## 3. Expire previous current records  ############
# ############## create new hist recs dataset ############## #
# we should have the modified register as expired

hd_new_hist_recs = """
 SELECT   t.customer_dim_id, t.customer_id, t.first_name, t.last_name, 
          t.email, t.activebool, t.address, t.address2, t.district, 
          t.postal_code, t.phone, t.create_date, t.last_update, t.active, 
          DATE(t.valid_from) AS valid_from ,
          DATE(FROM_UTC_TIMESTAMP(CURRENT_TIMESTAMP, 'EST')) AS valid_to,
          '0' AS dim_active  -- here we expire
 FROM     current_scd2 t
          INNER JOIN modfied_keys k
              ON k.customer_dim_id = t.customer_dim_id
 WHERE    t.dim_active = '1'
"""
df_new_hist_recs = scSpark.sql(hd_new_hist_recs)

df_new_hist_recs.createOrReplaceTempView("new_hist_recs")

# ############## review dataset ############## #
df_new_hist_recs.show(3, False)

# ############## 4. Isolate unaffected records  ################
# ############## create unaffected recs dataset ############## #
# row for customer_id 5 should not appear
hd_unaffected_recs = """
 SELECT   t.customer_dim_id, t.customer_id, 
          t.first_name, t.last_name, 
          t.email, t.activebool, 
          t.address, t.address2, 
          t.district, t.postal_code, 
          t.phone, t.create_date, 
          t.last_update, t.active, 
          t.valid_from ,t.valid_to,
          t.dim_active
 FROM     current_scd2 t
LEFT JOIN modfied_keys k
       ON k.customer_dim_id = t.customer_dim_id
    WHERE k.customer_dim_id IS NULL       
"""
df_unaffected_recs = scSpark.sql(hd_unaffected_recs)
df_unaffected_recs.createOrReplaceTempView("unaffected_recs")

# ############## review dataset ############## #
df_unaffected_recs.orderBy("customer_dim_id").show(6, False)

################ 5. Create records for new customers ######
# ############## create new recs dataset ############## #
# We should have the new record for Luka Doncic
hd_new_cust = """
 SELECT s.customer_id, s.first_name, 
        s.last_name, s.email, 
        s.activebool,s.address, 
        s.address2, s.district, 
        s.postal_code, s.phone,
        s.create_date, s.last_update, 
        s.active ,
        COALESCE (DATE(s.last_update) , DATE(s.create_date)) AS valid_from,
        DATE('9999-12-31') AS valid_to,
        1 AS dim_active        
 FROM     recent_staging_records s
LEFT JOIN current_scd2 t
       ON t.customer_id = s.customer_id
    WHERE t.customer_id IS NULL
"""
df_new_cust = scSpark.sql(hd_new_cust)
df_new_cust.createOrReplaceTempView("new_cust")

# ############## review dataset ############## #
df_new_cust.show(3, False)

# Get max surrogate key
v_max_key = scSpark.sql(
    "SELECT STRING(MAX(customer_dim_id)) FROM current_scd2"
).collect()[0][0]

print (v_max_key)

################ 6. Combine the datasets for new SCD2 #########
# ############## create new scd2 dataset ############## #

# Query for creating the whole table
hd_new_scd2 = """
 WITH a_cte
 AS   (
        SELECT     x.customer_id, x.first_name, 
                   x.last_name, x.email, 
                   x.activebool, x.address, 
                   x.address2, x.district, 
                   x.postal_code, x.phone,
                   x.create_date, x.last_update, 
                   x.active , x.valid_from,
                   x.valid_to , x.dim_active
        FROM       new_cust x        
        UNION ALL
        SELECT     y.customer_id, y.first_name, 
                   y.last_name, y.email, 
                   y.activebool, y.address, 
                   y.address2, y.district, 
                   y.postal_code, y.phone,
                   y.create_date, y.last_update, 
                   y.active , y.valid_from,
                   y.valid_to , y.dim_active
        FROM       new_curr_recs y
      )
  ,   b_cte
  AS  (
        SELECT  ROW_NUMBER() OVER(ORDER BY a.valid_from)
                    + BIGINT('{v_max_key}') AS customer_dim_id,
                a.customer_id, a.first_name, 
                a.last_name, a.email, 
                a.activebool, a.address, 
                a.address2, a.district, 
                a.postal_code, a.phone,
                a.create_date, a.last_update, 
                a.active , a.valid_from,
                a.valid_to , a.dim_active
        FROM    a_cte a
      )
  SELECT  customer_dim_id, customer_id, 
          first_name, 
          last_name, email, 
          activebool, address, 
          address2, district, 
          postal_code, phone,
          create_date, last_update, 
          active , valid_from, valid_to,
          dim_active
  FROM    b_cte
  UNION ALL
  SELECT  customer_dim_id, customer_id, 
          first_name, 
          last_name, email, 
          activebool, address, 
          address2, district, 
          postal_code, phone,
          create_date, last_update, 
          active , valid_from,valid_to,
          dim_active
  FROM    unaffected_recs
  UNION ALL
  SELECT  customer_dim_id, customer_id, 
          first_name, 
          last_name, email, 
          activebool, address, 
          address2, district, 
          postal_code, phone,
          create_date, last_update, 
          active , valid_from, valid_to,
          dim_active
  FROM    new_hist_recs
"""
df_new_scd2 = scSpark.sql(hd_new_scd2.replace("{v_max_key}", v_max_key))

#Insert into table
#df_new_scd2.write \
#        .mode('overwrite') \
#        .jdbc(url=url_dwh_target, table="public.dim_customer",properties=properties)
        
#df_new_scd2.coalesce(1).write.csv("test_csd2.csv", header=True)
# ############## review dataset ############## #

df_new_scd2.orderBy("customer_dim_id",ascending=False).show(3, False)

# Create a backup just so
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