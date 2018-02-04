// Databricks notebook source
// MAGIC %md 
// MAGIC 
// MAGIC # Assignment
// MAGIC 
// MAGIC Please go on and solve the exercises you will find below. Before you start, send the *link* to this notebook (the url you are seeing now in the navigation bar of your browser) in email to: *ceu-data@googlegroups.com*. As you shared your account with Ryan and Zoltan earlier, through this link we will be able to monitor your progress using Databricks's notebook reports. Do all the work in this notebook in order to pass this assignment. It's important that we can validate your progress and time spent working on the assignments.  
// MAGIC 
// MAGIC Work with your group mate. You can use the *Admin Console* through the top-right "human icon" to invite your group mate so you can both work on the same notebook, if needed. *It turned out that you can only invite collaborators up to 2 people. If you need to invite your group mate, please remove Ryan from the list. We can share and use Zoltan's account to access your notebook*. 
// MAGIC 
// MAGIC As you go on, these four pieces of the documentation might prove to be very helpful, keep them at hands.:
// MAGIC 
// MAGIC [RDD Documentation](https://spark.apache.org/docs/2.2.0/api/scala/index.html#org.apache.spark.rdd.RDD)
// MAGIC 
// MAGIC [DataFrame functions](https://spark.apache.org/docs/2.2.0/api/scala/index.html#org.apache.spark.sql.Dataset) (It's called `DataSet`, but don't be confused, it applies to `DataFrame`, too)
// MAGIC 
// MAGIC [Column functions](https://spark.apache.org/docs/2.2.0/api/scala/index.html#org.apache.spark.sql.Column)
// MAGIC 
// MAGIC [spark.sql.functions](https://spark.apache.org/docs/2.2.0/api/scala/index.html#org.apache.spark.sql.functions$)

// COMMAND ----------

// MAGIC %md 
// MAGIC 
// MAGIC ## The members of your group
// MAGIC 
// MAGIC Please fill in the CEU IDs for the members of your group:
// MAGIC 
// MAGIC  * CEU ID of member #1: **142730**
// MAGIC  * CEU ID of member #2: **166700**
// MAGIC 
// MAGIC *You got to be in a group of 2 people.*

// COMMAND ----------

// MAGIC %md ## Assignment: RDDs

// COMMAND ----------

// MAGIC %md 
// MAGIC **Lets take a look at our data**
// MAGIC 1. Read `/mnt/spark-data/questions-narrow.csv` into an RDD.
// MAGIC 2. Take a look at the first 5 lines and print them. 
// MAGIC 3. Check the number of rows in this file and print it to the screen.
// MAGIC 4. Get rid of the header.

// COMMAND ----------

// 1. Read /mnt/spark-data/questions-narrow.csv into an RDD.
val rdd = sc.textFile("/mnt/spark-data/questions-narrow.csv")

// 2. Take a look at the first 5 lines and print them. 
rdd.take(5).foreach(println)
// Or just without 'new line at the end'
rdd.take(5)

// COMMAND ----------

// 3. Check the number of rows in this file and print it to the screen.
println(rdd.count())
// Alternatively 
rdd.count()

// COMMAND ----------

// 4. Get rid of the header
val DataRDD = rdd.filter(!_.contains("site,"))
DataRDD.take(3).foreach(println)

// COMMAND ----------

// MAGIC %md
// MAGIC **Next Step!**
// MAGIC 
// MAGIC 
// MAGIC Create a `case class` for this RDD. Convert the rows to case-class objects. 
// MAGIC 
// MAGIC *You are free to, but you do not need to use any date specific scala type for the `creationDate` column. You can simply use this column as a `String`.*

// COMMAND ----------

case class Stack(site:String, date:String, viewCount:Int, post:String)

// COMMAND ----------

val StackRDD = DataRDD.map({ row =>
  val columns = row.split(",")
  Stack(columns(0), columns(1), columns(2).toInt, columns(3))
})

// COMMAND ----------

// MAGIC %md
// MAGIC **Going further**
// MAGIC 7. How many sites do you have we data from? Execute the appropriate action and print it to the screen.
// MAGIC 8. What are these sites? Print it to the screen.
// MAGIC 9. Sum up the total *viewCounts* of this dataset and print it.
// MAGIC 10. How many questions are there for the *cooking* site? 
// MAGIC 11. Answer this with a few lines of code: Which are the more popular questions on the *cooking* site, those that contain the word *sugar* or those that contains the word *chocolate*. Popularity is measured by adding up *viewCounts* for each site.

// COMMAND ----------

// 1. How many sites do you have we data from? Execute the appropriate action and print it to the screen.
StackRDD.map(stack => stack.site).distinct().count()

// COMMAND ----------

// 2. What are these sites? Print it to the screen.
StackRDD.map(stack => stack.site).distinct().collect().foreach(println)

// COMMAND ----------

// 3. Sum up the total viewCounts of this dataset and print it.
StackRDD.map(stack => (stack.site, stack.viewCount)).reduceByKey( (v1,v2) => v1 + v2).collect().foreach(println)

// COMMAND ----------

// 4. How many questions are there for the cooking site? 
StackRDD.map(_.site).filter(_ == "cooking").count()

// COMMAND ----------

// 5. Which are the more popular questions on the cooking site, those that contain the word sugar or those that contains the word chocolate. Popularity is measured by adding up viewCounts for each site.
// CHOCOLATE
val chocoRDD = StackRDD.filter(_.site == "cooking").filter(_.post.contains("chocolate"))
chocoRDD.map(p => (p.site, p.viewCount)).reduceByKey( (v1, v2) => v1 + v2).collect().foreach(println)

// COMMAND ----------

// 5. Which are the more popular questions on the cooking site, those that contain the word sugar or those that contains the word chocolate. Popularity is measured by adding up viewCounts for each site.
// SUGAR
val sugarRDD = StackRDD.filter(_.site == "cooking").filter(_.post.contains("sugar"))
sugarRDD.map(p => (p.site, p.viewCount)).reduceByKey( (v1, v2) => v1 + v2).collect().foreach(println)

// COMMAND ----------

// The most popular question on the cooking site is chocolate.

// COMMAND ----------

// MAGIC %md ## Assignment: DataFrames, SQL and Internals

// COMMAND ----------

// MAGIC %md 
// MAGIC 
// MAGIC 1. Read this file into a DataFrame: `/mnt/spark-data/birdstrikes.parquet`. This file contains incident records about birds getting hit by aircrafts. This data was published by the [FAA](https://wildlife.faa.gov/). 
// MAGIC 2. Cache it right away. Materialize the cache using `df.cache.count()`
// MAGIC 
// MAGIC *Warning, this dataset contains multiple duplicate records of the original dataset to make it large. That's OK.*

// COMMAND ----------

// 1. Read this file into a DataFrame: /mnt/spark-data/birdstrikes.parquet. This file contains incident records about birds getting hit by aircrafts. 
val incidents = spark.read.parquet("/mnt/spark-data/birdstrikes.parquet")

// COMMAND ----------

// 2. Cache it right away. Materialize the cache using df.cache.count()
incidents.cache().count()

// COMMAND ----------

// MAGIC %md
// MAGIC **Working with Spark UI**
// MAGIC 3. Get the following metrics from UI. Answer the questions in a `%md` cell:
// MAGIC    - What is your Job Id for the `.cache().count()` command?
// MAGIC    - How many partitions got cached?
// MAGIC    - What is the total size of this dataframe in the cache?
// MAGIC    - Take a look at the partitions: Which block is the smallest in the cache and how much size does it use in the Memory?
// MAGIC    - How many stages did the command execute? What are its stage ids?
// MAGIC    - Take a look at the (chronologically) first stage:
// MAGIC      - How many tasks were executed?
// MAGIC      - What is the Min/Median/Max task duration?
// MAGIC      - What is the Median GC time?
// MAGIC      - Open up the `Event Timeline` chart and find the task with the longest and shortest *Executor Computing Time*. What are these numbers?

// COMMAND ----------

// MAGIC %md
// MAGIC **Answers**
// MAGIC - What is your Job Id for the .cache().count() command?
// MAGIC    - Job id: 66
// MAGIC    - Job group: 1674354073663739051_5561837446631239826_1040045684a54fb18a000e05064bc3e9
// MAGIC - How many partitions got cached?
// MAGIC    - 9
// MAGIC - What is the total size of this dataframe in the cache?
// MAGIC    - 170.7 MB (~ 19.0 MB each)
// MAGIC - Take a look at the partitions: Which block is the smallest in the cache and how much size does it use in the Memory?
// MAGIC    - rdd_130_3 - 18.7 MB
// MAGIC - How many stages did the command execute? What are its stage ids?
// MAGIC    - 2 in total: 94,95
// MAGIC - How many tasks were executed?
// MAGIC    - 9
// MAGIC - What is the Min/Median/Max task duration?
// MAGIC    - 66ms/0.2s/0.2s (Min/Median/Max)
// MAGIC - What is the Median GC time?
// MAGIC    - 0ms 
// MAGIC - Open up the `Event Timeline` chart and find the task with the longest and shortest *Executor Computing Time*. What are these numbers?
// MAGIC    - Due to scaling issues there are multiple tasks with 0.2 sec 'Executor Computing Time': 0,1,6,4,3 - these are the longest. The shortest is the final task - 8, with 66ms. 
// MAGIC    - These are actual time the executors spent on computing the request. Ideally this should be the largest compared to the others, so things like (de)serialization, scheduling should be lot smaller

// COMMAND ----------

// MAGIC %md 
// MAGIC **Working with the data**
// MAGIC 4. Print the schema of this dataframe & print what type of aircrafts are in this dataframe?
// MAGIC 6. Create a temporary view for this dataframe and use SQL to see how many incidents correspond to each aircraft type. *If you don't know SQL, create a temporary view and execute and arbitrary SQL query on it. Then get the incidents by aircraft using the DataFrame API*
// MAGIC 7. Which was the most expensive incident? In which state did it happen? (Ignore any duplicates)
// MAGIC 8. How many states do we have in the dataset?
// MAGIC 9. Which are the states with the least incidents?

// COMMAND ----------

// 1. Print the schema of this dataframe & print what type of aircrafts are in this dataframe?
incidents.printSchema()
incidents.select($"aircraft").distinct().show()

// COMMAND ----------

// 2. Create a temporary view for this dataframe and use SQL to see how many incidents correspond to each aircraft type. If you don't know SQL, create a temporary view and execute and arbitrary SQL query on it. Then get the incidents by aircraft using the DataFrame API
incidents.createOrReplaceTempView("incidents_table")
spark.sql("SELECT aircraft, COUNT(*) FROM incidents_table GROUP BY aircraft ORDER BY count(*) DESC").show(10)

// COMMAND ----------

// 3. Which was the most expensive incident? In which state did it happen? (Ignore any duplicates)
incidents.select($"state",$"cost").distinct().orderBy($"cost".desc).show(10)

// COMMAND ----------

// 4. How many states do we have in the dataset?
incidents.select($"state").distinct().count()

// COMMAND ----------

// 5. Which are the states with the least incidents?
incidents.groupBy($"state").count().orderBy($"count".asc).show(10)

// COMMAND ----------

// MAGIC %md ## Assignment: Real-world analytics with DataFrames
// MAGIC 
// MAGIC The dataset contains a dump from [crunchbase.com](https://www.crunchbase.com/).
// MAGIC 
// MAGIC Original blog post regarding the data dump: [CrunchBase Hits 400k Profiles And 45k Funding Rounds, August Excel Download Now Available](https://techcrunch.com/2013/09/10/crunchbase-hits-400k-profiles-and-45k-funding-rounds-august-excel-download-now-available/)
// MAGIC 
// MAGIC In the next cells we provide you an example analytics that introduces some extra functions that you need to know in order to solve the exercise. Take a look at the documentation and the Spark Programming Guide if you find something unfamiliar. 
// MAGIC 
// MAGIC *The actual exercise to solve comes after the examples.*

// COMMAND ----------

// import all the functions. We will need some of them.
import org.apache.spark.sql.functions._

// COMMAND ----------

val companies = spark.read.json("/mnt/spark-data/companies.json")

// COMMAND ----------

// MAGIC %md Let's take a look at the schema of this file:

// COMMAND ----------

companies.printSchema()

// COMMAND ----------

// MAGIC %md #### Exercise guideline: What are the most popular non-US countries for opening offices?

// COMMAND ----------

display(
  companies.select($"name", $"offices")
)

// COMMAND ----------

// MAGIC %md Let's *explode* the arrays in the offices column into individual records: *(look up explode in spark.sql.functions)*

// COMMAND ----------

display(
  companies.select($"name", explode($"offices").alias("office"))
)

// COMMAND ----------

// MAGIC %md How can we extract the `country_code` only?

// COMMAND ----------

display(
  companies.select($"name", explode($"offices").as("office")).select($"office.country_code")
)

// COMMAND ----------

// MAGIC %md Let's get rid of *USA* and *Nulls* 

// COMMAND ----------

display(
  companies.select($"name", explode($"offices").as("office")).select($"office.country_code".as("country")).filter($"country" =!= "USA").filter($"country".isNotNull)
)

// COMMAND ----------

// MAGIC %md Now aggregate and count:

// COMMAND ----------

display(
  companies.select($"name", explode($"offices").as("office")).select($"office.country_code".as("country")).filter($"country" =!= "USA").filter($"country".isNotNull).groupBy($"country").count().orderBy($"count".desc)
)

// COMMAND ----------

// MAGIC %md **Exercise:** Show the total value of acquisitions by year. Only show acquisitions in *USD*. Order the results by year.  

// COMMAND ----------

// Your answer comes here. Feel free to add new cells if needed.

display(
  companies.select($"acquisitions")
)



// COMMAND ----------

display(
  companies.select(explode($"acquisitions").alias("acquisitions"))
)

// COMMAND ----------

display(
  companies.select($"name", explode($"acquisitions").alias("Acq"))
)

// COMMAND ----------

display(
  companies.select($"name", explode($"acquisitions").alias("Acq")).select($"Acq.price_amount")
)

// COMMAND ----------

// MAGIC %md Answer: Show the total value of acquisitions by year. Only show acquisitions in USD. Order the results by year.

// COMMAND ----------

display(
  companies.select(explode($"acquisitions").as("acq")).
  
  select($"acq.acquired_year".as("year"),$"acq.price_amount".as("price")).
  
  filter($"acq.price_currency_code" === "USD").
  
  filter($"acq.price_amount".isNotNull and $"acq.acquired_year".isNotNull).
  
  groupBy($"year").agg(sum($"price")).orderBy($"year".desc)
)
