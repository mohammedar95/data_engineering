# Project: Data Lake
Introduction
A music streaming startup, Sparkify, has grown their user base and song database even more and want to move their data warehouse to a data lake. Their data resides in S3, in a directory of JSON logs on user activity on the app, as well as a directory with JSON metadata on the songs in their app.

As a data engineer, I am tasked with building an ETL pipeline that extracts their data from S3, processes them using Spark, and loads the data back into S3 as a set of dimensional tables. This will allow their analytics team to continue finding insights in what songs their users are listening to.

I will be able to test their database and ETL pipeline by running queries given by the analytics team from Sparkify and compare your results with their expected results.

# Project Description
In this project, I will apply what you've learned on Spark and data lakes to build an ETL pipeline for a data lake hosted on S3. To complete the project, I will need to load data from S3, process the data into analytics tables using Spark, and load them back into S3. You'll deploy this Spark process on a cluster using AWS.


# How to Run:
in terminal, run etl.py to extract data and create etl for a data lake and host the output in S3. This will load all the data to the current output data in S3