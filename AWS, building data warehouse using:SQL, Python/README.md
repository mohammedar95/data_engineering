# Introduction
    A music streaming startup, Sparkify, has grown their user base and song database and want to move their processes and data onto the cloud. Their data resides in S3, in a directory of JSON logs on user activity on the app, as well as a directory with JSON metadata on the songs in their app.

    My task is building an ETL pipeline that extracts their data from S3, stages them in Redshift, and transforms data into a set of dimensional tables for their analytics team to continue finding insights in what songs their users are listening to. I be able to test their database and ETL pipeline by running queries given to  the analytics team from Sparkify and compare the results with their expected results.

# Project Description
    I apply implementation of data warehouses and AWS to build an ETL pipeline for a database hosted on Redshift. To complete the project, I will need to load data from S3 to staging tables on Redshift and execute SQL statements that create the analytics tables from these staging tables.

# How to Run:
    I am using Infrastructure-as-Code (IaC) from AWS to create a cluster for the ETL pipline. in the 'Create_Cluster_AWS' file you will be able to create cluster on Redshift then load data from S3 to staging tables to excute SQL statments.

## In 'Create_Cluster_AWS' file, Follow the steps
    STEP 1: Make sure you have an AWS secret and access key: using AWS account
    STEP 2: create IAM ROLE
    STEP 3: Redshift Cluster: create a cluster to load data
    STEP 4: Open an incoming TCP port to access the cluster ednpoint
    STEP 5: Connect to the cluster
    STEP 6: Clean up your resources: This option to delete the cluster once finished


# Notebook files:
    - craete_tables.py: using python to create all the tables for the log data
    - Create_Cluster_AWS: testing and creating cluster using IaC
    - dwh.cfg: this file will hold all the credentials for for accessing AWS 
    - etl.py: pipline to load data and extract data from S3 and craete staging in Redshift
    - sql_queries.py: contains all the SQL statemtns to (CREATE, INSERT, JOIN, DELETE Tables in Redshift)