
### Sparkify wanted to leverage their big data from the new music app. They are interested in knowing how their users are using the app and understaind what songs the users are listening to. The problem is that they dont sustain database to hold their data efficiently. Here, where I am going to create a database schema design and ETL pipline to pass all the data from JSON logs on user activity on the app as well as the metadata on the songs.


# Database Design:
The database design will hold all the metadata comming from the music app platform. In this case, sparkfy has two files 
that needs to be used for integrating the data. Therefore, I have created a Postgres database with tables designed to optimize queries 
on song play, and bring on data for the project. To create a database schema and ETL pipeline for this analysis. 
I needed to be able to test the  database and ETL pipeline by 
running queries to compare results between metadata file and database design.



# ETL Process (pipeline).
The ETL process will take into account many paramaters to create connection to the service to record data from JSON file. I have connected created database and conncted it to a server which will hold the database. Then created tables in the database. The tables have attributes corresponding to JSON file to map and structure data in a logical and most efficient way for the project. I have preformed analysis on the raw data and found out that it needs to  prepared, cleand, converted into an propriate way. ex time.
After creating the tables and wrangling data, I have create a pipline to store all metadata from song_file and log_file to the database.



## Explanation of the files in the repository:
    I will list them in the order of setting up the pipline
    
1. data: containts log_data and song_data JSON files for the music app.
# testing files
2. test.ipynb: is a testing notebook lab to do some testings such as connecting to database, create tables, drop tables, passing values to the tables created.
3. etl.ipynb: reads and processes a single file from song_data and log_data and loads the data into your tables. This notebook contains detailed instructions on the ETL process for each of the tables.
# workspace project
4. sql_queries.py: this is a python file to work on creating tables, dropping tables and passing values to tables using SQL.
4. create_tables.py: drops and creates your tables. You run this file to reset your tables before each time you run your ETL scripts.
5. etl.py: reads and processes files from song_data and log_data and loads them into your tables. You can fill this out based on your work in the ETL notebook.




# INSTRUCTION:
## I want to show how the workplace works which we will start with testing files (test.ipynb, etl.ipynb)
### Follow the steps to run everything smoothly:


1. open terminal and type: 
> python create_tables.py<
2. go to test.ipynb and run all the cells. This step is to create testing tables
3. go to elt.ipynb and run all cells. This is going to pass all the data from JSON and do data wrangling on some of the raw data and should be able to prepare the data on this file.
4. go back to test.ipynb and run cells (14 to 18) again. Now you will see the testing complete on sample data and some data shows in the tables.
5. Since all steps run successfuly, we can move on the practical application on the pipline of the ETL.
6. go to terminal and run 
> python create_tables.py<
> python etl.py<
7. and watch the magggggggic haaaapen lol. All files are processing I guess! right! right! right! **dancing**
