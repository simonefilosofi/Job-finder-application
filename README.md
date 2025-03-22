# Databases-Project-2023-2024
## A Brief Introduction
The following project aims at recreating the functionalities of a "Job Finder Application" by extracting data from a given csv file, while gaining some insightful analysis of the provided dataset. The users will be able to run the suggested queries and insert their desired inputs thanks to our python application with two different interfaces: a **SQL** based interface and a **NoSQL** one.

## How to Run Our Application🚀
For the best experience with our app we suggest to execute the files from a new terminal tab set to **fullscreen**. This is to ensure that the ASCII arts get displayed properly.

## MySQL App
1. To load the Dataset, first run the `db_builder.py` file; in the terminal, the script will ask as input the password for your localhost-root. Wait for the dataset to be fully loaded before continuing.
2. To open the application, run the `mysql_app.py` file; again in the terminal, the script will ask as input the password for your localhost-root. Once the password has been correctly inserted, the application will start by displaying the available queries.

### Available MySQL Queries 
1) The first query allows the user to explore the 'biased nature' of our dataset by comparing the offered salary and required experience for a selected role between males and females.
2) The second query takes as input a country selected by the user and serches for job offers available in that selected country.
3) The third query takes as input a company name and returns the job portals where the company is present and the count of job offers for each of those.
4) The fourth query takes as input a degree title and returns for each role available in our dataset the minimimum and maximum salary and the required experience for the role.
5) The fifth query takes two inputs from the user: an industry name and a contract type and accordingly returns the available role, the company name and the corresponding Job Id.
6) The sixth query searches for job offers posted during the last year given as input a sector.

*** 

## NoSQL App
For this bonus part of the project we used MongoDB, a NoSQL database, linked with our python script using the `pymongo` library.
In order to run the app **we assume you have a MongoDB server running on your localhost port** (to install it refer [here](https://www.mongodb.com/try/download/community)).
The database is built using _document embeddings_ and all the queries are stored in the same `nosql_app.py` file.

### Available NoSQL Queries
1) The first query takes as input a company name and returns the top 3 most required skills for that company, along with the count of the occurences.
2) The second query returns contact informations of a specific sector given as input by the user.
3) The third query finds out which companies have the highest number of job offerings in a state specified by the user: returning company name, website and number of job offerings posted.
4) The fourth query takes as input a sector name and returns the names of the top 3 CEOs in that sector, along with their company name and company size.
