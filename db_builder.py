import mysql.connector as mysql
from mysql.connector import Error
import pandas as pd
import getpass
import time
from rich.console import Console
from rich import print

# Remove pandas error message
pd.options.mode.chained_assignment = None
console = Console()

def createdatabase(user:str,passw:str):
    db = mysql.connect(
        host="localhost", 
        user=user, 
        passwd=passw)
    if db.is_connected():
        curs = db.cursor()
        curs.execute('SHOW DATABASES')
        result = curs.fetchall()

        for x in result:
            if 'Jobs' == x[0]:
                curs.execute(f'DROP DATABASE Jobs')
                db.commit()
                print('\n[bold #E48F45]The database already exists. The old one was dropped.')
                time.sleep(1)

        curs.execute("CREATE DATABASE Jobs")
        print('\n[bold green]The database was created!')

def creattables(user:str,passw:str):
    db = mysql.connect(
        host="localhost", 
        user=user, 
        passwd=passw, 
        database="Jobs")
    
    curs = db.cursor()

    location = "CREATE TABLE Location(" \
                    "longitude FLOAT(7,4) NOT NULL," \
                    "latitude FLOAT(7,4) NOT NULL," \
                    "location VARCHAR(255) NOT NULL," \
                    "country VARCHAR(255) NOT NULL," \
                    "PRIMARY KEY (longitude, latitude)" \
                    ");"
    
    role = "CREATE TABLE Role("\
                    "responsibilities VARCHAR(255) NOT NULL," \
                    "skills VARCHAR(300) NOT NULL," \
                    "job_description VARCHAR(500) NOT NULL," \
                    "job_title VARCHAR(255) NOT NULL," \
                    "role VARCHAR(255) NOT NULL PRIMARY KEY" \
                    ");"

    company = "CREATE TABLE Company("\
                    "company VARCHAR(255) NOT NULL PRIMARY KEY," \
                    "city VARCHAR(255)," \
                    "state VARCHAR(255)," \
                    "industry VARCHAR(255)," \
                    "sector VARCHAR(255)," \
                    "zip VARCHAR(100)," \
                    "website VARCHAR(255)," \
                    "ticker VARCHAR(100)," \
                    "ceo VARCHAR(255)" \
                    ");"
    
    offer = "CREATE TABLE Offer("\
                    "job_id BIGINT NOT NULL PRIMARY KEY," \
                    "work_type VARCHAR(100)," \
                    "qualifications VARCHAR(100)," \
                    "preference VARCHAR(100)," \
                    "benefits VARCHAR(255)," \
                    "contact VARCHAR(100)," \
                    "contact_person VARCHAR(100)," \
                    "company_size MEDIUMINT," \
                    "company VARCHAR(255)," \
                    "role VARCHAR(255)," \
                    "latitude FLOAT(7,4)," \
                    "longitude FLOAT(7,4)," \
                    "job_posting_date DATE," \
                    "job_portal VARCHAR(100)," \
                    "min_experience_years SMALLINT," \
                    "max_experience_years SMALLINT," \
                    "min_salary VARCHAR(10)," \
                    "max_salary VARCHAR(10)," \
                    "FOREIGN KEY (longitude,latitude) REFERENCES Location(longitude, latitude)," \
                    "FOREIGN KEY (company) REFERENCES Company(company)," \
                    "FOREIGN KEY (role) REFERENCES Role(role)" \
                    ");"
    
    curs.execute(location)
    curs.execute(role)
    curs.execute(company)
    curs.execute(offer)
    

def dataload(user:str, password:str):
    data = pd.read_csv('jobs_sample.csv', index_col=0).dropna()
    db = mysql.connect(
        host="localhost", 
        user=user, 
        passwd=password, 
        database="Jobs")
    
    curs = db.cursor()
    
    # Add data to the Company table
    ## The Company column is the primary key; we don't want to insert duplicates
    company = data.iloc[:, 21:]
    company.drop_duplicates(subset='Company', inplace = True)
    curs.executemany("""
    INSERT INTO Company (company, city, state, industry, sector, zip, ceo, website, ticker)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s);
""", [tuple(row[['Company', 'City', 'State', 'Industry', 'Sector', 'Zip', 'CEO', 'Website', 'Ticker']]) for _, row in company.iterrows()])
    db.commit()
    
    # Add data to the Location table
    ## The longitude and latitude columns are the primary key; we don't want to insert duplicates 
    location = data.iloc[:, [6, 7, 5, 4]]
    location.drop_duplicates(subset=['latitude', 'longitude'], inplace=True)
    curs.executemany("""
    INSERT INTO Location (longitude, latitude, location, country)
    VALUES (%s, %s, %s, %s);
    """, [tuple(row[['longitude', 'latitude', 'location', 'Country']]) for _, row in location.iterrows()])
    db.commit()

    # Add data to the Role table
    role = data.iloc[:, [14, 15 ,17 ,19 ,20]]
    role.drop_duplicates(subset = 'Role', inplace = True)
    curs.executemany("""
    INSERT INTO Role (role, job_title, job_description, skills, responsibilities)
    VALUES (%s, %s, %s, %s, %s);
    """, [tuple(row[['Role', 'Job Title', 'Job Description', 'skills', 'Responsibilities']]) for _, row in role.iterrows()])
    db.commit()

    # Add data to the Offer table
    offer = data.iloc[:, [0, 2, 8, 11, 12, 13, 18, 9, 1, 3, 21, 15, 7, 6, 10, 16]]
    # Splitting the Experience column
    offer['Min Experience Years'] = offer['Experience'].apply(lambda row: row[0])
    offer['Max Experience Years'] = offer['Experience'].apply(lambda row: row[5:7])
    offer.drop('Experience', axis = 1, inplace = True)
    # Splitting the Salary range column
    offer['Min Salary'] = offer['Salary Range'].apply(lambda row: row[0:4])
    offer['Max Salary'] = offer['Salary Range'].apply(lambda row: row[5:])
    def convert_salary(salary):
        numeric_part = salary[1:-1]  # Remove '$' at the beginning and 'K' at the end
        return int(numeric_part) * 1000
    offer['Min Salary'] = offer['Min Salary'].apply(convert_salary)
    offer['Max Salary'] = offer['Max Salary'].apply(convert_salary)
    # Formatting the Benefits column
    offer['Benefits'] = offer['Benefits'].apply(lambda row: row[2:-2])
    offer.drop('Salary Range', axis = 1, inplace = True)
    curs.executemany("""
    INSERT INTO Offer (job_id, work_type, qualifications, preference, benefits,  min_experience_years, max_experience_years, contact, contact_person, company_size, company, role, longitude, latitude, job_posting_date, job_portal, min_salary, max_salary)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s,%s,%s,%s,%s,%s,%s, %s, %s);
    """, [tuple(row[['Job Id', 'Work Type', 'Qualifications', 'Preference', 'Benefits', 'Min Experience Years', 'Max Experience Years', 'Contact', 'Contact Person', 'Company Size','Company','Role','longitude','latitude', 'Job Posting Date', 'Job Portal', 'Min Salary', 'Max Salary']]) for _, row in offer.iterrows()])
    db.commit()

if __name__ == '__main__':

    print("""
     ██╗ ██████╗ ██████╗ ███████╗    ██████╗  █████╗ ████████╗ █████╗ ███████╗███████╗████████╗
     ██║██╔═══██╗██╔══██╗██╔════╝    ██╔══██╗██╔══██╗╚══██╔══╝██╔══██╗██╔════╝██╔════╝╚══██╔══╝
     ██║██║   ██║██████╔╝███████╗    ██║  ██║███████║   ██║   ███████║███████╗█████╗     ██║   
██   ██║██║   ██║██╔══██╗╚════██║    ██║  ██║██╔══██║   ██║   ██╔══██║╚════██║██╔══╝     ██║   
╚█████╔╝╚██████╔╝██████╔╝███████║    ██████╔╝██║  ██║   ██║   ██║  ██║███████║███████╗   ██║   
 ╚════╝  ╚═════╝ ╚═════╝ ╚══════╝    ╚═════╝ ╚═╝  ╚═╝   ╚═╝   ╚═╝  ╚═╝╚══════╝╚══════╝   ╚═╝                                                                                                  
                                                                                """)
    user = 'root'
    password = getpass.getpass('Insert password for localhost --> ')

    try:
        with console.status("[italic]Creating the database...", spinner='line') as status:
            time.sleep(1)
            createdatabase(user=user, passw=password)
            time.sleep(1)
            console.log("[italic]Defining tables...")
            time.sleep(1)
            creattables(user=user, passw=password)
            time.sleep(1)
            status.update(status = "[italic]Filling the database...", spinner = 'material')
            time.sleep(1)
            dataload(user=user, password=password)
        print("\n[bold green]Database filled!")
    except Error as e:
        print(e)


