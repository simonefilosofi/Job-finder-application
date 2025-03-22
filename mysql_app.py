import mysql.connector as mysql
import time
import getpass
from rich import print
from rich.console import Console
from rich.table import Table
from rich.panel import Panel
from rich import box

console = Console()

def connect_to_database(user, password):
    db = mysql.connect(
            host="localhost", 
            user=user, 
            passwd=password)

    if db.is_connected():
        curs = db.cursor()
        curs.execute('Use Jobs')
        db.commit()
        return db, curs
    else:
        print("Failed to connect to the database.")
        return None, None

def display_menu():
            print("") 
            print(Panel(
' 0) - Quit\n \
1) - Calculate average salary and average required experience by gender for a selected role\n \
2) - Display job offers available in a specific country\n \
3) - Search by company name the job portals where the company is present and how many job offers are available\n \
4) - Discover the maximum and minimum salary for a given university degree\n \
5) - Find the working positions available in a specific industry according to the type of contract\n \
6) - Search for job offers made in the last year for available roles in a given sector',
                title = "[bold yellow]Select the query you want to execute"
            ))

def execute_query(curs, query):
    curs.execute(query)
    return curs.fetchall()

def query_1(curs,query_file):
    curs.execute("SELECT DISTINCT role FROM Offer ORDER BY role ASC")
    roles = [row[0] for row in curs.fetchall()]
    table = Table(title=f"Available roles", box=box.ASCII)
    table.add_column("Role Name")
    for i, role in enumerate(roles, start=1):
        table.add_row(f"{i}. {role}")
    console.print(table)
                
    # Get user input for the desired role
    try:
        choice = int(input("\nEnter the number corresponding to the role you prefer: "))
        if 1 <= choice <= len(roles):
            desired_role = roles[choice - 1]
            # Replace the placeholder with the desired role
            query = query_file[0].replace(":user_role", desired_role)
            curs.execute(query)
            rows = curs.fetchall()
                        
        if not rows:
            print('No results for this research!')
        else:
            table = Table(title=f"Results for {desired_role}:")
            table.add_column("Role", justify="center", no_wrap=True)
            table.add_column("Average Male Salary", justify="center", no_wrap=True)
            table.add_column("Average Male required Experience", justify="center", no_wrap=True)
            table.add_column("Average Female Salary", justify="center", no_wrap=True)
            table.add_column("Average Female required Experience", justify="center", no_wrap=True)

            for element in rows:
                table.add_row(element[0], str(element[1]), str(element[2]), str(element[3]) , str(element[4]))

            with console.pager():
                console.print(table)

    except Exception as e:
        print('[bold red]Please insert a valid input')             
    

def query_2(curs,query_file):
    curs.execute("SELECT DISTINCT country FROM Location ORDER BY country ASC")
    countries = [row[0] for row in curs.fetchall()]
    table = Table(title=f"Available countries", box=box.ASCII)
    table.add_column("Country Name")
    for country in countries:
        table.add_row(country)
    console.print(table)
    try:
        selected_country = input("\nSelect your Country: ")
        query = query_file[1].replace(":user_country", selected_country)
        curs.execute(query)
        rows = curs.fetchall()

        if not rows:
            print('No results for this research!')
        else:
            table = Table(title=f"{len(rows)} results for {selected_country}:")
            table.add_column("Job Title", justify="center", no_wrap=True)
            table.add_column("Company name", justify="center", no_wrap=True)
            table.add_column("Job Id", justify="center", no_wrap=True)
            table.add_column("Job posting date", justify="center", no_wrap=True)

            for element in rows:
                table.add_row(element[2], element[3], str(element[0]), str(element[1]))

            with console.pager():
                console.print(table)

    except Exception as e:
        print('[bold red]Please insert a valid input') 

def query_3(curs,query_file):
    curs.execute("SELECT DISTINCT company FROM Company ORDER BY company ASC")
    companies = [row[0] for row in curs.fetchall()]
    table = Table(title=f"Available companies", box=box.ASCII)
    table.add_column("Company Name")
    for i, company in enumerate(companies, start=1):
        table.add_row(f"{i}. {company}")
    console.print(table)
                
     # Get user input for the desired role
    try:
        choice = int(input("\nEnter the number corresponding to the company you prefer: "))
        if 1 <= choice <= len(companies):
            desired_company = companies[choice - 1]
        # Replace the placeholder with the desired role
        query = query_file[2].replace(":user_company", desired_company)
        curs.execute(query)
        rows = curs.fetchall()
        # Display results
        if not rows:
            print('No results for this research!')
        else:
            table = Table(title=f"{len(rows)} results for {desired_company}:")
            table.add_column("Job Portal", justify="center", no_wrap=True)
            table.add_column("Number of Job Offers", justify="center", no_wrap=True)
                        
            for element in rows:
                table.add_row(element[1], str(element[2]))

            with console.pager():
                console.print(table) 
    except Exception as e:
        print('[bold red]Please insert a valid input') 

def query_4(curs,query_file): 
    curs.execute("SELECT DISTINCT qualifications FROM Offer ORDER BY qualifications ASC")
    job_titles = [row[0] for row in curs.fetchall()]
    table = Table(title=f"University Degrees:", box=box.ASCII)
    table.add_column("Degree Name")
    for job_title in job_titles:
        table.add_row(job_title)
    console.print(table)
    try:
        selected_degree = input("\nSelect your university degree: ")
        query = query_file[3].replace(':user_input', selected_degree)
        curs.execute(query)
        rows = curs.fetchall()
         # Display results
        if not rows:
            print('No results for this research!')
        else:
            table = Table(title=f"Results for {selected_degree}:")
            table.add_column("Role", justify="center", no_wrap=True)
            table.add_column("Max Salary", justify="center", no_wrap=True)
            table.add_column("Min Salary", justify="center", no_wrap=True)
            table.add_column("Min Experience", justify="center", no_wrap=True)
            table.add_column("Max Experience", justify="center", no_wrap=True)

            for element in rows:
                table.add_row(str(element[1]), str(element[2]), str(element[3]), str(element[5]),str(element[4]))

            with console.pager():
                console.print(table)

    except Exception as e:
        print('[bold red]Please insert a valid input') 

def query_5(curs,query_file):
    curs.execute("SELECT DISTINCT industry FROM Company ORDER BY industry ASC")
    industries = [row[0] for row in curs.fetchall()]
    table = Table(title=f"Available industries", box=box.ASCII)
    table.add_column("Industry Name")
    for i, industry in enumerate(industries, start=1):
        table.add_row(f"{i}. {industry}")
    console.print(table)
                
    # Get user input for the desired role
    try:
        choice = int(input("\nEnter the number corresponding to the industry you prefer: "))
        if 1 <= choice <= len(industries):
            desired_industry = industries[choice - 1]
            selected_work_type= str(input("\nEnter the contract type you prefer among the following: Full-Time, Part-Time, Contract, Temporary, Intern : "))
            if selected_work_type not in ['Full-Time','Part-Time','Contract','Temporary','Intern']:
                print('[bold red]Plese select a valid Contract Type')
            # Replace the placeholder with the desired role
            else:
                query = query_file[4].replace(":user_industry", desired_industry).replace(":user_work_type", selected_work_type)
                curs.execute(query) 
                rows = curs.fetchall()

             # Display results
                if not rows:
                    print('No results for this research!')
                else:
                    table = Table(title=f"{len(rows)} results for {desired_industry} industry and {selected_work_type} contract:")
                    table.add_column("Role Avaiable", justify="center", no_wrap=True)
                    table.add_column("Company", justify="center", no_wrap=True)
                    table.add_column("Job ID", justify="center", no_wrap=True)
                        
                    for element in rows:
                        table.add_row(str(element[0]), str(element[2]), str(element[1]))

                    with console.pager():
                        console.print(table) 
    except Exception as e:
        print('[bold red]Please insert a valid input') 

def query_6(curs,query_file):
    curs.execute("SELECT DISTINCT sector FROM Company ORDER BY sector ASC")
    sectors = [row[0] for row in curs.fetchall()]
    table = Table(title=f"Available sectors", box=box.ASCII)
    table.add_column("Sector Name")
    for sector in sectors:
        table.add_row(sector)
    console.print(table)
    # Get user input for the desired role
    try:
        selected_sector= str(input("\nEnter the sector you prefer: "))
        query = query_file[5].replace(":user_sector", selected_sector)
        curs.execute(query)
        rows = curs.fetchall()

        if not rows:
            print('No results for this research!')
        else:
            table = Table(title=f"{len(rows)} results for {selected_sector}:")
            table.add_column("Job ID", justify="center", no_wrap=True)
            table.add_column("Role", justify="center", no_wrap=True)
            table.add_column("Company", justify="center", no_wrap=True)
            table.add_column("Job posting date", justify="center", no_wrap=True)

            for element in rows:
                table.add_row(str(element[0]), str(element[1]), str(element[2]), str(element[3]))

            with console.pager():
                console.print(table)

    except Exception as e:
        print('[bold red]Please insert a valid input') 
                    

def main():
    print("""
     ██╗ ██████╗ ██████╗ ███████╗    ███████╗██╗███╗   ██╗██████╗ ███████╗██████╗      █████╗ ██████╗ ██████╗ 
     ██║██╔═══██╗██╔══██╗██╔════╝    ██╔════╝██║████╗  ██║██╔══██╗██╔════╝██╔══██╗    ██╔══██╗██╔══██╗██╔══██╗
     ██║██║   ██║██████╔╝███████╗    █████╗  ██║██╔██╗ ██║██║  ██║█████╗  ██████╔╝    ███████║██████╔╝██████╔╝
██   ██║██║   ██║██╔══██╗╚════██║    ██╔══╝  ██║██║╚██╗██║██║  ██║██╔══╝  ██╔══██╗    ██╔══██║██╔═══╝ ██╔═══╝ 
╚█████╔╝╚██████╔╝██████╔╝███████║    ██║     ██║██║ ╚████║██████╔╝███████╗██║  ██║    ██║  ██║██║     ██║     
 ╚════╝  ╚═════╝ ╚═════╝ ╚══════╝    ╚═╝     ╚═╝╚═╝  ╚═══╝╚═════╝ ╚══════╝╚═╝  ╚═╝    ╚═╝  ╚═╝╚═╝     ╚═╝     
                                                                                                              """)
    password = getpass.getpass('Insert password for localhost --> ')
    db, curs = connect_to_database(user='root', password=password)
    
    if db is None or curs is None:
        return
    
    with open('queries.sql') as f:
        query_file = f.read().split(";")

    while True:
        display_menu()
        
        try:
            desired_query = int(input('\nYour choice -> '))
        
            if desired_query not in range(0, 7):
                print('[bold red]Please insert a valid number')

            elif desired_query == 0:
                break

            elif desired_query == 1:
                query_1(curs, query_file)

            elif desired_query == 2:
                query_2(curs, query_file)

            elif desired_query == 3:
                query_3(curs, query_file)

            elif desired_query == 4:
                query_4(curs, query_file)

            elif desired_query == 5:
                query_5(curs, query_file)

            elif desired_query == 6:
                query_6(curs, query_file)
        
        except Exception:
            print("[bold red]Please insert a number")
 

    db.close()
    console.print("Goodbye 👋", style='bold #96EFFF')

if __name__ == "__main__":
    main()
    



