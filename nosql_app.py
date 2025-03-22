import pandas as pd
from pymongo import MongoClient
import pprint
from bson.objectid import ObjectId
from rich.console import Console
from rich.table import Table
from rich.panel import Panel
from rich import print, box

client = MongoClient('localhost', 27017)
jobs_db = client.Jobs
jobs_collection = jobs_db.jobs_collection
printer = pprint.PrettyPrinter()
console = Console()


def load_data(data):
    jobs_dict = data.to_dict(orient = 'records')

    docs = []
    for job in jobs_dict:  
    
        doc = {
        "job_Id": job['Job Id'],
        "work_type": job['Work Type'],
        "qualifications": job['Qualifications'],
        "preference": job['Preference'],
        "benefits": job['Benefits'],
        "experience": job['Experience'],
        "salary_range":job['Salary Range'],
        "contact":job['Contact'],
        "contact_person":job['Contact Person'],
        "company_size": job['Company Size'],
        "job_posting_date": job['Job Posting Date'],
        "job_portal": job['Job Portal'],
        "Role": {
            "role": job['Role'],
            "job_title": job['Job Title'],
            "job_descritpion": job['Job Description'],
            "skills":job['skills'],
            "responsibilities":job['Responsibilities']
        },
        "Company": {
            "company_name":job['Company'],
            "sector": job['Sector'],
            "industry": job['Industry'],
            "city":job['City'],
            "state": job['State'],
            "zip":job['Zip'],
            "CEO":job['CEO'],
            "website":job['Website'],
            "ticker": job['Ticker']
        },
        "Location": {
            "longitude": job['longitude'],
            "latitude": job['latitude'],
            "location": job['location'],
            "country": job['Country']
        }   
    }
        docs.append(doc)

    jobs_collection.insert_many(docs)

            
def query1():
    available_companies = jobs_collection.distinct("Company.company_name")
    table = Table(title=f"Available companies", box=box.ASCII)
    table.add_column("Company Names")
    for n, company in enumerate(available_companies, start=1):
        table.add_row(f"{n} - {company}")
    console.print(table)
        
    user_input_company = int(input("\nInsert the number corresponding to the desired Company -> "))

    if user_input_company in range(1, len(available_companies) + 1): 

        pipeline_query1 = [
            {
                "$match": {
                    "Company.company_name": available_companies[user_input_company - 1]
                }
            },
            {
                "$unwind": "$Role.skills"
            },
            {
                "$group": {
                    "_id": "$Role.skills",
                    "count": {"$sum": 1}
                }
            },
            {
                "$sort": {"count": -1}
            },
            {
                "$limit": 3
            },
            {
                "$project": {
                    "_id": 0,
                    "skill": "$_id",
                    "count": 1
                }
            }
        ]

        result = jobs_collection.aggregate(pipeline_query1)

        table = Table(title=f"Top 3 most common skills required by {available_companies[user_input_company - 1]}", leading=1, show_lines=True)
        table.add_column("Skill Description")
        table.add_column("Count", justify="center")
        for item in result:
            table.add_row(str(item['skill']), str(item['count']))

        with console.pager():
            console.print(table)
    else:
        print("\n[bold red]Please insert a valid number.")


def query2():
    available_industries = jobs_collection.distinct("Company.industry")
    table = Table(title=f"Available Industries", box = box.ASCII)
    table.add_column("Industry Names", no_wrap=True)
    for n, industry in enumerate(available_industries, start=1):
        table.add_row(f"{n} - {industry}")
    console.print(table)

    user_input_industry = int(input("\nSpecify the desired Industry number -> "))

    if user_input_industry in range(1, len(available_industries) + 1): 
        pipeline_query2 = [
            {
                "$match": {
                    "Company.industry": available_industries[user_input_industry - 1]
                }
            },
            {
                "$group": {
                    "_id": {
                        "industry": "$Company.industry",
                        "contact_person": "$contact_person",
                        "contact": "$contact" 
                    },
                    "job_count": {"$sum": 1}
                }
            },
            {
                "$project": {
                    "_id": 0,
                    "industry": "$_id.industry",
                    "contact": "$_id.contact",
                    "contact_person": "$_id.contact_person",
                    "job_count": 1
                }
            }
        ]

        result = jobs_collection.aggregate(pipeline_query2)

        # Display the results
        table = Table(title=f"Contact People for the {available_industries[user_input_industry - 1]} industry", leading=1, show_lines=True)
        table.add_column("Contact", style="dim", width=30)
        table.add_column("Contact Person", style="dim", width=30)
        table.add_column("Jobs Offered", style="dim", width=10)

        for entry in result:
            table.add_row(entry["contact"], entry["contact_person"], str(entry["job_count"]))

        with console.pager():
            console.print(table)
    
    else:
        print("\n[bold red]Please insert a valid number.")




def query3():

    available_states = jobs_collection.distinct("Company.state")
    table = Table(title=f"States in the database", box = box.ASCII)
    table.add_column("State Names", no_wrap=True)
    for n, state in enumerate(available_states, start=1):
        table.add_row(f"{n} - {state}")
    console.print(table)

    user_input_state = int(input("\nEnter the state number to find companies with the most job offerings -> "))

    if user_input_state in range(1, len(available_states) + 1):
        pipeline_query3 = [
            {
                "$match": {
                    "Company.state": available_states[user_input_state - 1]
                }
            },
            {
                "$group": {
                    "_id": "$Company.company_name",
                    "job_count": {"$sum": 1},
                    "website": {"$first": "$Company.website"},
                }
            },
            {
                "$sort": {"job_count": -1}
            }
        ]

        result = jobs_collection.aggregate(pipeline_query3)

        table = Table(title=f"Companies with the highest number of Job Offerings in {available_states[user_input_state - 1]}", show_lines=True)
        table.add_column("Company Name")
        table.add_column("Website")
        table.add_column("Number of Job Offerings")

        for item in result:
            table.add_row(item["_id"], item["website"], str(item["job_count"]))

        with console.pager():
            console.print(table)
    
    else:
        print("\n[bold red]Please insert a valid number.")



def query4():
    available_sectors = jobs_collection.distinct('Company.sector')
    table = Table(title=f"Available Sectors", box = box.ASCII)
    table.add_column("Sector Names", no_wrap=True)
    for n, sector in enumerate(available_sectors, start=1):
        table.add_row(f"{n} - {sector}")
    console.print(table)

    user_input_sector = int(input("Specify the desired 'Sector Name' corresponding number -> "))

    if user_input_sector in range(1, len(available_sectors) + 1):
        pipeline_query4 = [
            {
                "$match": {
                    "Company.sector": available_sectors[user_input_sector - 1]
                }
            },
            {
                "$group": {
                    "_id": "$Company.company_name",
                    "CEO": {"$first": "$Company.CEO"},
                    "max_size": {"$max": "$company_size"}
                }
            },
            {
                "$sort": {
                    "max_size": -1
                }
            },
            {
                "$limit": 3
            },
            {
                "$project": {
                    "_id": 0,
                    "CEO": 1,
                    "Company": "$_id",
                    "company_size": "$max_size"
                }
            }
        ]
        result = jobs_collection.aggregate(pipeline_query4)

        # Display the results
        table = Table(title=f"Top 3 CEOs of the largest companies in the {available_sectors[user_input_sector - 1]} sector", leading=1, show_lines=True)
        table.add_column("CEO", style="dim", width=30)
        table.add_column("Company Name", style="dim", width=30)
        table.add_column("Company Size", style="dim", width=15)

        for entry in result:
            table.add_row(str(entry["CEO"]),str(entry["Company"]), str(entry["company_size"]))

        with console.pager():
            console.print(table)

    else:
        print("\n[bold red]Please insert a valid number.")
    


if __name__ == '__main__':
    # Print out existing databases and collections
    # print(client.list_database_names())
    # print(jobs_db.list_collection_names())

    print("""
     ██╗ ██████╗ ██████╗ ███████╗    ███████╗██╗███╗   ██╗██████╗ ███████╗██████╗      █████╗ ██████╗ ██████╗     ██████╗     ██████╗ 
     ██║██╔═══██╗██╔══██╗██╔════╝    ██╔════╝██║████╗  ██║██╔══██╗██╔════╝██╔══██╗    ██╔══██╗██╔══██╗██╔══██╗    ╚════██╗   ██╔═████╗
     ██║██║   ██║██████╔╝███████╗    █████╗  ██║██╔██╗ ██║██║  ██║█████╗  ██████╔╝    ███████║██████╔╝██████╔╝     █████╔╝   ██║██╔██║
██   ██║██║   ██║██╔══██╗╚════██║    ██╔══╝  ██║██║╚██╗██║██║  ██║██╔══╝  ██╔══██╗    ██╔══██║██╔═══╝ ██╔═══╝     ██╔═══╝    ████╔╝██║
╚█████╔╝╚██████╔╝██████╔╝███████║    ██║     ██║██║ ╚████║██████╔╝███████╗██║  ██║    ██║  ██║██║     ██║         ███████╗██╗╚██████╔╝
 ╚════╝  ╚═════╝ ╚═════╝ ╚══════╝    ╚═╝     ╚═╝╚═╝  ╚═══╝╚═════╝ ╚══════╝╚═╝  ╚═╝    ╚═╝  ╚═╝╚═╝     ╚═╝         ╚══════╝╚═╝ ╚═════╝                                                                                                                              
""")

    data = pd.read_csv("jobs_sample.csv", index_col=0)

    # Check if there's the need to load the data
    if jobs_collection.count_documents(filter = {}) == 0:
        load_data(data)


    while True:
        print("")
        print(Panel(
    ' 0) - Quit\n \
1) - Find the top 3 most required skills by company\n \
2) - Discover who you should contact if you want to work in a given industry\n \
3) - Find out which companies have the highest number of job offerings in a given state\n \
4) - Discover the CEOs of the largest companies by sector',
    title = "[bold yellow]Select the query you want to execute"
        )
    )
        try:
            desired_query = int(input('\nYour choice -> '))
            
            if desired_query == 0:
                break

            elif desired_query == 1:
                query1()
            
            elif desired_query == 2:
                query2()
            
            elif desired_query == 3:
                query3()

            elif desired_query == 4:
                query4()

            elif desired_query not in [0,1,2,3,4]:
                print("\n[bold red]Please insert a valid number.")

        except ValueError:
            print("\n[bold red]Please insert a valid number.")

    console.print("\nGoodbye 👋", style = 'bold #96EFFF')