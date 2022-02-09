import asyncio
import aiohttp
import ssl as ssl_lib
import datetime
from datetime import date
import pandas as pd
import requests
from requests.exceptions import HTTPError
import certifi

COUNTER = 0
RATE_LIMIT = 5000
GIT_TOKEN = ""
TOKEN_EXHAUST = 0 
GIT_SECRET_LIMIT = {}
NO_COMMITS = 0
repo_name = "kubernetes/kubernetes"
min_date = '2010-01-01'
max_date = '2016-08-01'

#Fetch commit data for one page
async def fetch(session, url):
    global GIT_TOKEN
    headers={'Authorization':"Token "+ GIT_TOKEN} 
    try:
        # get_token_value()
        ssl_context = ssl_lib.create_default_context(cafile=certifi.where())
        response = await session.get(url, headers=headers, ssl=ssl_context)
        response.raise_for_status()
        ratevalue = int(response.headers['X-RateLimit-Remaining'])
        print("ratelimit ", ratevalue)
        
    except HTTPError as http_err:
        print(f"HTTP error occurred: {http_err}")
    except Exception as err:
        print(f"An error ocurred: {err}") 
    return await response.json()

#Async function to fetch commit data for all page
async def fetch_all(urls, loop):
    connector = aiohttp.TCPConnector(limit=30)
    async with aiohttp.ClientSession(loop=loop,trust_env=True, connector=connector) as session:
        try: 
            results = await asyncio.gather(*[fetch(session, url) for url in urls], return_exceptions=True)
        except Exception as err:
            print("Exception in results in fetch_all", err)
        return results

def commit_data(df):
    new = df["Domains"].str.split("@", n = 1, expand = True)
    df['domains'] = new[1]
    df['count'] = 1  
    df.index = pd.to_datetime(df['Commit_Date'])
    df=df.groupby(by=[pd.Grouper(freq="M"),'domains'])["count"].sum()
    return df
        
#Function to get dataframe with API values
def get_df_from_api(repo_name, since_date, until_date):
    global GIT_TOKEN
    since_date = since_date + "T00:00:00Z"
    until_date = until_date + "T23:59:59Z"
    url_list = []
    htmls = []
    loop = asyncio.get_event_loop()
    api_dataframe = pd.DataFrame()
    url = "https://api.github.com/repos/"+repo_name+"/commits?per_page=100"+"&since="+since_date+"&until="+until_date
    headers={'Authorization':"Token "+ GIT_TOKEN}
    res=requests.get(url,headers=headers)
    last_page = res.links.get('last')
    if last_page is not None:
        last_page_number = int(last_page['url'].split('page=')[-1]) + 1
        print("last page number", last_page_number)
    else:
        last_page_number = 2
        print("last page number",last_page_number)
    for i in range(1, last_page_number):
        url_list.append('https://api.github.com/repos/'+repo_name+'/commits?simple=yes&per_page=100&page='+str(i)+"&since="+since_date+"&until="+until_date)
    htmls = loop.run_until_complete(fetch_all(url_list, loop))
    print("------------------ htmls ------------")
    print("Since date: ",since_date)
    print("until date:", until_date) 

    email_list =[]
    commit_sha_list = []
    commit_date_list = []
    for html in htmls:
        if type(html) is list:
            for commits in html:
                email_list.append(commits['commit']['author']['email'])
                commit_sha_list.append(commits['sha'])
                commit_date_list.append(commits['commit']['committer']['date'])

    api_dataframe = pd.DataFrame({'Author_Email': email_list, "Commit_Sha": commit_sha_list, "Commit_Date": commit_date_list})
    api_dataframe['Reponame'] = repo_name
    api_dataframe= api_dataframe[["Reponame", "Author_Email", "Commit_Sha", "Commit_Date"]]
    api_dataframe.columns = ['Reponame', 'Author_Email', 'Commit_Sha', 'Commit_Date']
    return api_dataframe

if __name__ == "__main__":
    
    df_api = pd.DataFrame()
    consolidated_data = pd.DataFrame()

    #Validate Date
    try:
        if min_date > max_date:
            raise Exception("The Date parameters are invalid. Please query with valid dates")      
    except :
        print("Invalid Date Parameters")
        exit()

    # Validate API Connection
    try:   
        print("GIT_TOKEN TO VALIDATE API CONNECTION", GIT_TOKEN)
        url = "https://api.github.com/repos/"+repo_name+"/commits?simple=yes&per_page=100&page=1"
        headers={'Authorization':"Token "+ GIT_TOKEN}
        res=requests.get(url,headers=headers)
        if res.status_code != 200 and res.status_code == 404:
            raise Exception("Repository does not Exist")
        if res.status_code != 200 and res.status_code == 403:
            raise Exception("Access forbidden")    
    except Exception as e:
        print("Invalid Token", res)
        exit()


    df_api = get_df_from_api(repo_name, min_date, max_date)
    
    generic_user_domain = ['gmail.com','users.noreply.github.com', 'yahoo.com', 'hotmail.com']
    df_api['Domains'] = ["Individual contributors" if extension in generic_user_domain else extension for extension in df_api['Author_Email']]

    consolidated_csv = commit_data(df_api)
        
    consolidated_csv.to_csv('consolidated.csv')
    
        
    
   




       
   
    
    










