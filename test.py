import pandas as pd

def commit_data(df):
    new = df["Domains"].str.split("@", n = 1, expand = True)
    df['domains'] = new[1]
    df['count'] = 1  
    df.index = pd.to_datetime(df['Commit_Date'])
    df=df.groupby(by=[pd.Grouper(freq="M"),'domains'])["count"].sum()
    return df

df_api = pd.read_csv('consolidated.csv')

generic_user_domain = ['gmail.com','users.noreply.github.com', 'yahoo.com', 'hotmail.com']
df_api['Domains'] = ["Individual contributors" if extension in generic_user_domain else extension for extension in df_api['Author_Email']]
email_api = commit_data(df_api)
    
email_api.to_csv('output.csv')