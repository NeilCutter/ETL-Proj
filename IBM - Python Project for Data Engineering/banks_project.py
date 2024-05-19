from bs4 import BeautifulSoup
import requests
import pandas as pd
import numpy as np
import sqlite3
from datetime import datetime


def log_progress(message):
    timestamp_format = "%Y-%h-%d-%H:%M:%S"
    now = datetime.now()
    timestamp = now.strftime(timestamp_format)

    filename = "code_log.txt"
    with open(filename, "a") as f:
        f.write(timestamp + " : " + message + "\n")


def extract(url, table_attribs):
    response = requests.get(url).text
    soup = BeautifulSoup(response, "html.parser")
    table = soup.find_all("table")
    data = table[0].find("tbody")
    rows = data.find_all("tr")
    df = pd.DataFrame(columns=table_attribs)

    for row in rows:
        col = row.find_all("td")
        if len(col) != 0:
            data_dict = {
                "Name": col[1].find_all("a")[1].contents[0],
                "MC_USD_Billion": col[2].contents[0],
            }
            df1 = pd.DataFrame([data_dict])
            market_cap = df1["MC_USD_Billion"].tolist()
            market_cap = [float(x.strip()) for x in market_cap]
            df1["MC_USD_Billion"] = market_cap
            df = pd.concat([df, df1], ignore_index=True)
    return df


def transform(df, csv_path):
    df_er = pd.read_csv(csv_path)
    exchange_rate = df_er.set_index('Currency').to_dict()['Rate']

    cols = ['MC_EUR_Billion','MC_GBP_Billion','MC_INR_Billion']
    for col in cols:
        currency = col.split('_')[1]
        df[col] = [np.round(x*exchange_rate[currency], 2) for x in df["MC_USD_Billion"]]

    return df

def load_to_csv(df, output_path):
    df.to_csv(output_path)


def load_to_db(df, sql_connection, table_name):
    df.to_sql(name=table_name, con=sql_connection, if_exists='replace', index=False)


def run_query(query_statement, sql_connection):
    print(pd.read_sql(query_statement, sql_connection))


# Initial Process
log_progress("Preliminaries complete. Initiating ETL process")

# Extraction
url = "https://web.archive.org/web/20230908091635/https://en.wikipedia.org/wiki/List_of_largest_banks"
attribs = ["Name", "MC_USD_Billion"]
df_extract = extract(url, attribs)
log_progress("Data extraction complete. Initiating Transformation process")

# Transform
csv_path = './exchange_rate.csv'
df = transform(df_extract, csv_path)
log_progress('Data transformation complete. Initiating Loading process')

# Loading Data to CSV
output_path = './Largest_banks_data.csv'
load_to_csv(df, output_path)
log_progress('Data saved to CSV file')

# Loading Data to Database
table_name = 'Largest_banks'
sql_connection = sqlite3.connect('banks.db')
load_to_db(df, sql_connection, table_name)
log_progress('SQL Connection initiated')
log_progress('Data loaded to Database as a table, Executing queries')

# Writing Queries
query_statement = 'SELECT * FROM Largest_banks'
run_query(query_statement, sql_connection)
log_progress('Process Complete')

log_progress('Server Connection closed')
