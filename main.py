# this is the RESTful API server
# /util folder is used to store utility functions

from fastapi import FastAPI, File, UploadFile
from fastapi.middleware.cors import CORSMiddleware
import json
import csv
import numpy as np 
import pandas as pd 
import sqlalchemy
from sqlalchemy import create_engine

from util import calc_pnl
from util import dashboard_totals

app = FastAPI()

origins = ["*"]

# enable CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"]
)

pd.set_option("display.max_rows", None, "display.max_columns", None)

# # AWS RDS database connection handling
# database_username = 'kevinthosatria'
# database_password = 'kevinthosatria'
# database_ip       = 'satori-pnl-db.clkwznmucv7d.us-east-2.rds.amazonaws.com:3306'
# database_name     = 'satoridb'
# database_connection = create_engine('mysql+mysqlconnector://{0}:{1}@{2}/{3}'.format(database_username, database_password, database_ip, database_name))
    
# Heroku database connection handling
database_username = 'b3e42ad0cd1c56'
database_password = 'cf9e5802'
database_ip       = 'us-cdbr-east-05.cleardb.net'
database_name     = 'heroku_342aa3d35f902ab'
database_connection = create_engine('mysql+mysqlconnector://{0}:{1}@{2}/{3}'.format(database_username, database_password, database_ip, database_name))


rs = database_connection.execute('SHOW TABLES')
for row in rs:
    print(row)

print("-----------------------------------------------")

trade_df = 0
price_df = 0

# @app.on_event('startup')
# def init_data():
#     # pre-initialize trade_df and price_df as global variables for higher performance, make sure db is already pre-filled with data first
#     trade_df = pd.read_sql_table('trade', database_connection)
#     trade_df['trade_datetime'] = pd.to_datetime(trade_df['trade_datetime']).dt.tz_localize(None) # convert to datetime data type 
#     trade_df['trade_datetime_rounded_down_hour'] = trade_df['trade_datetime'].dt.floor('h') # round down to nearest hour

#     price_df = pd.read_sql_table('price', database_connection)
#     price_df['price_datetime'] = pd.to_datetime(price_df['price_datetime']) # convert to datetime data type 

def upload_db_trade(file):
    # transform csv file to python dataframe
    df = pd.read_csv(file)

    # get rid of tabs \t
    df['date'] = df['date'].str.replace("\t ","")
    df['datetime'] = df['datetime'].str.replace("\t ","")
    df['subaccount'] = df['subaccount'].str.replace("\t ","")
    df['instrument'] = df['instrument'].str.replace("\t ","")
    df['side'] = df['side'].str.replace("\t ","")
    df['market'] = df['market'].str.replace("\t ","")
    df['feecurrency'] = df['feecurrency'].str.replace("\t ","")
    df['fee_type'] = df['fee_type'].str.replace("\t ","")

    # drop columns to match database schema
    db_df = df.drop(['timestamp', 'date', 'counterparty', 'account', 'subaccount', 'id', 'source', 'remarks'], axis=1)    

    # rename datetime column to match database schema
    db_df = db_df.rename(columns={'datetime': 'trade_datetime'})

    # insert db_df into mysql https://stackoverflow.com/questions/16476413/how-to-insert-pandas-dataframe-via-mysqldb-into-database
    db_df.to_sql(con=database_connection, name='trade', if_exists='replace')

    # re-assign trade_df value
    trade_df = pd.read_sql_table('trade', database_connection)
    trade_df['trade_datetime'] = pd.to_datetime(trade_df['trade_datetime']).dt.tz_localize(None) # convert to datetime data type 
    trade_df['trade_datetime_rounded_down_hour'] = trade_df['trade_datetime'].dt.floor('h') # round down to nearest hour
    
    print(trade_df.head())

    return {"message" : "trade upload complete"}


def upload_db_price(file):
    # transform csv file to python dataframe
    df = pd.read_csv(file)

    # drop columns to match database schema
    db_df = df.drop(['date', 'timestamp', 'counterparty', 'source', 'remarks'], axis=1)

    # rename datetime column to match database schema
    db_df = db_df.rename(columns={'datetime': 'price_datetime'})

    # insert db_df into mysql 
    db_df.to_sql(con=database_connection, name='price', if_exists='replace')

    # re-assign price_df value
    price_df = pd.read_sql_table('price', database_connection)
    price_df['price_datetime'] = pd.to_datetime(price_df['price_datetime']) # convert to datetime data type


@app.post("/upload/trade")
async def upload_trade(trade_csv: UploadFile = File(...)):
    upload_db_trade(trade_csv.file)


@app.post("/upload/price")
async def upload_price(price_csv: UploadFile = File(...)):
    upload_db_price(price_csv.file)

@app.get("/dashboard/pnl")
async def get_dashboard_pnl():
    # query database and transform returned query results into dataframes
    trade_df = pd.read_sql_table('trade', database_connection)
    price_df = pd.read_sql_table('price', database_connection)

    # calculate pnl, use global variables of trade_df, price_df, database_connection
    pnl_json = calc_pnl(trade_df, price_df)
    return pnl_json

@app.get("/dashboard/totals")
async def get_dashboard_totals():
    totals_json = dashboard_totals()

    return totals_json

@app.get("/")
async def test():
    return {"message": "Hello world"}



