# -*- coding: utf-8 -*-
"""
Created on Sat Jan 16 14:44:13 2021

@author: hp
"""

import time
from authlib.integrations.requests_client import OAuth2Session
import pandas as pd

    
client_id = r'A8f8B0300v456j7n'
client_secret = r'N35620nff35Ndb05'
#redirect_uri = 'https://your.callback/uri'
scope = ['all']
fx_price_index_df = pd.DataFrame()

def fixer_api():
    global fx_price_index_df
    
    #OAuth2Session for Authorization Code
    # using requests implementation
    client = OAuth2Session(client_id, client_secret, scope=scope)
    
    
    #Redirect to Authorization Endpoint
    #Assuming the endpoint exists
    authorization_endpoint = 'https://data.fixer.io/oauth/auth'
    uri, state = client.create_authorization_url(authorization_endpoint)
    print(uri)
    
    #Fetch Token
    #authorization_response = ''
    token_endpoint = 'https://data.fixer.io/oauth/token'
    token = client.fetch_token(token_endpoint, grant_type='client_credentials')
    
    #Prints token after fetching
    print(token)
    
    
    
    #retrieve data from Fixer IO API
    try:
        client = OAuth2Session(client_id, client_secret, token=token)
        account_url = "http://data.fixer.io/api/latest?access_key=4288a67481d97bf125686d7066a7bd5b&base=EUR&symbols=BPI,USD,EUR,GBP"
        response = client.get(account_url)
    
    #In case the token is expired, refresh token
    except: 
        new_token = client.refresh_token(token_endpoint, refresh_token=token.refresh_token)
        token = new_token
        client = OAuth2Session(client_id, client_secret, token=token)
        account_url = "http://data.fixer.io/api/latest?access_key=4288a67481d97bf125686d7066a7bd5b&base=EUR&symbols=BPI,USD,EUR,GBP"
        response = client.get(account_url)
        
    
    #Store requsted data as JSON in a variable
    k = response.json()
    
    
    if fx_price_index_df.empty:
        
        #Store data from API in our dataframe
        fx_price_index_df = pd.DataFrame(k)
        fx_price_index_df.reset_index(level=0, inplace=True)
        
        #Rename columns retrieved from Fixer API
        fx_price_index_df.rename(columns={'index': 'Exchange_Currency', 'base':'Base_Currency', 'rates':'Rate', 'date':'Date'}, inplace=True)
        fx_price_index_df['timestamp'] = pd.to_datetime(fx_price_index_df['timestamp'],unit='s')

    else:
        temp_df = pd.DataFrame(k)
        temp_df.reset_index(level=0, inplace=True)
        temp_df.rename(columns={'index': 'Exchange_Currency', 'base':'Base_Currency', 'rates':'Rate', 'date':'Date'}, inplace=True)
        temp_df['timestamp'] = pd.to_datetime(temp_df['timestamp'],unit='s')
        
        #Using Merge to make sure table "contains only one rate per day per currency" as per requirement
        temp_df = temp_df.merge(fx_price_index_df, on=['Exchange_Currency', 'Date'], how='left', indicator=True).query('_merge == "left_only"').drop('_merge', 1)
        
        #Append new results to existing dataframe
        fx_price_index_df = fx_price_index_df.append(temp_df)
 
        fx_price_index_df.reset_index(level=0, inplace=True, drop=True)
        fx_price_index_df = fx_price_index_df[['Base_Currency', 'Exchange_Currency', 'Rate', 'Date', 'timestamp']]
        del temp_df
        
while True:
        
    #Call function in continuous loop
    fixer_api()

    print("Application called")
    #"Application scheduled to run every 5 minutes" as per requirement
    time.sleep(300)
