# -*- coding: utf-8 -*-
"""
Created on Thu Jan  14 00:16:46 2020

@author: hp
"""

import requests
import pandas as pd
import time



#API URL with parameters (FICTIONAL)
url = "https://consciousgrowth.1234.co m?access_key=any_key&base=EUR&symbols=BPI,USD,EUR,GBP"

#Create SQL_readable Dataframe for further work
fx_price_index_df = pd.DataFrame()


def conscious_api():
    global fx_price_index_df
    
    #Request data from given URL with parameters
    response = requests.get(url)
    
    #Store requsted data as JSON in a variable
    k = response.json()
    
    
    if fx_price_index_df.empty:
        
        #Store data from API in our dataframe
        fx_price_index_df = pd.DataFrame(k)
        fx_price_index_df.reset_index(level=0, inplace=True)
        
        #considering the GetConscious server also sends the same output as the Fixer API
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
    conscious_api()

    print("Application called")
    #"Application scheduled to run every 5 minutes" as per requirement
    time.sleep(300)
    
