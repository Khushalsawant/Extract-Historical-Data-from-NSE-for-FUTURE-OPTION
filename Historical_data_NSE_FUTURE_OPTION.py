# -*- coding: utf-8 -*-
"""
Created on Sun Jul 28 18:08:08 2019

@author: khushal
"""


import time
start_time = time.time()


from urllib.request import build_opener, HTTPCookieProcessor, Request
from urllib.parse import urlencode
from http.cookiejar import CookieJar

import datetime
import zipfile

import io
import pandas as pd
import numpy as np
import requests

from io import StringIO


"https://www.nseindia.com/content/historical/DERIVATIVES/2019/JUN/fo03JUN2019bhav.csv.zip"

def get_bhavcopy_url(d):
    """take date and return bhavcopy url"""
    bhavcopy_base_url = "https://www.nseindia.com/content/historical/DERIVATIVES/%s/%s/fo%sbhav.csv.zip"
    
    year = d.strftime('%Y')
    #print(year)
    month = d.strftime('%b').upper()
    #print(month)
    date = d.strftime('%d%b%Y').upper()
    #print(date)
    url = bhavcopy_base_url % (year, month, date)
    return url

def nse_opener():
    """
    builds opener for urllib2
    :return: opener object
    """
    cj = CookieJar()
    return build_opener(HTTPCookieProcessor(cj))

def convert_sec(n): 
    return str(datetime.timedelta(seconds = n))

if __name__ == "__main__":
    FO_historical_data_df = pd.DataFrame()    
    headers = {'Accept' : '*/*',
                'Accept-Language' : 'en-US,en;q=0.5',
                'Host': 'nseindia.com',
                'Referer': "https://www.nseindia.com/live_market/dynaContent/live_watch/get_quote/GetQuote.jsp?symbol=INFY&illiquid=0&smeFlag=0&itpFlag=0",
                'User-Agent' : 'Mozilla/5.0 (Windows NT 6.1; WOW64; rv:28.0) Gecko/20100101 Firefox/28.0',
                'X-Requested-With': 'XMLHttpRequest'
                }
    Start_date = datetime.datetime.now() + datetime.timedelta(-8)
    Current_date = datetime.datetime.now() + datetime.timedelta(-1)
    date_value_df = pd.date_range(start=Start_date, end=Current_date)
    path_for_zip_extract = "C:/Users/khushal/Downloads/FO_Historical_data"
    opener = nse_opener()
    #print("date_value_df = ",date_value_df)
    for d in date_value_df:
        NSE_URL = get_bhavcopy_url(d)
        response = requests.get(NSE_URL)        # To execute get request 
        if response.status_code == 200:
            response = opener.open(Request(NSE_URL, None, headers))
            
            zip_file_handle = io.BytesIO(response.read())
            zf = zipfile.ZipFile(zip_file_handle)
            filename = "fo"+d.strftime('%d%b%Y').upper()+"bhav.csv"
            
            bytes_data = zf.read(filename)
            s=str(bytes_data,'utf-8')
            
            #print(type(bytes_data))
            
            # Access file from memory space
            data = StringIO(s) 
            df=pd.read_csv(data)
            
            #print(df)
            FO_historical_data_df = FO_historical_data_df.append(df)
            # printing all the contents of the zip file 
            #zf.printdir('File Name') 
            
            # extracting all the files 
            #zf.extractall(path_for_zip_extract)
            print(" Extrctig file for %s" %d.strftime('%d%b%Y').upper())
        elif response.status_code != 200:
            print("status_code = ",response.status_code)
            print(" Un-reachable NSE_URL = ",NSE_URL)
    
    print(FO_historical_data_df)
    final_file_details = path_for_zip_extract + "FO_historical_data.csv"
    FO_historical_data_df.to_csv(final_file_details)
    '''
    URL = get_bhavcopy_url(d)
    response = requests.get(URL,verify=False)        # To execute get request 

    print("status_code = ",response.status_code) 
    #.strftime("%d%b%Y")
    '''
    n =  time.time() - start_time
    print("---Execution Time ---",convert_sec(n))