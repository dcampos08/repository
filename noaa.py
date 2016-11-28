# -*- coding: utf-8 -*-
"""
Created on Thu Apr 28 22:33:39 2016

@author: pcooman
"""
import httplib2
import urllib3
from bs4 import BeautifulSoup, SoupStrainer
import shutil
import zipfile
import os
import pandas as pd
import numpy as np

# change into the correct working directory
os.chdir('/Users/dcampos/Desktop/EE')

# set the base url (we will read the index.html of this url)
base_url = 'http://www.ncdc.noaa.gov/orders/qclcd/'
try:
    os.mkdir(os.getcwd() + '/data')
except Exception:
    print('data directory already exists')
    pass

http = httplib2.Http()
status, response = http.request(base_url)

links = np.array([])
# Load data if it exists
try:
    data = pd.read_csv(os.getcwd() + '/noaa_daily_weather.csv')
except Exception:
    print('appending to existing data')
    pass

for link in BeautifulSoup(response, parseOnlyThese=SoupStrainer('a', href = True)):
    link_text = ''    
    try:
        new_link = link['href']
        if '.zip' in new_link:
            links = np.append(links, link['href']) 
    except Exception:
        pass
    
for i in range(len(links)):
    link_text = links[i]    
    print(link_text)

    url = base_url + link_text
    c = urllib3.PoolManager()
        
    with c.request('GET',url, preload_content=False) as resp, open(os.getcwd() + '/data/' + link_text, 'wb') as out_file:
            shutil.copyfileobj(resp, out_file)
        
    resp.release_conn()
        
    # extract the zip file
    myzip = zipfile.ZipFile(os.getcwd() + '/data/' + link_text)
    myzip.extractall(os.getcwd() + '/data')
    
    # read in only the *daily.txt as a .csv and concatenate into one big data
    if 'data' not in locals():
        data = pd.read_csv(os.getcwd() + '/data/' + link_text[5:-4] + 'daily.txt')
    else:
        data_new = pd.read_csv(os.getcwd() + '/data/' + link_text[5:-4] + 'daily.txt')
        data = pd.concat([data, data_new], ignore_index=True)
            
    # delete all files in the /data/ folder    
    dirPath = os.getcwd() + '/data/'
    fileList = os.listdir(dirPath)
    for fileName in fileList:
        os.remove(dirPath + fileName)

# Send to a csv
data.to_csv('noaa_daily_weather.csv')       
