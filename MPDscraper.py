#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed Jul 11 14:33:28 2018

@author: Donald Braman <donald.braman@gmail.com> Garrett Eason <easoncharles9@gmail.com>
Beautiful Soup MPD scraper
Set messages of interest, location of output, and districts of interest (1-7) and run to scrape MPD arrest data.
"""
#%% Packages
import pandas as pd
import requests
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry
from bs4 import BeautifulSoup
import re
import progressbar #used progressbar2
from os import listdir
from os.path import isfile, join

#%% User Inputs
# 5472 -beginning of 2008 1D
# 8979 - beginning of 2009 1D (looks like Arrest Reports start in 2009)
# 10181 - first Arrest Report instance I believe (missing this one and the one after because of incorrect time)
# 10199 - first working observation
# Only one arrest report was posted in 2013, with no arrest reports being reported 2012 11 and 12 (I double checked)
# 21422 - New format started in 2014 04
# 21562 - problem here KeyError: 'Arrest Date' - just ran the code again and it works fine...
# 36764
# 14725
location = 'C:/Users/Sade/Desktop/MPD_raw_data/' # Storage location of files
min_range = 10181 # Starting message number.
max_range = 36764 # Ending message number.
district_list = [1] # Districts to be searched.
counter = min_range
message_range = range(counter ,max_range)

#%% Functions
def requests_retry_session(
    retries=20,
    backoff_factor=0.5,
    status_forcelist=(500, 502, 504),
    session=None,
):
    session = session or requests.Session()
    retry = Retry(
        total=retries,
        read=retries,
        connect=retries,
        backoff_factor=backoff_factor,
        status_forcelist=status_forcelist,
    )
    adapter = HTTPAdapter(max_retries=retry)
    session.mount('http://', adapter)
    session.mount('https://', adapter)
    return session

def get_arrests(soup):
  df = pd.DataFrame()
  for table in soup.find_all('table'):
      for stable in table.find_all('table'):
        for sstable in stable.find_all('table'):
          table_list = pd.read_html(str(sstable), flavor='bs4')
          keys = table_list[0][0]
          values = table_list[0][1]
          arrest_dict = dict(zip(keys,values))
          row = pd.DataFrame(arrest_dict, index=[0])
          df = df.append(row)
  if len(df) != 0:
      df['Arrest Date'] = pd.to_datetime(df['Arrest Date'])
      df = df.reset_index(drop=True)
      if 'DOB' in list(df.columns):
          df['DOB'] = pd.to_datetime(df['DOB'])
          df['Age'] = [None]*len(df)
          for i in df.index:
              try:
                  df.loc[i,'Age'] = int((df.loc[i, 'Arrest Date'] - df.loc[i, 'DOB']).days / (365.25))
              except:
                  pass
          df = df.drop('DOB', axis = 1)
          df = df[['Arrest Date', 'Arrest Location', 'Arrest Number', 'Felony/Misdemeanor', 'Gender', 'Offender First Name', 'Offender Last Name', 'Offense', 'Officer', 'PSA', 'Age']]  
  return df

def get_arrests_old(soup):
    Arrests=[]
    Dt = []
    Location = []
    PSA = []
    Last = []
    First = []
    Sex = []
    Offense = []
    Type = []
    Officer = []
    tables = soup.findAll(text=re.compile('ARREST#'))[0].parent
    for line in tables:
        if bool(re.findall('ARREST#', str(line))):
            try:
                Arrests.append(int(re.findall(r'\d+',str(line))[0]))
            except:
                Arrests.append('')
        if bool(re.findall('DT-TM:', str(line))):
            try:
                Dt.append(re.match(r'.*\: (.*)', str(line)[1:]).group(1))
            except:
                Dt.append('')
        if bool(re.findall('LOCATION:', str(line))):
            try:
                if bool(re.findall('PSA:', str(line))):
                    PSA.append(re.match(r'.*\: (.*)', str(line)[1:]).group(1))
                    Location.append(re.match(r'.*\: (.*?)\-', str(line)[1:]).group(1))
                else:
                    Location.append(re.match(r'.*\: (.*)', str(line)[1:]).group(1))
                    PSA.append('')
            except:
                Location('')
                PSA.append('')
        if bool(re.findall('OFFENDER:', str(line))):
            try:
                Last.append(re.match(r'.*\: (.*)\,', str(line)[1:]).group(1))
                First.append(re.match(r'.*\, (.*)', str(line)[1:]).group(1))
            except:
                Last.append('')
                First.append('')
        if bool(re.findall('SEX:', str(line))):
            try:
                Sex.append(re.match(r'.*\: (.*)', str(line)[1:]).group(1))
            except:
                Sex.append('')
        if bool(re.findall('OFFENSE:', str(line))):
            try:
                Offense.append(re.match(r'.*\: (.*)', str(line)[1:]).group(1))
            except:
                Offense.append('')
        if bool(re.findall('TYPE:', str(line))):
            try:
                Type.append(re.match(r'.*\: (.*)', str(line)[1:]).group(1)[0])
            except:
                Type.append('')
        if bool(re.findall('OFFICER:', str(line)[1:])):
            try:
                Officer.append(re.match(r'.*\: (.*)', str(line)[1:]).group(1))
            except:
                Officer.append('')
    Age = [None]*len(Arrests)
    df = pd.DataFrame({'Age':Age, 'Arrest Date':Dt, 'Arrest Location':Location, 'Arrest Number':Arrests, 'Felony/Misdemeanor':Type, 'Gender':Sex, 'Offender First Name':First, 'Offender Last Name':Last, 'Offense':Offense, 'Officer':Officer, 'PSA':PSA})
    df['Arrest Date'] = pd.to_datetime(df['Arrest Date'])
    return df.reset_index(drop=True)

def save_to_csv(arrests, district_number):
  date = arrests['Arrest Date'][0].date()
  filename = 'MPD-'+ str(district_number) + 'D_' + str(date) + '.csv'
  arrests.to_csv(location+filename, header=True, index=False, encoding='utf-8')

#%% Scraping loops

#bar = progressbar.ProgressBar(maxval=max_range-counter, widgets=[progressbar.Bar(marker='=', left='[', right=']', fill=' ', fill_left=True), ' ', progressbar.ETA(), ' ', progressbar.Percentage()])
bar = progressbar.ProgressBar(maxval=max_range-counter, widgets=[progressbar.ETA(), ' ', progressbar.Percentage()])

for district_number in district_list:
  bar.start()
  for message_number in message_range:
    counter = message_number
    print(bar.update(message_number-min_range+1))
    target_url = 'https://groups.yahoo.com/neo/groups/MPD-'+str(district_number)+'D/conversations/messages/'+str(message_number)
    r = requests_retry_session().get(target_url)
    soup = BeautifulSoup(r.text, 'html.parser') 
    daily_report_new = bool(soup.findAll(text='LISTSERV Daily Arrest Report'))
    data_available = bool(soup.findAll(text='No Data Available'))
    if daily_report_new:
        if not data_available:
            arrests = get_arrests(soup)
            if len(arrests) != 0:
                save_to_csv(arrests, district_number)
    try:
        daily_report_old = bool(soup.findAll(text=re.compile('ARREST#'))[0])
        if daily_report_old:
            arrests = get_arrests_old(soup)
            save_to_csv(arrests, district_number)
    except:
        pass
  print('District ' + str(district_number) + ' is Complete!')
  bar.finish()


#%% Concating
filenames = [location+f for f in listdir(location) if isfile(join(location, f))]
complete_arrests = pd.concat( [ pd.read_csv(f) for f in filenames ] )
complete_arrests = complete_arrests[['Arrest Date', 'Arrest Location', 'Arrest Number', 'Felony/Misdemeanor', 'Gender', \
         'Offender First Name', 'Offender Last Name', 'Offense', 'Officer', 'PSA', 'Age']]
complete_arrests = complete_arrests.reset_index(drop=True)
complete_arrests['Felony/Misdemeanor'] = complete_arrests['Felony/Misdemeanor'].replace('U', '')
complete_arrests['Gender'] = complete_arrests['Gender'].replace('U','')
complete_arrests.to_csv( location+'complete_arrests.csv', index=False )

