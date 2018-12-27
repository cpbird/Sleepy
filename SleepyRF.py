# -*- coding: utf-8 -*-
"""
Created on Thu Dec 27 09:54:41 2018

Sleepy Refactor

@author: cpbird
About:
    
    This project is aimed at comparing my personal data about sleeping and other health metrics from two sources:
        -Cpap Machine
        -Fitbit
                
Python Version 3.6
    
DATA:    
Pulling all the cpap data was done using sleepyhead:
https://sleepyhead.jedimark.net/
Sleepyhead has a export csv function that I used to create the csv file needed
The fitbit data is exported through their site and comes in JSON format.

"""
import pandas as pd
from datetime import timedelta, date
from pandas.io.json import json_normalize
import json
import os
import math



#So I can see all the data in console:
pd.options.display.max_rows = 600
#Read in Cpap data, only used certain columns, a lot of columns are not necessary
cpap = pd.read_csv("/Users/cpbir/OneDrive/Desktop/CSProj/CSProj/Cpap/Cpap.csv",usecols=(0,4,5), names= ['dateOfSleep', 'Duration', 'AHI'])

#Now to read in fitbit json data, its a bit more work than the cpap
#Choose a date range, find relevant files, only include ones that exist in files list

def daterange(date1, date2):
    for n in range(int ((date2 - date1).days)+1):
        yield date1 + timedelta(n)

start_dt = date(2017, 8, 1)
end_dt = date(2018, 11, 30)

files=[]
for dt in daterange(start_dt, end_dt):
    date= dt.strftime("%Y-%m-%d")
    filename= "/Users/cpbir/OneDrive/Desktop/CSProj/CSProj/Fitbit/sleep-%s.json" %date
    if os.path.isfile(filename):
        files.append(filename)   
#Now iterate through the desired files list to retrieve data
pieces = []
#Added pieces2 to unpack nested data for ahi vs restless data later
pieces2=[]

for f in files:
    frame = pd.read_json(f,orient='columns')    
    frame['date'] = date
    pieces.append(frame) 
for f in files:
    with open(f) as fr:
        data = json.load(fr)
        frame = json_normalize(data)
        pieces2.append(frame) 
#Concatenate into one DataFrame
fitbit = pd.concat(pieces, ignore_index=True)
fitbit2 = pd.concat(pieces2, ignore_index=True)

#Fitbit has multiple entries for certain dates, so need to consolidate that
fb_new=fitbit.groupby(['dateOfSleep']).agg({'duration': 'sum'}).reset_index()

#Now to make the duration the same unit in both dataframes and merge them (from ms to hrs)

fb_new['duration_fb'] = [t/3600000 for t in fb_new.duration]


#the cpap data is in HH:MM:SS, so convert that to basic hours int
def time_to_hours(time_str):  
    try:  
        hours, minutes, seconds = time_str.split(':')  
    except ValueError:  
        return -1  
    return (int(hours)*60 + int(minutes) + int(seconds)/60.0)/60

cpap['duration_cpap'] = [time_to_hours(t) for t in cpap.Duration]


#Clean up cpap dataframe, make date column datetime for merge
cpap = cpap.rename(columns={'dateOfSleep': 'date'})
cpap=cpap.drop(cpap.index[0])
cpap['date'] = pd.to_datetime(cpap['date'])


fb_new = fb_new.rename(columns={'dateOfSleep': 'date'})
fb_new['date'] = pd.to_datetime(fb_new['date'])

#fitbit sleep data is listed as the next day, offset it -1 to align with cpap
fb_new['date'] = pd.to_datetime(fb_new['date']).apply(pd.DateOffset(-1))



#Merge Dataframes into one and create pivot table of relevant data

sleep_data = pd.merge(fb_new, cpap)
sleep_pivot = sleep_data.pivot_table(values = ['duration_fb','duration_cpap'],index='date')

'''

Next section is commented out but it is all the print instructions to see the 
data comparisions

'''
##########################################################
'''
****THIS SECTION PRINTS TO CONSOLE*****


print(sleep_pivot)
print(sleep_pivot.corr(method='pearson', min_periods=1))
print(sleep_pivot.describe())
sleep_pivot.plot(figsize=(15,5))
sleep_pivot.plot.scatter(x='duration_fb' ,y='duration_cpap')
sleep_pivot.plot.box()
sleep_pivot.plot.density()
sleep_pivot['diff']= sleep_pivot['duration_fb'] - sleep_pivot['duration_cpap']
print(sleep_pivot['diff'].plot())
'''
###########################################################


'''
****THIS SECTION WRITES TO FILE****

sleep_pivot.describe().to_csv("sleep_describe.csv")

sleep_pivot.corr(method='pearson', min_periods=1).to_csv("sleep_corr.csv")

'''

#############################################################




'''
Now to another observation: sleep vs next day heart rate

'''

files=[]
for dt in daterange(start_dt, end_dt):
    date= dt.strftime("%Y-%m-%d")
    filename= "/Users/cpbir/OneDrive/Desktop/CSProj/CSProj/Fitbit/resting_heart_rate-%s.json" %date
    if os.path.isfile(filename):
        files.append(filename) 
#The json is nested and to get the relevant data i needed to normalize it
pieces=[]      
for f in files:
    with open(f) as fr:
        data = json.load(fr)
        frame = json_normalize(data)
        pieces.append(frame) 


#Concatenate into one DataFrame
fitbit_hr = pd.concat(pieces, ignore_index=True)



#Now I need to filter these results in order to remove any entries with 0 value for heart rate
#First fix column name so I can access it
fitbit_hr.columns= fitbit_hr.columns.str.strip().str.replace('value.','_')
#Now filter out the 0 value entries, make datetime column for merge
#fitbit heart rate data is listed as the next day, offset it -1 to align with cpap
fitbit_hr = fitbit_hr[fitbit_hr._value > 0]
fitbit_hr['date'] = pd.to_datetime(fitbit_hr['_date']).apply(pd.DateOffset(-1))


hr_data = pd.merge(fitbit_hr, cpap)

#Make dataframe only have relevant columns:
hr_data = hr_data[['date', 'duration_cpap', '_value', 'AHI']].copy()

#It wasn't including ahi in pivot because of ahi type, made sure to make it int
hr_data.AHI = pd.to_numeric(hr_data.AHI, errors='coerce')

#Now make pivot for duration vs heart rate
hr_pivot = hr_data.pivot_table(values = ['_value','duration_cpap', 'AHI'],index='date')
hr_pivot_dur = hr_data.pivot_table(values = ['_value','duration_cpap'],index='date')
hr_pivot_ahi = hr_data.pivot_table(values = ['_value','AHI'],index='date')
'''
Next section is graphing basic hr data

'''


'''

hr_pivot.plot.scatter(x='_value' ,y='duration_cpap')
hr_pivot.plot.scatter(x='_value' ,y='AHI')

hr_pivot_ahi.corr(method='pearson', min_periods=1).to_csv("ahi_corr.csv")
hr_pivot_dur.corr(method='pearson', min_periods=1).to_csv("dur_corr.csv")


'''




#Need to figure out a way to evaluate the combo of duration+ahi to make a quality


def ahi_quality1(ahi):
    ahi_q1 = (-1.2318 + (100.1506 - -1.2318)/(1 + (ahi/10.09376)**3.088323))
    if ahi_q1<0:
        ahi_q1=0
    if ahi_q1>100:
        ahi_q1=100
    return ahi_q1

def dur_quality1(dur):
    if dur > 11.4:
        dur=11.4
    dur_q1 = (1.648459e-12 - 70.7381*dur + 30.90952*dur**2 - 3.60119*dur**3 + 0.1297619*dur**4)
    if dur_q1<0:
        dur_q1=0
    if dur_q1>100:
        dur_q1=100
    return dur_q1

def ahi_quality2(ahi):
    ahi_q2 = -100 + (100 - -100)/(1 + (ahi/20)**1.584963)
    if ahi_q2<0:
        ahi_q2=0
    if ahi_q2>100:
        ahi_q2=100
    return ahi_q2

def dur_quality2(dur):
    dur_q2 =  (-20 + 1.666667*dur + 5*dur**2 - 0.4166667*dur**3 + 3.453101e-16*dur**4)
    if dur_q2<0:
        dur_q2=0
    if dur_q2>100:
        dur_q2=100
    return dur_q2

#New more severe algorithms to see if it will show any change in correlation

def ahi_quality_severe(ahi):
    if ahi>12:
        ahi=12
    ahi_severe = 99.93123 - 9.722397*ahi + 3.372815*ahi**2 - 0.507827*ahi**3 + 0.01977178*ahi**4
    if ahi_severe<0:
        ahi_severe=0
    if ahi_severe>100:
        ahi_severe=100
    return ahi_severe
def dur_quality_severe(dur):
    dur_severe = 100.0008*math.exp((-(dur - 8.00004)**2/(2*1.114704**2)))
    if dur_severe<0:
        dur_severe=0
    if dur_severe>100:
        dur_severe=100
    return dur_severe

def quality_weighted1(w_ahi, w_dur, ahi_q1, dur_q1):
    q1 = w_ahi*ahi_q1+w_dur*dur_q1
    return q1

def quality_weighted2(w_ahi, w_dur, ahi_q2, dur_q1):
    q2 = w_ahi*ahi_q2+w_dur*dur_q1
    return q2

def quality_weighted3(w_ahi, w_dur, ahi_q1, dur_q2):
    q3 = w_ahi*ahi_q1+w_dur*dur_q2
    return  q3

def quality_weighted4(w_ahi, w_dur, ahi_q2, dur_q2):
    q4 = w_ahi*ahi_q2+w_dur*dur_q2
    return  q4

def quality_weighted_severe(w_ahi, w_dur, ahi_severe, dur_severe):
    q_severe=  w_ahi*ahi_severe+ w_dur*dur_severe
    return q_severe

hr_pivot['ahi_q1'] = [ahi_quality1(x) for x in hr_pivot['AHI']]
hr_pivot['ahi_q2'] = [ahi_quality2(x) for x in hr_pivot['AHI']]
hr_pivot['dur_q1'] = [dur_quality1(x) for x in hr_pivot['duration_cpap']]
hr_pivot['dur_q2'] = [dur_quality2(x) for x in hr_pivot['duration_cpap']]

hr_pivot['ahi_severe'] = [ahi_quality_severe(x) for x in hr_pivot['AHI']]
hr_pivot['dur_severe'] = [dur_quality_severe(x) for x in hr_pivot['duration_cpap']]




#Weight(Future option to change weighting):
w_ahi = .30
w_dur = .70

#First weighted run of data with AHI = 30% and Duration=70%
hr_pivot['quality_1'] = hr_pivot[['ahi_q1','dur_q1']].apply(lambda x: quality_weighted1(w_ahi, w_dur, *x), axis=1)

hr_pivot['quality_2'] = hr_pivot[['ahi_q2','dur_q1']].apply(lambda x: quality_weighted2(w_ahi, w_dur, *x), axis=1)

hr_pivot['quality_3'] = hr_pivot[['ahi_q1','dur_q2']].apply(lambda x: quality_weighted3(w_ahi, w_dur, *x), axis=1)

hr_pivot['quality_4'] = hr_pivot[['ahi_q2','dur_q2']].apply(lambda x: quality_weighted4(w_ahi, w_dur, *x), axis=1)

hr_pivot['quality_severe'] = hr_pivot[['ahi_severe', 'dur_severe']].apply(lambda x: quality_weighted_severe(w_ahi, w_dur, *x), axis=1)
#Compare the quality data:


quality_compare = hr_pivot[['quality_1', 'quality_2', 'quality_3', 'quality_4', 'quality_severe']].copy()

'''
Next section commented out is comparing quality measures:



quality_compare.plot.box()

print(quality_compare.describe())
'''
'''
****THIS SECTION WRITES TO FILE****

quality_compare.describe().to_csv("quality_describe.csv")
'''


def hr_analyze_byDay(quality):
    print(hr_pivot[quality].describe())
    hr_pivot[quality].plot()
    quality_data = hr_pivot[[ '_value', quality]].copy()
    quality_data.plot()
    quality_data.plot.scatter(x='_value', y=quality)
    print(quality_data.corr(method='pearson', min_periods=1))

#Pick which quality measurement to use:
'''
hr_analyze_byDay('quality_1')
hr_analyze_byDay('quality_2')
hr_analyze_byDay('quality_3')
hr_analyze_byDay('quality_4')
hr_analyze_byDay('quality_severe')
'''
    
'''
Plot 7 day average of hr and 7 day average of quality score

'''

week_data = hr_pivot[['_value', 'quality_1', 'quality_2','quality_3', 'quality_4', 'quality_severe']]

#make index a series
week_data = week_data.reset_index()
dic = {'date': 'last', '_value': 'mean', 'quality_1': 'mean', 'quality_2':'mean', 'quality_3':'mean', 'quality_4':'mean', 'quality_severe':'mean'}

week_data = week_data.groupby(week_data.index//7).agg(dic)



def hr_analyze_byWeek(quality):
    print(week_data[quality].describe())
    week_data[quality].plot()
    quality_data = week_data[[ '_value', quality]].copy()
    quality_data.plot()
    quality_data.plot.scatter(x='_value', y=quality)
    print(quality_data.corr(method='pearson', min_periods=1))
    
'''
hr_analyze_byWeek('quality_1')   

hr_analyze_byWeek('quality_severe')
'''

    
'''
Comparing AHI and fitbit sleep restless count/minutes. I unpacked the nested
data into frame fitbit2 earlier


Added this late so it needs to be refactored, a lot of this is just redoing format
issues that could have been done before copying the dataframes. 

'''


#Make dataframe only have relevant columns:
fitbit2 = fitbit2[['dateOfSleep', 'levels.summary.awake.count', 'levels.summary.awake.minutes', 'levels.summary.restless.count','levels.summary.restless.minutes', 'levels.summary.wake.count', 'levels.summary.wake.minutes']].copy()
fitbit2.columns= fitbit2.columns.str.strip().str.replace('levels.summary.','')
#print(list(fitbit2))
#Fitbit has multiple entries for certain dates, so need to consolidate that
fb_new2=fitbit2.groupby(['dateOfSleep']).agg('sum').reset_index()

fb_new2 = fb_new2.rename(columns={'dateOfSleep': 'date'})
fb_new2['date'] = pd.to_datetime(fb_new2['date'])

#fitbit sleep data is listed as the next day, offset it -1 to align with cpap
fb_new2['date'] = pd.to_datetime(fb_new2['date']).apply(pd.DateOffset(-1))

#Lets look at one variable 

sleep_data2 = pd.merge(fb_new2, cpap)
sleep_data2.AHI = pd.to_numeric(sleep_data2.AHI, errors='coerce')

def createPivot(x, y):
    restless_pivot = sleep_data2.pivot_table(values = [x,y ],index='date')
    print(restless_pivot.corr(method='pearson', min_periods=1))
    restless_pivot.plot.scatter(x=x ,y=y) 



#createPivot('AHI', 'restless.count')
#createPivot('AHI', 'restless.minutes')
#createPivot('AHI', 'awake.count')
#createPivot('AHI', 'awake.minutes')
#createPivot('AHI', 'wake.count')    
#createPivot('AHI', 'wake.minutes')
