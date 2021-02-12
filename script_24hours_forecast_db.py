import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt
import numpy as np
import warnings
import pyodbc
import pickle

warnings.filterwarnings('ignore')

#import xgboost

import dill
import sys

import glob
import re

import time

def unix_to_utc(df):
    time_str = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(df))
    return time_str

def clean_weather_rainfall(df):
    if df != str(0):
        df = df[7:-1]
    else:
        df = float(df)
    return float(df)



CONN_STR = (
   r'Driver={ODBC Driver 13 for SQL Server};'
   r'Server=tcp:servian-hack-at.database.windows.net,1433;'
   r'Database=Air_Quality;'
   r'Uid=tim;'
   r'Pwd=Servian@2021;'
   r'Encrypt=yes;'
   r'TrustServerCertificate=no;'
   r'Connection Timeout=30;'
)


def fetch_models(parameter_name=None, station_id=None):
    conn = pyodbc.connect(CONN_STR)
    
    query = r'SELECT * FROM dbo.STD_Forecast_Models'

    if parameter_name != None or station_id != None:
        query = query + ' WHERE '
        
        if parameter_name != None and station_id != None:
            query = query + str.format('parameter_name = \'{0}\' AND station_id = \'{1}\'', parameter_name, station_id)
        elif parameter_name != None:
            query = query + str.format('parameter_name = \'{0}\'', parameter_name)
        else:
            query = query + str.format('station_id = \'{0}\'', station_id)

    df = pd.read_sql(query, conn)

    return df


def make_predictions(parameter):
    parameter_dict = {
        "Particles TSP": "TSP",
        'Particle PM2.5': 'PM25',
        'Particle PM10': 'PM10',
        'Visibility': 'visibility',
        'Carbon monoxide': 'carbon_monoxide',
        'Nitrogen dioxide': 'nitrogen_dioxide',
        'Ozone': "ozone",
        'Sulfur dioxide': "sulfur_dioxide"
    }
    com_df = pd.DataFrame()
    pickle_df = fetch_models(parameter_name = parameter_dict[parameter])

#for abc in glob.glob("C:/Users/arnab/Desktop/ServianAzureHack/XGBoost_pkl/TSP/*.pkl"):
    for _, pickle_row in pickle_df.iterrows():
        station_id = pickle_row['station_id']#abc[56:59]
        pollutant = parameter
        mes_str = station_pol_complete[(station_pol_complete['pollutant'] == pollutant) & (station_pol_complete['station_id'] == station_id)].measurement.values[0]

        mes_str = mes_str[1:-1] #To remove square brackets
        mes_str = mes_str.replace("'","") #Replacing single quotes

        #         mes_df = weather_df[weather_df['station_id'] == station_id].iloc[1,:]
        mes_rows = weather_df[weather_df['station_id'] == station_id].shape[0]
        #print(mes_str)
        result_dic = {'station_id': [],'dt':[],'yp': []}
        for row in range(mes_rows):
            mes_df = weather_df[weather_df['station_id'] == station_id].iloc[row,:]
            mes_list = re.split('; |, |\*|\n',mes_str)

            # print(mes_list)
            feature_dic = {}
            for mes in mes_list:
                feature_dic[mes] = mes_df[mes]
            # print(feature_dic)
            feature_df = pd.DataFrame(feature_dic,index =[0])
            X = feature_df.values
            #with open(abc, 'rb') as f:
                #PREDICTOR = dill.load(f)
            #PREDICTOR = dill.load(pickle_row["model"])
            PREDICTOR = pickle.loads(pickle_row["model"])
            #print(PREDICTOR)
            ypred = PREDICTOR.predict(X)


            result_dic['station_id'].append(station_id)
            result_dic['dt'].append(weather_df.iloc[row,0])
            result_dic['yp'].append(ypred)

        result_df = pd.DataFrame(result_dic)

        com_df = com_df.append(result_df)
    print(com_df.head())
#         print(com_df)
#com_df.to_csv("C:/Users/arnab/Desktop/ServianAzureHack/XGBoost_pkl/TSP/TSP.csv")



if __name__ == "__main__":
    weather_df = pd.read_csv(r'C:\Users\arnab\Desktop\ServianAzureHack\weather_forecast.csv')
    station_pol_complete = pd.read_csv(r'C:\Users\arnab\Desktop\ServianAzureHack\station_pol_complete.csv')
    try :
        weather_df.drop(columns=['Unnamed: 0'],inplace=True)
    except:
        print('No column name Unnamed: 0')
    try :
        station_pol_complete.drop(columns=['Unnamed: 0'],inplace=True)
    except:
        print('No column name Unnamed: 0')

    make_predictions('Ozone')



# @Tim : After you use the API to get the Weather Data, it would be uncleaned. store it as a CSV
#@Tim : Load that CSV as read_csv and below 2 lines are for cleaning. Remove any column name 'Unnamed: 0'
# weather_df.rename(columns={'temp':'Temperature','wind_speed':'Wind speed','rain':'Rainfall','wind_deg':'Wind direction','humidity':'Humidity','uvi':'Solar radiation'},inplace=True)
# weather_df['Rainfall'] = weather_df['Rainfall'].fillna(0)


#weather_df.drop(columns=['Unnamed: 0'],inplace=True)
#weather_df['Rainfall'] = weather_df[weather_df['Rainfall'] != str(0)]['Rainfall'].apply(clean_weather_rainfall)

#station_pol_complete = pd.read_csv(r'C:\Users\big-g\source\repos\Servian Hackathon\Servian Hackathon\XGBoost_pkl\station_pol_complete.csv')
#station_pol_complete.drop(columns=['Unnamed: 0'],inplace=True)

#if __name__ == "__main__":
    #pickle_df = fetch_models(parameter_name='TSP')
    #print(pickle_df)




