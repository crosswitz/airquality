import requests
import pandas as pd

# data= extract_current_weather_data(station_pol_complete)

# data

import time
def unix_to_utc(df):
    time_str = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(df))
    return time_str

#Getting 48 hours weather data for all locations
def weather_forecast_data():
    station_df = pd.read_csv('C:/Users/arnab/Desktop/ServianAzureHack/Station.csv')
    ak = '6ff0de1197cd7adf82bed10fd29359b8'
    complete_df = pd.DataFrame()
    for i in range(station_df.shape[0]):
        lat = station_df.iloc[i,8]
        lon = station_df.iloc[i,7]
        try:
            open_weather_url = 'https://api.openweathermap.org/data/2.5/onecall?lat='+str(lat)+'8&lon='+str(lon)+'&units=metric'+'&exclude="daily","minutely","alerts"&appid='+ak
            weather_response = requests.get(open_weather_url)
            data = weather_response.json()
            weather_forecast_df = pd.DataFrame(data['hourly'])
            weather_forecast_df['dt'] = weather_forecast_df['dt'].apply(unix_to_utc)
            weather_forecast_df['station_id'] = station_df.iloc[i,1]
#         print(weather_forecast_df.head(2))
            complete_df = complete_df.append(weather_forecast_df)
        except:
            print('Invalid Latitude and longitude')
        
    return complete_df

def clean_weather_rainfall(df):
    if df != 0:
#         print(df)
        df = str(df)
        df = df[7:-1]
    else:
        df = float(df)
    return float(df)

def weather_forecast_clean(weather_df):
    weather_df.rename(columns={'temp':'Temperature','wind_speed':'Wind speed','rain':'Rainfall','wind_deg':'Wind direction','humidity':'Humidity','uvi':'Solar radiation'},inplace=True)
    weather_df['Rainfall'] = weather_df['Rainfall'].fillna(0)
    weather_df['Rainfall'] = weather_df['Rainfall'].apply(clean_weather_rainfall)
    
    return weather_df

weather_df = weather_forecast_data()
weather_clean_df = weather_forecast_clean(weather_df)

weather_clean_df.to_csv('C:/Users/arnab/Desktop/ServianAzureHack/weather_clean.csv')
