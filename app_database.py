import streamlit as st
import pandas as pd
import numpy as np
from PIL import Image
import time
import requests
import dill
import pytz
import datetime as pydt
from sqlalchemy import create_engine
import psycopg2
import os
import plotly.express as px
import plotly.graph_objects as go
import re
import glob
from datetime import date
import io

import pyodbc

CONN_STR = (
   r'Driver={ODBC Driver 13 for SQL Server};'
   r'Server=tcp:servian-hack-at.database.windows.net,1433;'
   r'Database=Air_Quality;'
     #r'Driver=SQL Server;'
     #r'Server=DESKTOP-MBKAO4T\SQLEXPRESS;'
     #r'Database=Sample_Azure;'
     #r'Trusted_Connection=Yes;'
   r'Uid=tim;'
   r'Pwd=Servian@2021;'
   r'Encrypt=yes;'
   r'TrustServerCertificate=no;'
   r'Connection Timeout=300;'
)


def fetch_prediction(parameter_name=None, station_id=None):
    conn = pyodbc.connect(CONN_STR)
    
    query = r'SELECT * FROM dbo.RES_AQ_Forecast'

    if parameter_name != None or station_id != None:
        query = query + ' WHERE '
        
        if parameter_name != None and station_id != None:
            query = query + str.format('pollutant = \'{0}\' AND station_id = \'{1}\'', parameter_name, station_id)
        elif parameter_name != None:
            query = query + str.format('pollutant = \'{0}\'', parameter_name)
        else:
            query = query + str.format('station_id = \'{0}\'', station_id)
    print(query)
    df = pd.read_sql(query, conn)
    print('Database output')
    print(df[df['dt'] == '2021-02-12 10:00:00'])

    return df

def fetch_stations(station_id=None):
    conn = pyodbc.connect(CONN_STR)
    
    query = r'SELECT * FROM dbo.REF_Station_List'

    if station_id != None:
        query = query + ' WHERE station_id = {0}'.format(station_id)

    df = pd.read_sql(query, conn)

    return df

def fetch_weather_forecast():
    conn = pyodbc.connect(CONN_STR)
    
    query = r'SELECT * FROM dbo.STD_Weather_Forecast'

    df = pd.read_sql(query, conn)

    return df

def fetch_weather_pol():
    conn = pyodbc.connect(CONN_STR)
    
    query = r'''
            SELECT 
                mf.* 
                , sl.station_name
                , sl.region_id
                , sl.link
                , sl.start_date
                , sl.end_date
                , sl.longitude
                , sl.latitude
            FROM REF_Model_Features mf
            INNER JOIN REF_Station_List sl
            ON sl.station_id = mf.station_id'''

    df = pd.read_sql(query, conn)

    return df

def fetch_image(tag):
    conn = pyodbc.connect(CONN_STR)
    crsr = conn.cursor()
    query = "SELECT image FROM REF_Images WHERE tag = '{0}'".format(tag)
    crsr.execute(query)
    row = crsr.fetchone()
    
    pre_img = io.BytesIO(row.image)
    image = Image.open(pre_img)

    crsr.close()
    return image


def aq_forecasting_stations(df,f_title):
    df['yp'] = df['yp'].clip(lower=0)
    df['yp'] = df['yp'].apply(int)
    
    # print(df.dtypes)
    fig = px.scatter_mapbox(df,
                        lat='latitude',
                        lon='longitude',
                        color='Hazard Risk',
                        size= 'yp',
                        size_max=40,
                        title=f_title,
                        zoom=10,
                        template='plotly_dark',
                        )
    fig.add_trace(go.Scattermapbox(
        lat=df.latitude,
        lon=df.longitude,
        mode='markers',
        marker=dict(symbol='square-stroked', size=10),
        showlegend=False
    ))
    fig.update_layout(
        paper_bgcolor="rgb(50,50,50)",
        title_font_size=20,
        height=500,
        mapbox=dict(
            accesstoken=MAPBOX_TOKEN,
            zoom=10,
            center=dict(lat=-27.4774, lon=153.028),
            style='dark'
        ),
    margin=dict(l=0, r=0, t=80, b=20))

    return fig

def yp_int(df):
    df = df[1:-1]
    print(df)
    # print(df)
    if float(df) < 0:
        df = 0
    return(float(df))

def PM25(pm25_df):
    
    result_df = pm25_df.copy()
    result_df['yp'] = result_df['yp'].apply(yp_int)
    result_df['Hazard Risk'] = 'No Risk'
    result_df['Hazard Risk'] = np.where((result_df['yp'] < 20), 'No Risk', result_df['Hazard Risk'])
    result_df['Hazard Risk'] = np.where(((result_df['yp'] >= 20) & (result_df['yp'] < 40)),'Mask Recommended', result_df['Hazard Risk'])
    result_df['Hazard Risk'] = np.where((result_df['yp'] > 40), 'High Risk', result_df['Hazard Risk'])

    return result_df

def PM10(pm10_df):
    
    result_df = pm10_df.copy()
    print(result_df.columns)
    result_df['yp'] = result_df['yp'].apply(yp_int)
    result_df['Hazard Risk'] = 'No Risk'
    result_df['Hazard Risk'] = np.where((result_df['yp'] < 50), 'No Risk', result_df['Hazard Risk'])
    result_df['Hazard Risk'] = np.where(((result_df['yp'] >= 50) & (result_df['yp'] < 120)),'Mask Recommended', result_df['Hazard Risk'])
    result_df['Hazard Risk'] = np.where((result_df['yp'] > 120), 'High Risk', result_df['Hazard Risk'])
    return result_df

def tsp(tsp_df):
    
    result_df = tsp_df.copy()
    result_df['yp'] = result_df['yp'].apply(yp_int)
    result_df['Hazard Risk'] = 'No Risk'
    result_df['Hazard Risk'] = np.where((result_df['yp'] < 80), 'No Risk', result_df['Hazard Risk'])
    result_df['Hazard Risk'] = np.where(((result_df['yp'] >= 80) & (result_df['yp'] < 250)),'Mask Recommended', result_df['Hazard Risk'])
    result_df['Hazard Risk'] = np.where((result_df['yp'] > 250), 'High Risk', result_df['Hazard Risk'])
    return result_df

def vis(vis_df):
    
    result_df = vis_df.copy()
    result_df['yp'] = result_df['yp'].apply(yp_int)
    result_df['Hazard Risk'] = 'No Risk'
    result_df['Hazard Risk'] = np.where((result_df['yp'] < 235), 'No Risk', result_df['Hazard Risk'])
    result_df['Hazard Risk'] = np.where(((result_df['yp'] >= 235) & (result_df['yp'] < 340)),'Mask Recommended', result_df['Hazard Risk'])
    result_df['Hazard Risk'] = np.where((result_df['yp'] > 340), 'High Risk', result_df['Hazard Risk'])
    return result_df


def co(co_df):

    result_df =co_df.copy()
    result_df['yp'] = result_df['yp'].apply(yp_int)
    result_df['Hazard Risk'] = 'No Risk'
    result_df['Hazard Risk'] = np.where((result_df['yp'] < 6), 'No Risk', result_df['Hazard Risk'])
    result_df['Hazard Risk'] = np.where(((result_df['yp'] >= 6) & (result_df['yp'] < 8)),'Mask Recommended', result_df['Hazard Risk'])
    result_df['Hazard Risk'] = np.where((result_df['yp'] > 8), 'High Risk', result_df['Hazard Risk'])

    result_df['yp'] = result_df['yp'].apply(lambda x:x*100)
    return result_df


def no2(no2_df):
    
    result_df =no2_df.copy()
    result_df['yp'] = result_df['yp'].apply(yp_int)
    result_df['Hazard Risk'] = 'No Risk'
    result_df['Hazard Risk'] = np.where((result_df['yp'] < 0.12), 'No Risk', result_df['Hazard Risk'])
    result_df['Hazard Risk'] = np.where(((result_df['yp'] >= 0.12) & (result_df['yp'] < 0.15)),'Mask Recommended', result_df['Hazard Risk'])
    result_df['Hazard Risk'] = np.where((result_df['yp'] > 0.15), 'High Risk', result_df['Hazard Risk'])

    result_df['yp'] = result_df['yp'].apply(lambda x:x*1000)
    return result_df

def ozone(ozone_df):

    result_df =ozone_df.copy()
    result_df['yp'] = result_df['yp'].apply(yp_int)
    result_df['Hazard Risk'] = 'No Risk'
    result_df['Hazard Risk'] = np.where((result_df['yp'] < 0.1), 'No Risk', result_df['Hazard Risk'])
    result_df['Hazard Risk'] = np.where(((result_df['yp'] >= 0.1) & (result_df['yp'] < 0.15)),'Mask Recommended', result_df['Hazard Risk'])
    result_df['Hazard Risk'] = np.where((result_df['yp'] > 0.15), 'High Risk', result_df['Hazard Risk'])

    result_df['yp'] = result_df['yp'].apply(lambda x:x*1000)
    return result_df

def so2(so2_df):
    
    result_df =so2_df.copy()
    result_df['yp'] = result_df['yp'].apply(yp_int)
    result_df['Hazard Risk'] = 'No Risk'
    result_df['Hazard Risk'] = np.where((result_df['yp'] < 0.2), 'No Risk', result_df['Hazard Risk'])
    result_df['Hazard Risk'] = np.where(((result_df['yp'] >= 0.2) & (result_df['yp'] < 0.25)),'Mask Recommended', result_df['Hazard Risk'])
    result_df['Hazard Risk'] = np.where((result_df['yp'] > 0.25), 'High Risk', result_df['Hazard Risk'])

    result_df['yp'] = result_df['yp'].apply(lambda x:x*1000)
    return result_df

def hour_error(df):
    today_e = date.today()
    de = today_e.strftime("%Y-%m-%d")
    day_error_hour = str(de)+' '+str(21)+':00:00'

    df = df[df['dt'] == day_error_hour]

    return df

MAPBOX_TOKEN = os.environ['MAPBOX_TOKEN']

#image = Image.open('C:/Users/arnab/Desktop/ServianAzureHack/air-pollution1.jpg')
image = fetch_image('Air Pollution')
st.image(image, caption='Air Pollution', use_column_width=True)

#station_df = pd.read_csv('C:/Users/arnab/Desktop/ServianAzureHack/Station.csv')
station_df = fetch_stations()

# station_df.rename(columns={'longitude':'lon','latitude':'lat'},inplace=True)
# st.map(station_df)

# station_pol_complete = pd.read_csv(r'C:\Users\arnab\Desktop\ServianAzureHack\station_pol_complete.csv')
station_pol_complete = fetch_weather_pol()
try:
    station_pol_complete.drop(columns =['Unnamed: 0'], inplace =True)
except:
    print('Invalid column name')

#station_pol_complete = pd.read_csv('C:/Users/arnab/Desktop/ServianAzureHack/station_pol_complete.csv')
#weather_df = pd.read_csv('C:/Users/arnab/Desktop/ServianAzureHack/weather_forecast.csv')
weather_df = fetch_weather_forecast()

# fig = aq_monitoring_stations(station_df,f_title = 'Monitoring Stations in Queensland')

# st.plotly_chart(fig, use_container_width=True)
day_flag = 0
st.title('AirQuality Monitoring and Forecasting')
tz_Brisbane = pytz.timezone('Australia/Brisbane')
datetime_bris = pydt.datetime.now(tz_Brisbane)
current_time = datetime_bris.strftime("%Y:%m:%d %H:%M:%S")
# print("Brisbane time:", current_time)
st.write('Current time in Brisbane is ', current_time)
st.write('Please select the Day: ')
select_day = st.selectbox('Day', ['Current','Today','Tomorrow'])
day_hour = ''

if select_day != 'Current':
    st.write('Please select the Hour: ')
    select_hour = st.selectbox('Hour', [x for x in range(1,24)])
    # select_ampm = st.selectbox('AM/PM', ['AM','PM'])
    if select_hour < 10:
        select_hour = '0'+str(select_hour)
    else:
        select_hour = str(select_hour)

    if select_day == 'Today':
        today = date.today()
        d3 = today.strftime("%Y-%m-%d")
        print("d3 =", d3)
        day_hour = str(d3)+' '+select_hour+':00:00'
        st.write('You have selected :',day_hour)
    else:
        today = date.today()
        tomorrow = today + pydt.timedelta(days=1)
        d3 = tomorrow.strftime("%Y-%m-%d")
        print("d3 =", d3)
        day_hour = str(d3)+' '+select_hour+':00:00'
        print(day_hour)
        st.write('You have selected :',day_hour)
else:
    day_hour = datetime_bris.strftime("%Y-%m-%d %H:00:00")
    st.write('You have selected :',day_hour)
button = 0
button = st.button('Show the Pollutant Concentration')
if button:
    st.markdown('## Air Quality Parameters')
    st.markdown('### 1. Particle PM2.5')
    st.markdown('Airborne particles less than 2.5 micrometres in diameter, referred to as PM2.5,'
                'can be hazardous to human health or cause a nuisance when present in the air at elevated levels.' 
                'They are capable of penetrating the lower airways of humans and can cause possible negative health effects.'   
                'The guideline for Particle PM2.5 is 60µg/m³ (1hr avg) and 25µg/m³ (24hr avg).'
                'Particle PM2.5 is measured in micrograms per cubic metre')

    #pm25_df = pd.read_csv('C:/Users/arnab/Desktop/ServianAzureHack/XGBoost_pkl/PM25/PM25.csv')
    pm25_df = fetch_prediction('Particle PM2.5') #TM
    try:
        pm25_df.drop(columns=['Unnamed: 0'],inplace=True)
    except:
        print('No column name Unnamed: 0')

    # print(pm25_df.head())
    pm25_f_df = pm25_df[pm25_df['dt']==day_hour].copy()
    # print(pm25_f_df.shape)
    if pm25_f_df.shape[0] == 0:
        pm25_f_df = hour_error(pm25_df)
        # print(pm25_f_df)  
    
    
    pm25_forecast_df = PM25(pm25_f_df)
    pm25_forecast_df = pm25_forecast_df.merge(station_df,how='inner',on='station_id')
    # print(pm25_forecast_df)
    fig = aq_forecasting_stations(pm25_forecast_df,f_title ='PM2.5 forecast in Queensland')
    st.plotly_chart(fig, use_container_width=True)

    st.markdown('### 2. Particle PM10')
    st.markdown('Airborne particles less than 10 micrometres in diameter, referred to as PM10, can be hazardous to' 
        'human health or cause a nuisance when present in the air at elevated levels. They are capable of penetrating '
        'the lower airways of humans and can cause possible negative health effects.'
        'The guideline for Particle PM10 is 120µg/m³ (1hr avg) and 50µg/m³ (24hr avg).'
        'Particle PM10 is measured in micrograms per cubic metre.Particle PM10')
    #pm10_df = pd.read_csv('C:/Users/arnab/Desktop/ServianAzureHack/XGBoost_pkl/PM10/PM10.csv')
    pm10_df = fetch_prediction('Particle PM10') 
    try:
        pm10_df.drop(columns=['Unnamed: 0'],inplace=True)
    except:
        print('No column name Unnamed: 0')


    pm10_f_df = pm10_df[pm10_df['dt']==day_hour].copy()
    # print(pm25_f_df.shape)
    if pm10_f_df.shape[0] == 0:
        pm10_f_df = hour_error(pm10_df)

    pm10_forecast_df = PM10(pm10_f_df)
    pm10_forecast_df = pm10_forecast_df.merge(station_df,how='inner',on='station_id')
    fig = aq_forecasting_stations(pm10_forecast_df,f_title ='PM10 forecast in Queensland')
    st.plotly_chart(fig, use_container_width=True)

    st.markdown('### 3. Particles TSP')
    st.markdown('Airborne particles up to about 100 micrometres in diameter are referred to as TSP'
                '(total suspended particles). These particles are generated by combustion and non-combustion processes,'
                'including windblown dust, sea salt, earthworks, mining activities, industrial processes, motor vehicle '
                'engines and fires.'
                'The guideline for Particles TSP is 250µg/m³ (1hr avg) and 80µg/m³ (24hr avg).'
                'Particles TSP is measured in micrograms per cubic metre.')

    #tsp_df = pd.read_csv('C:/Users/arnab/Desktop/ServianAzureHack/XGBoost_pkl/TSP/TSP.csv')
    tsp_df = fetch_prediction('Particles TSP') 
    try:
        tsp_df.drop(columns=['Unnamed: 0'],inplace=True)
    except:
        print('No column name Unnamed: 0')

    tsp_f_df = tsp_df[tsp_df['dt']==day_hour].copy()
    
    if tsp_f_df.shape[0] == 0:
        tsp_f_df = hour_error(tsp_df)
    tsp_forecast_df = tsp(tsp_f_df)
    tsp_forecast_df = tsp_forecast_df.merge(station_df,how='inner',on='station_id')
    fig = aq_forecasting_stations(tsp_forecast_df,f_title ='TSP forecast in Queensland')
    st.plotly_chart(fig, use_container_width=True)

    st.markdown('### 4. Visibility')
    st.markdown('Aerosols and fine particles can reduce visibility. Smoke from fires or haze are'
                'common causes of poor visibility.'
                'The guideline for Visibility is 235Mm⁻¹ (1hr avg).'
                'Visibility is measured in inverse megametres.')

    #vis_df = pd.read_csv('C:/Users/arnab/Desktop/ServianAzureHack/XGBoost_pkl/visibility/visibility.csv')
    vis_df = fetch_prediction('Visibility')
    try:
        vis_df.drop(columns=['Unnamed: 0'],inplace=True)
    except:
        print('No column name Unnamed: 0')

    vis_f_df = vis_df[vis_df['dt']==day_hour].copy()
    # print(pm25_f_df.shape)
    if vis_f_df.shape[0] == 0:
        vis_f_df = hour_error(vis_df)
    vis_forecast_df = vis(vis_f_df)
    vis_forecast_df = vis_forecast_df.merge(station_df,how='inner',on='station_id')
    fig = aq_forecasting_stations(vis_forecast_df,f_title ='Visibility forecast in Queensland')
    st.plotly_chart(fig, use_container_width=True)


    st.markdown('### 5. Carbon Monoxide')
    st.markdown('Carbon monoxide is a colourless, odourless gas formed when substances containing '
                'carbon (such as petrol, gas, coal and wood) are burned with an insufficient supply of air.'
                ' It has serious health impacts on humans and animals, especially those with cardiovascular disease.'
                'The guideline for Carbon monoxide is 9ppm (8hr avg).'
                'Carbon monoxide is measured in parts per million.')


    #co_df = pd.read_csv('C:/Users/arnab/Desktop/ServianAzureHack/XGBoost_pkl/carbon_monoxide/co.csv')
    co_df = fetch_prediction('Carbon monoxide')
    try:
        co_df.drop(columns=['Unnamed: 0'],inplace=True)
    except:
        print('No column name Unnamed: 0')

    co_f_df = co_df[co_df['dt']==day_hour].copy()
    # print(pm25_f_df.shape)
    if co_f_df.shape[0] == 0:
        co_f_df = hour_error(co_df)

    co_forecast_df = co(co_f_df)
    co_forecast_df = co_forecast_df.merge(station_df,how='inner',on='station_id')
    fig = aq_forecasting_stations(co_forecast_df,f_title ='Carbon Monoxide forecast in Queensland')
    st.plotly_chart(fig, use_container_width=True)


    st.markdown('### 6. Nitrogen Dioxide')
    st.markdown('Nitrogen dioxide is an acidic and highly corrosive gas. Nitrogen oxides are critical'
                'components of photochemical smog. Long-term exposure to high levels of nitrogen dioxide'
                'can cause chronic lung disease and affect the senses.'
                'The guideline for Nitrogen dioxide is 0.12ppm (1hr avg).'
                'Nitrogen dioxide is measured in parts per million.')

    #no2_df = pd.read_csv('C:/Users/arnab/Desktop/ServianAzureHack/XGBoost_pkl/nitrogen_dioxide/no2.csv')
    no2_df = fetch_prediction('Nitrogen dioxide')
    try:
        no2_df.drop(columns=['Unnamed: 0'],inplace=True)
    except:
        print('No column name Unnamed: 0')

    no2_f_df = no2_df[no2_df['dt']==day_hour].copy()
   
    if no2_f_df.shape[0] == 0:
        no2_f_df = hour_error(no2_df)

    no2_forecast_df = no2(no2_f_df)
    no2_forecast_df = no2_forecast_df.merge(station_df,how='inner',on='station_id')
    fig = aq_forecasting_stations(no2_forecast_df,f_title ='Nitrogen Dioxide forecast in Queensland')
    st.plotly_chart(fig, use_container_width=True)


    st.markdown('### 7. Ozone')
    st.markdown('Ozone is a colourless, highly reactive gas with a distinctive odour.'
                'The upper atmosphere ozone layer (at altitudes of 15–35km) protects the earth' 
                'from harmful ultraviolet radiation from the sun. The ozone layer reduction represents'
                'a global atmosphere issue.'
                'The guideline for Ozone is 0.1ppm (1hr avg).'
                'Ozone is measured in parts per million.')

    #ozone_df = pd.read_csv('C:/Users/arnab/Desktop/ServianAzureHack/XGBoost_pkl/ozone/ozone.csv')
    ozone_df = fetch_prediction('Ozone')
    try:
        ozone_df.drop(columns=['Unnamed: 0'],inplace=True)
    except:
        print('No column name Unnamed: 0')

    ozone_f_df = ozone_df[ozone_df['dt']==day_hour].copy()
    
    if ozone_f_df.shape[0] == 0:
        ozone_f_df = hour_error(ozone_df)
    ozone_forecast_df = ozone(ozone_f_df)
    ozone_forecast_df = ozone_forecast_df.merge(station_df,how='inner',on='station_id')
    fig = aq_forecasting_stations(ozone_forecast_df,f_title ='Ozone forecast in Queensland')
    st.plotly_chart(fig, use_container_width=True)

    st.markdown('### 8. Sulfur Dioxide')
    st.markdown('Sulfur dioxide (SO2) is a colourless gas with a sharp, irritating odour. It is '
                'produced by burning fossil fuels and by the smelting of mineral ores that contain sulfur.'
                'The guideline for Sulfur dioxide is 0.2ppm (1hr avg).'
                'Sulfur dioxide is measured in parts per million.')

    #so2_df = pd.read_csv('C:/Users/arnab/Desktop/ServianAzureHack/XGBoost_pkl/sulfur_dioxide/so2.csv')
    so2_df = fetch_prediction('Sulfur dioxide')
    try:
        so2_df.drop(columns=['Unnamed: 0'],inplace=True)
    except:
        print('No column name Unnamed: 0')

    so2_f_df = so2_df[so2_df['dt']==day_hour].copy()
    # print(pm25_f_df.shape)
    if so2_f_df.shape[0] == 0:
        so2_f_df = hour_error(so2_df)
    so2_forecast_df = so2(so2_f_df)
    so2_forecast_df = so2_forecast_df.merge(station_df,how='inner',on='station_id')
    fig = aq_forecasting_stations(so2_forecast_df,f_title ='Sulfur Dioxide forecast in Queensland')
    st.plotly_chart(fig, use_container_width=True)


st.sidebar.markdown("### About the Author's ")
image_arnab = fetch_image('Arnab')
st.sidebar.image(image_arnab,use_column_width=True)

# read_arnab_image('complete_image', 'DSI+09_ArnabM_Headshot_001')
st.sidebar.markdown('### Arnab Mukherjee')
st.sidebar.markdown('Associate Consultant, Servian ')
st.sidebar.markdown('Email : arnab.mukherjee@servian.com')


image_tim = fetch_image('Tim')
st.sidebar.image(image_tim,use_column_width=True)
st.sidebar.markdown('### Timothy Moore')
st.sidebar.markdown('Associate Consultant, Servian ')
st.sidebar.markdown('Email : timothy.moore@servian.com')