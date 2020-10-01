#Export data to postgres,

from sqlalchemy.engine.url import URL
from sqlalchemy import create_engine, MetaData, Table, Column, Integer, String, DateTime, Float, inspect
from os import path
import json
from datetime import datetime
from decouple import config

db_string = config("PGSTRING")

engine = create_engine(db_string)
inspector = inspect(engine)


# Create connection
conn = engine.connect()

meta = MetaData(engine)


t1 = Table('weather_almanac', meta,
               Column('id', Integer, primary_key=True),
               Column('date', DateTime),
               Column('min_temp', Float),
               Column('mean_temp', Float),
               Column('max_temp', Float),
               Column('mean_sea_level_pressure', Float),
               Column('mean_dew_point', Float),
               Column('total_precip', Float),
               Column('visibility', Float),
               Column('mean_wind_speed', Float),
               Column('max_sustained_wind_speed', Float),
               Column('max_wind_gust', Float)
               )
#t1.create()

print(inspector.get_columns('weather_almanac'))

# Get the json from the weather_data file and convert it back to a dict.

with open("./weather_data.json", 'r') as f:
    weather_dict = json.load(f)

#print(weather_dict)


def gust_check(data):
    if "Maximum Wind Gust" in data:
        return data["Maximum Wind Gust"]
    else:
        return 0.0

def pressure_check(data):
    if "Mean Sea Level Pressure" in data:
        return data["Mean Sea Level Pressure"]
    else:
        return 0.0

def insert_weather_from_dict():
    for k, v in weather_dict:
        date = datetime.strptime(k, '%Y-%m-%d')

        #print(v)

        #Construct the statement for the query.
        sql_query = [
            {'date': date},
            {'min_temp': v["Minimum Temperature"]},
            {'mean_temp': v["Mean Temperature"]},
            {'max_temp': v["Maximum Temperature"]},
            {'mean_sea_level_pressure': v["Mean Sea Level Pressure"]},
            {'mean_dew_point': v["Mean Dew Point"]},
            {'total_precip': v["Total Precipitation"]},
            {'visibility': v["Visibility"]},
            {'mean_wind_speed': v["Mean Wind Speed"]},
            {'max_sustained_wind_speed': v["Maximum Sustained Wind Speed"]},
            {'max_wind_gust': gust_check(v)}
        ]
        print(sql_query)

        # Insert date = k, min_temp = v["Minimum Temperature"]
        table = Table('weather_almanac', meta)
        conn.execute(table.insert().values(
            date=date,
            min_temp=v["Minimum Temperature"],
            mean_temp=v["Mean Temperature"],
            max_temp=v["Maximum Temperature"],
            mean_sea_level_pressure=v["Mean Sea Level Pressure"],
            mean_dew_point=v["Mean Dew Point"],
            total_precip=v["Total Precipitation"],
            visibility=v["Visibility"],
            mean_wind_speed=v["Mean Wind Speed"],
            max_sustained_wind_speed=v["Maximum Sustained Wind Speed"],
            max_wind_gust=gust_check(v)
        ))

insert_weather_from_dict()