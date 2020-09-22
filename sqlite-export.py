# Use SQLAlchemy to export Json to SQLite.

# TODO: fix this (line 67) Thursday.
# sqlalchemy.exc.CompileError: Unconsumed column names:

from sqlalchemy.engine.url import URL
from sqlalchemy import create_engine, MetaData, Table, Column, Integer, String, DateTime, Float, inspect
from os import path
import json
from datetime import datetime

sqlite_db = {'drivername': 'sqlite', 'database': 'db.sqlite'}

engine = create_engine(URL(**sqlite_db))
inspector = inspect(engine)

# Create connection
conn = engine.connect()
# Begin transaction
# trans = conn.begin()

meta = MetaData(engine)

# If the DB doesn't exist, create it.
if not path.exists("db.sqlite"):
    t1 = Table('weather_data', meta,
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
    t1.create()

print(inspector.get_columns('weather_data'))

# Get the json from the weather_data file and convert it back to a dict.

with open("./weather_data.json", 'r') as f:
    weather_dict = json.load(f)

print(weather_dict)


def gust_check(data):
    if "Maximum Wind Gust" in data:
        return data["Maximum Wind Gust"]
    else:
        return 0.0

def insert_weather_from_dict():
    for k, v in weather_dict:
        print(k)
        date = datetime.strptime(k, '%Y-%m-%d')
        # need to check if v has the data required. some of the data is missing and should be defaulted
        # to 0.0

        # Insert date = k, min_temp = v["Minimum Temperature"]
        table = Table('weather_data', meta)
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