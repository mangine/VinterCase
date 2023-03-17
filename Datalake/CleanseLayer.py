from threading import Condition, Thread
from Customtypes.Pipe import Pipe
from Datasources.Datasource import APIDatasource
from ORM.postgres import ORM
import time
import sqlalchemy
import pandas as pd
import json

def wind_speed_10m_cleanse(data):
    return data / 2

def wind_dir_10m(data):
    return data if data > 0 else 0

def t_2m(row):
    if row["parameter"] == "t_2m:C":
        return max(15,float(row["value"]))
    else:
        return row["value"]

class CleanseMeteomaticsWide:
    def run_cleanse(self, df):
        df["wind_speed_10m:ms"] = df["wind_speed_10m:ms"].apply(wind_speed_10m_cleanse)
        df["wind_dir_10m:d"] = df["wind_dir_10m:d"].apply(wind_dir_10m)

        return df
    
class CleanseMeteomaticsLong:
    def run_cleanse(self, df):
        df["value"] = df.apply(t_2m, axis=1)

        return df
    
class CleanseMeteomaticsProcessor:
    def __init__(self, orm : ORM, cv : Condition, pipe : Pipe):
        self.running = False
        self.thread = None
        self.orm = orm
        self.cv = cv
        self.pipe = pipe

    def start_thread(self):
        self.thread = Thread(target=self.start)
        self.thread.start()

    def stop(self):
        self.running = False
        with self.cv:
            self.cv.notify_all()
        if self.thread is not None:
            self.thread.join()

    def start(self):
        self.running = True
        connection = self.orm.get_connection()
        metadata = sqlalchemy.MetaData()
        table_wide = sqlalchemy.Table("wide", metadata,
                    sqlalchemy.Column("wind_speed_10m:ms", sqlalchemy.DECIMAL(11,2)),
                    sqlalchemy.Column("wind_dir_10m:d", sqlalchemy.DECIMAL(11,2)),
                    sqlalchemy.Column("wind_gusts_10m_1h:ms", sqlalchemy.DECIMAL(11,2)),
                    sqlalchemy.Column("wind_gusts_10m_24h:ms", sqlalchemy.DECIMAL(11,2)),
                    sqlalchemy.Column("lat", sqlalchemy.String(32)),
                    sqlalchemy.Column("lon", sqlalchemy.String(32)),
                    sqlalchemy.Column("date", sqlalchemy.DateTime()),
                    keep_existing=True, schema="cleansed")
        
        table_long = sqlalchemy.Table("long", metadata,
                    sqlalchemy.Column("parameter", sqlalchemy.String(64)),
                    sqlalchemy.Column("lat", sqlalchemy.String(32)),
                    sqlalchemy.Column("lon", sqlalchemy.String(32)),
                    sqlalchemy.Column("date", sqlalchemy.DateTime()),
                    sqlalchemy.Column("value", sqlalchemy.String(128)),
                    keep_existing=True, schema="cleansed")

        while self.running:

            with self.cv:
                self.cv.wait(5)
                
                df = self.pipe.pop_data("wide")
                if not df is None:
                    cleanser = CleanseMeteomaticsWide()
                    df2 = cleanser.run_cleanse(df)

                    #insert data into cleansed schema
                    df2 = df2.to_dict(orient='records')
                    connection.execute(table_wide.insert(), df2)

                    connection.commit()
                    print("New wide data available")
                    
                df = self.pipe.pop_data("long")
                if not df is None:
                    cleanser = CleanseMeteomaticsLong()
                    df2 = cleanser.run_cleanse(df)

                    #insert data into cleansed schema
                    df2["value"] = df2["value"].astype('string')
                    df2 = df2.to_dict(orient='records')
                    connection.execute(table_long.insert(), df2)

                    connection.commit()
                    print("New long data available")
                