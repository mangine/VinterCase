from typing import List
from Customtypes.Pipe import Pipe
from Datasources.Datasource import APIDatasource
from threading import Thread, Condition
import time
import pandas as pd
from ORM.postgres import ORM
import sqlalchemy
import json

class RawMeteomaticsProcessor:
    def __init__(self, source : APIDatasource, orm : ORM, incv : Condition, cv : Condition, pipe : Pipe):
        self.source = source
        self.running = False
        self.thread = None
        self.orm = orm
        self.incv = incv
        self.cv = cv
        self.pipe = pipe

    def start(self):
        self.running = True

        self.pipe.add_line("wide")
        self.pipe.add_line("long")

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
                    keep_existing=True, schema="raw")
        
        table_long = sqlalchemy.Table("long", metadata,
                    sqlalchemy.Column("parameter", sqlalchemy.String(64)),
                    sqlalchemy.Column("lat", sqlalchemy.String(32)),
                    sqlalchemy.Column("lon", sqlalchemy.String(32)),
                    sqlalchemy.Column("date", sqlalchemy.DateTime()),
                    sqlalchemy.Column("value", sqlalchemy.String(128)),
                    keep_existing=True, schema="raw")

        while self.running:
            #print("Going to look for raw data")
            json_data = self.source.pop_data()
            numrecs = 0
            with self.incv:
                numrecs = len(self.source.avail_data)
                print("Found {} records (is None={})".format(numrecs, json_data is None))

            #print("Popped data is None: {}".format(json_data is None))
            if json_data is None:
                with self.incv:
                    self.incv.wait(30)
                continue

            df = pd.DataFrame(columns = ['parameter', 'lat', 'lon', 'date', 'value'])

            for parameter in json_data["data"]:
                param_name = parameter["parameter"]
                for location in parameter["coordinates"]:
                    for datapoint in location["dates"]:
                        dfrow = pd.DataFrame([{'parameter': param_name, 'lat':str(location["lat"]), 'lon':str(location["lon"]), 'date':datapoint["date"], 'value':datapoint["value"]}])
                        df = pd.concat([df, dfrow], ignore_index=True)

            #long table created, now split metrics to create a long table and a wide
            df_wide = df.loc[(df["parameter"] == "wind_speed_10m:ms") | (df["parameter"] == "wind_dir_10m:d") | (df["parameter"] == "wind_gusts_10m_1h:ms") | (df["parameter"] == "wind_gusts_10m_24h:ms")]
            df_long = df.loc[(df["parameter"] != "wind_speed_10m:ms") & (df["parameter"] != "wind_dir_10m:d") & (df["parameter"] != "wind_gusts_10m_1h:ms") & (df["parameter"] != "wind_gusts_10m_24h:ms")]


            #conver long to wide table
            df_wide = df_wide.pivot(index=['date', 'lat', 'lon'], columns=['parameter'], values=['value'])
            columns = []
            for c in df_wide.columns:
                columns.append(c[0] if c[0] != '' and c[0] != 'value' else c[1])
            df_wide.columns = columns
            df_wide = df_wide.reset_index()

            #data avail
            #notifiy data availabiltiy
            with self.cv:
                if len(df_wide.axes[0]) > 0:
                    self.pipe.add_data("wide", df_wide.copy())
                if len(df_long.axes[0]) > 0:
                    self.pipe.add_data("long", df_long.copy())
                self.cv.notify_all()
            
            #transform data into dict for sqlalchemy input
            if len(df_wide.axes[0]) > 0:
                df_wide = df_wide.to_dict(orient='records')
                connection.execute(table_wide.insert(), df_wide)

            if len(df_long.axes[0]) > 0:
                df_long["value"] = df_long["value"].astype('string')
                df_long = df_long.to_dict(orient='records')
                connection.execute(table_long.insert(), df_long)

            #write to db
            connection.commit()

        
    def start_thread(self):
        self.thread = Thread(target=self.start)
        self.thread.start()

    def stop(self):
        self.running = False
        if self.thread is not None:
            self.thread.join()
    
