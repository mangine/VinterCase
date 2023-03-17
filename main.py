from __future__ import annotations
from datetime import datetime
from enum import Enum
from typing import List, Optional
from fastapi import Body, FastAPI
from pydantic import BaseModel, Field
from ORM.postgres import ORM
from datalake import DataLake
import sqlalchemy
from sqlalchemy import select


app = FastAPI()
app.orm = ORM()
app.dl = DataLake(app.orm)
app.dl.start()


@app.get("/", status_code=400)
async def root():
    return {"version": 1, "status": "Use /wide and /long for data fetching."}


class LocationsEnum(str, Enum):
    '''
    SAO_PAULO = '-22.0702705,-48.4333875'
    LONDON = '51.5073219,-0.1276474'
    BERLIN = '52.5170365,13.3888599'
    '''
    SAO_PAULO = 'SAO_PAULO'
    LONDON = 'LONDON'
    BERLIN = 'BERLIN'

    @staticmethod
    def GetCoordinates(value):
        if value is LocationsEnum.SAO_PAULO:
            return '-22.070271,-48.433388'
        elif value is LocationsEnum.LONDON: 
            return '51.507322,-0.127647'
        elif value is LocationsEnum.BERLIN:
            return '52.517036,13.38886'

        d = {LocationsEnum.SAO_PAULO :'-22.0702705,-48.4333875', LocationsEnum.LONDON: '51.5073219,-0.1276474', LocationsEnum.BERLIN: '52.5170365,13.3888599'}
        return d[value]
    
class Locations(BaseModel):
    locations: List[LocationsEnum] = Field(
        default=[], title="Locations to be read"
    )

class MetricEnum(str, Enum):
    LONG = 'LONG'
    WIDE = 'WIDE'
    ALL = 'ALL'

class Metrics(BaseModel):
    metrics: MetricEnum = Field(
        default=MetricEnum.ALL, title="Metrics to read"
    )

@app.post("/locations")
async def set_locations(locations: Locations):
    locs = [LocationsEnum.GetCoordinates(l) for l in locations.locations]
    app.dl.sources[0].set_locations(locs)
    return {"version": 1, "status": "OK"}


@app.post("/metrics")
async def set_metrics(metrics: Metrics):
    if metrics.metrics is MetricEnum.LONG:
        app.dl.sources[0].set_metrics(["t_2m:C","t_max_2m_24h:C","t_min_2m_24h:C","msl_pressure:hPa","precip_1h:mm","precip_24h:mm","weather_symbol_1h:idx","weather_symbol_24h:idx","uv:idx","sunrise:sql","sunset:sql"])
        return {"version": 1, "status": "Metrics is now " + metrics.metrics.value}
    
    elif metrics.metrics is MetricEnum.WIDE:
        app.dl.sources[0].set_metrics([",".join(["wind_speed_10m:ms","wind_dir_10m:d","wind_gusts_10m_1h:ms","wind_gusts_10m_24h:ms"])])
        return {"version": 1, "status": "Metrics is now " + metrics.metrics.value}
    
    elif metrics.metrics is MetricEnum.ALL:
        app.dl.sources[0].set_metrics([",".join(["wind_speed_10m:ms","wind_dir_10m:d","wind_gusts_10m_1h:ms","wind_gusts_10m_24h:ms"]), "t_2m:C","t_max_2m_24h:C","t_min_2m_24h:C","msl_pressure:hPa","precip_1h:mm","precip_24h:mm","weather_symbol_1h:idx","weather_symbol_24h:idx","uv:idx","sunrise:sql","sunset:sql"])
        return {"version": 1, "status": "Metrics is now " + metrics.metrics.value}

    else:
        return {"version": 1, "status": "Invalid metric type, accepted values: LONG, WIDE, ALL. No changes were made"}


class GetDataQueryParameters(BaseModel):
    start: datetime= Field(
        title="Starting date from the reading values"
    )
    until: datetime= Field(
        title="Until what date to bring values"
    )
    location: LocationsEnum = Field(
        title="Locations to be read"
    )

@app.get("/longdata")
async def get_data(start : datetime, until : datetime, location : LocationsEnum):
    conn = app.orm.get_connection()
    metadata = sqlalchemy.MetaData()
    table = sqlalchemy.Table("long", metadata, sqlalchemy.Column("parameter", sqlalchemy.String(64)),
                    sqlalchemy.Column("lat", sqlalchemy.String(32)),
                    sqlalchemy.Column("lon", sqlalchemy.String(32)),
                    sqlalchemy.Column("date", sqlalchemy.DateTime()),
                    sqlalchemy.Column("value", sqlalchemy.String(128)), schema="cleansed")

    locationcoors = [0,0]
    try:
        locationcoors = LocationsEnum.GetCoordinates(location).split(",")
    except:
        return {"version": 1, "status": "Invalid location, accepted values: SAO_PAULO, LONDON, BERLIN.", "data": []}


    stmt = select(table).where(table.columns.lat == locationcoors[0]).where(table.columns.lon == locationcoors[1]).where(table.c.date >= start).where(table.c.date <= until)
    print("SELECT * FROM cleansed.long WHERE lat = {} AND lon = {} AND date >= '{}' AND date <= '{}'".format(locationcoors[0], locationcoors[1], start, until))
    results = conn.execute(stmt).all()
    return {"version": 1, "status": "OK.", "data": [{"parameter":p[0], "date":p[3], "value": p[4]} for p in results]}


@app.get("/widedata")
async def get_data(start : datetime, until : datetime, location : LocationsEnum):
    conn = app.orm.get_connection()
    metadata = sqlalchemy.MetaData()
    table = sqlalchemy.Table("wide", metadata,
                    sqlalchemy.Column("wind_speed_10m:ms", sqlalchemy.DECIMAL(11,2)),
                    sqlalchemy.Column("wind_dir_10m:d", sqlalchemy.DECIMAL(11,2)),
                    sqlalchemy.Column("wind_gusts_10m_1h:ms", sqlalchemy.DECIMAL(11,2)),
                    sqlalchemy.Column("wind_gusts_10m_24h:ms", sqlalchemy.DECIMAL(11,2)),
                    sqlalchemy.Column("lat", sqlalchemy.String(32)),
                    sqlalchemy.Column("lon", sqlalchemy.String(32)),
                    sqlalchemy.Column("date", sqlalchemy.DateTime()),
                    keep_existing=True, schema="cleansed"
                    )

    locationcoors = [0,0]
    try:
        locationcoors = LocationsEnum.GetCoordinates(location).split(",")
    except:
        return {"version": 1, "status": "Invalid location, accepted values: SAO_PAULO, LONDON, BERLIN.", "data": []}
    
    stmt = select(table).where(table.c.lat == locationcoors[0]).where(table.c.lon == locationcoors[1]).where(table.c.date >= start).where(table.c.date <= until)
    results = conn.execute(stmt).all()
    return {"version": 1, "status": "OK.", "data": [{"date":p[6], "wind_speed_10m:ms":p[0], "wind_dir_10m:d":p[1], "wind_gusts_10m_1h:ms": p[2],  "wind_gusts_10m_24h:ms": p[3]} for p in results]}