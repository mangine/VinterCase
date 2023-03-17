# Vinter Case

## Third-Party Dependencies
- sqlalchemy
- pandas
- requests
- uvicorn

## Installing

1. Clone the repository
2. Edit file "ORM/postgres.py" to add your own postgres server
3. Edit file "Datasources/Meteomatics.py" **get_auth** method with your Meteomatics credentials
4. Run `$ python setup.py` to connect and setup the database
5. Run `$ uvicorn main:app` to start the service

**ATENTION!** When running the case for the first time a data load will be made, make sure to set locations and meetrics you want data from to begin.

## Quick Startup (skip if you want the full experience)

    POST /locations
with this json body:

    {"locations":["LONDON","BERLIN","SAO_PAULO"]}

And then

    POST /metrics
with this json body:

    {"metrics":"ALL"}

The Data Lake will start after a minute and you will be able to get data from 

    /widedata
or

    /longdata

An example url is: 
http://127.0.0.1:8000/widedata?start=2023-03-15T06%3A00%3A00&until=2023-03-16T06%3A00%3A00&location=LONDON

## Playing with the Data Lake

### Viewing the API documentation

After running `$ uvicorn main:app` go to [http://127.0.0.1:8000/docs](http://127.0.0.1:8000/docs) to view the live documentation.


### Setting which locations to read data from meteomatics API:

All data that the Data Lake reads is configurable. You may set what data to read using a POST to /locations
The body must be in the following format:

    {"locations":[ *LOCATION LIST* ]}

**Location list** accepts the following values: "BERLIN", "LONDON" and "SAO_PAULO".

> **Note:** If a new location is added, historical data (from 1 day ago) will be loaded in the next query to the source API. After that, live data will be fetched every minute form selected locations.

### Setting which metrics to read data from meteomatics API:

As with locations, metrics to load into the Data Lake must be chosen. You may set what data to read using a POST to /metrics
The body must be in the following format:

    {"metrics": *METRIC NAME*}

**METRIC NAME** accepts "WIDE", "LONG" and "ALL", where "WIDE" only loads wind data in wide format and "LONG" loads only long data. "ALL" loads all kinds of data.

> **Note:** If a new metric group is added, historical data (from 1 day ago) will be loaded in the next query to the source API. After that, live data will be fetched every minute form selected metrics.

## Querying data

You may query data from:

    GET /widedata
For wide format data, or:

    GET /longdata
For long format data.

For both cases the accepted querystring is:

    start: <ISO 8601 date format>
    until: <ISO 8601 date format>
    location: <BERLIN/LONDON/SAO_PAULO>
	
## Thank you
