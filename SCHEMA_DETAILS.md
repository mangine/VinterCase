# Vinter Case

## Schema choice details

The schema choice was mostly limited by the requirements of the project.
Since 4 tables were need and 2 types of data format too, this made the obvious route to be:

|Schema|Table|
|--|--|
|raw|wide|
|raw|long|
|cleansed|wide|
|cleansed|long|

So in the end there are 4 data tables, 1 for each data format and 2 for each Data Lake "layer": 

- raw layer: raw data, no cleansing, only basic formating (since Data Lake is structured)
- cleansed layer: where data is treated, cleaned before moving to the DB (here I only implemented a few cleansing methods, as example, since all data is already cleansed from source)

Each table structure followed the kind of data it should store, so table "long" is in format for long data input and table "wide" is in format for wide data input (in this case wide is 4 metrics "wide")

### Why no metadata?
During the case design process, a metadata table with information of data in the Data Lake layers or even dynamic infrastructure data was possible and even used in the early stages. However, that made the case too complex on configuration and less fun in the execution. 
