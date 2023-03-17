# Vinter Case

## Folder Structure

### Root folder (/)
- datalake.py: this file is the Data Lake coordinator, it includes "Datasources", "Raw Processes" and "Cleansing Processes". The code was made so new Datasources, new raw processes or cleansing processes can be added. This way the code tries to mimic an actual Data Lake infrastructure (with Queues and Events represented by a single Queue and Thread Condition Variables).
- main.py: This file contains all the core inits and the endpoints for the REST API. The REST API models and results **SHOULD** be separated from the endpoint definition for better usage, however, since it is a small project it was completely done in the same file.
- setup.py: Run this file to create database schemas and tables so you can use the Case program.
- It also constains: README.md, SCHEMA_DETAILS.md and this FOLDER_STRUCTURE.md file with all documentation.

### Custom types folder (/Customtypes)
- BetterList.py: this is a simple implementation of a string list that returns True or False on append and remove. It servers as a proxy to python's List so the code can have better feedback without having to implement checks all the time.
- Pipe.py: this is a simple implementation of a Queue for the Data Lake layers to use. It implements "topics" and have a buffer for pushing and pop'ing data. It also avoids throwing errors on pop'ing so the code can be cleaner when using this datatype.

### Datalake folder (/Datalake)
- CleanseLayer.py: this file contains the definitions for the cleasing processes for each "topic" in the Pipe. It also implements a class named "CleanseMeteomaticsProcessor" with is a cleanse process. The cleanse process run in a separated thread and has thread-safety on the Pipe. It also share a Condition Variable that act as an Event to tell the cleansing process more data is available at the Pipe. Within this file you can also find 3 methods for data cleansing (again, as an example since all data is already clean). The resulting data is fed into the Data Lake "cleansed" schema.
- RawLayer.py: RawLayer works very similarly to the CleanseLayer, it has a Condition Variable as an event from the Data source crawler and a pipe to receive data (in this case in a APIDatasource reference) and a pipe to deliver data (in this case an actual Pipe used in cleasing). The process run in a separate thread. The resulting data is fed into the Data Lake "raw" schema and Pipes for cleansing processes.

### Datasources folder (/Datasources)
- Datasource.py: this file defines the class "APIDatasource" which is supposed to be a base class for **ANY** API fetching source. It implements multithreaded requests for performance and feeds a simple Pipe to make source data available. The class run in a separate thread and has a dynamic pooling interval defined by the child class.
- Meteomatics.py: this files implements a child class of "APIDatasource" by implementing only 2 methods: "get_auth" needed to get authentication data; and "get_urls" used to get all urls needed to load data. Other methods are helpers for changes in the Data Lake configuration. These changes can lead to "full loads" (1 day period) of data from the source so the Data Lake always have data on the configured metrics and locations.

### ORM folder (/ORM)
- postgres.py: this file contains basic methods to connect to the Database through SQLAlchemy ORM. It also provides a method to get new connections (as they are needed for each layer and endpoint).
