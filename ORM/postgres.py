import sqlalchemy as db

class ORM:

    def __init__(self):
        self.engine = db.create_engine(YOUR_POSTGRES_CONNECTION_STRING, pool_timeout=120)
    
    def get_connection(self):
        return self.engine.connect()
    
