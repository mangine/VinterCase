#esse pipeline cuida do datalake
from Customtypes.Pipe import Pipe
from Datalake.CleanseLayer import CleanseMeteomaticsProcessor
from Datasources.Meteomatics import Meteomatics
from ORM.postgres import ORM
from Datalake.RawLayer import RawMeteomaticsProcessor
from threading import Condition



class DataLake:
    def __init__(self, orm : ORM):
        self.cv = None
        self.pipe = None
        self.orm = orm
        self.sources = []
        self.processors = []
        self.cleansers = []

    def start(self):
        #setup pipes and conditions
        self.cv = Condition()
        self.incv = Condition()
        self.pipe = Pipe()

        self.sources = [Meteomatics(self.incv)]
        for source in self.sources:
            source.start_thread(60)

        #source to raw
        self.processors = [RawMeteomaticsProcessor(self.sources[0], self.orm, self.incv, self.cv, self.pipe)]
        for processor in self.processors:
            processor.start_thread()

        #raw to cleansed
        self.cleansers = [CleanseMeteomaticsProcessor(self.orm, self.cv, self.pipe)]
        for cleanser in self.cleansers:
            cleanser.start_thread()

    def stop(self):
        print("Stopping, may take up to 1 minute...")
        for source in self.sources:
            source.stop()

        for processor in self.processors:
            processor.stop()

        for cleanser in self.cleansers:
            cleanser.stop()

        print("Data Lake stopped")
