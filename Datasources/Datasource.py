from abc import ABC
from typing import Dict, List
from concurrent.futures import ThreadPoolExecutor, as_completed
import requests
import time
from threading import Condition, Thread
import json

class APIDatasource(ABC):

    def __init__(self, max_workers : int, incv : Condition):
        self.started = False
        self.max_workers = max_workers
        self.avail_data = []
        self.thread = None
        self.incv = incv

    def get_urls(self) -> List[str]:
        pass

    def get_auth(self):
        pass

    def pop_data(self):
        with self.incv:
            if len(self.avail_data) == 0:
                return None
            return self.avail_data.pop()

    def process_result(self, result : requests.Response):
        with self.incv:
            if(result.status_code == 200):
                self.avail_data.append(json.loads(result.text))
            else:
                print(result.text)

    def get_data(self, url):
        resp = requests.get(url=url, auth=self.get_auth(), timeout=10)
        return resp

    def start_thread(self, delay_in_seconds : int):
        self.thread = Thread(target=self.start,args=(delay_in_seconds,))
        self.thread.start()
    
    def stop(self):
        self.started = False
        if self.thread is not None:
            self.thread.join()

    def start(self, delay_in_seconds : int):
        thpool = ThreadPoolExecutor(self.max_workers)
        self.started = True

        
        while self.started:
            next_request = (1 + time.time() // delay_in_seconds)*delay_in_seconds #(time.time() % delay_in_seconds)

            futures = []

            for url in self.get_urls():
                futures.append(thpool.submit(self.get_data, url=url))

            for future in as_completed(futures):
                self.process_result(future.result())

            with self.incv:
                self.incv.notify_all()

            if time.time() < next_request:
                time.sleep(max(1,next_request - time.time()))

