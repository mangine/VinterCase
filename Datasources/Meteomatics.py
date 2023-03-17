from threading import Condition
from typing import List
from Datasources.Datasource import APIDatasource
from Customtypes.BetterList import BetterList
from datetime import datetime, timezone, timedelta


class Meteomatics(APIDatasource):
    def __init__(self, incv : Condition):
        self.locations = BetterList()
        self.metrics = BetterList()
        self.first_load_metrics = []
        self.first_load_locations = []
        super().__init__(3, incv)
    
    def get_auth(self):
        return (YOUR_USERNAME, YOUR_PASSWORD)

    def set_metrics(self, metrics : List[str]):
        self.first_load_metrics = [m for m in metrics if not m in self.metrics.items]
        self.metrics.items = metrics

    def set_locations(self, locations : List[str]):
        self.first_load_locations = [m for m in locations if not m in self.locations.items]
        self.locations.items = [m for m in locations if m in self.locations.items]

    def get_urls(self) -> List[str]:
        locations = "+".join(self.locations.items)
        urls = []
        if len(self.locations.items) > 0:
            for metric in self.metrics.items:
                if metric in self.first_load_metrics:
                    self.first_load_metrics.remove(metric)
                    datestr = (datetime.now(timezone.utc) - timedelta(days=1)).isoformat() + "--" + datetime.now(timezone.utc).isoformat()
                    url = "https://api.meteomatics.com/{}/{}/{}/json".format(datestr + ":PT5M", metric, locations)
                else:
                    datestr = datetime.now(timezone.utc)
                    datestr = datestr.replace(second=0, microsecond=0)
                    datestr = datestr.isoformat()
                    url = "https://api.meteomatics.com/{}/{}/{}/json".format(datestr, metric, locations)
                urls.append(url)
        for location in self.first_load_locations:
            for metric in self.metrics.items:
                datestr = (datetime.now(timezone.utc) - timedelta(days=1)).isoformat() + "--" + datetime.now(timezone.utc).isoformat()
                url = "https://api.meteomatics.com/{}/{}/{}/json".format(datestr + ":PT5M", metric, location)
                urls.append(url)
        self.locations.items += self.first_load_locations
        self.first_load_locations = []

        return urls


