from typing import List


class BetterList:
    def __init__(self):
        self.items = []

    def append(self, item:str) -> bool:
        if not item in self.items:
            self.items.append(item)
            return True
        return False

    def remove_metric(self, item: str) -> bool:
        if item in self.items:
            self.items.remove(item)
            return True
        return False
    
    def set(self, items : List[str]):
        self.items = items
        return True