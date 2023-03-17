class Pipe:
    def __init__(self):
        self.lines = {}
    
    def add_line(self, name : str):
        if not name in self.lines:
            self.lines[name] = []

    def add_data(self, line : str, data):
        self.lines[line].append(data)
    
    def pop_data(self, line):
        try:
            return self.lines[line].pop()
        except:
            return None
    