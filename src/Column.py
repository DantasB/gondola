class Column:
    def __init__(self, name, type, size):
        self.name = name
        self.type = type
        self.size = size

    def metadata(self):
        return f"{self.name}|{self.type}|{self.size}"
