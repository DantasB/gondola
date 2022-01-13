class Column:
    def __init__(self, name, column_type, size):
        self.name = name
        self.column_type = column_type
        self.size = size

    def metadata(self):
        return f"{self.name}|{self.column_type}|{self.size}"
