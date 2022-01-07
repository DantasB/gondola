class Schema:
    def __init__(self,columns=[]) -> None:
        self.columns = columns
    def add_column(self,name):
        self.columns.append(name)
