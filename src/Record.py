class Record:
    def __init__(self, size, content=[]):
        self.isEmpty = True if content == [] else False
        self.block = None
        self.offset = None

    def to_string(self):
        raise NotImplementedError

    def is_empty(self):
        return self.isEmpty
