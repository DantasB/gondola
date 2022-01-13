class Record:
    def __init__(self, size, content=''):
        self.content = content
        self.size = size
        self.block = None
        self.offset = None

    def to_string(self):
        return self.content

    def is_empty(self):
        return True if self.content == '' else False

    def write(self, content):
        self.content = content
