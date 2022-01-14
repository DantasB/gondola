class Record:
    def __init__(self,  content='', offset=None, size=0):
        self.offset = offset
        self.content = content
        if len(content) > 0:
            self.size = len(content.encode('utf-8'))
            self.attributes = content.split('|')
        else:
            self.size = size

    def __getitem__(self, key):
        return self.attributes[key]

    def to_string(self):
        return self.content

    def is_empty(self):
        return True if self.content == '' else False

    def write(self, content):
        self.content = content
