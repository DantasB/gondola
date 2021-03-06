class Record:
    def __init__(self, content="", offset=None, size=0):
        self.offset = offset
        self.is_empty = True
        self.size = size
        if len(content) > 0:
            self.size = len(content.encode("utf-8")) + 1
            if content[0] != "#":
                self.content = content
                self.attributes = content.split("|")
                self.id = self.attributes[0]
                self.is_empty = False

    def __getitem__(self, key):
        return self.attributes[key]

    def to_string(self):
        if not self.is_empty:
            return self.content
        else:
            if self.size == 1:
                return ''
            return "#" + " " * (self.size - 2)

    def is_empty(self):
        return True if self.is_empty else False

    def write(self, content):
        self.content = content
