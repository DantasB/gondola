class Block:
    def __init__(self, buffer=""):
        self.records = [r for r in buffer.split("\n")[:-1]]

        # split gera um ultimo item vazio

    def clear(self, ix):
        self.records[ix] = ""

    def to_string(self):
        buffer = ""
        for r in self.records:
            buffer.append(r + "\n")
        return buffer

    def write(self, offset, record):
        if len(self.records) == offset:
            self.append(record)
        elif len(self.records) > offset:
            self.update(offset, record)
        else:
            raise Exception("[ERROR] Wrong number of data in the block")

    def append(self, record):
        self.records.append(record)

    def update(self, offset, record):
        self.records[offset] = record
