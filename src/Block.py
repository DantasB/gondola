class Block:
    def __init__(self, buffer=""):
        self.records = [r for r in buffer.split("\n")[:-1]]
        # split gera um ultimo item vazio

    def append(self, record):
        self.records.append(record)

    def clear(self, ix):
        self.records[ix] = ""

    def to_string(self):
        buffer = ""
        for r in self.records:
            buffer.append(r + "\n")
        return buffer
