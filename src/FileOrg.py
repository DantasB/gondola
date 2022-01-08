import Block


class FileOrg:
    def __init__(self, file_path):
        self.file = open(file_path, "a+")

    def insert(self):
        raise NotImplementedError

    def select(self):
        raise NotImplementedError

    def delete(self):
        raise NotImplementedError

    def reorganize(self):
        raise NotImplementedError

    def append_block(self, block):
        self.file.seek(0, 2)  # set pointer to file end
        self.file.write(block.to_string())

    def read_block(self, ix):
        self.file.seek(ix * BLOCK_SIZE)
        buffer = self.file.read(BLOCK_SIZE)
        return Block(buffer)
