import Block


class FileOrg:
    def __init__(self, data_path, metadata_path):
        self.data_file = open(data_path, "w+")
        self.metadata_file = open(metadata_path, "w+")

    def insert(self):
        raise NotImplementedError

    def select(self):
        raise NotImplementedError

    def delete(self):
        raise NotImplementedError

    def reorganize(self):
        raise NotImplementedError

    def write_block(self, block, ix):
        self.data_file.seek(ix * BLOCK_SIZE + HEADER_SIZE)
        self.data_file.write(block.to_string())

    def append_block(self, block):
        self.data_file.seek(0, 2)  # set pointer to data_file end
        self.data_file.write(block.to_string())

    def read_block(self, ix):
        self.data_file.seek(ix * BLOCK_SIZE + HEADER_SIZE)
        buffer = self.data_file.read(BLOCK_SIZE)
        return len(buffer), Block(buffer)
