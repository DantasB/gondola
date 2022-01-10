import Block


class FileOrg:
    def __init__(self, relation_name):
        file_path = f"./{relation_name}.cbd"
        metadata_path = f"./{relation_name}_meta.cbd"
        self.data_file = open(file_path, "r+")
        self.metadata_file = open(metadata_path, "r+")

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
