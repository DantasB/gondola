import Block
import os


class FileOrg:
    def __init__(self, relation_name, schema_header):
        file_path = f"./{relation_name}/data.cbd"
        metadata_path = f"./{relation_name}/metadata.cbd"
        self.data_file = open(file_path, "r+")
        self.data_file.seek(0)
        self.data_file.write(schema_header)
        self.metadata_file = open(metadata_path, "r+")
        self.empty_list = []
        self.block_count = 0
        self.record_count = 0

    def select(self, filter):
        r = []
        ix = 0
        while True:
            try:
                block = self.read_block(ix)
            except:
                break
            for record in block.records:
                if filter(record):
                    r.append(record)
            ix += 1
        return r

    def delete(self, filter):
        ix = 0
        while True:
            try:
                block = self.read_block(ix)
            except:
                break
            for offset, record in enumerate(block.records):
                if filter(record):
                    block.clear(offset)
                    self.empty_list.append((ix, offset))
                    self.write_block(block, ix)
            ix += 1

    def insert(self):
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
        if len(buffer) == 0:
            raise Exception("[ERROR] NO MORE BLOCKS TO READ")
        return len(buffer), Block(buffer)
