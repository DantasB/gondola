from Base import Loader
import Block
import os


class FileOrg(Loader):
    def __init__(self, relation_name, schema_header=None):
        data_path = f"./src/{relation_name}/data.cbd"
        metadata_path = f"./src/{relation_name}/metadata.cbd"
        self.data_file = self.load_file(data_path)
        self.metadata_file = self.load_file(metadata_path)
        if schema_header:
            self.data_file.write(schema_header)
        if len(self.metadata_file.readlines()) <= 1:
            self.metadata_file.write('\n')  # emptylist
            self.metadata_file.write('0\n')  # block_count
            self.metadata_file.write('0\n')  # record_count
            self.metadata_file.flush()
        self.metadata_file.seek(0)
        self.empty_list = self.load_empty_list(self.metadata_file)
        self.block_count = int(self.metadata_file.readline())
        self.record_count = int(self.metadata_file.readline())

    def __empty_list_to_str(self):
        return '|'.join([','.join(map(str, value)) for value in self.empty_list])

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
            for record in block.records:
                if filter(record):
                    block.clear(record.offset)
                    self.empty_list.append((ix, record.offset))
            self.write_block(block, ix)
            ix += 1

    def insert(self):
        raise NotImplementedError

    def reorganize(self):
        raise NotImplementedError

    def write_block(self, block, ix):
        self.data_file.seek(ix * self.BLOCK_SIZE + self.HEADER_SIZE)
        self.data_file.write(block.to_string())

    def append_block(self, block):
        self.data_file.seek(0, 2)  # set pointer to data_file end
        self.data_file.write(block.to_string())
        self.block_count += 1

    def read_block(self, ix):
        self.data_file.seek(ix * self.BLOCK_SIZE + self.HEADER_SIZE)
        buffer = self.data_file.read(self.BLOCK_SIZE)
        if len(buffer) == 0:
            raise Exception("[ERROR] NO MORE BLOCKS TO READ")
        return Block(buffer)

    def write_record(self, block_ix, block_offset, record):
        block = self.read_block(block_ix)
        new_empty = block.write(block_offset, record)
        self.write_block(block, block_ix)
        return new_empty

    def persist(self):
        lines = []
        lines.append(self.__empty_list_to_str())
        lines.append(self.block_count + "\n")
        lines.append(self.record_count + '\n')
        self.metadata_file.seek(0)
        self.metadata_file.writelines(lines)
