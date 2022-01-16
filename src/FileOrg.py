from Record import Record
from Base import Loader
from Block import Block
import os


class FileOrg(Loader):
    def __init__(self, relation_name, schema_header=None):
        self.data_path = f"./src/Database/{relation_name}.data.cbd"
        self.metadata_path = f"./src/Database/{relation_name}.metadata.cbd"
        self.already_existed = os.path.exists(self.data_path)
        self.data_file = self.load_file(self.data_path)
        metadata_file = self.load_file(self.metadata_path)
        self.schema_header = schema_header
        if schema_header:
            self.data_file.write(schema_header)
        if len(metadata_file.readlines()) <= 1:
            metadata_file.write('\n')  # emptylist
            metadata_file.write('0\n')  # block_count
            metadata_file.write('0\n')  # record_count
            metadata_file.write('False\n')  # need_reorganize
        metadata_file.seek(0)
        self.empty_list = self.load_list(metadata_file.readline())
        self.block_count = int(metadata_file.readline())
        self.record_count = int(metadata_file.readline())
        self.need_reorganize = False
        metadata_file.close()

    def empty_list_to_str(self):
        return '|'.join([','.join(map(str, value)) for value in self.empty_list])+'\n'

    def select(self, filter):
        r = []
        ix = 0
        while True:
            try:
                block = self.read_block(ix)
            except:
                break
            for record in block.records:
                if not record.is_empty and filter(record):
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
                if not record.is_empty and filter(record):
                    self.record_count -= 1
                    block.clear(record.offset)
                    self.empty_list.append((ix, record.offset))
            self.write_block(block, ix)
            ix += 1

    def insert(self):
        raise NotImplementedError

    def reorganize(self):
        records = self.select(lambda r: True)
        self.reset()
        for r in records:
            self.insert(r)

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
        metadata_file = open(self.metadata_path, 'w')
        lines = []
        lines.append(self.empty_list_to_str())
        lines.append(str(self.block_count) + "\n")
        lines.append(str(self.record_count) + '\n')
        metadata_file.seek(0)
        metadata_file.writelines(lines)
        metadata_file.close()

    def data_clear(self):
        self.data_file.truncate(0)
        self.data_file.seek(0)
        self.data_file.write(self.schema_header)

    def metadata_clear(self):
        metadata_file = self.load_file(self.metadata_path)
        metadata_file.write('\n')  # emptylist
        metadata_file.write('0\n')  # block_count
        metadata_file.write('0\n')  # record_count
        metadata_file.write('False\n')  # need_reorganize
        self.empty_list = []
        self.block_count = 0
        self.record_count = 0
        self.need_reorganize = False
        metadata_file.close()

    def reset(self):
        self.data_clear()
        self.metadata_clear()
