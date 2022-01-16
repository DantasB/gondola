from src.structures.file_org import FileOrg
from src.structures.block import Block
from src.structures.record import Record


class Hash(FileOrg):

    def __init__(self, relation_name, schema_header):
        super().__init__(relation_name, schema_header)
        self.blocks_in_a_bucket = self.BUCKET_SIZE/self.BLOCK_SIZE
        if not self.already_existed:
            f = open(self.metadata_path, 'a')
            f.write('\n')
            f.close()
        f = open(self.metadata_path, 'r')
        f_lines = f.readlines()
        overflow_list = f_lines[-1].strip('\n').split('|')
        if overflow_list[0] != '':
            self.overflow_list = [int(bucket) for bucket in overflow_list]
        else:
            self.overflow_list = []
        f.close()

    def hashing(self, key: int) -> hash:
        return key % self.ALLOCATED_BUCKETS

    def fill(self, n_blocks):
        for _ in range(n_blocks):
            self.append_block(Block())

    def insert_bucket(self, ix, record):
        first_block_ix = ix * self.blocks_in_a_bucket
        if first_block_ix >= self.block_count:
            self.fill(first_block_ix-self.block_count + 1)
        for i in range(self.blocks_in_a_bucket):
            b = self.read_block(first_block_ix + i)
            try:
                b.insert(record)
                self.write_block(b, first_block_ix + i)
                return
            except:
                pass
        raise Exception('Need Overflow')

    def search_bucket(self, ix, record):
        first_block_ix = ix * self.blocks_in_a_bucket
        if first_block_ix >= self.block_count:
            raise Exception('Empty Bucket')
        for i in range(self.blocks_in_a_bucket):
            b = self.read_block(first_block_ix + i)
            for recovered_record in b.records:
                if(int(record.id) == recovered_int(record.id)):
                    return recovered_record
        raise Exception('Need Overflow')

    def delete_bucket(self, ix, record):
        first_block_ix = ix * self.blocks_in_a_bucket
        if first_block_ix >= self.block_count:
            raise Exception('Empty Bucket')
        for i in range(self.blocks_in_a_bucket):
            b = self.read_block(first_block_ix + i)
            for recovered_record in b.records:
                if(int(record.id) == recovered_int(record.id)):
                    b.clear(recovered_record.offset)
                    return
        raise Exception('Need Overflow')

    def search_overflow(self, bucket, record):
        for i, b in enumerate(self.overflow_list):
            if b == bucket:
                try:
                    self.search_bucket(i + self.ALLOCATED_BUCKETS + 1, record)
                    return
                except:
                    pass
        raise Exception('Cant find record')

    def insert_overflow(self, bucket, record):
        for i, b in enumerate(self.overflow_list):
            if b == bucket:
                try:
                    self.insert_bucket(i + self.ALLOCATED_BUCKETS + 1, record)
                    return
                except:
                    pass
        self.overflow_list.append(bucket)
        self.insert_overflow(bucket, record)

    def delete_overflow(self, bucket, record):
        for i, b in enumerate(self.overflow_list):
            if b == bucket:
                try:
                    self.delete_bucket(i + self.ALLOCATED_BUCKETS + 1, record)
                    return
                except:
                    pass
        raise Exception('Cant find record')

    def insert(self, record) -> None:
        bucket = self.hashing(int(record.id))
        try:
            self.insert_bucket(bucket, record)
        except:
            self.insert_overflow(bucket, record)

    def select_one(self, record):
        bucket = self.hashing(int(record.id))
        try:
            self.search_bucket(bucket, record)
        except:
            self.search_overflow(bucket, record)

    def delete(self, record: Record) -> None:
        bucket = self.hashing(int(record.id))
        try:
            self.delete_bucket(bucket, record)
        except:
            self.delete_overflow(bucket, record)

    def select_list(self, list_of_records):
        selected = []
        for record in list_of_records:
            selected.append[self.select_one(int(record.id))]
        return selected

    def persist(self):
        self.heap.persist()
        metadata_file = open(self.metadata_path, 'w')
        lines = []
        lines.append(self.empty_list_to_str())
        lines.append(str(self.block_count) + "\n")
        lines.append(str(self.record_count) + '\n')
        lines.append(str(self.need_reorganize) + '\n')
        metadata_file.seek(0)
        metadata_file.writelines(lines)
        metadata_file.close()
        f = open(self.metadata_path, 'a')
        stringfied = [str(id) for id in self.overflow_list]
        f.write('|'.join(stringfied) + '\n')
