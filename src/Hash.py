import enum
from shutil import ExecError
from src.structures.file_org import FileOrg
from src.structures.block import Block
from structures.record import Record
from typing import List


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

    def insert_overflow(self, bucket, record):
        for i, b in enumerate(self.overflow_list):
            if b == bucket:
                try:
                    self.insert_bucket(i + self.ALLOCATED_BUCKETS + 1, record)
                except:
                    pass
        # need new overflow

    def insert(self, record) -> None:
        bucket = self.hashing(int(record.id))
        first_block_ix = bucket * self.blocks_in_a_bucket
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
        raise Exception('Needs Bucket Overflow')

    def select_one(self, record):
        bucket = self.hashing(int(record.id))
        first_block_ix = bucket * self.blocks_in_a_bucket
        for i in range(self.blocks_in_a_bucket):
            b = self.read_block(first_block_ix + i)
            for recovered_record in b.records:
                if(int(record.id) == recovered_int(record.id)):
                    return recovered_record
        raise Exception('[ERROR] Cant find record')

    def select_many(self, list_of_records: List[Record]) -> List[Record]:
        selected = []
        for record in list_of_records:
            selected.append[self.select_one(int(record.id))]
        return selected

    def select_range(self, starting_record: Record, ending_record: Record) -> List[Record]:
        return self.select(lambda r: (int(r.id) >= starting_int(record.id)) and (int(r.id) <= ending_int(record.id)))

    def delete(self, record: Record) -> None:
        bucket = self.hashing(int(record.id))
        first_block_ix = bucket * self.blocks_in_a_bucket
        for i in range(self.blocks_in_a_bucket):
            b = self.read_block(first_block_ix + i)
            for recovered_record in b.records:
                if(int(record.id) == recovered_int(record.id)):
                    del recovered_record
        raise Exception('[ERROR] Cant find and delete record')
