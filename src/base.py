import os


class Loader:
    BLOCK_SIZE = int(63*3)
    HEADER_SIZE = int(48)
    RECORDS_IN_A_BLOCK = int(3)
    RECORD_SIZE = int(63)
    BUCKET_SIZE = int(3 * BLOCK_SIZE)
    ALLOCATED_BUCKETS = int(10)

    def load_file(self, file_path):
        if not os.path.exists(file_path):
            temp = open(file_path, 'x')
            temp.close()
        f = open(file_path, "r+")
        return f

    def load_list(self, line):
        tuples = line.rstrip('\n').split('|')
        if tuples[0] != '':
            return [tuple(map(int, t.split(','))) for t in tuples]
        else:
            return []
