import os


class Loader:
    BLOCK_SIZE = 42
    HEADER_SIZE = 31
    RECORDS_IN_A_BLOCK = 3
    RECORD_SIZE = 14

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
