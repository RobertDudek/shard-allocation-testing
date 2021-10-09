from Task import Task


class Reader:
    @staticmethod
    def read_file(file_name):
        tasks = []
        with open(file_name) as f:
            for line in f:
                if line == '\n':
                    continue
                shard, dTS, TS, length = line.split()
                tasks.append(Task(int(shard), float(dTS), float(TS), float(length)))
        return tasks