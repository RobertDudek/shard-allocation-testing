class Task:
    def __init__(self, shard, deltaTS, TS, length):
        self.shard = shard
        self.deltaTS = deltaTS
        self.TS = TS
        self.length = length

    def __str__(self):
        return f'Shard: {self.shard}, deltaTS: {self.deltaTS}, TS: {self.TS}, length: {self.length}'

    def __repr__(self):
        return f'{self.shard} {self.deltaTS} {self.TS} {self.length}'
