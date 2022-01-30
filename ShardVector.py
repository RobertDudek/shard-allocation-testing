

class ShardVector:

    def __init__(self, shard, vect, sum):
        self.shard = shard
        self.load_vector = vect
        self.sum = sum
        self.avg = sum/len(self.load_vector)
