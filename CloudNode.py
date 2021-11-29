

class CloudNode:

    def __init__(self, id, WS_vector, unbalanced):
        self.id = id
        self.FS_subset = []
        self.WS_vector = WS_vector
        self.unbalanced = unbalanced
        self.WS_sum = None
        self.active = True