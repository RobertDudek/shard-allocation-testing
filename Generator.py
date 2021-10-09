import numpy as np
from Task import Task
# import butools possible library for phase distribution


class Generator:

    def __init__(self, seed=None):
        self.rng = np.random.default_rng(seed=seed)

    def generate_exp(self, scale=1.0, size=1000):
        return self.rng.exponential(scale=scale, size=size)

    @staticmethod
    def generate_phase(lambda1, lambda2, alpha1, alpha2):
        a = (alpha1, alpha2)
        m = [[lambda1, 0], [0, lambda2]]
        raise NotImplementedError

    def generate_int(self, low=1, high=1000, size=1000):
        return self.rng.integers(low=low, high=high, size=size)

    def generate_erlang(self, k=3, scale=1.0, size=1000):
        return self.rng.gamma(shape=k, scale=scale, size=size)

    def generate_file(self, size=1000, scale_exp_low=1.0, scale_exp_high=2.0,
                      scale_length_erlang=1.0, length_erlang_k=3, phase_length=5, file_name=None, phase_enable=False):

        shards = self.generate_int(size=size)
        dTS = self.generate_exp(scale=scale_exp_low, size=size)
        dTS_low = self.generate_exp(scale=scale_exp_low, size=size//2)
        dTS_high = self.generate_exp(scale=scale_exp_high, size=size//2 + size%2)

        # maybe alternate every n items in phases will be ok?
        if phase_enable:
            dTS = []
            for i in range(len(dTS_high)):
                dTS.extend(dTS_low[i*phase_length:(i+1)*phase_length])
                dTS.extend(dTS_high[i * phase_length:(i+1)*phase_length])
        TS = np.cumsum(dTS)
        lengths = self.generate_erlang(k=length_erlang_k, scale=scale_length_erlang)
        tasks = [Task(s, dTS, TS, l) for s, dTS, TS, l in zip(shards, dTS, TS, lengths)]
        if file_name:
            with open(file_name, mode='w') as f:
                for t in tasks:
                    print(repr(t), file=f)
        return tasks