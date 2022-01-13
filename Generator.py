import numpy as np
from Task import Task
# import butools possible library for phase distribution


class Generator:

    def __init__(self, seed=None):
        self.rng = np.random.default_rng(seed=seed)

    def generate_exp(self, scale=1.0, size=1000):
        return self.rng.exponential(scale=scale, size=size)

    def generate_int(self, low=1, high=1000, size=1000):
        return self.rng.integers(low=low, high=high, size=size)

    def generate_erlang(self, k=3, scale=1.0, size=1000):
        return self.rng.gamma(shape=k, scale=scale, size=size)

    def generate_file(self, size=1000, scale_exp_low=1.0, scale_exp_high=2.0,
                      scale_length_erlang=1.0, length_erlang_k=3, file_name=None, phases=1, homogeneity=1):
        if phases < 1 or phases > size/2:
            raise ValueError('The number of phases has to be between 1 and half the size')
        if homogeneity < 0 or homogeneity > 1:
            raise ValueError('Homogeneity needs to be between 0 and 1.')

        shards = self.generate_int(size=size)
        dTS = []
        for i in range(phases):
            for j in range(size//phases):

                if self.rng.random() < homogeneity:
                # first half prefer short task
                    if j < (size//phases/2):
                        dTS.extend(self.generate_exp(scale_exp_low, 1))
                # second half prefer long task
                    else:
                        dTS.extend(self.generate_exp(scale_exp_high, 1))
                else:
                    if j < (size//phases/2):
                        dTS.extend(self.generate_exp(scale_exp_high, 1))
                    else:
                        dTS.extend(self.generate_exp(scale_exp_low, 1))

        # fill rest with exp_low, when {phases} doesn't divide {size}
        dTS.extend(self.generate_exp(scale_exp_low, size % phases))
        TS = np.cumsum(dTS)
        lengths = self.generate_erlang(k=length_erlang_k, scale=scale_length_erlang, size=size)
        tasks = [Task(s, dTS, TS, l) for s, dTS, TS, l in zip(shards, dTS, TS, lengths)]
        if file_name:
            with open(file_name, mode='w') as f:
                for t in tasks:
                    print(repr(t), file=f)
        return tasks