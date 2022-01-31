import numpy as np
from Task import Task
import warnings


class Generator:

    def __init__(self, seed=None):
        self.rng = np.random.default_rng(seed=seed)

    def generate_exp(self, scale=1.0, size=1000):
        return self.rng.exponential(scale=scale, size=size)

    def generate_int(self, low=1, high=1000, size=1000):
        return self.rng.integers(low=low, high=high+1, size=size)

    def generate_erlang(self, k=3, scale=1.0, size=1000):
        return self.rng.gamma(shape=k, scale=scale, size=size)

    def generate_file(self, load=0.50, submission_times_CV_delta=0.50, task_mean_length=100, homogeneity=1, size=10000,
                      shards_num=1000, length_erlang_k=3, file_name=None, phases=10):
        if phases < 1 or phases > size/2:
            raise ValueError('The number of phases has to be between 1 and half the size')
        if homogeneity < 0 or homogeneity > 1:
            raise ValueError('Homogeneity needs to be between 0 and 1.')
        if submission_times_CV_delta < 0 or submission_times_CV_delta > 1:
            raise ValueError('Submission_times_CV_delta needs to be between 0 and 1.')
        if load < 0 or load > 1:
            raise ValueError('Load needs to be between 0 and 1.')
        if task_mean_length < 0:
            raise ValueError('Task mean length needs to be between higher than 0.')
        if size != int(size):
            raise ValueError('Size needs to be an integer.')
        if shards_num != int(shards_num):
            raise ValueError('Number of shards needs to be an integer.')
        if length_erlang_k != int(length_erlang_k):
            warnings.warn("By using non-integer k you are using a gamma distribution, "
                          "the extension of the erlang distribution")

        submission_time_mean_length = task_mean_length/load
        scale_exp_low = submission_times_CV_delta * submission_time_mean_length
        scale_exp_high = (1+submission_times_CV_delta) * submission_time_mean_length
        scale_length_erlang = task_mean_length/length_erlang_k
        shards = self.generate_int(size=size, high=shards_num)
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