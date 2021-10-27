import math


class TasksAllocator:

    def __init__(self, tasks, n_intervals):
        '''
        :param tasks: obiekty zadań
        :param n_intervals: ile przedziałów
        :param intervals: lista przedziałów (górne granice)
        :param shards
        :param shard_load_vect: słownik shard:wektor
        :param summed_vects: słownik shard:miara miejska dla wektora
        :param wts
        :param n_cloud_nodes: ile węzłów chmury
        :param norm_wts
        '''

        self.tasks = tasks
        self.n_intervals = n_intervals
        self.intervals = None
        self.shards = []
        self.shard_load_vect = None
        self.summed_vects = {}
        self.wts = None
        self.n_cloud_nodes = 100
        self.norm_wts = None

    def find_shards_load_vectors(self):
        self.tasks.sort(key=lambda task: task.TS, reverse=False)
        #delta - szerokość naszych przedziałów, co ile kolejny przedział
        delta = math.ceil((self.tasks[-1].TS+self.tasks[-1].length) / self.n_intervals)
        self.intervals = [*range(delta, math.ceil(self.tasks[-1].TS+self.tasks[-1].length)+delta, delta)]
        print(self.intervals)
        for task in self.tasks:
            self.shards.append(task.shard)

        #słownik, klucz szard : wartość wektor obciążeń
        intervals = [[0]* self.n_intervals] * len(self.shards)
        self.shards_load_vect = dict(zip(self.shards, intervals))

        for shard in self.shards:
            shard_vect = [0]*self.n_intervals
            for task in self.tasks:
                if task.shard != shard:
                    #jeżeli w zadaniu mamy inny shard, sprawdź kolejne zadanie
                    continue
                for i in range(len(self.intervals)):
                    if task.TS < self.intervals[i]:
                        #jeżeli długość zadania mieści się w przedziale
                        if task.TS + task.length < self.intervals[i]:
                            shard_vect[i] += (task.length/delta)
                            break
                        #jeżeli długość się nie mieści
                        else:
                            shard_vect[i] += (self.intervals[i] - task.TS)/delta
                            shard_vect[i+1] += (task.length - (self.intervals[i] - task.TS))/delta
                            break
                self.shards_load_vect.update({shard: shard_vect})
        #print(self.shards_load_vect)

    def find_wts(self):
        #sumuj obciążenia dla poszczególnych shardów
        self.summed_vects = dict(zip(self.shards_load_vect.keys(), [sum(v) for v in self.shards_load_vect.values()]))
        print(self.summed_vects)
        #sumuj obciążenie dla wszystkich shardów
        self.wts = sum(self.summed_vects.values())
        print(self.wts)

    def find_norm_wts(self):
        self.norm_wts = 1/self.n_cloud_nodes*self.wts
        print(self.norm_wts)