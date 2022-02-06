import math


class TasksAllocator:

    def __init__(self, tasks, n_intervals, n_cloud_nodes):
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
        self.wts = []
        self.n_cloud_nodes = n_cloud_nodes
        self.norm_wts = None

    def find_shards_load_vectors(self):
        self.tasks.sort(key=lambda task: task.TS+task.length, reverse=False)
        #delta - szerokość naszych przedziałów, co ile kolejny przedział
        delta = math.ceil((self.tasks[-1].TS+self.tasks[-1].length) / self.n_intervals)
        self.intervals = [*range(delta, math.ceil(self.tasks[-1].TS+self.tasks[-1].length)+delta, delta)]
        #print(self.intervals)
        shards = set()
        for task in self.tasks:
            shards.add(task.shard)
        self.shards = sorted(list(shards))


        #słownik, klucz szard : wartość wektor obciążeń
        intervals = [[0 for _ in range(len(self.intervals))] for _ in range(len(self.shards))]

        for task in self.tasks:
            mapped_shard = self.shards.index(task.shard)
            first_interval = math.floor(task.TS/delta)
            last_interval = math.ceil((task.TS + task.length)/delta - 1)
            # jeżeli długość zadania mieści się w przedziale
            if first_interval == last_interval:
                intervals[mapped_shard][first_interval] += (task.length/delta)
            # jeżeli długość się nie mieści
            else:
                # część w pierwszym przedziale
                part_inside_first_interval = self.intervals[first_interval] - task.TS
                intervals[mapped_shard][first_interval] += part_inside_first_interval / delta
                # pełne przedziały
                for i in range(first_interval+1, last_interval, 1):
                    # print(f"pełny przedział w {task = }")
                    intervals[mapped_shard][i] = 1
                # część w ostatnim przedziale
                part_inside_last_interval = (task.TS+task.length) - self.intervals[last_interval-1]
                intervals[mapped_shard][last_interval] += part_inside_last_interval / delta

        self.shards_load_vect = dict(zip(self.shards, intervals))
        #print("WEKTORY OBCIĄŻEŃ W SŁOWNIKU")
        #print(self.shards_load_vect)

    def find_wts(self):
        #changed to summing in intervals, return vector
        self.wts = [0] * (len(self.intervals))
        #print(self.wts)
        for v in self.shards_load_vect.values():
            for i in range(len(v)):
                self.wts[i] += v[i]
        #print("WTS")
        #print(self.wts)

    def find_norm_wts(self):
        self.norm_wts = [0] * (len(self.intervals))
        for i in range(len(self.wts)):
            self.norm_wts[i] = 1/self.n_cloud_nodes*self.wts[i]
        #print("NORM WTS")
        #print(self.norm_wts)
