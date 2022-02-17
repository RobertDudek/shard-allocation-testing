from CloudNode import CloudNode

class ShardAllocator:

    def __init__(self, shardVectors, cloud_nodes, tasks, norm_wts, WS_vector, minusNWTS):
        self.shardVectors = shardVectors
        self.WS_vector = WS_vector
        self.minusNWTS = minusNWTS
        self.tasks = tasks
        self.norm_wts = norm_wts
        self.cloudNodes = []
        self.cloud_nodes = cloud_nodes

        for i in range(cloud_nodes):
            # DONE: przy tworzeniu wezlow podaj wektor obciazenia wypelniony zerami oraz wektor niezrownowazenia (Ws-NWTS)
            cloudNo = CloudNode(i, self.WS_vector, self.minusNWTS)
            self.cloudNodes.append(cloudNo)

    def rr(self):
        cloud_idx = 0
        for shard in self.shardVectors:
            self.cloudNodes[cloud_idx].FS_subset.append(shard)
            self.cloudNodes[cloud_idx].WS_vector = [x + y for x, y in zip(self.cloudNodes[cloud_idx].WS_vector, shard.load_vector)]
            if cloud_idx+1 < self.cloud_nodes:
                cloud_idx +=1
            else:
                cloud_idx = 0
        return self.cloudNodes

    def bestfit(self):
        self.shardVectors.sort(key=lambda x: x.avg, reverse=True)
        for shard in self.shardVectors:
            self.cloudNodes.sort(key=lambda x: sum(x.WS_vector)/len(x.WS_vector), reverse=False)
            #print("najmniej ma: ", cloudNodes[0].id, " czyli: ", sum(cloudNodes[0].WS_vector)/len(cloudNodes[0].WS_vector))
            self.cloudNodes[0].FS_subset.append(shard)
            self.cloudNodes[0].WS_vector = [x + y for x, y in zip(self.cloudNodes[0].WS_vector, shard.load_vector)]
        return self.cloudNodes

    def salp(self):
        #5.	Przetwarzaj kolejno elementy lwi listy LW.
        self.shardVectors.sort(key=lambda x: x.sum, reverse=True)
        for shard in self.shardVectors:
            remember_id = -1
            max_mod_substraction = -100
            compare_modules = 0
            for cloudNo in self.cloudNodes:
                if cloudNo.active:
                    #suma wektora obciazenia wezla + wektora obciazenia danego shardu hipotetycznie
                    after_sum = [x + y for x, y in zip(cloudNo.WS_vector, shard.load_vector)]
                    #wektor niezrownowazenia przed dodaniem - mamy w obiekcie cloudNo jako unbalanced
                    #wektor niezrownowazenia po dodaniu hipotetycznie:
                    after_unbalanced = [x - y for x, y in zip(after_sum, self.norm_wts)]
                    #maksymalizacja wartości różnicy między modułami dwóch wektorów niezrównoważenia obciążenia
                    before_module = sum(abs(number) for number in cloudNo.unbalanced)
                    after_module = sum(abs(number) for number in after_unbalanced)
                    compare_modules = after_module - before_module
                    if compare_modules > max_mod_substraction:
                        max_mod_substraction = compare_modules
                        remember_id = cloudNo.id
            #Gdyby jakies szardy nie byly przypisane do wezla tutaj mozna printowac
            # if remember_id == -1:
            #     print(compare_modules)
            #     print(shard.shard)
            if remember_id != -1:
                index = [x.id for x in self.cloudNodes].index(remember_id)
                self.cloudNodes[index].FS_subset.append(shard)
                self.cloudNodes[index].WS_vector = [x + y for x, y in zip(self.cloudNodes[index].WS_vector, shard.load_vector)]
                self.cloudNodes[index].unbalanced = [x - y for x, y in zip(self.cloudNodes[index].WS_vector, self.norm_wts)]
                check_module_ws = sum(abs(number) for number in self.cloudNodes[index].WS_vector)
                check_module_nwts = sum(abs(number) for number in self.norm_wts)
                if check_module_ws > check_module_nwts:
                    self.cloudNodes[index].active = False
        return self.cloudNodes

    def delay(self):
        time = [0] * self.cloud_nodes
        shardsInClouds = list(map(lambda shards: list(map(lambda x: x.shard, shards)), list(map(lambda sh: sh.FS_subset, self.cloudNodes))))
        delay = 0
        for task in self.tasks:
            cloudIndex = next((shardsInClouds.index(x) for x in shardsInClouds if task.shard in x), None)
            if task.TS >= time[cloudIndex]:
                time[cloudIndex] = task.TS + task.length
            else:
                time[cloudIndex] += task.length
                delay += time[cloudIndex] - (task.TS + task.length)
        return round(delay,2)

    def balance_level(self):
        count = 0
        balance = []
        for cloudNo in self.cloudNodes:
            #print("cloud no: ", cloudNo.id, " poziom zrownowazenia: ", list(zip(self.norm_wts, cloudNo.WS_vector)))
            balance.append(sum([abs(x - y)/x for x, y in zip(self.norm_wts, cloudNo.WS_vector)]))
            #print(list(map(lambda sh: sh.shard, cloudNo.FS_subset)))
            count += (len(cloudNo.FS_subset))
        return round(sum(balance)/len(balance), 2)

    def check(self):
        count = 0
        for cloudNo in self.cloudNodes:
            #print(len(cloudNo.FS_subset))
            count += (len(cloudNo.FS_subset))
        #print("Shardow na wejsciu: " + str(len(self.shardVectors)))
        #print("Shardy przypisane: " + str(count))
