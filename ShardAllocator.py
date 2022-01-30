from CloudNode import CloudNode

class ShardAllocator:

    def __init__(self, shardVectors, cloud_nodes, tasks, norm_wts, WS_vector, minusNWTS):
        self.shardVectors = shardVectors
        #self.cloudNodes = cloudNodes
        self.WS_vector = WS_vector
        self.minusNWTS = minusNWTS
        self.tasks = tasks
        self.norm_wts = norm_wts
        self.cloudNodes = []
        for i in range(cloud_nodes):
            # DONE: przy tworzeniu wezlow podaj wektor obciazenia wypelniony zerami oraz wektor niezrownowazenia (Ws-NWTS)
            cloudNo = CloudNode(i, self.WS_vector, self.minusNWTS)
            self.cloudNodes.append(cloudNo)

    def rr(self):
        cloudNodes = self.cloudNodes
        cloud_nodes = len(cloudNodes)
        cloud_idx = 0 #zaczynamy od zerowego indeksu chmury
        for shard in self.shardVectors:
            cloudNodes[cloud_idx].FS_subset.append(shard)
            cloudNodes[cloud_idx].WS_vector = [x + y for x, y in zip(cloudNodes[cloud_idx].WS_vector, shard.load_vector)]
            if cloud_idx+1 < cloud_nodes:
                cloud_idx +=1
            else:
                cloud_idx = 0
        return cloudNodes

    def bestfit(self):
        cloudNodes = self.cloudNodes
        self.shardVectors.sort(key=lambda x: x.avg, reverse=True)
        for shard in self.shardVectors:
            cloudNodes.sort(key=lambda x: sum(x.WS_vector)/len(x.WS_vector), reverse=False)
            #print("najmniej ma: ", cloudNodes[0].id, " czyli: ", sum(cloudNodes[0].WS_vector)/len(cloudNodes[0].WS_vector))
            cloudNodes[0].FS_subset.append(shard)
            cloudNodes[0].WS_vector = [x + y for x, y in zip(cloudNodes[0].WS_vector, shard.load_vector)]
        return cloudNodes

    def salp(self):
        cloudNodes = self.cloudNodes
        #5.	Przetwarzaj kolejno elementy lwi listy LW.
        self.shardVectors.sort(key=lambda x: x.sum, reverse=True)
        for shard in self.shardVectors:
            remember_id = -1
            max_mod_substraction = -100
            compare_modules = 0
            for cloudNo in cloudNodes:
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
                index = [x.id for x in cloudNodes].index(remember_id)
                cloudNodes[index].FS_subset.append(shard)
                cloudNodes[index].WS_vector = [x + y for x, y in zip(cloudNodes[index].WS_vector, shard.load_vector)]
                cloudNodes[index].unbalanced = [x - y for x, y in zip(cloudNodes[index].WS_vector, self.norm_wts)]
                check_module_ws = sum(abs(number) for number in cloudNodes[index].WS_vector)
                check_module_nwts = sum(abs(number) for number in self.norm_wts)
                if check_module_ws > check_module_nwts:
                    cloudNodes[index].active = False
        return cloudNodes

    def delay(self, cloudNodes, tasks):
        cloud_nodes = len(self.cloudNodes)
        time = [0] * cloud_nodes
        shardsInClouds = list(map(lambda shards: list(map(lambda x: x.shard, shards)), list(map(lambda sh: sh.FS_subset, cloudNodes)))) #kurna nawet nie pytaj
        delay = 0
        for task in self.tasks:
            cloudIndex = next((shardsInClouds.index(x) for x in shardsInClouds if task.shard in x), None)
            if task.TS >= time[cloudIndex]:
                time[cloudIndex] = task.TS + task.length
            else:
                time[cloudIndex] += task.TS
                delay += time[cloudIndex] - (task.TS + task.length)
        return(delay)