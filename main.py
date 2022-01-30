from Generator import Generator
from Reader import Reader
from TasksAllocator import TasksAllocator
from ShardVector import ShardVector
from CloudNode import CloudNode
from ShardAllocator import ShardAllocator



if __name__ == '__main__':
    gen = Generator()
    cloud_nodes = 100
    r = gen.generate_file(file_name='Example1.txt', scale_exp_low=1.0, scale_exp_high=10.0, phases=5, homogeneity=1.0)
    #r = Reader.read_file('Example2.txt')
    #print(*r, sep='\n')
    #print('Mean deltaTS:', r[~0].TS/len(r), 'Mean length:', sum(x.length for x in r)/len(r))
    ###
    #100 przedziałów, 100 węzłów
    tasksAll = TasksAllocator(r, 100, cloud_nodes)
    tasksAll.find_shards_load_vectors()
    tasksAll.find_wts()
    tasksAll.find_norm_wts()
    minusNWTS = list(map(lambda x: x * -1, tasksAll.norm_wts))
    WS_vector = [0] * len(tasksAll.intervals)
    ###
    #Umieść wektory obciążenia Wi wszystkich fragmentów danych na liście LW posortowanej ze względu na malejący moduł.
    shardVectors = []
    for k, v in tasksAll.shards_load_vect.items():
        shardVec = ShardVector(k, v, sum(v))
        shardVectors.append(shardVec)
    #shardVectors.sort(key=lambda x: x.sum, reverse=True)


    print("-----------RoundRobin-----------")
    rr = ShardAllocator(shardVectors, cloud_nodes, r, tasksAll.norm_wts, WS_vector, minusNWTS)
    cloudNodesRR = rr.rr()
    # Kontrola przypisanych shardów
    rr.check()

    print("sredni poziom zrownowazenia: ", rr.balance_level())
    print("opoznienie: ", rr.delay())

    print("-----------BestFit--------------")
    bestfit = ShardAllocator(shardVectors, cloud_nodes, r, tasksAll.norm_wts, WS_vector, minusNWTS)
    cloudNodesBF = bestfit.bestfit()
    # Kontrola przypisanych shardów
    bestfit.check()

    print("sredni poziom zrownowazenia: ", bestfit.balance_level())
    print("opoznienie: ", bestfit.delay())

    print("-----------Salp-----------------")
    salp = ShardAllocator(shardVectors, cloud_nodes, r, tasksAll.norm_wts, WS_vector, minusNWTS)
    cloudNodesSALP = salp.salp()
    # Kontrola przypisanych shardów
    salp.check()

    print("sredni poziom zrownowazenia: ", salp.balance_level())
    print("opoznienie: ", salp.delay())
