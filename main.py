from Generator import Generator
from Reader import Reader
from TasksAllocator import TasksAllocator
from ShardVector import ShardVector
from CloudNode import CloudNode
from ShardAllocator import ShardAllocator
import numpy as np
import matplotlib.pyplot as plt

def throwupplots(load, submission_times_CV_delta, task_mean_length, rr_res, bf_res, salp_res):
    gen = Generator()
    cloud_nodes = 100
    # Teraz można używać parametrów (D - Dziedzina):
    # 1) load = obciążenie, testować ok 0.2-0.8  D=(0, 1) domyślnie 0.5
    # 2) submission_times_CV_delta = wsp. zmienności czasów przekładania,
    # ale w % naszych możliwości (nie bezwględnie), testować ok 0.1-0.9 D=(0, 1) domyślnie 0.5
    # 3) homogenity = jednorodność, testować 0.5-1.0 D=(0, 1) domyślnie 1.0
    # 4) task_mean_length = śr. dł. zadań, nie wiem po co ale można je też zmieniać,
    # ja bym nie dawał za krótkich, domyślnie 100 D=(0, inf) (od tego i obciążenia zależy śr. dł. cz. przedkładania),
    # (k erlanga to domyślnie 3, length_erlang_k jak chcecie zmienić rozkład)
    # 5) phases = liczba faz, testować może <1,1/4*size> (?) D=<1, 1/2*size> (int) domyślnie 10
    # 6) shards_num = ilość szardów, D=<1,inf) (int) domyślnie 1,000
    # 7) size = ile wygenerować zadań, z założenia można iść w nieskończoność, D=(1,inf) (int) domyślnie 10,000 ,
    r = gen.generate_file(file_name='Example1.txt', load=load, submission_times_CV_delta=submission_times_CV_delta, homogeneity=1.0,
                          task_mean_length=task_mean_length, phases=10, shards_num=1000, size=10000)
    # r = Reader.read_file('Example2.txt')
    # print(*r, sep='\n')
    #print('Mean deltaTS:', r[~0].TS / len(r), 'Mean length:', sum(x.length for x in r) / len(r))
    ###
    # 100 przedziałów, 100 węzłów
    tasksAll = TasksAllocator(r, 100, cloud_nodes)
    tasksAll.find_shards_load_vectors()
    tasksAll.find_wts()
    tasksAll.find_norm_wts()
    minusNWTS = list(map(lambda x: x * -1, tasksAll.norm_wts))
    WS_vector = [0] * len(tasksAll.intervals)
    ###
    # Umieść wektory obciążenia Wi wszystkich fragmentów danych na liście LW posortowanej ze względu na malejący moduł.
    shardVectors = []
    for k, v in tasksAll.shards_load_vect.items():
        shardVec = ShardVector(k, v, sum(v))
        shardVectors.append(shardVec)
    # shardVectors.sort(key=lambda x: x.sum, reverse=True)

    #print("-----------RoundRobin-----------")
    rr = ShardAllocator(shardVectors, cloud_nodes, r, tasksAll.norm_wts, WS_vector, minusNWTS)
    cloudNodesRR = rr.rr()
    # Kontrola przypisanych shardów
    rr.check()
    rr_res[0].append(rr.balance_level())
    rr_res[1].append(rr.delay())
    #print("sredni poziom zrownowazenia: ", rr.balance_level())
    #print("opoznienie: ", rr.delay())

    #print("-----------BestFit--------------")
    bestfit = ShardAllocator(shardVectors, cloud_nodes, r, tasksAll.norm_wts, WS_vector, minusNWTS)
    cloudNodesBF = bestfit.bestfit()
    # Kontrola przypisanych shardów
    bestfit.check()
    bf_res[0].append(bestfit.balance_level())
    bf_res[1].append(bestfit.delay())
    #print("sredni poziom zrownowazenia: ", bestfit.balance_level())
    #print("opoznienie: ", bestfit.delay())

    #print("-----------Salp-----------------")
    salp = ShardAllocator(shardVectors, cloud_nodes, r, tasksAll.norm_wts, WS_vector, minusNWTS)
    cloudNodesSALP = salp.salp()
    # Kontrola przypisanych shardów
    salp.check()
    salp_res[0].append(salp.balance_level())
    salp_res[1].append(salp.delay())
    #print("sredni poziom zrownowazenia: ", salp.balance_level())
    #print("opoznienie: ", salp.delay())
    return rr_res, bf_res, salp_res

if __name__ == '__main__':
    rr_res=[[],[]]
    bf_res=[[],[]]
    salp_res = [[],[]]
    t = np.arange(0.1,0.9, 0.05)
    for n in t:
        rr_res, bf_res, salp_res = throwupplots(load=0.5, submission_times_CV_delta=n, task_mean_length=100, rr_res=rr_res, bf_res=bf_res, salp_res=salp_res)

    plot1 = plt.figure(1)
    plt.plot(t, rr_res[0], 'r-', label='RoundRobin')
    plt.plot(t, bf_res[0], 'b-', label='BestFit')
    plt.plot(t, salp_res[0], 'g-', label='SALP')
    plt.legend(loc="best")
    plt.xlabel("Średni rozmiar zadań")
    plt.ylabel("Poziom zrównoważenia")
    plot2 = plt.figure(2)
    plt.plot(t, rr_res[1], 'r-', label='RoundRobin')
    plt.plot(t, bf_res[1], 'b-', label='BestFit')
    plt.plot(t, salp_res[1], 'g-', label='SALP')
    plt.legend(loc="best")
    plt.xlabel("Średni rozmiar zadań")
    plt.ylabel("Średnie opóźnienie")
    plt.show()
