from Generator import Generator
from Reader import Reader
from TasksAllocator import TasksAllocator
from ShardVector import ShardVector
from CloudNode import CloudNode


if __name__ == '__main__':
    gen = Generator()
    cloud_nodes = 100
    r = gen.generate_file(file_name='Example1.txt', scale_exp_low=5.0)
    #r = Reader.read_file('Example1.txt')
    print(*r, sep='\n')
    print('Mean deltaTS:', r[~0].TS/len(r), 'Mean length:', sum(x.length for x in r)/len(r))
    ###
    #100 przedziałów, 100 węzłów
    tasksAll = TasksAllocator(r, 100, cloud_nodes)
    tasksAll.find_shards_load_vectors()
    tasksAll.find_wts()
    tasksAll.find_norm_wts()
    ###
    #Umieść wektory obciążenia Wi wszystkich fragmentów danych na liście LW posortowanej ze względu na malejący moduł.
    shardVectors = []
    for k, v in tasksAll.shards_load_vect.items():
        shardVec = ShardVector(k, v, sum(v))
        shardVectors.append(shardVec)
    shardVectors.sort(key=lambda x: x.sum, reverse=True)
    ###
    #Dla każdego węzła utwórz pusty podzbiór fragmentów danych FSi i pusty wektor obciążenia węzła WSi.
    # Wszystkie węzły zaznacz jako aktywne.
    cloudNodes = []
    for i in range(cloud_nodes):
        cloudNo = CloudNode()
        cloudNodes.append(cloudNo)


