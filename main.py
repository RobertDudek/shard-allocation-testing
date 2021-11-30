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
    minusNWTS = list(map(lambda x: x * -1, tasksAll.norm_wts))
    WS_vector = [0] * len(tasksAll.intervals)
    #print(abs(sum(minusNWTS)))
    for i in range(cloud_nodes):
        # DONE: przy tworzeniu wezlow podaj wektor obciazenia wypelniony zerami oraz wektor niezrownowazenia (Ws-NWTS)
        cloudNo = CloudNode(i, WS_vector, minusNWTS)
        cloudNodes.append(cloudNo)
        #print(cloudNo.WS_vector)

    #5.	Przetwarzaj kolejno elementy lwi listy LW.
    for shard in shardVectors:
        remember_id = -1
        max_mod_substraction = -100
        compare_modules = 0
        for cloudNo in cloudNodes:
            if cloudNo.active:
                #suma wektora obciazenia wezla + wektora obciazenia danego shardu hipotetycznie
                after_sum = [x + y for x, y in zip(cloudNo.WS_vector, shard.load_vector)]
                #wektor niezrownowazenia przed dodaniem - mamy w obiekcie cloudNo jako unbalanced
                #wektor niezrownowazenia po dodaniu hipotetycznie:
                after_unbalanced = [x - y for x, y in zip(after_sum, tasksAll.norm_wts)]
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
            cloudNodes[index].unbalanced = [x - y for x, y in zip(cloudNodes[index].WS_vector, tasksAll.norm_wts)]
            check_module_ws = sum(abs(number) for number in cloudNodes[index].WS_vector)
            check_module_nwts = sum(abs(number) for number in tasksAll.norm_wts)
            if check_module_ws > check_module_nwts:
                cloudNodes[index].active = False

    #CHECK
    count = 0
    for cloudNo in cloudNodes:
        print(len(cloudNo.FS_subset))
        count += (len(cloudNo.FS_subset))
    print("Shardow na wejsciu: " + str(len(shardVectors)))
    print("Shardy przypisane: " + str(count))

