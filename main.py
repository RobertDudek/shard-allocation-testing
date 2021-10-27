from Generator import Generator
from Reader import Reader
from TasksAllocator import TasksAllocator


if __name__ == '__main__':
    gen = Generator()
    r = gen.generate_file(file_name='Example1.txt', scale_exp_low=5.0)
    #r = Reader.read_file('Example1.txt')
    print(*r, sep='\n')
    print('Mean deltaTS:', r[~0].TS/len(r), 'Mean length:', sum(x.length for x in r)/len(r))
    ###
    tasksAll = TasksAllocator(r, 100)
    tasksAll.find_shards_load_vectors()
    tasksAll.find_wts()
    tasksAll.find_norm_wts()
