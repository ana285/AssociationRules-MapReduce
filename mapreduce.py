from mpi4py import MPI
import shutil
import os
import dispatcher as master
import worker as worker

path_data_file = "retail.dat.txt"
path_file = "mapping"

# Fie multimea cu tranzactii. Fiecare tranzactie e o colectie de obiecte
# se imparte in grupuri de cate 20 de tranzactii
# grupul se asigneaza unui mapper liber (primele 100 de linii sau cum dorim noi)
# fiecare mapper ia la nivel de tranzactie bucata de analiza. Pentru o linie are o multime de valori. 
# Rolul mapperului e sa selecteze toate partile posibile ale acelor subvalori 
# (pentru {A,B,C} => {A, 1}, {B, 1}, {C, 1}, {A, B, 1}, {B, C, 1}, {A, C, 1}, {A, B, C, 1})
# - generare fisiere de tipul idobiect_1_rank_timestamp
# Combiner: ma uit pe itemseturi si contorizez duplicatele
# La final output de itemset - valoare de tip support local


def main():
    comm = MPI.COMM_WORLD
    rank = comm.Get_rank()
    p = comm.Get_size()
    print('--- MAPPING phase started ---')
    if rank == 0:
        print('I am the coordinator with rank ', rank)
        coordinatorMapJob(comm, p)
    else:
        print('I am the worker ', rank)
        workerMapJob(rank, comm)

    comm.Barrier()
    print("All finished")

    print("--- REDUCING phase started ---")
    if rank == 0:
        master.reducer(comm, p)
        print("Master finished\n")
    else:
        worker.reducer(comm)
        print("Worker " + str(rank) + " FINISHED\n")


def coordinatorMapJob(comm, p):
    print('Coordinator job started')
    try:
        shutil.rmtree(path_file)
    except OSError as e:
        print("Error: %s - %s." % (e.filename, e.strerror))
    try:
        os.mkdir(path_file)
    except OSError:
        print("Creation of the directory %s failed" % path_file)
    else:
        print("Successfully created the directory %s " % path_file)
    master.mapper(comm, p)
    print("MASTER FINISH\n")


def workerMapJob(rank, comm):
    print('Worker job started for ', rank)
    worker.mapper(comm)
    print("Worker " + str(rank) + " FINISHED\n")



if __name__ == "__main__":
    main()