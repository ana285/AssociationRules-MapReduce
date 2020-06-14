import json
import os
import worker as s
from mpi4py import MPI
from base64 import b16encode, b16decode

ROOT = 0
MAP = 1
REDUCE = 2
path_file = "mapping"
path_data_file = "retail.dat.txt"



def read_data(file_path):
    data_file = open(file_path, 'r') 
    lines = data_file.readlines()
    return lines


def mapper(comm, p):
    workers = []
    for j in range(p):
        if j != ROOT:
            workers.append(j)
        # all the workers are free in the beginning

    data = read_data("retail.dat.txt")

    for line in data:
        if len(workers) != 0:
            j = workers.pop(0)
            print("Free worker: " + str(j))
            comm.isend(line, dest=j, tag=MAP)
        else:
            status = MPI.Status()
            comm.recv(source=MPI.ANY_SOURCE, tag=MAP, status=status)
            j = status.Get_source()
            workers.append(j)

    for j in range(p):
        comm.isend("empty", dest=j, tag=MAP)


def reducer(comm, p):
    workers = []
    for j in range(p):
        if j != ROOT:
            workers.append(j)

    end_digraph = {}

    for o in os.listdir(path_file):
        if os.path.isdir(os.path.join(path_file, o)):
            temp_name = o.split("_")
            first_key = temp_name[0]


            if first_key in end_digraph.keys():
                continue
            if len(workers) != 0:
                j = workers.pop(0)
                end_digraph[first_key] = -1
                print("Free worker: " + str(j))
                comm.isend(first_key, dest=j, tag=REDUCE)
            else:
                status = MPI.Status()
                data = comm.recv(source=MPI.ANY_SOURCE, tag=REDUCE, status=status)
                j = status.Get_source()
                key = data[0]
                value = data[1]
                end_digraph[key] = value
                print("received from slave " + str(j) + "data " + str(data))

                workers.append(j)
    for j in range(p):
        comm.isend("empty", dest=j, tag=REDUCE)
    with open('final.json', 'w') as outfile:
        json.dump(end_digraph, outfile)
