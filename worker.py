from base64 import b16encode, b16decode
import os
from mpi4py import MPI
import calendar;
import time;
import glob

ROOT = 0
MAP = 1
REDUCE = 2
main_path_file = "mapping"


def create_mapping(dir_name, key, value, rank):
    ts = calendar.timegm(time.gmtime())
    path_file = dir_name + "/" + str(key) + "_" + str(value) + "_" + str(rank) + "_" + str(ts)

    try:
        os.mkdir(path_file)
    except OSError:
        print("Creation of the directory %s failed" % path_file)
    else:
        print("Successfully created the directory %s " % path_file)


def mapper(comm):
    my_rank = comm.Get_rank()
    data = comm.recv(source=ROOT, tag=MAP)
    while data != "empty":
        print("slave " + str(my_rank) + " received values:" + data + "\n")

        # (pentru {A,B,C} => {A, 1}, {B, 1}, {C, 1}, {A, B, 1}, {B, C, 1}, {A, C, 1}, {A, B, C, 1})
        # - generare fisiere de tipul idobiect_1_rank_timestamp

        numbers = [int(i) for i in data.split() if i.isdigit()]
        map_elements = {}

        for number in numbers:
            if(number in map_elements.keys()):
                map_elements[number] += 1
            else:
                map_elements[number] = 1
        
        for key in map_elements.keys():
            create_mapping(main_path_file, key, map_elements[key], my_rank)

        print("starting slave send")
        comm.send(1, dest=ROOT, tag=MAP)
        print("end slave send")
        data = comm.recv(source=ROOT, tag=MAP)


def reducer(comm):
    my_rank = comm.Get_rank()
    key = comm.recv(source=ROOT, tag=REDUCE)
    while key != "empty":
        print("slave " + str(my_rank) + " received value " + key + "\n")
        final_value = 0

        for name in glob.glob(main_path_file + "/" + key + "_*"):
            if(os.path.isdir(name)):
                values = name.split("_")
                value = int(values[1])
                final_value += value

        print("starting slave send")
        print([key, final_value])
        comm.send([key, final_value], dest=ROOT, tag=REDUCE)
        print("end slave send")
        key = comm.recv(source=ROOT, tag=REDUCE)
