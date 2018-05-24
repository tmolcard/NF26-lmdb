# -*- coding: utf-8 -*-

import os

import lmdb
import csv
import pickle
import hashlib


def formating_row(row):
    try:
        st_lat = float(row[5])
        st_lon = float(row[6])
        ed_lat = float(row[9])
        ed_lon = float(row[10])

        return (st_lat, st_lon, ed_lat, ed_lon)

    except:
        print("invalid row")
        return None
        

path = "201708-citibike-tripdata.csv"
size =  1000000000 # os.path.getsize(path)*10

data_env = lmdb.open('data', map_size = size)


with open(path, 'r') as csvfile:
    reader = csv.reader(csvfile, delimiter=',', quotechar='"')
    next(reader) # skip header

    with data_env.begin(write=True) as data_writer:
        db = data_env.open_db()
        data_writer.drop(db, delete=False)

        for row in reader:
            value = pickle.dumps(formating_row(row))
            key   = hashlib.sha256(value).digest()
            
            data_writer.put(key, value)

data_env.close()