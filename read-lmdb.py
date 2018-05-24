# -*- coding: utf-8 -*-

import operator
import math

import lmdb
import pickle

import random

def get_k_rand_values(data_cursor, k):
    # returns k random values of the dataset

    data_cursor.first()
    it = data_cursor.iternext(keys=False, values=True)

    centers = []

    random.seed()

    for i, value in enumerate(it):
        
        if i < k:
            centers.append(pickle.loads(value))
        else:
            r = random.randrange(0, i, 1)
            if r < k:
                centers[r] = pickle.loads(value)
            
    return(centers)


def dist(x, y):
    # returns the squared euclidian distance

    diff = tuple(map(operator.sub, x, y))
    s_diff = tuple(map(operator.mul, diff, diff))
    return sum(s_diff)


def which_partition(value, centers):
    # returns the index of the closest center

    distances = [dist(value, x) for x in centers]
    return distances.index(min(distances))


def update_centers(data_cursor, old_centers):
    
    k = len(old_centers)
    if k == 0: return

    p = len(old_centers[0])
    if p == 0: return

    data_cursor.first()

    centers = [(0,)*p for i in range(k)]
    count   = [0,]*k

    it = data_cursor.iternext(keys=False, values=True)

    for bin_value in it:
        value = pickle.loads(bin_value)

        p = which_partition(value, old_centers) # get the appropriate partition
        count[p] += 1                           # update the number of member

        a = [(1-1/count[p]) * x for x in centers[p]]
        b = [(1/count[p]) * x for x in value]

        # Precise the center position
        centers[p] = tuple(map(operator.add, a, b))

    return centers

                
def k_means(data_cursor, k, eps = 0.00001, itermax = 40):
    data_cursor.first()
    
    # chose center
    centers      = get_k_rand_values(data_cursor, k)
    centers_diff = None

    iteration = 0

    while centers_diff is None or centers_diff > eps:
        
        # mise à jour des centres
        old_centers = centers
        centers     = update_centers(data_cursor, old_centers)

        centers_diff = sum(map(dist, centers, old_centers))

        iteration += 1
        print("iteration n°", iteration)

        if iteration == itermax:
            print("Nombre maximal d'iterrations atteind.")
            break

    return centers
    


## Algo

data_env = lmdb.open('data')

with data_env.begin() as data_txn:
    data_cursor = data_txn.cursor()

    centers = k_means(data_cursor, 4)


data_env.close()
