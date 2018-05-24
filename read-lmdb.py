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
    

def update_cluster(data_cursor, cluster_cursor, centers):
    
    data_cursor.first()
    it_data = data_cursor.iternext(keys=True, values=True)

    for key, bin_value in it_data:
        
        value = pickle.loads(bin_value)
        partition = which_partition(value, centers)

        cluster_cursor.put(key, pickle.dumps(partition))
    
    data_cursor.first()
    cluster_cursor.first()


def update_centers(data_cursor, cluster_cursor, k):
    centers = []
    for i in range(k):
        value_sum   = (0, 0, 0, 0)
        value_count = 0

        cluster_cursor.first()
        it_cluster = cluster_cursor.iternext(keys=True, values=True)

        for key, cluster in it_cluster:
            if pickle.loads(cluster) == i:
                value = pickle.loads(data_cursor.get(key))
                value_sum = tuple(map(operator.add, value_sum, value))
                value_count += 1
            
        center_i = tuple(map(lambda x: x/value_count, value_sum))
        centers.append(center_i)

    data_cursor.first()
    cluster_cursor.first()

    return centers

                
def k_means(data_cursor, cluster_cursor, k, eps = 0.00001, itermax = 20):
    data_cursor.first()
    cluster_cursor.first()
    
    # chose center
    centers      = get_k_rand_values(data_cursor, k)
    centers_diff = None

    iteration = 0

    while centers_diff is None or centers_diff > eps:
        
        # mise à jour des partitions
        update_cluster(data_cursor, cluster_cursor, centers)

        # mise à jour des centres
        centers_old = centers
        centers     = update_centers(data_cursor, cluster_cursor, k)

        centers_diff = sum(map(dist, centers, centers_old))

        iteration += 1
        print("iteration n°", iteration)

        if iteration == itermax:
            print("Nombre maximal d'iterrations atteind.")
            break

    return centers


## Algo

size =  1000000000 # os.path.getsize(path)*10

# données
data_env = lmdb.open('data')

data_txn = data_env.begin()
data_cursor = data_txn.cursor()

# partitions

cluster_env = lmdb.open('cluster', map_size = size)

cluster_txn = cluster_env.begin(write=True)
cluster_cursor = cluster_txn.cursor()


# algo

k_means(data_cursor, cluster_cursor, 4)

print(cluster_cursor.first())

data_env.close()
cluster_env.close()



# cluster_env = lmdb.open('data')

# cluster_txn = cluster_env.begin()
# cluster_cursor = cluster_txn.cursor()

# cluster_cursor.first()
# it = cluster_cursor.iternext()
# for k, v in it:
#     print("toto")


rand
