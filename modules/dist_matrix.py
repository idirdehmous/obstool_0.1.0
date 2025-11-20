#-*- coding :utf-8 -*- 
import os  , sys 
import multiprocessing as mp 
import numpy           as np 
#import haversine  as hav 
import gc   

sys.path.insert(0,"/home/idehmous/Desktop/rmib_dev/github/pyodb_1.1.0/build/lib.linux-x86_64-cpython-39")

from pyodb_extra  import OdbEnv 
env= OdbEnv ("/home/idehmous/Desktop/rmib_dev/github/pkg", "libodb.so")
env.InitEnv ()

from pyodb import odbGcdistance 




def gcdist_chunk_worker(full_latlon, i0, i1):
    lat = full_latlon[:,0]
    lon = full_latlon[:,1]

    sub_lat = lat[i0:i1]
    sub_lon = lon[i0:i1]

    block = odbGcdistance(sub_lon, sub_lat, lon, lat)

    return i0, i1, block




class gcDistance:
    def __init__(self):
        pass



    def ComputeDistances(self,  latlon, chunk_size=10, workers=None):
        #if latlon is None or len(latlon) == 0:
        #    print(f"WARNING: No data found  ")
        #    return None

        #if workers is None:
        #print(  mp.cpu_count())

        workers=16
        N = len(latlon[:,0])
        
        dist = np.zeros((N, N), dtype=np.float64)

        # Extract arrays
        lat = latlon[:, 0]
        lon = latlon[:, 1]

        chunk_size= 400

         
        with mp.Pool(processes=workers) as pool:

            tasks = []
            for i0 in range(0, N, chunk_size):
                i1 = min(i0 + chunk_size, N)
                tasks.append(pool.apply_async(gcdist_chunk_worker, (latlon, i0, i1, )))

            # Récupération
            for t in tasks:
                i0, i1, block = t.get()
                dist[i0:i1, :] = block

        return dist
