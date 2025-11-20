import os , sys 
import numpy as np 
import multiprocessing as mp
from multiprocessing import shared_memory



sys.path.insert(0,"/home/idehmous/Desktop/rmib_dev/github/pyodb_1.1.0/build/lib.linux-x86_64-cpython-39")

from pyodb_extra  import OdbEnv
env= OdbEnv ("/home/idehmous/Desktop/rmib_dev/github/pkg", "libodb.so")
env.InitEnv ()

from pyodb import odbGcdistance



def gcdist_chunk_worker_shm(i0, i1, N):
    """
    Worker shared memory version.
    Only receives i0, i1 and global N.
    Accesses shared memory arrays directly.
    """

    # Retrieve shared memories
    shm_lon = shared_memory.SharedMemory(name="shm_lons")
    shm_lat = shared_memory.SharedMemory(name="shm_lats")

    # Recreate NumPy arrays from shared memory
    lon = np.ndarray((N,), dtype=np.float64, buffer=shm_lon.buf)
    lat = np.ndarray((N,), dtype=np.float64, buffer=shm_lat.buf)

    # Slice chunk
    sub_lon = lon[i0:i1]
    sub_lat = lat[i0:i1]

    # Call your C function
    block = odbGcdistance(sub_lon, sub_lat, lon, lat)

    # Close local handles (but do NOT unlink)
    shm_lon.close()
    shm_lat.close()

    return (i0, i1, block)




class DistMatrix:
    def __init__(self, lons, lats):
        self.lons = np.asarray(lons, dtype=np.float64)
        self.lats = np.asarray(lats, dtype=np.float64)
        self.N = len(self.lons)

    def GcdistParallel(self, chunk_size=200, workers=None):

        N = self.N
        lon = self.lons
        lat = self.lats

        if workers is None:
            workers = mp.cpu_count()

        # ------------------------------
        # Allocate Shared Memory buffers
        # ------------------------------
        shm_lons = shared_memory.SharedMemory(create=True, size=lon.nbytes, name="shm_lons")
        shm_lats = shared_memory.SharedMemory(create=True, size=lat.nbytes, name="shm_lats")

        # Copy data once
        np.ndarray(lon.shape, dtype=np.float64, buffer=shm_lons.buf)[:] = lon
        np.ndarray(lat.shape, dtype=np.float64, buffer=shm_lats.buf)[:] = lat

        # Prepare chunk ranges
        chunk_ranges = [(i0, min(i0 + chunk_size, N)) 
                        for i0 in range(0, N, chunk_size)]

        distmat = np.zeros((N, N), dtype=np.float64)

        try:
            with mp.Pool(processes=workers) as pool:
                
                # starmap only sends small ints: (i0, i1, N)
                results = pool.starmap(
                    gcdist_chunk_worker_shm,
                    [(i0, i1, N) for (i0, i1) in chunk_ranges]
                )

            # Fill the final matrix
            for (i0, i1, block) in results:
                distmat[i0:i1] = block

        finally:
            # Cleanup shared memory
            shm_lons.close()
            shm_lons.unlink()

            shm_lats.close()
            shm_lats.unlink()

        return distmat
