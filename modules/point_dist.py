import os , sys
from ctypes import cdll

import numpy as np
from numpy import radians , degrees
import pandas as pd
#from point_dist    import gcDistance 
#import numba 
#from numba import jit , njit , prange , float32
#from math import sin , cos , atan , sqrt , isnan
sys.path.insert(0, "/hpcperm/cvah/tuning/ww_oslo/pyodb_1.1.0/build/lib.linux-x86_64-cpython-310" )

# Pyodb modules
from pyodb_extra.environment  import OdbEnv

odb_install_dir=os.getenv( "ODB_INSTALL_DIR" )
env= OdbEnv(odb_install_dir, "libodb.so")
env.InitEnv ()


from pyodb import gcDist

def MatrixDist ( lons , lats , varname ):
    print(  "Computing matrix distances for parameter {} ...".format( varname) )
    n=len(lons)
    mat=gcDist ( list(lons),list(lats) ,int( n)   )
    print( "Done " ) 
    matdist= np.asarray( mat, dtype="float32"  )
    del mat 
    return matdist 




