#-*-coding: utf-8 -*-
import numpy as np 
import pandas as pd 
from pyproj import CRS, Transformer
from sklearn.metrics.pairwise import haversine_distances




# COORDS SHOULD MUST BR IN WGS84 CRS 
wgs84_crs = CRS("EPSG:4326")
transformer = Transformer.from_crs(wgs84_crs, wgs84_crs, always_xy=True)

# COMPUTE THE DISTANCES BETWEEN THE departures PAIRS !
def DupList(l_):
    dlist=  [  j for j in l_  for i in   l_  ]
    return dlist


def ReshapeList( l_ ):
    slist =  [[ j for j in l_ ] for i in l_ ]
    l0    =  []
    for _  in slist :
        l0=l0+ _
    return l0


def Haversine(lon1, lat1, lon2, lat2) :   
    """Calculate the great circle distance between two points
    on the earth (specified in decimal degrees)
    All args must be of equal length."""
    # Radius of Earth in kilometers
    R = 6371.0
    phi1 = np.radians(lat1)
    phi2 = np.radians(lat2)
    delta_phi = np.radians(lat2 - lat1)
    delta_lambda = np.radians(lon2 - lon1)
    a = np.sin(delta_phi / 2)**2 + np.cos(phi1) * np.cos(phi2) * np.sin(delta_lambda / 2)**2
    c = 2 * np.arctan2(np.sqrt(a), np.sqrt(1 - a))
    distance = R * c
    return distance



def MatrixDist ( lons , lats , varname ):
    """
    Method : Compute distances between pairs of coordinates
             using haversine formula !

    """
    R    =6371000
    alpha=R/1000.

    xy=[  np.radians( [transformer.transform(lon, lat)[0],transformer.transform(lon, lat)[1] ][::-1]) for lon , lat in zip( lons , lats   )  ]
    print("Computing distances between coordinates pairs. obs parameter {}...".format(varname )    )
    matdist= haversine_distances (  xy  )*alpha 
    return matdist 


"""def MatrixDist (lons , lats ):
       # USING scipy --> very slow !!!
    coord=[]
    for lon , lat in zip ( lons ,lats ):
       coord.append([transformer.transform(lon, lat)[0],transformer.transform(lon, lat)[1] ]   )
    matdist =cdist(coord, coord, metric=lambda u, v:    Haversine(u[0], u[1], v[0], v[1]))
    for i in range(matdist.shape[0]):
        for j in range(matdist.shape[0]):
            print( matdist[i,j]) 
    return  matdist"""

