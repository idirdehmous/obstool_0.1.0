#-*-coding: utf-8 -*-
import numpy as np 
from pyproj import CRS, Transformer
from scipy.spatial.distance import cdist


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


def MatrixDist (lons , lats ):

    """ DISTANCES ARE COMPUTED WITH heversine method . THE ACCURACY DIFFERS
        FROM R (spDists   IMPLEMENTS DeMeeus method )   difference = +/- 50m
        IF RESULTS DIFFERENTS TEST spDist "C" source code.
    """

    coord=[]
    for lon , lat in zip ( lons ,lats ):

        coord.append([transformer.transform(lon, lat)[0],transformer.transform(lon, lat)[1] ]   )
    matdist =cdist(coord, coord, metric=lambda u, v:    Haversine(u[0], u[1], v[0], v[1]))
    return  matdist

