# -*- coding: utf-8 -*-
import  os
import  sys
import  ctypes 
from    ctypes import cdll , CDLL
import  pandas as pd  
from    pandas import DataFrame ,cut  

import  numpy as np 
from    itertools import *

import  multiprocessing  as mp 
import  time, random 
from    math import atan , tan , pi 


num_cpu = mp.cpu_count()  # Number of CPU cores available



# python  MODULES 
#sys.path.insert(0,"/home/idehmous/Desktop/rmib_dev/tuning/pyodb_1.1.0/build/lib.linux-x86_64-cpython-39" )
from pyodb_extra.odb_glossary import  OdbLexic
from pyodb_extra.parser       import  StringParser
from pyodb_extra.environment  import  OdbEnv
from pyodb_extra.odb_ob       import  OdbObject
from pyodb_extra.exceptions   import  *


sys.path.insert(0, "./modules")

from handle_df  import  SplitDf ,ConcatDf  ,GroupDf

from rows2df    import  DfFromRows
from conv_stats import  DHLStat 


dbpath   = sys.argv[1]



odb_install_dir=os.getenv("ODB_INSTALL_DIR")
# INIT ENV 
env= OdbEnv(odb_install_dir , "libodb.so")
env.InitEnv ()

# --> NOW pyodb could be imported  !
from pyodb   import  odbFetch
from pyodb   import  odbConnect , odbClose 
from pyodb   import  odbDca  
from pyodb   import  gcDist 





# AIREP 
OBSTYPE = "13"
VARNO   = "29"
DISTANCE= "550." # Km



db      = OdbObject ( dbpath )
db_attr = db.GetAttrib()

db_type = db_attr["type"]
db_name = db_attr["name"]


# CREATE DCA IF THEY ARN'T ALREADY THERE 
# ALWAYS EXPORT IOASSIGN FILE , OTHERWISE dcagen WILL CRASH WITOUT FINISHING !!!!!
if not os.path.isdir (dbpath+"/dca"):
   os.environ["IOASSIGN "]           =dbpath+"/IOASSIGN"
   ic=odbDca ( dbpath=dbpath, db= db_type, ncpu=8  )



# THE METHOD COULD BE CALLED WITH sql QUERY or  sql file  (ONE IS MANDATORY !)
# The arguments are positional , should be replaced by keyword args 
# args :
nfunctions=0    # (type == integer )Number of columns considring the functions in the sql statement  (degrees, rad, avg etc ...)
query_file=None     # (type == str     )The sql file if used rather than sql request string 
pool      =None     # (type == str     )The pool number (  must be a string  "2", "33"   , etc   )
float_fmt =None     # (type == str     )Number of digits for floats (same syntax  as in C  )
verbose   =False     # (type == bool    )Verbosity 
header    =False     # (type == bool    )Get the columns names 
pbar     = False 


# FUNCTIONS 
# dist(reflat, reflon, refdist_km, lat, lon) :  Given reflon,reflat and radius refdist -----> returns 1 if the lat/lon inside the circle 
# distance(lat1, lon1, lat2, lon2)           :  Compute the distance between lat1,lon1  lat2,lon2  (in meters ) 
# km(lat1, lon1, lat2, lon2)                 :  As distance but expressed in KM  




def Chunk( lst , chunk_size   ):
    if len(lst)    >  int( chunk_size ):
       return [lst[i:i+chunk_size] for i in range(0, len(lst) , int(chunk_size ) )]
    elif len(lst)   <= int(chunk_size):
       return lst 


def Flatten(list_of_items):
    "Flatten one level of nesting."
    return chain.from_iterable(list_of_items)



def RefCoords (obstype=None , varno=None ,verbose=False,  header=False , progress=True ):
    # reference coordinates 
    # This query is always static 
    sql_latlon="SELECT seqno, entryno,degrees(lat),degrees(lon), an_depar, fg_depar,obsvalue  from hdr,body where obstype=="+str(obstype)+" and varno=="+str(varno) \
               +" ORDER BY seqno"
    nfunc     =2
    ref_rows  =odbFetch(dbpath ,sql_latlon , nfunc  ,query_file, pool,float_fmt ,progress, verbose, header)
    lats=[ coord[2] for coord in  ref_rows    ]
    lons=[ coord[3] for coord in  ref_rows    ]
    seqno  =[item [0]   for item in  ref_rows ]
    entryno=[item [1]   for item in  ref_rows ]
    return lats, lons,  seqno , entryno  



def BuildQuery (  sq1 , sq2    ):
    max_distance=DISTANCE   
    obstype=OBSTYPE
    varno  =VARNO 
    tables="hdr,body"


    query="SELECT obstype,codetype,statid,varno,degrees(lat),degrees(lon ),vertco_reference_1,date,time,an_depar,fg_depar,obsvalue"\
          +"  FROM hdr, body " \
          +"  WHERE obstype=="+str(obstype)+" AND varno=="+str(varno)+"  AND seqno >= "+str(sq1)+"  AND seqno <" +str(sq2)+ " ORDER BY seqno "


    return query 



def PointSelection (  sq1 , sq2  ):
    os.environ["ODB_MAXHANDLE"]= "200"
    #print( "Process id {}  ".format(  mp.current_process())   )
    print( "Extract ODB data from {} to {}  ".format( sq1, sq2 )     )
    obstype =OBSTYPE
    varno   =VARNO 
    idx     =[]
    dates   =[]
    tables="hdr,body"
    nfunctions=2
    q=       BuildQuery ( sq1 , sq2  )    

    rows=odbFetch(dbpath , 
                        q , 
                nfunctions ,
                query_file , 
                      pool ,
                 float_fmt ,
                    False, 
                    False, 
                    False  )    

    # Handle rows 
    lats=[]
    lons=[]
    an_d=[]
    fg_d=[]
    for row in rows:
        lats.append(row[4])
        lons.append(row[5])
        an_d.append(row[9 ] )
        fg_d.append(row[10] )

    # Compute distances 
    dist=gcDist ( lons , lats , len(lats )     )


    # Build a numpy array 
    idx=[]
    [  idx.append(i)   for i in  product(range(len(rows)) , repeat=2) ]
    d1=[ i[0] for i in idx ]
    d2=[ i[1] for i in idx ]

    lat=[i[0] for i in product(lats , repeat=2)    ]
    lon=[i[1] for i in product(lons , repeat=2)    ]

    an1=[i[0] for i in product(an_d , repeat=2)    ]
    an2=[i[1] for i in product(an_d , repeat=2)    ]

    fg1=[i[0] for i in product(fg_d , repeat=2)    ]
    fg2=[i[1] for i in product(fg_d , repeat=2)    ]

    ndim=len(dist)
    if len( d1)==ndim and len(d2)==ndim and len(an1)==ndim and  len(an2)==ndim and  len(fg1)== ndim  and len(fg2)== ndim: 
       # Np array  
       data=[ d1, d2,  dist , an2, an1 , fg2, fg1]
       data_arr = np.array( data  ).T
    else:
       data_arr    =None 
    return  data_arr 





# AIREP 
zlat , zlon , seq  , ent  =  RefCoords ( 13 , 29  )

#
num_proc=len(zlat)

# N proc pools 
pool_size=16

print("Creating pool of worker processes")

# Split on chunks 
seqno_chunks=Chunk(  seq, 32 )
sq_sets    =[]
for sq in seqno_chunks:
    sq_sets.append( [sq[0], sq[-1]]   )


# Run in prallel pool 
with mp.Pool(processes=pool_size) as pool:
     out = pool.starmap(PointSelection ,  [(  seq[0], seq[1]) for  seq in sq_sets]    )



quit()
# Gather arrays 
np_arr = np.vstack(out)

print( np_arr.shape ) 
quit()

# DF WOLRD !
pd.set_option('display.max_rows',None )

bin_max_dist=100
bin_int     =10 
delta_t     = 60

colnames=[ "d1","d2","dist" , "OA1", "OA2" , "FG1", "FG2"]
df_data = pd.DataFrame(np_arr, columns = colnames )

df_data = df_data.astype({'d1'  : 'int32'  , 
                          'd2'  : 'int32'  , 
                          'dist': 'float32',
                          "OA1" : 'float64',
                          "OA2" : 'float64',
                          "FG1" : 'float64',
                          "FG2" : 'float64' })

# Subset df 
df_data     = df_data.query("dist <=  "+str(bin_max_dist) )

rdf=DfFromRows()
cdf= rdf.CutDf(    df_data    ,   bin_max_dist , bin_int )

sp     = SplitDf    (cdf)
sub_df = sp.SubsetDf(   )


# Maybe change the binning size !
new_max_dist= 100
new_bin_dist= 10
delta_t     = 60
stat=DHLStat (  sub_df  , new_max_dist , new_bin_dist , delta_t )

print( stat.getStatFrame ('airep_v' )  )



#print ( ndist_df  , sub_df) 
quit()
SubsetDf (self , max_dist ,bin_int   )
def CutDf  (self, ndf   ,  bin_max_dist , bin_int):
          lDint = list(np.arange(bin_int, bin_max_dist +bin_int , bin_int ))
          cDint = list(np.arange(bin_int, bin_max_dist +bin_int , bin_int))

          # Partitions over bins inplace !
          # Binning by  bin_int  Km 
          dbin   =[0,1]+lDint
          dlabel =[0  ]+cDint
          dbin_serie   =cut(  ndf['dist'], bins=dbin , labels=dlabel, right=True, include_lowest=True, retbins=True )
          ndf["dbin"] =dbin_serie[0] 
          return ndf  

