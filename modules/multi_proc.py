# -*- coding: utf-8 -*-
import  os
import  sys
import  re 
import  numpy        as np 
from    itertools    import *
import  multiprocessing  as mp 
import resource  

sys.path.insert(0, "/hpcperm/cvah/tuning/ww_oslo/pyodb_1.1.0/build/lib.linux-x86_64-cpython-310" )

# ODB  MODULES 
from pyodb_extra.odb_glossary import  OdbLexic
from pyodb_extra.parser       import  StringParser
from pyodb_extra.environment  import  OdbEnv
from pyodb_extra.odb_ob       import  OdbObject
from pyodb_extra.exceptions   import  *



odb_install_dir=os.getenv("ODB_INSTALL_DIR")
# INIT ENV 
env= OdbEnv(odb_install_dir , "libodb.so")
env.InitEnv ()

# --> NOW pyodb could be imported  !
from pyodb   import  odbFetch
from pyodb   import  gcDist 

from build_sql   import  SqlHandler
from obstype_gen import  ObsType 

def Flatten(list_of_items):
    "Flatten one level of nesting."
    return chain.from_iterable(list_of_items)



def haversine(lat1, lon1, lat2, lon2):
    R = 6378.137  # Radius of Earth in kilometers
    lat1, lon1, lat2, lon2 = map(np.radians, [lat1, lon1, lat2, lon2])  # Convert to radians
    dlat = lat2 - lat1
    dlon = lon2 - lon1
    a = np.sin(dlat / 2)**2 + np.cos(lat1) * np.cos(lat2) * np.sin(dlon / 2)**2
    c = 2 * np.arctan2(np.sqrt(a), np.sqrt(1 - a))
    
    # Distance in kilometers
    return R * c



def Distances_in_chunk(chunk_data, full_data, start_idx, chunk_size):
    """
    Compute great circle distances between all points in a chunk with all other points.
    Arguments:
    - chunk_data: Data for the current chunk (latitudes and longitudes).
    - full_data: Full dataset for computing distances across chunks.
    - start_idx: Starting index of the chunk in the full data.
    - chunk_size: The number of points in the chunk.
    Returns:
    - A distance matrix chunk for the given chunk.
    """
    distances_chunk = np.zeros((chunk_size, len(full_data)))
    #an_chunk=np.zeros((chunk_size, len(full_data)))
    #fg_chunk=np.zeros((chunk_size, len(full_data)))

    for i in range(chunk_size):
        lat1, lon1 = chunk_data[i]
        lat2, lon2 = full_data [:, 0] , full_data[:, 1]         # Full data lat/lon

        # The distance is computed with haversine formula 
        # In the R the great circle is computed with Vicenty's formula 
        # Could be implemented later
        # The diffrence in accuracy is just 0.01 %
        distances_chunk[i, :] = haversine(lat1, lon1, lat2, lon2 ) 

#        lat1, lon1, lat2, lon2 = map(np.radians, [lat1, lon1, lat2, lon2])
#        point1 =(lat1, lon1)
#        point2 =(lat2, lon2)
#        distances_chunk[i, :] = geodesic(point1, point2).kilometers.all()
    return start_idx, distances_chunk







def ComputeDistances(data, chunk_size=1000):
    """
    Compute the full distance matrix for all points in the data using chunks and multiprocessing.
    Arguments:
    - data: Latitudes and longitudes of points (n_samples, 2).
    - chunk_size: Number of samples to process in each chunk.
    Returns:

    - A distance matrix of shape (n_samples, n_samples).
    """
    n_samples = len(data)
    distance_matrix = np.zeros((n_samples, n_samples))

    # Split the data into chunks for parallel computation
    with mp.Pool(processes=mp.cpu_count()) as pool:
        results = []

        for start_idx in range(0, n_samples, chunk_size):
            chunk_end_idx = min(start_idx + chunk_size, n_samples)
            chunk_data    = data[start_idx:chunk_end_idx]

            # Submit task to the pool
            results.append(pool.apply_async(
                Distances_in_chunk,  (chunk_data, data, start_idx, chunk_end_idx - start_idx) ))

        # Collect all results
        for result in results:
            start_idx, distances_chunk = result.get()
            distance_matrix[start_idx:start_idx+distances_chunk.shape[0], :] = distances_chunk
    return distance_matrix





class MpRequest:
    def __init__(self, dbpath , sql_query , varobs ):
        self.dbpath = dbpath 
        self.query  = sql_query
        self.varobs = varobs 
        print( self.varobs ) 
        return None 

    def ParallelEnv(self):
        ncpu      = mp.cpu_count()  
        th_info   = sys.thread_info[1]
        if th_info == "semaphore":
           pass 
        if resource.getrlimit(resource.RLIMIT_CPU) == -1:
           pass 
        else:
           resource.setrlimit(resource.RLIMIT_CPU, ( -1, -1) )
        return ncpu  , th_info  



    def AlterQuery ( self, sql_string ):
        """
        Remove orginal select statement and replece 
        with seqno and entryno and ORDER them by seqno  
        """
        rr=sql_string.lower().split()                # BE SURE THAT THE QUERY IS IN LOWER CASE
        rj=" ".join(rr).partition('from')            # JOIN EVERYTHING AND USE "from" KEYWORD AS SEPARATOR
        from_token = rj[1:]                          # THE SELECT STETEMENT IS AT INDEX 0

        obstype_pattern =r"obstype\s*==\s*(\d+)"
        varno_pattern   =r"varno\s*==\s*(\d+)"

        var_found  =re.search (obstype_pattern, from_token[1])
        obst_found =re.search (varno_pattern, from_token[1])
        if var_found  and   obst_found:
           new_sql="SELECT seqno ,entryno FROM   "+from_token[1] + "  ORDER BY  seqno"
           return new_sql  
        else:
           return None 


    def Chunk( self, lst , chunk_size   ):
        if len(lst)    > chunk_size  and chunk_size >=2 :   # nchunks should be 2 at least
           return [lst[i:i+chunk_size] for i in range(0, len(lst) , int(chunk_size ) )]
        else:
           #len(lst)   <= int(chunk_size):
           return lst


  
    def Seqno (self  ):
        # Get the seqno numbers of the rows & entries  
        sql_seqno=  self.AlterQuery(self.query )  
        query_file=None 
        nfunc     =0 
        pool      =None 
        fmt_float =None
        progress  =True 
        verbose   =False 
        header    =False 
        try:
           seq_rows  =odbFetch( self.dbpath ,sql_seqno  , nfunc  ,query_file ,pool ,fmt_float,progress, verbose  , header)
           seqno     =[item [0]   for item in seq_rows ]
           entryno   =[item [1]   for item in seq_rows ]
           return seqno, entryno 
        except:
           Exception 
           print("Failed to fetch the number of rows in the SQL statement " )
           sys.exit (1)


    def SelectBySeqno   (self ):
        os.environ["ODB_MAXHANDLE"]= "200"
#        print( "Process id {}  ".format(  mp.current_process())  , "seqno rows range :",  sq1, sq2  )
#        print( "Extract ODB data from {} to {}  ".format( sq1, sq2 )     )
        nfunc    =0
        query_file=None 
        pool     = None   
        
        float_fmt= None 
        progress = False 
        verbose  = False   
        header   = False 
        seqno_cond =" AND seqno >="+str(sq1) +" AND "+" seqno <= "+str(sq2)
        query=self.query+" "+seqno_cond 
        rows=odbFetch(self.dbpath, 
                       query, 
                       nfunc,
                 query_file, 
                      pool , 
                 float_fmt ,
                  progress , 
                   verbose , 
                    header  
                        )

        # Handle rows 
        out_rows = [   row[:-2] for row  in rows ]
        return  out_rows 


    def DispatchQuery(self , nchunk , nproc ):
       
       # N proc pools 
       # Set default parameters 
       ncpu     =self.ParallelEnv()[0]
       pool_size=nproc 

       # Split into chunks 
       seq , _  = self.Seqno ()
       seqno_chunks=self.Chunk(  seq, nchunk    )
    
       # Reset ncpu to 1 if nchunk =1 
       if nchunk ==1 :   nproc = 1 
       sq_sets    =[]
       is_sublist =False 
       for sq in seqno_chunks:
            if isinstance ( sq , list ):
               sq_sets.append( [sq[0], sq[-1]]   )
               is_sublist =True 

       if is_sublist==False:
          if len( sq_sets )==0 or nchunk == 1:    # Simple list no sublist
              sq_sets.append(min(seqno_chunks) )
              sq_sets.append(max(seqno_chunks) )

       elif seqno_chunks ==None:
          is_sublist =False 
          print("Failed to split the seqno list into chunks " )
          sys.exit (0)

       sq_sets=[[1557, 1560], [1561, 1564] ]

       pool_size =1 
       rows  =  self.SelectBySeqno ()
       lats= [ row[4]   for row in rows   ]
       lons= [ row[5]   for row in rows   ]
       an_d= [ row[9 ]  for row in rows   ]
       fg_d= [ row[10]  for row in rows   ]


#       llat=[ p for p in  product(  [ row[4 ]    for row in rows] ,  repeat =2 )] 
#       llon=[ p for p in  product(  [ row[5 ]    for row in rows] ,  repeat =2 )]

       # Build a numpy array 
       idx=[]
       [  idx.append(i)   for i in  product(range(len(rows)) , repeat=2) ]
       d1=[ i[0] for i in idx ]
       d2=[ i[1] for i in idx ]

       #an_d=[ p for p in  product(  [ row[9 ]    for row in rows] ,  repeat =2 )]  
       #fg_d=[ p for p in  product(  [ row[10 ]   for row in rows] ,  repeat =2 )]
       an1=[i[0] for i in product(an_d , repeat=2)    ]
       an2=[i[1] for i in product(an_d , repeat=2)    ]

       fg1=[i[0] for i in product(fg_d , repeat=2)    ]
       fg2=[i[1] for i in product(fg_d , repeat=2)    ]


       latlon=np.array( [lats,lons] ).T
       
       dist=ComputeDistances ( latlon  , chunk_size=2  )

       gcdist=list(dist.reshape(len(lats)**2 ) )
       data=[d1,d2, gcdist,  an2 , an1 , fg2, fg1  ]
       data_arr = np.array( data  ).T
       return  data_arr 




