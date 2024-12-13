# -*- coding: utf-8 -*-
import  os
import  sys
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

from build_sql import    SqlHandler



class MpRequest:
    def __init__(self, dbpath , sql_query ):
        self.dbpath = dbpath 
        self.query=sql_query

        return None 

    def ParallelEnv(self):
        ncpu      = mp.cpu_count()  
        th_info   = sys.thread_info[1]

        if th_info == "semaphore":
           pass 
#        else:
#       SET ANOTHER LOCK FOR MULTI threading !
        if resource.getrlimit(resource.RLIMIT_CPU) == -1:
           pass 
        else:
           resource.setrlimit(resource.RLIMIT_CPU, ( -1, -1) )
        return ncpu  , th_info  


    def Chunk( self, lst , chunk_size   ):
        if len(lst)    >  int( chunk_size ):
           return [lst[i:i+chunk_size] for i in range(0, len(lst) , int(chunk_size ) )]
        elif len(lst)   <= int(chunk_size):
           return lst

  
    def Seqno (self, obstype=None , varno=None ,verbose=False,  header=False , progress=True ):
        # Get the seqno numbers of the rows entries  
        # This query is static 
        sql_seqno ="SELECT seqno, entryno  from hdr,body where obstype=="+str(obstype)+" and varno=="+str(varno)+" ORDER BY seqno"
        nfunc=0
        query_file=None 
        pool      =None 
        float_fmt =None

        seq_rows  =odbFetch( self.dbpath ,sql_seqno  , nfunc  ,query_file, pool,float_fmt ,progress, verbose, header)
        seqno     =[item [0]   for item in seq_rows ]
        entryno   =[item [1]   for item in seq_rows ]
        return seqno , entryno


    def SelectBySeqno   (self, sq1,sq2  ):
        os.environ["ODB_MAXHANDLE"]= "200"
#        print( "Process id {}  ".format(  mp.current_process())  , sq1, sq2  )
#        print( "Extract ODB data from {} to {}  ".format( sq1, sq2 )     )
        idx     =[]
        dates   =[]
        nfunc    =0
        query_file=None 
        pool     = None   
        float_fmt= None 
        progress = True            
        verbose  = False   
        header   = False 

   
        seqno_cond =" AND seqno >="+str(sq1) +" AND "+" seqno < "+str(sq2)
        query=self.query+" "+seqno_cond 

        rows=odbFetch(self.dbpath, 
                      self.query, 
                       nfunc,
                 query_file, 
                      pool , 
                 float_fmt ,
                  progress , 
                   verbose , 
                    header  
                    )

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




    def DispatchQuery(self , **kwargs):
       obstype  =kwargs["obstype" ]
       varno    =kwargs["varno"   ] 
       nchunk   =kwargs["nchunks" ]
       nproc    =kwargs["nproc"   ]


       
       # N proc pools 
       # Set default parameters 
       pool_size=nproc 
       ncpu     =self.ParallelEnv()[0]

       if kwargs["nchunks"] != None: self.ncpu      =ncpu
       if kwargs["nproc" ]  != None: self.pool_size =ncpu 

       # Split into chunks 
       seq,_  = self.Seqno (  obstype , varno , False,  False ,   True   )
       seqno_chunks=self.Chunk(  seq, nchunk    )
       
       sq_sets    =[]
       for sq in seqno_chunks:
           if isinstance ( sq , list ):
              sq_sets.append( [sq[0], sq[-1]]   )

       if len( sq_sets )==0:
           sq_sets.append(min(seqno_chunks) )
           sq_sets.append( max(seqno_chunks) )
      

       # Run in prallel pools (of processes not ODB pools !!!)
       with mp.Pool(processes=pool_size) as pool:
            out = pool.starmap(self.SelectBySeqno  ,  [(  seq[0], seq[1]) for  seq in sq_sets]    )

       return out 

