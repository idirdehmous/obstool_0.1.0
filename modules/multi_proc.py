# -*- coding: utf-8 -*-
import  os
import  sys
import  re 
import  numpy        as np 
from    itertools    import *
import  multiprocessing  as mp 
import  resource  

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



from build_sql   import  SqlHandler
from obstype_gen import  ObsType 
from dist_matrix import  gcDistance 




class MpRequest:
    def __init__(self, dbpath , sql_query , varobs ):
        self.dbpath = dbpath 
        self.query  = sql_query
        self.varobs = varobs 
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


                                                  

    def Flatten(self, list_):
        "Flatten one level of nesting."
        return chain.from_iterable(list_)



    def Chunk( self, lst , chunk_size   ):
        if len(lst)    > chunk_size  and chunk_size >=2 :   # nchunks should be 2 at least
           return [lst[i:i+chunk_size] for i in range(0, len(lst) , int(chunk_size ) )]
        else:
           return lst


  
    def Seqno (self  ):
        # Get the rows sequence number in ODB 
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


    def SelectBySeqno   (self ,   sq1, sq2 ):
        os.environ["ODB_MAXHANDLE"]= "200"
        #print( "Process id {}  ".format(  mp.current_process())  , "seqno rows range :",  sq1, sq2  )
        pp=os.getpid() 
        print("Get ODB rows by seqno. {} to {}  process: {}".format( sq1, sq2 , pp  ) )
        nfunc    = 2
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
        return  rows 


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

       # Run in prallel pools (of processes not ODB pools !!!)
       if nchunk >1    and  nproc  > 1 :   # Parallel
          with mp.Pool(processes=pool_size) as _pool:
               out = _pool.starmap(self.SelectBySeqno  ,[ (seq[0], seq[1]) for  seq in sq_sets]    )
       rows=list( self.Flatten(out ) )  
       self.ClosePool(_pool)
       return rows 

    def ClosePool (self, pool ):

        pool.close()
        pool.join()
        del pool 
        return None 







class GatherRows():
      def __init__(self):
          self.gc = gcDistance ()
          return None 


      def Rows2Array(self, rows ):
          lats= [ row[4]   for row in rows   ]
          lons= [ row[5]   for row in rows   ]
          an_d= [ row[9 ]  for row in rows   ]
          fg_d= [ row[10]  for row in rows   ]

          # Build a numpy array 
          idx=[]
          [  idx.append(i)   for i in  product(range(len(rows)) , repeat=2) ]
          d1=[ i[0] for i in idx ]
          d2=[ i[1] for i in idx ]

          an1=[i[0] for i in product(an_d , repeat=2)    ]
          an2=[i[1] for i in product(an_d , repeat=2)    ]

          fg1=[i[0] for i in product(fg_d , repeat=2)    ]
          fg2=[i[1] for i in product(fg_d , repeat=2)    ]

          # Matrix distances 
          latlon=np.array( [lats,lons] ).T
          dist=self.gc.ComputeDistances ( latlon  , chunk_size=10  )

          gcdist=list(dist.reshape(len(lats)**2 ) )

          data=[d1,d2, gcdist,  an2 , an1 , fg2, fg1  ]
          data_arr = np.array( data  ).T
          return  data_arr 





