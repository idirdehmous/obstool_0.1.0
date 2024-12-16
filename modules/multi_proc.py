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


class MpRequest:
    def __init__(self, dbpath , sql_query , varobs ):
        self.dbpath = dbpath 
        self.query  = sql_query
        self.varobs = varobs 

        self.name_varno=[ (item.split("_")[0], item.split("_")[1])  for item in self.varobs ]
        types            = ObsType ()
        _  ,  self.varno_dict = types.ObsDict()

        self.novar_dict= {}    # The reversed varno dict 
        for k , v in self.varno_dict.items():  self.novar_dict[v]=k 

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



    def AlterQuery ( self, sql_string ):
        """
        Remove orginal select statement and replece 
        with seqno and entryno and ORDER them by seqno  
        """
        rr=sql_string.lower().split()                       # BE SURE THAT THE QUERY IS IN LOWER CASE
        rj=" ".join(rr).partition('from')            # JOIN EVERYTHING AND USE "from" KEYWORD AS SEPARATOR
        from_token = rj[1:]                          # THE SELECT STETEMENT IS AT INDEX 0

        obstype_pattern =r"obstype\s*==\s*(\d+)"
        varno_pattern   =r"varno\s*==\s*(\d+)"

        var_found  =re.search (obstype_pattern, from_token[1])
        obst_found =re.search (varno_pattern, from_token[1])
        if var_found  and   obst_found:
           new_sql="SELECT seqno ,entryno FROM   "+from_token[1] + "  ORDER BY  seqno"
           print (new_sql )
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
        nfunc=0 
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


    def SelectBySeqno   (self, sq1,sq2  ):
        os.environ["ODB_MAXHANDLE"]= "200"
        print( "Process id {}  ".format(  mp.current_process())  , "seqno rows range :",  sq1, sq2  )
#        print( "Extract ODB data from {} to {}  ".format( sq1, sq2 )     )
        idx     =[]
        dates   =[]
        nfunc    =0
        query_file=None 
        pool     = None   
        
        float_fmt= None 
        progress = False 
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
        keys=[]    # 
        lats=[]
        lons=[]
        an_d=[]
        fg_d=[]
        name= self.name_varno[0][0]
        for row in rows:            

            key=name+"_"+self.varno_dict[row[3] ] 
            if key  not in keys:
               keys.append(key ) 
            lats.append(row[4])
            lons.append(row[5])
            an_d.append(row[9 ] )
            fg_d.append(row[10] )

        # Compute distances matrix  
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




    def DispatchQuery(self , nchunk=4 , nproc=4 ):
       
       # N proc pools 
       # Set default parameters 
       ncpu     =self.ParallelEnv()[0]
       pool_size=nproc 

       # Split into chunks 
       seq , _  = self.Seqno ()
       seqno_chunks=self.Chunk(  seq, nchunk    )

       seqno_chunks=[ [207726 , 207976] ]
       print( seqno_chunks ) 
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
           with mp.Pool(processes=pool_size) as pool:
                out = pool.starmap(self.SelectBySeqno  ,  [(  seq[0], seq[1]) for  seq in sq_sets]    )
                return out 

       if is_sublist ==True and nproc == 1  :   # Process chunks in sequential  !
          for ll in sq_sets:
              out = self.SelectBySeqno ( ll [0] , ll[-1]   )
              return out

       elif is_sublist ==False and  nproc==1:   # Simple list and cpu ==1    #
              out=self.SelectBySeqno ( sq_sets[0] , sq_sets[1] )
              return out 
       elif sublist ==False and nproc !=1:
           print("Can't process one seqno chunk on multiple CPU(s)  ,NOT IMPLEMENTED YET !" )
           sys.exit (0)

