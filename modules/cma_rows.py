import os ,sys  
sys.path.insert(  0, "./modules" )
from   ctypes     import cdll , CDLL
from   pandas     import DataFrame , cut 
from   collections  import defaultdict  
import pandas as pd 
import numpy as np
from datetime import datetime
from itertools import  *
import re 

sys.path.insert(0, "/hpcperm/cvah/tuning/ww_oslo/pyodb_1.1.0/build/lib.linux-x86_64-cpython-310" )


# Pyodb modules 
from pyodb_extra.environment  import OdbEnv  
from pyodb_extra.parser       import StringParser 


odb_install_dir=os.getenv( "ODB_INSTALL_DIR" )
env= OdbEnv(odb_install_dir, "libodb.so")
env.InitEnv ()


from pyodb  import   odbFetch
from pyodb  import   odbDca


# obstool modules
from build_sql            import SqlHandler  
from multi_proc           import MpRequest , GatherRows



class OdbCCMA:
    def __init__(self):
        return None 


    def FetchByObstype(self, **kwarg):

        # ARGUMENT TO SEND, GET ROWS 
        args=["varobs" ,
              "dbpath"    , 
              "sql_query" , 
              "sqlfile"   ,
              "pools"     , 
              "fmt_float" , 
              "verbose"   , 
              "get_header", 
              "progress_bar" , 
              "return_rows",
              "nchunk"    ,
              "nproc"]
        
        kargs=[] ; kvals=[]
        for k , v in   kwarg.items(): 
            if k in args:
               self.varobs   =kwarg["varobs"]
               self.path     =kwarg["dbpath"]
               self.query    =kwarg["sql_query" ]
               self.queryfile=kwarg["sqlfile"   ]    
               self.pool     =kwarg["pools"     ]
               self.verbose  =kwarg["verbose"   ]
               self.header   =kwarg["get_header"]
               self.pbar     =kwarg["progress_bar"]
               self.rrows    =kwarg["return_rows"]
               self.nchunk   =kwarg["nchunk"    ]
               self.nproc    =kwarg["nproc"     ]
               self.fmt_float=None 
            else:
              print("Unexpected argument :" , k)


        sql=SqlHandler()
        nfunc , sql_query = sql.CheckQuery( self.query   )  

        mq=MpRequest (self.path ,  sql_query, self.varobs  )        
        gt=GatherRows()

        nchunks=self.nchunk
        nproc  =self.nproc 

        rows=mq.DispatchQuery( nchunk=nchunks, nproc=nproc)
        arr =gt.Rows2Array(rows ) 
        return arr  







class OdbECMA:
    def __init__(self):
        return None 
    
    def FetchByObstype( self, **kwarg ):
        # ARGUMENT TO SEND TO ODB   (C Code )
        args=["dbpath", "sql_query", "sqlfile","pools", "fmt_float", "verbose", "get_header"]

        kargs=[] ; kvals=[]
        
        for k , v in   kwarg.items():
            if k in args:
               self.path     =kwarg["dbpath"]
               self.query    =kwarg["sql_query" ]
               self.queryfile=kwarg["sqlfile"   ]
               self.pool     =kwarg["pools"     ]
               self.verbose  =kwarg["verbose"   ]
               self.header   =kwarg["get_header"]
               self.fmt_float=None
            else:
              print("Unexpected argument :" , k)

        nfunc , sql_query = CheckQuery( self.query   )

        """rows=  odbFetch(self.path  ,
                        sql_query  ,
                        nfunc      ,
                        self.queryfile ,
                        self.pool ,
                        self.fmt_float ,
                        self.verbose ,
                        self.header  )
                        
        dbpath="/hpcperm/cvah/tuning/odbs/2024010500/CCMA/"

        sql=SqlHandler()

        obstype=2
        varno  =2


         cols=[  "" ,
        "obstype" ,
         "varno"]

        tabs=["hdr, body"]
        obs_dict={'obs_name': 'airep', 'obstype': 2, 'codetype': None, 'varno': [2, 3, 4], 'vertco_reference_1': None, 'sensor': None, 'level_range': None}

        has_level=None
        vertco_type=None

        mq.DispatchQuery( dbpath=dbpath , obstype=obstype , varno=varno ,nchunks=16 ,nproc=4   )
        """


        return None 
