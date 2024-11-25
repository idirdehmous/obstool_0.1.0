import os ,sys  
sys.path.insert(  0, "./modules" )
from   ctypes     import cdll , CDLL

# Pyodb modules 
from pyodb_extra.environment  import OdbEnv  
from pyodb_extra.parser       import StringParser 


odb_install_dir=os.getenv( "ODB_INSTALL_DIR" )
env= OdbEnv(odb_install_dir, "libodb.so")
env.InitEnv ()


from pyodb  import   odbFetch
from pyodb  import   odbDca


# obstool modules
from build_sql   import SqlHandler  

sql=SqlHandler()





class OdbCCMA:
    def __init__(self):
        return None 

    def FetchByObstype(self, **kwarg):

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

        nfunc , sql_query = sql.CheckQuery( self.query   )  
        
        rows=  odbFetch(self.path  , 
                        sql_query  , 
                        nfunc      , 
                        self.queryfile , 
                        self.pool , 
                        self.fmt_float , 
                        self.verbose , 
                        self.header  )
        

        return rows 




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

        rows=  odbFetch(self.path  ,
                        sql_query  ,
                        nfunc      ,
                        self.queryfile ,
                        self.pool ,
                        self.fmt_float ,
                        self.verbose ,
                        self.header  )

        return rows 
