# -*- coding :utf-8 -*- 
import os ,sys  
from   ctypes        import cdll , CDLL
from   pandas        import DataFrame , cut 
from   collections   import defaultdict  
import pandas as pd 
import numpy  as np
from   datetime      import datetime
from   itertools     import  *
import re 
import gc  
import logging 

sys.path.insert(0,"/hpcperm/cvah/tuning/pkg/src/pyodb_1.1.0/lib/python3.12/site-packages")


# Pyodb modules 
from pyodb_extra.environment  import OdbEnv  
from pyodb_extra.parser       import StringParser 


odb_install_dir=os.getenv( "ODB_INSTALL_DIR" )
if odb_install_dir is None:
   print("--The path to the libodb.so location is missing :  export  ODB_INSTALL_DIR=/path/to/../lib " )
   sys.exit()
env= OdbEnv(odb_install_dir, "libodb.so")
env.InitEnv ()

from pyodb  import   odbFetch , odbDict 
from pyodb  import   odbDca



# OBSTOOL MODULES
from build_sql             import SqlHandler  
from multi_proc            import MpRequest 
from obstype_info          import ObsType 


class OdbCCMA:
    __slots__ = [ 'types'    , 'varno_dict',
                 'd_io'     , 'varobs',    'path'   ,'query' ,
                 'queryfile', 'pool'  ,    'verbose','header',
                 'pbar'     , 'header',    'rrows'  ,'nchunk',
                 'cdtg'  ,  'fmt_float' , 'verb']

    def __init__(self):
        
        #self.d_io = DataIO()
        return None 

    def FetchByObstype(self, **kwarg):
        # ARGUMENT TO SEND, GET ROWS 
        args=["varobs"    ,
              "dbpath"    , 
              "sql_query" , 
              "sqlfile"   ,
              "pools"     , 
              "fmt_float" , 
              "get_header", 
              "progress_bar" , 
              "return_rows",
              "nchunk"    ,
#              "nproc"     ,
              "datetime"  ,
              "verbosity"   ]
        
        kargs=[] ; kvals=[]
        for k , v in   kwarg.items(): 
            if k in args:
               #self.obs_kind =kwarg["obs_kind"]
               self.varobs   =kwarg["varobs"  ]
               self.path     =kwarg["dbpath"  ]
               self.query    =kwarg["sql_query" ]
               self.queryfile=kwarg["sqlfile"   ]    
               self.pool     =kwarg["pools"     ]
               self.header   =kwarg["get_header"]
               self.pbar     =kwarg["progress_bar"]
               self.rrows    =kwarg["return_rows"]
               self.nchunk   =kwarg["nchunk"    ]
#               self.nproc    =kwarg["nproc"     ]
               self.cdtg     =kwarg["datetime"]
               self.verb     =kwarg["verbosity"]
               self.fmt_float=None 
        
            else:
              print("Unexpected argument :" , k)
        
        self.types           = ObsType ()
        _ , self.varno_dict  = self.types.ConvDict() 

        # IF SAT IS ADDED LATER ...
        #print("Unknown observation category. Possible values: 'conv' or 'satem'")
        #sys.exit(0)
    
        # SQL 
        sql=SqlHandler()
        nfunc , sql_query = sql.CheckQuery(self.query)  
        if self.verb == 2:
           print("Sending query {} ".format( sql_query  ) )
        query_file=None
        pool     = None
        float_fmt= None
        progress = True 
        verbose  = False
        header   = False

        rows=odbDict( self.path,
                       sql_query,
                       nfunc,
                       query_file,
                           pool  ,
                       float_fmt ,
                        progress ,
                         verbose ,
                         header     )

        df  = pd.DataFrame(rows) 
        # THE  DATA PIPLE STOPS HERE FOR THE MOMENT 
        #return df  
        return None 


   



# THIS  PART WAS FOR THE pyodb WHEN RETURNING A LIST OF LISTS 
# TO BE ADAPTED TO A DICT
class GatherRows:
      __slots__ = [ 'gcd', 'tp', 'varno', 'novar', 'obs_dict', 'sensor']
      def __init__(self  ):
          self.gcd =gcDistance ()

          self.tp =ObsType ()
          self.obs_dict ,self.varno =self.tp.ConvDict()
          # Reverse the dict 
          self.novar = { v:k for k,v in self.varno.items() }
          d_io  =DataIO ()
          return None 


   
      def Np2Df (self , cdtg ,  data_arr_gen  ) :
          df_list=[]
          colnames=[ "d1","d2","dist" , "OA1", "OA2" , "FG1", "FG2"]
          for d in data_arr_gen  : 
              for var  , values in d.items():
                  df_data = pd.DataFrame( values  , columns = colnames )
                  var_col =[]
                  dte_col =[]
                  var_col.append(   [var      for i   in range(len(df_data)) ] )
                  dte_col.append(   [cdtg     for i   in range(len(df_data)) ] )
                  df_data ["var" ] =var_col[0] 
                  df_data ["date"] =dte_col[0]
          return {var:df_data }



      def Rows2Array(self , db_path , cdtg , obs_kind , var  ):
          if df_data  is not None:
             lats= [ row  for row in df_data["lats"]  ]
             lons= [ row  for row in df_data["lons"]  ]
             an_d= [ row  for row in df_data["an_d"]  ]
             fg_d= [ row  for row in df_data["fg_d"]  ]
             # Build a numpy array 
             idx=[]
             [  idx.append(i)   for i in  product(range(len(lats)) , repeat=2) ]
             d1=[ i[0] for i in idx ]
             d2=[ i[1] for i in idx ]

             an1=[i[0] for i in product(an_d , repeat=2)    ]
             an2=[i[1] for i in product(an_d , repeat=2)    ]
             fg1=[i[0] for i in product(fg_d , repeat=2)    ]
             fg2=[i[1] for i in product(fg_d , repeat=2)    ]


             # Matrix distances  (Great circle distances )
             latlon=np.array( [lats,lons] ).T
             dist=np.array(list(self.gcd.ComputeDistances ( cdtg , var , latlon    , chunk_size=50 ) )     )
             gcdist=list(dist.reshape(len(lats)**2 ) )

             data=[d1,d2, gcdist,  an2 , an1 , fg2, fg1  ]
             data_arr = np.array( data  ).T

             var_col =[]
             dte_col =[]
             var_col.append(   [var       for i   in range(len(d1)) ] )
             dte_col.append(   [cdtg      for i   in range(len(d1)) ] )

             colnames=[ "d1","d2","dist" , "OA1", "OA2" , "FG1", "FG2"]
             df_data = pd.DataFrame( data_arr  , columns = colnames )
             df_data ["var" ] =var_col[0] 
             df_data ["date"] =dte_col[0]

             

if __name__ == '__main__':
    # __main__  GUARD statement !
    # Set up logging configuration for debugging
    logging.basicConfig(level=logging.DEBUG)
    caller = gcDistance ()     
    caller.ComputeDistances()
