import os ,sys  
sys.path.insert(  0, "./modules" )
from   ctypes     import cdll , CDLL
from collections  import defaultdict  
import numpy as np

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
from point_dist  import MatrixDist 



sql=SqlHandler()





class OdbCCMA:
    def __init__(self):
        return None 

    def FetchByObstype(self, **kwarg):

        # ARGUMENT TO SEND TO ODB   (C Code )
        args=["dbpath"    , 
              "sql_query" , 
              "sqlfile"   ,
              "pools"     , 
              "fmt_float" , 
              "verbose"   , 
              "get_header", 
              "progress_bar",
              "return_rows"]
        
        kargs=[] ; kvals=[]
        for k , v in   kwarg.items(): 
            if k in args:
               self.path     =kwarg["dbpath"]
               self.query    =kwarg["sql_query" ]
               self.queryfile=kwarg["sqlfile"   ]    
               self.pool     =kwarg["pools"     ]
               self.verbose  =kwarg["verbose"   ]
               self.header   =kwarg["get_header"]
               self.pbar     =kwarg["progress_bar"]
               self.rrows    =kwarg["return_rows"]
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
                        self.pbar ,
                        self.verbose , 
                        self.header  )
        
        if self.rrows:
           return rows 
        else:
           _self.rows=rows 
           return _self.rows 


    def Latlon2Bins(self, stats , lats , lons , an_depar , fg_depar , bin_max_dist=100 , bin_int=10 ):

        """
        Method: Split the distance/departures matrix into 
                bins with bins intrvals  
                default : bin_int = 10      Km 
                        : maximum distance = 100 Km

        """
        matdist=pdist.MatrixDist( lons , lats    )
        d1=[]
        d2=[]
        for i in range(matdist.shape[0]):
              for j in range(matdist.shape[1]):
                  d1.append(i)
                  d2.append(j)
        # Swap d1 and d2 to match the same indices in R (  idx -1 )
        dfdist = DataFrame(  {"d1"  : d2 , 
                              "d2"  : d1 ,
                              "dist":matdist.reshape(matdist.shape[0]*matdist.shape[1]) 
                              } )

        # SPLIT DF 
        ndist_df=  dfdist.query("dist <=  "+str(bin_max_dist) )
        
        #OTHER DATA 
        data_df = DataFrame(   { "statid"  :stats,      
                                 "lat"     :lats,
                                 "lon"     :lons , 
                                 "an_depar":an_depar ,
                                 "fg_depar":fg_depar}  )


        # COPY THE DF , AVOID PANDAS WARNING. WORKING ON A SLICED DataFrame
        ndf=ndist_df.copy()
        ndf.loc[:, 'OA1'] = data_df.loc[ndf['d1'], 'an_depar'].values
        ndf.loc[:, 'OA2'] = data_df.loc[ndf['d2'], 'an_depar'].values
        ndf.loc[:, 'FG1'] = data_df.loc[ndf['d1'], 'fg_depar'].values
        ndf.loc[:, 'FG2'] = data_df.loc[ndf['d2'], 'fg_depar'].values

        """# Binning
        lDint = list(np.arange(bin_int, bin_max_dist +bin_int , bin_int ))
        cDint = list(np.arange(bin_int, bin_max_dist +bin_int , bin_int))

        # Partitions over bins inplace !
        # Binning by  bin_int  Km 
        dbin   =[0,1]+lDint
        dlabel =[0  ]+cDint
        dbin_serie   =cut(  ndf['dist'], bins=dbin , labels=dlabel, right=True, include_lowest=True, retbins=True )
        ndf["dbin"] =dbin_serie[0]
        #ds_dict[obs_name].append(ndf  )
        return  ndf """



    def GetByVarno(  self , rows , dobs   ): 
        # 'obstype@hdr',             0
        # 'codetype@hdr',            1
        # 'statid@hdr',              2
        # 'varno@body',              3
        # 'degrees(lat)',            4
        # 'degrees(lon)',            5
        # 'vertco_reference_1@body', 6
        # 'date@hdr',                7
        # 'time@hdr',                8
        # 'an_depar@body',           9
        # 'fg_depar@body',           10

        obs_name = dobs["obs_name"].lower()
        code    = dobs["codetype"]
        varno   = dobs["varno"   ]
        stats   =defaultdict(list)
        lats    =defaultdict(list)
        lons    =defaultdict(list)
        an_depar=defaultdict(list)
        fg_depar=defaultdict(list)



        if varno != None:
           if isinstance (varno , int ):            
              int_var =varno   
              key  =obs_name+"_"+str(int_var) 
              for row in rows: 
                  stats[key].append   ( row[2]  )    # 2 --> statid 
                  lats[key].append    ( row[4]  )    # 4 --> lat
                  lons[key].append    ( row[5]  )    # 5 --> lon
                  an_depar[key].append( row[9]  )    # 9 --> an_dep
                  fg_depar[key].append( row[10] )    # 10--> fg_dep 
           elif isinstance ( varno, list ):
              lst_var =varno  
              keys=[ obs_name+"_v"+str(v)  for  v   in lst_var     ]


              for k in keys:
                  vkey=int( k[-1:]   )
                  st_  = [row[2 ]   for row in rows  if  int(row[3]) ==vkey ]
                  lat_ = [row[4 ]   for row in rows  if  int(row[3]) ==vkey ] 
                  lon_ = [row[5 ]   for row in rows  if  int(row[3]) ==vkey ]
                  an_  = [row[9 ]   for row in rows  if  int(row[3]) ==vkey ]
                  fg_  = [row[10]   for row in rows  if  int(row[3]) ==vkey ]

                  stats[k]= st_
                  lats[k] = lat_
                  lons[k] = lon_
                  an_depar[k]=an_
                  fg_depar[k]=fg_
              
           else:
              print("WARNING : Unknown type of varno ", type(varno )  )
        return  stats,  lats , lons, an_depar , fg_depar






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
