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
from multi_proc           import MpRequest



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
        nchunks=self.nchunk
        nproc  =self.nproc 

        rows=mq.DispatchQuery( nchunk=nchunks, nproc=nproc)
        return rows 






    def GetByVarobs(  self , rows , dobs   ):
        # row columns & index as fetched from ODB

        """# 'obstype@hdr',          0
        # 'codetype@hdr',            1
        # 'statid@hdr',              2
        # 'varno@body',              3
        # 'degrees(lat)',            4
        # 'degrees(lon)',            5
        # 'vertco_reference_1@body', 6
        # 'date@hdr',                7
        # 'time@hdr',                8
        # 'an_depar@body',           9
        # 'fg_depar@body',           10"""

        obs_name =dobs["obs_name"].lower()
        code     =dobs["codetype"]
        varno    =dobs["varno"   ]
        stats    =defaultdict(list)
        lats     =defaultdict(list)
        lons     =defaultdict(list)
        an_depar =defaultdict(list)
        fg_depar =defaultdict(list)


        # Build a key based on varno to collect the  coords and departures  !
        # START WITH varno !
        if varno != None:
           if isinstance (varno , int ):
              vkey = obs_name+"_"+str(var_dict[varno])
              st_  = [row[2 ]   for row in rows  if  int(row[3]) ==varno ]
              lat_ = [row[4 ]   for row in rows  if  int(row[3]) ==varno ]
              lon_ = [row[5 ]   for row in rows  if  int(row[3]) ==varno ]
              an_  = [row[9 ]   for row in rows  if  int(row[3]) ==varno ]
              fg_  = [row[10]   for row in rows  if  int(row[3]) ==varno ]
              if len(lat_)!=0  and len(lon_)!=0  and len(an_)!=0 and len(fg_)!=0:
                 stats[vkey]    = st_
                 lats [vkey]    = lat_
                 lons [vkey]    = lon_
                 an_depar[vkey] = an_
                 fg_depar[vkey] = fg_
              else:
                 print( "WARNING : Function returned empty arrays for varno : " , varno  )
              return  stats,  lats , lons, an_depar , fg_depar


           elif isinstance ( varno, list ):
              for v in varno:
                  st_  = [row[2 ]   for row in rows  if  int(row[3]) ==v ]
                  lat_ = [row[4 ]   for row in rows  if  int(row[3]) ==v ]
                  lon_ = [row[5 ]   for row in rows  if  int(row[3]) ==v ]
                  an_  = [row[9 ]   for row in rows  if  int(row[3]) ==v ]
                  fg_  = [row[10]   for row in rows  if  int(row[3]) ==v ]
                  k=obs_name+"_"+str(var_dict[v])
                  if len(lat_)!=0  and len(lon_)!=0  and len(an_)!=0 and len(fg_)!=0:
                     stats   [k] = st_
                     lats    [k] = lat_
                     lons    [k] = lon_
                     an_depar[k] = an_
                     fg_depar[k] = fg_
                  else:
                     print("WARNING: Function returned empty arrays for varno : " , v )
              return  stats,  lats , lons, an_depar , fg_depar


        # No varno provided , varno =None 
        # Build the key based on codetype 
        elif  varno ==None and code != None:            
            if isinstance ( code, int   ):
               ckey = obs_name+"_c"+str(code)
               st_  = [row[2 ]   for row in rows if int (row[1]) == code ]
               lat_ = [row[4 ]   for row in rows if int (row[1]) == code ]
               lon_ = [row[5 ]   for row in rows if int (row[1]) == code ]
               an_  = [row[9 ]   for row in rows if int (row[1]) == code ]
               fg_  = [row[10]   for row in rows if int (row[1]) == code ]
               if len(lat_)!=0  and len(lon_)!=0  and len(an_)!=0 and len(fg_)!=0:
                  stats    [ckey]  = st_
                  lats     [ckey]  = lat_
                  lons     [ckey]  = lon_
                  an_depar [ckey]  = an_
                  fg_depar [ckey]  = fg_
               else:
                  print( "WARNING: Function returned empty arrays for codetype: " , code )
               return  stats,  lats , lons, an_depar , fg_depar
           
            elif isinstance ( code,  list  ):
                for c in code:              
                    ckey=obs_name+"_c"+str(c  )
                    print( ckey ) 
                    st_  = [row[2 ]   for row in rows  if  int(row[1]) ==c ]
                    lat_ = [row[4 ]   for row in rows  if  int(row[1]) ==c ]
                    lon_ = [row[5 ]   for row in rows  if  int(row[1]) ==c ]
                    an_  = [row[9 ]   for row in rows  if  int(row[1]) ==c ]
                    fg_  = [row[10]   for row in rows  if  int(row[1]) ==c ]
                    if len(lat_)!=0  and len(lon_)!=0  and len(an_)!=0 and len(fg_)!=0:
                       stats    [ckey]  = st_
                       lats     [ckey]  = lat_
                       lons     [ckey]  = lon_
                       an_depar [ckey]  = an_
                       fg_depar [ckey]  = fg_
                    else:
                        print( "WARNING: Function returned empty arrays for codetype : " , c )
                return  stats,  lats , lons, an_depar , fg_depar
        elif varno == None and code == None:
             # Build dictionnary lists keys based on the obsname 
             key=obs_name 
             st_  = [row[2 ]   for row in rows  ]
             lat_ = [row[4 ]   for row in rows  ]
             lon_ = [row[5 ]   for row in rows  ]
             an_  = [row[9 ]   for row in rows  ]
             fg_  = [row[10]   for row in rows  ]
             if len(lat_)!=0  and len(lon_)!=0  and len(an_)!=0 and len(fg_)!=0:
                stats    [key]  = st_
                lats     [key]  = lat_
                lons     [key]  = lon_
                an_depar [key]  = an_
                fg_depar [key]  = fg_
             else:
                print( "WARNING: Function returned empty arrays for obs type : " , obs_name )
             return  stats,  lats , lons, an_depar , fg_depar

            




    def Rows2Bins(self, stats , 
                         lats , 
                         lons , 
                         an_depar , 
                         fg_depar , 
                         varobs,
                         cdtg ,     bin_max_dist=100 , bin_int=10 ):
        """
        Method: Split the distance/departures matrix into 
                bins with bins intrvals  
                default : bin_int          = 10  Km 
                        : maximum distance = 100 Km
        """
        list_df=[]
        df_dict=defaultdict(list)
        for k , v in stats.items():         
            if k in varobs:  
                vvar= [ k for i in  range(len(stats[k] )) ] 
                stat= stats[k]
                #llat= np.asarray(lats[k] , dtype='float32' )
                #llon= np.asarray(lons[k] , dtype='float32' )
                #an_d= np.asarray(an_depar[k], dtype='float32')
                #fg_d= np.asarray(fg_depar[k], dtype='float32')
                llat= lats[k]
                llon= lons[k]
                an_d= an_depar[k]
                fg_d= fg_depar[k]

                matdist =    MatrixDist( llon, llat  )
                idx=[]
                [  idx.append(i)   for i in  product(range(len(llon)) , repeat=2) ]
                d1=[ i[0] for i in idx ]
                d2=[ i[1] for i in idx ]

                
                # ADD varname 
                var=repeat (k , len(llon))
                lvar=[ v for v in var   ]

                # ADD DATE/CYCLE
                #date_list =self.SetDate (cdtg , len(matdist)  ) # or d2 

                # Swap d1 and d2 to match the same indices in R (  idx -1 )
                dfdist = DataFrame(  {
                                      "d1"  : d2 , 
                                      "d2"  : d1 ,
                                      "dist": matdist  }) 

                dfdist['d1']   = dfdist['d1'].astype('uint32')
                dfdist['d2']   = dfdist['d2'].astype('uint32')
                dfdist['dist'] = dfdist['dist'].astype('float32')
                
                del matdist  
                del d1
                del d2 
                # SPLIT DF 
                ndist_df=  dfdist.query("dist <=  "+str(bin_max_dist) )
                 

                # ADD DATE/CYCLE  
                #date_list =self.SetDate (cdtg , len(stat)  ) 
              
                #OTHER DATA 
                data_df = DataFrame(   {
                                         "statid"  :stat,      
                                         "lat"     :llat,
                                         "lon"     :llon , 
                                         "an_depar":an_d,
                                         "fg_depar":fg_d})
       
                
                # COPY THE DF , AVOID PANDAS WARNING. WORKING ON A SLICED DataFrame
                ndf=ndist_df.copy()
                ndf.loc[:, 'OA1'] = data_df.loc[ndf['d1'], 'an_depar'].values.astype('float32')
                ndf.loc[:, 'OA2'] = data_df.loc[ndf['d2'], 'an_depar'].values.astype('float32')
                ndf.loc[:, 'FG1'] = data_df.loc[ndf['d1'], 'fg_depar'].values.astype('float32')
                ndf.loc[:, 'FG2'] = data_df.loc[ndf['d2'], 'fg_depar'].values.astype('float32')

                # Binning
                lDint = list(np.arange(bin_int, bin_max_dist +bin_int , bin_int ))
                cDint = list(np.arange(bin_int, bin_max_dist +bin_int , bin_int))

                # Partitions over bins inplace !
                # Binning by  bin_int  Km 
                dbin   =[0,1]+lDint
                dlabel =[0  ]+cDint
                dbin_serie   =cut(  ndf['dist'], bins=dbin , labels=dlabel, right=True, include_lowest=True, retbins=True )
                ndf["dbin"] =dbin_serie[0]
                df_dict[k].append( ndf  )
        return df_dict







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
