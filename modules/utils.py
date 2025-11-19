# -*-coding :utf-8 -*- 
import os , sys  , gc  
import pandas as pd 
import numpy  as np 
from   itertools   import product 
from   collections import defaultdict  

sys.path.insert(0,"/home/idehmous/Desktop/rmib_dev/github/pyodb_1.1.0/build/lib.linux-x86_64-cpython-39")

# pyodb modules 
from pyodb_extra.odb_ob    import  OdbObject  
from pyodb_extra           import  OdbEnv


env= OdbEnv ("/home/idehmous/Desktop/rmib_dev/github/pkg", "libodb.so")
env.InitEnv ()

# --> NOW pyodb could be imported  !
import pyodb   
from pyodb   import  odbDict , odbGcdistance
from pyodb   import  odbConnect , odbClose 
from pyodb   import  odbDca  


# Obstool, Desroziers & Jarvinen Tools modules 
from build_sql       import SqlHandler
from obstype_info    import ObsType
from setting         import Setting  , Conv
from handle_df       import *





__all__=["DCAFiles", "OdbReader", "Rows2Df"]




class DCAFiles:
    """
    Class :  @Handles, checks and creates the DCA (Direct Access Column )files 
             if they are not already in the ODB 

             @If the DCA files are already there , this class is not called 
    """
    def __init__(self):
        return None

    def CheckDca( self,   dbpath , sub_base=None   , verbose = False   ):
        # Prepare DCA files if not  in ODB 
        db      = OdbObject ( dbpath )
        dbname  = db.GetAttrib()["name"]
        
        if not os.path.isdir ("/".join(  [dbpath , "dca"] )  ):
           if verbose in [2,3 ]:
              print( "No DCA files in {} 'directory'".format(dbpath ) )
           env.OdbVars["CCMA.IOASSIGN"]="/".join(  (dbname, "CCMA.IOASSIGN" ) )
           env.OdbVars["ECMA.IOASSIGN"]="/".join(  (dbname, "ECMA.IOASSIGN" ) )
           status =    odbDca ( dbpath=dbpath , db=dbname , ncpu=8  )
           if status < 0 :
               print("Failed to create DCA files ... \n Another attempt will be done with odbDict !" )
        else :
           if  verbose ==True :
               print("DCA files already in database: '{}'".format( dbname )  )
           else:
               pass 







class OdbReader:
    """
    Class : @Prepare the query according to the user setting (Obs list , period etc)
            The SQL query is sent directly to the ECMA or CCMA ODB .

            @Returns data are as python dictionary 
    """
    def __init__(self , dbpath ,type_  ):
       # Path to the directory containing the ODB(s)
       self.odb_path = dbpath    
       self.odb_type = type_

       if  not os.path.isdir (self.odb_path): 
           print("ODB(s) directory '{}' not found.".format( self.odb_path ))  
           sys.exit(0)

       # Setting 
       self.st    = Setting ()
        
       # DCA Files 
       self.dca_f = DCAFiles()

       # CONV 
       self.conv      = Conv()
       self.conv_obs  = self.conv.conv_obs
       self.cols      = self.conv.cols 
       self.tables    = self.conv.tables
       self.other_sql = self.conv.other_sql
    
       # SQL      
       self.sql   =SqlHandler()

       # DF 
       self.rd    =Rows2Df () 
       self.cnt   =ConcatDf() 

       # return a list of dicts
       self.dlist = defaultdict(list)




    def get_odb_rows  (self,     period      ,      
                                 obs_list    ,
                                 max_dist    ,
                                 cycle_inc  =3, 
                                 chunk_size = None , 
                                 pbar       =False , 
                                 verbosity  =0):
        vrb=verbosity
        if vrb not in [0,1,2,3]:
           print("Min and max verbosity levels: 0 ->  3.  Got :  ", vrb )
           print("Fallback to default value:  verbosity=", vrb  )  


        # Default dict  (Collect dataframes with variable as a key )
        df_vars =  defaultdict(list)

        # Diags period 
        period_interval=  [ min( period ) , max(period)  ]

        # ODB  Paths 
        paths =  [ "/".join( (self.odb_path , prd , self.odb_type )) for prd in period  ]

        # Obs attributes 
        obs_names , codetype , obstype , varno , lrange , vertco ,sensor =self.st.set_obs_list(obs_list  )
        for i, cma_path in enumerate(paths):
            # UPDATE IOASSIGN , ODB_SRCPATH & ODB_DATAPATH
            os.environ["IOASSIGN"]=cma_path+"/IOASSIGN"
            os.environ["ODB_SRCPATH_CCMA"] =cma_path
            os.environ["ODB_DATAPATH_CCMA"]=cma_path
            os.environ["ODB_IDXPATH_CCMA" ]=cma_path

            # Check DCA directory  (if not there they will be created )
            dca_f=DCAFiles()
            dca_f.CheckDca ( cma_path  )

            if vrb == [ 2 , 3]:
               print("ODB PATHS set to  :")
               print("IOASSIGN          :",cma_path+"IOASSIGN" )
               print("ODB_SRCPATH_CCMA  :",cma_path )
               print("ODB_DATAPATH_CCMA :",cma_path )
               print("ODB_IDXPATH_CCMA  :",cma_path )
            
            for jo, obs in enumerate(obs_names) :
                query=self.sql.BuildQuery(       columns       =self.cols      ,
                                                 tables        =self.tables    ,
                                                 obs           =obs            ,
                                                 obstype       =obstype  [jo]  ,
                                                 obsvano       =varno    [jo]  , 
                                                 codetype      =codetype [jo]  ,                                                  
                                                 lrange        =lrange   [jo]  ,
                                                 vertco_type   ="height"       ,
                                                 vertco        =vertco   [jo]  , 
                                                 sensor        =sensor   [jo]  ,
                                                 remaining_sql =self.other_sql )

                


                nfunc , sql_query = self.sql.CheckQuery( query)
                cdtg =  period [i]
                if vrb in [1, 2, 3]:
                   print( "Process observation type {} ODB date {} ".format( obs , cdtg   ))                       
                   query_file=None   ;
                   poolmask = None   ; 
                   pool      =None   ; 
                   float_fmt = 10    ;   # 10 digits float values  
                   verbose = False 

                   # Progress bar & Verbosity inside pyodb  
                   # Progress bar is very useful for huge ODBs
                   if vrb in [3 ] : verbose= True 
                   if vrb in [1,2]: pbar   = False 

                   try:
                      rows=   odbDict (cma_path  ,
                                       sql_query , 
                                       nfunc     , 
                                       float_fmt , 
                                       query_file, 
                                       poolmask  , 
                                       pbar      ,
                                       verbose )
                   
                      # Build a DF with latlon distances                     
                      df_dist=  self.rd.DfDist ( rows ,  max_dist    )

                      # Subset  <= max_dist  
                      spl_df =  SplitDf ( df_dist  , obs , cdtg  )
                      sub_df =  spl_df.SubsetDf ()
                   
                      self.dlist[obs].append( sub_df  )  

                      del rows 
                      gc.collect()    # Memory garbage ! 
                   except:
                      pyodb.EmptyResultError
                      print( "Warning : SQL request {} returned no data for variable {}".format(query, obs )  )
                      # Append empty DF  
                      self.dlist[obs].append(pd.DataFrame[{}])
                      pass  
        return self.dlist  









class Rows2Df :

    """
    Class:   @Build the dataframes with O-G and O-A departures products. 
             @To speed up the computation of the distances between the 
              latlon pairs , the Great Circle distance function has been 
              wrapped in the C  inside pyodb  module.   

             @The returned DF is a subset of the whole 1st one such as dist<= max_dist
    """

    def __init__(self):
        return None 



    def DfDist (self ,rows , max_dist , verbosity =0 ):
        vrb=verbosity
        if vrb not in [0,1,2,3]:
            print("WARNING : Min and max verbosity levels: 0 ->  3.  Got :  ", vrb )
            print("Fallback to default value:  verbosity=", vrb  ) 

        if rows is None:
           print("Rows from ODB not available for  var :  {}".format(var ) )
           sys.exit()
        else:
           lats= rows["degrees(lat)" ] 
           lons= rows["degrees(lon)" ] 
           an_d= rows["an_depar@body"] 
           fg_d= rows["fg_depar@body"] 

           # ARGS order  :  lons1, lats1, lon2, lat2 : Compute distances between latlon pairs 
           dist      =  odbGcdistance ( lons, lats, lons, lats  )
           dist_1d   =  dist.reshape  ( len(lats)**2            )
           
           # Indices
           d1, d2 = zip(*product(range(len(lats)), repeat=2))

           # data
           an1, an2 = zip(*product(an_d, repeat=2))
           fg1, fg2 = zip(*product(fg_d, repeat=2))

           #  Attach  distances and departures 
           df_dist= pd.DataFrame( {"d1"  :d1  ,
                                   "d2"  :d2  ,
                                   "dist":dist_1d, 
                                   "OA1" :an1 , 
                                   "OA2" :an2 , 
                                   "FG1" :fg2 , 
                                   "FG2" :fg1} 
                                   )
          
           df_dist_ =  df_dist [df_dist["dist"] <= max_dist ]
           return df_dist_ 

    def PrepDf   (self , dlist ):                         
        # Concat dfs by  vars  
        cnt=  ConcatDf ()
        cdf=  cnt.ConcatByDate (dlist)
        return cdf   




