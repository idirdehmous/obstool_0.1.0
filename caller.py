import os,sys
from   ctypes import cdll , CDLL
from   pandas import DataFrame , cut 
import numpy as np
from   datetime import datetime ,timedelta
from   pyproj import CRS, Transformer
from   collections import defaultdict 
import matplotlib.pyplot as plt
import pandas as pd 



# pyodb_extra  MODULES 
sys.path.insert(0,"./modules" )
sys.path.insert(1,"/hpcperm/cvah/tuning/ww_oslo/pyodb_1.1.0/build/lib.linux-x86_64-cpython-310")
from pyodb_extra.odb_glossary import  OdbLexic
from pyodb_extra.parser       import  StringParser
from pyodb_extra.environment  import  OdbEnv
from pyodb_extra.odb_ob       import  OdbObject
from pyodb_extra.exceptions   import  *
from pyodb_extra.pool_factory import  PoolSlicer




# ADDED MODULES FOR obstool !
from dca             import DCAFiles
from build_sql       import SqlHandler 
from cma_rows        import OdbCCMA , OdbECMA
from obstype_gen     import ObsType
from conv_stats      import DHLStat , CleanDf 
from rows2df         import DfFromRows
from handle_df       import SplitDf ,ConcatDf  ,GroupDf


# TEMPORARY MODULES
import  plot_stats 



# EXPORT THE DIRECTORY CONTAINING libodb.so 
# ( ONLY THE TOP DIRECTORY    /path/to/../../   bin , include , lib etc  )

odb_install_dir=os.getenv( "ODB_INSTALL_DIR" )
env= OdbEnv(odb_install_dir, "libodb.so")
env.InitEnv ()

# NOW pyodb CAN BE IMPORTED 
from pyodb  import   odbFetch
from pyodb  import   odbDca


from pyodb import odbVarno 



StartTime = datetime.now()


# PATH: contains   YYYYMMDDHH/CCMA 
odb_path="/mnt/HDS_ALD_TEAM/ALD_TEAM/idehmous/depot_tampon/METCOOP_ODB"

# PERIOD DATES 
bdate="2024010500"
edate="2024010500"

# CYCLE inc 
cycle_inc=3 

# CREATE DATE TIME LIST
period=[]
bdate =datetime.strptime( bdate , "%Y%m%d%H")
edate =datetime.strptime( edate , "%Y%m%d%H")
delta =timedelta(hours=int(cycle_inc))
while bdate <= edate:
      strdate=bdate.strftime("%Y%m%d%H")
      period.append( strdate )
      bdate += delta


# What we need as obstype   ?
#          CONV 
# GNSS         
# SYNOP  TYPE  : 11=surface,14=surface+automatic, 21=ships,24=ships+automatic,  varno : 39=t2m, 58=rh2m,41=u10m, 42=v10m, 1=z
# RADAR  varno :  q=29 , DOW=195
# AIREP   ALL  : t=2  , u=3 , v=4 
#     + LEVELS : vertco_reference_1:  >=25000   <=35000 (T, U , V )
# DRIBU varno  : z=1 , 42=v10m  , 41=u10m   , 39=t2m
# TEMP varno   : t=2 , u=3 , v=4 , q=7
# LEVELS       : vertco_reference_1:  >=40000     <=60000
#         SATEM 
# NAME    :sensor 
# AMSUA   : 3
# AMSUB   : 4
# MHS     :15 
# IASI    :16  
# ATMS    :19  
# MWHS    :73  
# SEVIRI  :29 

# CAUTION !: NEVER CHANGE THE ORDER OF COLUMNS 
# TO ADD NEW ONEs , ONE CAN JUST DO AN APPEND !

# obstype , codetype ,statid , varno , lat, lon, vertco_reference_1,date,time, an_depar, fg_depar,obsvalue
# col index shifted by 1 (to match the column No in original bash script )
cols      =[ "",
             "obstype"                 ,  # 1
             "codetype"                ,  # 2
             "statid"                  ,  # 3
             "varno"                   ,  # 4
             "degrees(lat)"            ,  # 5
             "degrees(lon)"            ,  # 6 
             "vertco_reference_1"      ,  # 7
             "date"                    ,  # 8
             "time"                    ,  # 9
             "an_depar"                ,  # 10
             "fg_depar"                ,  # 11
             "obsvalue"                ,  # 12
             ]
# Considered tables & additional sql statement 
# export ODB_CONSIDER_TABLES="/hdr/desc/body"  
tables        = ["hdr","desc","body"]
tbl_env       = "/".join( tables  )
other_sql     = "(an_depar is not NULL) AND (fg_depar is not NULL)"

        
obstype=[ 'gpssol' ,
          'synop'  ,
          'dribu'  ,
          'airep'  ,
          'airepl' ,
          'radar'  ,
          'temp'   ,
          'templ'  ,
          'amsua'  ,
          'amsub'  ,
          'atms'   ,
          'iasi'   ,
          'mwhs'   ,
          'msh'    ,
          'seviri' ]


obstype=['airep']

# THESE LINES WILL BE REPLACED BY ObstType dictionnary INSTANCE
# BUILD OBS DICTS 
types       = ObsType ()
obs_dict, _ = types.ObsDict()

obs_list =[]
for obs in obs_dict:
    if obs["obs_name"] in obstype:
       obs_list.append( obs )

# Obs + codetype or varno ( used as keys  for data tracking  )
varobs        = types.SelectObs (obs_list )

# 1- Get data from ccma( equivalence in bash script  -> "get_ccma=yes"  )
# INSTANTIATE 
ccma=OdbCCMA()
sql=SqlHandler () 



bin_max_dist=100 
bin_int     =10 

# Gather stats by enumetation ! 
#ds_dict=defaultdict(list)



pd.set_option('display.max_rows',30)

all_data =defaultdict(list)

#for cdtg in period:
#    for dobs in obs_list:


def ProcessData(  cdtg , obs_dict   ):
    # CCMA PATH 
    ccma_path ="/".join( [odb_path , cdtg  , "CCMA"] ) 

    # Check DCA directory  (if not there they will be created )
    dca_f=DCAFiles()
    dca_f.CheckDca ( ccma_path  )

    # If level range requiered or not 
    llev=False 
    lev_range= dobs["level_range"]
    if lev_range != None :
       llev=True 

    # BUILD & CHECK sql query 
    query=sql.BuildQuery(       columns       =cols      ,
                                tables        =tables    ,
                                obs_dict      =dobs      ,
                                has_levels    =llev      ,
                                vertco_type   ="height"  ,   
                                remaining_sql =other_sql )
   
    print ("ODB date         :" ,   cdtg  ) 
    print ("Getting rows for :", dobs["obs_name"] ,  "...")
        
    # UPDATE ODB_SRCPATH & ODB_DATAPATH 
    os.environ["IOASSIGN"]=ccma_path+"/IOASSIGN"
    os.environ["ODB_SRCPATH_CCMA"] =ccma_path
    os.environ["ODB_DATAPATH_CCMA"]=ccma_path


    # SEND query & GET ROWS 
    data_arr =ccma.FetchByObstype ( varobs    = varobs      , 
                                     dbpath      =ccma_path , 
                                     sql_query  =query      ,
                                     sqlfile     = None     ,
                                     pools       = None     ,
                                     progress_bar=False     , 
                                     verbose     = False    ,
                                     get_header =False      ,                                 
                                     return_rows =True      ,
                                     nchunk      = 2        , 
                                     nproc       = 2     )

    # Return data_array as DF 
    colnames=[ "d1","d2","dist" , "OA1", "OA2" , "FG1", "FG2"]
    df_data = pd.DataFrame( data_arr, columns = colnames )
    var_col =[]
    dte_col =[]
    var_col.append(   [dobs["obs_name"]   for i in range(len(df_data)) ] )
    dte_col.append(   [cdtg   for i in range(len(df_data)) ] )
    df_data ["var" ] =var_col[0] 
    df_data ["date"] =dte_col[0]

    return  df_data 

df_list=[]
for cdtg in period:
    for dobs in obs_list:
        df_data =ProcessData (cdtg  , dobs )
        df_list.append(df_data)

new_max_dist=100  # Km
new_bin_dist=10   # Km
delta_t     =60   # Time interval between two obs in [ min ]


rdf=DfFromRows()

for df in df_list:
    # Subset df 
    df_dist  = df_data.query("dist <=  "+str(bin_max_dist) )
    cdf      = rdf.CutDf(    df_dist    ,   bin_max_dist , bin_int )
    sp     = SplitDf    (cdf)
    sub_df = sp.SubsetDf(   )

    stat=DHLStat (  sub_df  , new_max_dist , new_bin_dist , delta_t )
    print( stat.getStatFrame ()  )
    plot_stats.PlotDf  ( stat.getStatFrame ()  , "airep_v" )





EndTime = datetime.now()
Duration=EndTime - StartTime
print( "Duration\n" , Duration  )


quit()

