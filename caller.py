import os,sys
from   ctypes import cdll , CDLL
from   pandas import DataFrame , cut 
import numpy as np
from   datetime import datetime ,timedelta
from   pyproj import CRS, Transformer
from   collections import defaultdict 
import matplotlib.pyplot as plt




# pyodb_extra  MODULES 
sys.path.insert(0,"./modules" )
#sys.path.insert(1,"/hpcperm/cvah/tuning/ww_oslo/pyodb_1.1.0/build/lib.linux-x86_64-cpython-310")
from pyodb_extra.odb_glossary import  OdbLexic
from pyodb_extra.parser       import  StringParser
from pyodb_extra.environment  import  OdbEnv
from pyodb_extra.odb_ob       import  OdbObject
from pyodb_extra.exceptions   import  *
from pyodb_extra.pool_factory import  PoolSlicer

# ADDED MODULES FOR obstool !
from dca             import DCAFiles
from build_sql       import SqlHandler 
from get_rows        import OdbCCMA , OdbECMA
from obstype_gen     import ObsType
import point_dist as pdist 
from conv_stats      import DHLStat 


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




StartTime = datetime.now()


# PATH: contains   YYYYMMDDHH/CCMA 
odb_path="/hpcperm/cvah/tuning/odbs"

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



# NEEDS CCMA ONLY 
# sql columns 
ccma_keys=["obs_name","obstype" ,"codetype"        ,"varno"        ,"vertco_reference_1" ,"sensor", "level_range"]
# obs attributes 
# CONV
gnss      =["gpssol"    , 1        ,110            ,128            , None     ,None    ,None ]
synop     =["synop"     , 1        ,[11,14,170,182],None           , None     ,None    ,None ]
synop_var =["synopv"    , 1        ,[11,14,170,182],[1,42,41,58,39], None     ,None    ,None ]
dribu     =["dribu"     , 4        ,None           ,[1,39,41,42]   , None     ,None    ,None ]
ascat     =["ascat"     , 9        ,None           ,None           , None     ,None    ,None ]
radar     =["radar"     , 13       ,None           ,[29,195]       , None     ,None    ,None ]
airep     =["airep"     , 2        ,None           ,[2,3,4]        , None     ,None    ,None ]
airep_l   =["airepl"    , 2        ,None           ,[2,3,4]        , None     ,None    ,[25000,35000] ]
temp      =["temp"      , 5        ,None           ,[2,3,4,7]      , None     ,None    ,None ]
temp_l    =["templ"     , 5        ,None           ,[2,3,4,7]      , None     ,None    ,[40000,60000] ]

# SAT 
amsua   =["amsua"       , 7        ,None           ,None           , None     ,3       ,None]
amsub   =["amsub"       , 7        ,None           ,None           , None     ,4       ,None]
mhs     =["msh"         , 7        ,None           ,None           , None     ,15      ,None]
iasi    =["iasi"        , 7        ,None           ,None           , None     ,16      ,None]
atms    =["atms"        , 7        ,None           ,None           , None     ,19      ,None]
mwhs    =["mwhs"        , 7        ,None           ,None           , None     ,73      ,None]
seviri  =["seviri"      , 7        ,None           ,None           , None     ,29      ,None]

# Needed columns FOR obstool :
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




# CONV 
#obs_list=[{'obs_name': 'gpssol', 'obstype': 1, 'codetype': 110, 'varno': 128, 'vertco_reference_1': None, 'sensor': None, 'level_range': None},]                      # TESTED 
#obs_list=[{ "obs_name" : "synop","obstype" : 1  ,"codetype": [11, 14, 170, 182] ,"varno": None,"vertco_reference_1": None,"sensor": None,"level_range": None},]       # TESTED ( Maybe not needed in obstool ) 
#obs_list=[{'obs_name': 'synop_v', 'obstype': 1, 'codetype': [11, 14, 170, 182], 'varno': [1, 42, 41, 58, 39], 'vertco_reference_1': None, 'sensor': None, 'level_range': None},]  # TESTED 
#obs_list=[{'obs_name': 'dribu', 'obstype': 4, 'codetype': None, 'varno': [1, 39, 41, 42], 'vertco_reference_1': None, 'sensor': None, 'level_range': None},]          # TESTED gives NAN , few obs 
#obs_list=[{'obs_name': 'ascat', 'obstype': 9, 'codetype': None, 'varno': None, 'vertco_reference_1': None, 'sensor': None, 'level_range': None},]                     # TESTED 
#obs_list=[{'obs_name': 'airep', 'obstype': 2, 'codetype': None, 'varno': [2, 3, 4], 'vertco_reference_1': None, 'sensor': None, 'level_range': None} ,]               # TESTED 
#obs_list=[{'obs_name': 'airep_l', 'obstype': 2, 'codetype': None, 'varno': [2, 3, 4], 'vertco_reference_1': None, 'sensor': None, 'level_range': [25000, 35000]} ,]   # TESTED give NAN  , few obs 
#obs_list =[{'obs_name': 'radar', 'obstype': 13, 'codetype': None, 'varno': [29, 195], 'vertco_reference_1': None, 'sensor': None, 'level_range': None},]              # TESTED 
#obs_list=[{'obs_name': 'temp', 'obstype': 5, 'codetype': None, 'varno': [2, 3, 4, 7], 'vertco_reference_1': None, 'sensor': None, 'level_range': None}, ]             # TESTED 
#obs_list=[{'obs_name': 'temp_l', 'obstype': 5, 'codetype': None, 'varno': [2, 3, 4, 7], 'vertco_reference_1': None, 'sensor': None, 'level_range': [40000, 60000]} ,] # TESTED 


# SAT 
#obs_list=[{'obs_name': 'amsua', 'obstype': 7, 'codetype': None, 'varno': None, 'vertco_reference_1': None, 'sensor': 3, 'level_range': None},]    # TESTED 
#obs_list=[{'obs_name': 'amsub', 'obstype': 7, 'codetype': None, 'varno': None, 'vertco_reference_1': None, 'sensor': 4, 'level_range': None},]     
#obs_list=[{'obs_name': 'msh', 'obstype': 7, 'codetype': None, 'varno': None, 'vertco_reference_1': None, 'sensor': '15 ', 'level_range': None},]   # TESTED 

#obs_list=[{'obs_name': 'iasi', 'obstype': 7, 'codetype': None, 'varno': None, 'vertco_reference_1': None, 'sensor': 16, 'level_range': None},]
#obs_list=[{'obs_name': 'atms', 'obstype': 7, 'codetype': None, 'varno': None, 'vertco_reference_1': None, 'sensor': 19, 'level_range': None},]
#obs_list=[{'obs_name': 'mwhs', 'obstype': 7, 'codetype': None, 'varno': None, 'vertco_reference_1': None, 'sensor': 73, 'level_range': None},]


#obs_list=[{'obs_name': 'seviri', 'obstype': 7, 'codetype': None, 'varno': None, 'vertco_reference_1': None, 'sensor': 29, 'level_range': None} ,]  # TESTED 


obstype  =['gpssol' ,
          'synop_v',
          'dribu'  ,
          'airep'  ,
          'airep_l',
          'radar'  ,
          'temp'   ,
          'temp_l' ,
          'amsua'  ,
          'amsub'  ,
          'atms'   ,
          'iasi'   ,
          'mwhs'   ,
          'msh'    ,
          'seviri' ]

obstype=['radar']

# THESE LINES WILL BE REPLACED BY ObstType dictionnary INSTANCE
# BUILD OBS DICTS 
type_         = ObsType ()
obs_dict      = type_.ObsDict()

obs_list =[]
for obs in obs_dict:
    if obs["obs_name"] in obstype:
       obs_list.append( obs )


# Obs + codetype or varno ( used as keys  for data handling )
varobs        = type_.SelectObs (obs_list )

# 1- Get data from ccma( equivalence in bash script  -> "get_ccma=yes"  )
# INSTANTIATE 
ccma=OdbCCMA()
sql=SqlHandler () 



bin_max_dist=100 
bin_int     =10 

# Gather stats by enumetation ! 
#ds_dict=defaultdict(list)

for cdtg in period:
    for dobs in obs_list:
        
        # CCMA PATH 
        ccma_path ="/".join( [odb_path , cdtg  , "CCMA"] ) 

        # Check DCA directory  (if not there they will be created )
        df=DCAFiles()
        df.CheckDca ( ccma_path  )

        # If level range requiered or not 
        llev=False 
        lev_range= dobs["level_range"]
        if lev_range != None :
           llev=True 

        # BUILD & CHECK sql query 
        query=sql.BuildQuery(  columns   =cols     ,
                           tables        =tables   ,
                           obs_dict      =dobs     ,
                           has_levels    =llev     ,
                           vertco_type   ="height" ,   
                           remaining_sql=other_sql )

    
        print ("ODB date         :" ,   cdtg  ) 
        print ("Getting rows for :", dobs["obs_name"] ,  "...")
        
        # UPDATE ODB_SRCPATH & ODB_DATAPATH 
        os.environ["IOASSIGN"]=ccma_path+"/IOASSIGN"
        os.environ["ODB_SRCPATH_CCMA"] =ccma_path
        os.environ["ODB_DATAPATH_CCMA"]=ccma_path


        # SEND query & GET ROWS 
        rows =ccma.FetchByObstype (dbpath   =ccma_path , 
                                   sql_query=query   ,
                                   sqlfile  = None   ,
                                   pools    = None   ,
                                   progress_bar=True , 
                                   verbose  = False  ,
                                   get_header=False  , 
                                   return_rows=True )
        # COLUMNS FOR OBSTOOL (first binning ) 
        stat , lat , lon , an_depar , fg_depar =ccma.GetByVarobs( rows , dobs  )

        list_df                                =ccma.Rows2Bins  ( stat , lat , lon , an_depar , fg_depar , varobs )
        # Desroziers /Hollingsworth-Lonnberg stats 
        # Final binning option set here!
        new_max_dist=100  # Km
        new_bin_dist=10   # Km
        delta_t     =60   # Time interval between two obs in [ min ]
        for  d in list_df:
            stat=DHLStat (  d   , new_max_dist , new_bin_dist, delta_t  )
            sdf=stat.getStatFrame ()
            print( sdf) 
            plot_stats.PlotDf  (   sdf   , "radar_29" )
       #     del sdf
    #del rows 
    #del list_df 

EndTime = datetime.now()
Duration=EndTime - StartTime
print( "Duration\n" , Duration  )


quit()

