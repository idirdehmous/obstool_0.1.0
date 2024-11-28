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
edate="2024010521"

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

# Get obs_list as dict
"""obs_list=[   gnss, radar, synop ,synop_var,
             dribu,ascat,radar,airep,airep_l,
             temp,
            temp_l, 
            seviri ]
amsua,
amsub,
mhs,
iasi,
atms,
mwhs,
seviri"""


# THESE LINES WILL BE REPLACED BY ObstType dictionnary INSTANCE
# BUILD OBS DICTS 
type_         = ObsType ()  
obs_list      = type_.ObsDict() 
varobs        = type_.SelectObs (obs_list )   


obs_list=[
{'obs_name': 'gpssol', 'obstype': 1, 'codetype': 110, 'varno': 128, 'vertco_reference_1': None, 'sensor': None, 'level_range': None},
#{ "obs_name" : "synop","obstype" : 1  ,"codetype": [11, 14, 170, 182] ,"varno": None,"vertco_reference_1": None,"sensor": None,"level_range": None},]
{'obs_name': 'synop_v', 'obstype': 1, 'codetype': [11, 14, 170, 182], 'varno': [1, 42, 41, 58, 39], 'vertco_reference_1': None, 'sensor': None, 'level_range': None},
{'obs_name': 'dribu', 'obstype': 4, 'codetype': None, 'varno': [1, 39, 41, 42], 'vertco_reference_1': None, 'sensor': None, 'level_range': None},

#{'obs_name': 'ascat', 'obstype': 9, 'codetype': None, 'varno': None, 'vertco_reference_1': None, 'sensor': None, 'level_range': None},
{'obs_name': 'airep', 'obstype': 2, 'codetype': None, 'varno': [2, 3, 4], 'vertco_reference_1': None, 'sensor': None, 'level_range': None} ,
{'obs_name': 'airep_l', 'obstype': 2, 'codetype': None, 'varno': [2, 3, 4], 'vertco_reference_1': None, 'sensor': None, 'level_range': [25000, 35000]}]

#{'obs_name': 'radar', 'obstype': 13, 'codetype': None, 'varno': [29, 195], 'vertco_reference_1': None, 'sensor': None, 'level_range': None},]
#{'obs_name': 'temp', 'obstype': 5, 'codetype': None, 'varno': [2, 3, 4, 7], 'vertco_reference_1': None, 'sensor': None, 'level_range': None} ]
#{'obs_name': 'temp_l', 'obstype': 5, 'codetype': None, 'varno': [2, 3, 4, 7], 'vertco_reference_1': None, 'sensor': None, 'level_range': [40000, 60000]} ]
#{'obs_name': 'amsua', 'obstype': 7, 'codetype': None, 'varno': None, 'vertco_reference_1': None, 'sensor': 3, 'level_range': None}
#{'obs_name': 'amsub', 'obstype': 7, 'codetype': None, 'varno': None, 'vertco_reference_1': None, 'sensor': 4, 'level_range': None}
#{'obs_name': 'msh', 'obstype': 7, 'codetype': None, 'varno': None, 'vertco_reference_1': None, 'sensor': '15 ', 'level_range': None}
#{'obs_name': 'iasi', 'obstype': 7, 'codetype': None, 'varno': None, 'vertco_reference_1': None, 'sensor': 16, 'level_range': None}
#{'obs_name': 'atms', 'obstype': 7, 'codetype': None, 'varno': None, 'vertco_reference_1': None, 'sensor': 19, 'level_range': None}
#{'obs_name': 'mwhs', 'obstype': 7, 'codetype': None, 'varno': None, 'vertco_reference_1': None, 'sensor': 73, 'level_range': None}
#{'obs_name': 'seviri', 'obstype': 7, 'codetype': None, 'varno': None, 'vertco_reference_1': None, 'sensor': 29, 'level_range': None}

varobs=['gpssol_v128','airep_v2'  ,'airep_v3'  ,'airep_v4',
         'synop_v_v1','synop_v_v42', 'synop_v_v41', 'synop_v_v58', 'synop_v_v39',
        'airep_l_v2' ,'airep_l_v3','airep_l_v4', 
        'dribu_v1'  ,'dribu_v41' ,'dribu_v42','dribu_v39' ]

#, 'synop_v_v42', 'synop_v_v41', 'synop_v_v58', 'synop_v_v39']
#, 'synop_v_v1', 'synop_v_v42', 'synop_v_v41', 'synop_v_v58', 'synop_v_v39', 'dribu_v1', 'dribu_v39', 'dribu_v41', 'dribu_v42', 'ascat', 'radar_v29', 'radar_v195', 'airep_v2', 'airep_v3', 'airep_v4', 'airep_l_v2', 'airep_l_v3', 'airep_l_v4', 'temp_v2', 'temp_v3', 'temp_v4', 'temp_v7', 'temp_l_v2', 'temp_l_v3', 'temp_l_v4', 'temp_l_v7']

#obs_list=[{  "obs_name"          : "airep",
#           "obstype"           :  2 ,
#           "codetype"          : None,
#           "varno"             : [2, 3, 4] ,
#           "vertco_reference_1": None,
#           "sensor"            : None,
#           "level_range"       : None }]



# 1- Get data from ccma( equivalence in bash script  -> "get_ccma=yes"  )
# INSTANTIATE 
ccma=OdbCCMA()
sql=SqlHandler () 



bin_max_dist=100 
bin_int     =10 
ds_dict=defaultdict(list)

#def GetRows ( cdtg   ):
for cdtg in period:
    for dobs in obs_list:
        ccma_path ="/".join( [odb_path , cdtg  , "CCMA"] ) 

        # Check DCA directory  (if not there they will be created )
        df=DCAFiles()
        df.CheckDca ( ccma_path  )

        # If required level range or not 
        llev=False 
        #lev_range= dobs["level_range"]
        #if lev_range != None :
        #   llev=False  

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
                                   sql_query=query  ,
                                   sqlfile  = None  ,
                                   pools    = None  ,
                                   progress_bar=True , 
                                   verbose  = False  ,
                                   get_header=False , 
                                   return_rows=True )

        # COLUMNS FOR OBSTOOL (fist binning ) 
        stat , lat , lon , an_depar , fg_depar =ccma.GetByVarno ( rows , dobs  )
        list_df=ccma.Rows2Bins ( stat , lat , lon , an_depar , fg_depar , varobs )

       
        #return list_df 
        


quit()

# Desroziers /Hollingsworth-Lonnberg stats 
new_max_dist=100  # Km
new_bin_dist=10   # Km
delta_t     =60   # Time interval between two obs in [ min ]

for cdtg in period:
    df_list =GetRows (cdtg )
#    for df in df_list:
#        stat=DHLStat ( df , new_max_dist , new_bin_dist, delta_t  )
#        sdf=stat.getStatFrame ()
#        print( sdf ) 








    """fig, (ax1, ax2 , ax3) =  plt.subplots( 3,1  , figsize=( 10, 13 ) )
    sdf.plot(  x="dist"  , y="COV_HL"    , ax=ax2  , label="COV_HL"   , lw=2)
    sdf.plot(  x="dist"  , y="COV_DR-B"  , ax=ax2  , label="COV_DR-B" , lw=2)
    sdf.plot(  x="dist"  , y="COV_DR-R"  , ax=ax2  , label="COV_DR-R" , lw=2)
    ax2.set_ylabel("Covariance [m/s]")
    sdf.plot(  x="dist"  , y="COR_HL"    , ax=ax1  , label="COR_HL"   , lw=2, xlabel="Distance [Km]")
    sdf.plot(  x="dist"  , y="COR_DR-B"  , ax=ax1  , label="COR_DR-B" , lw=2, xlabel="Distance [Km]")
    sdf.plot(  x="dist"  , y="COR_DR-R"  , ax=ax1  , label="COR_DR-R" , lw=2, xlabel="Distance [Km]")
   
    ax1.set_ylabel("Correlation [m/s]" )
    ax1.set_xlabel( "Distance [Km]" )
    ax1.set_ylim( -1 ,1 )
    ax1.axhline(y = 0.2, color = 'b', linestyle = '--')
 
    sdf.plot.bar ( x="dist"   , y="nobs" , ax=ax3 , label="Nobs", color="grey")
    ax3.set_xlabel( "Distance [Km]" )
    plt.savefig("airep_v_2024010500_python.png")"""


EndTime = datetime.now()
Duration=EndTime - StartTime
print( "Duration\n" , Duration  )


# NOW ROWS GO TO THE STATISTICS modules !!!
# TO be continued ...




quit()

