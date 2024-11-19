import os,sys
from   ctypes import cdll , CDLL
import numpy as np

# CREATED MODULES 
sys.path.insert(0,"./modules" )

from pyodb_extra.odb_glossary import  OdbLexic
from pyodb_extra.parser       import  StringParser
from pyodb_extra.environment  import  OdbEnv
from pyodb_extra.odb_ob       import  OdbObject
from pyodb_extra.exceptions   import  *
from pyodb_extra.pool_factory import  PoolSlicer


from obstype_gen import ObsType  
from extractor   import SqlHandler , OdbCCMA , OdbECMA 


odb_install_dir=os.getenv( "ODB_INSTALL_DIR" )
env= OdbEnv(odb_install_dir, "libodb.so")
env.InitEnv ()

from pyodb  import   odbFetch
from pyodb  import   odbDca



# What we need as obstype
# GNSS
# SYNOP  TYPE   : 11=surface,14=surface+automatic, 21=ships,24=ships+automatic,  varno : 39=t2m, 58=rh2m,41=u10m, 42=v10m, 1=z
# RADAR  varno  :  q:29 , DOW:195
# AIREP   ALL + GIVEN LEVELS : vertco_reference_1:  >=25000   <=35000 (T, U , V )
# DRIBU varno: z:1 , 42:v10m  , 41:u10m   , 39:t2m
# TEMP varno   : t:2 , u:3 , v:4 , q:7
# LEVELS       :  vertco_reference_1:  >=40000     <=60000

# CAUTION !: NEVER CHANGE THE ORDER OF COLUMNS 
# TO ADD NEW ONES , ONE CAN JUST DO AN APPEND !
# columns                            col index shifted by 1 (to match the column no in ogriginal bash script )
cols      =[ " "                       ,  # 0   doesn't exist 
             "obstype"                 ,  # 1
             "codetype"                ,  # 2
             "statid"                  ,  # 3
             "varno"                   ,  # 4
             "degrees(lat)"            ,  # 5
             "degrees(lon)"            ,  # 6 
             "vertco_type"             ,  # 7
             "vertco_reference_1"      ,  # 8
             "sensor"                  ,  # 9
             "date"                    ,  # 10
             "time"                    ,  # 11
             "datum_status.active"     ,  # 12
             "datum_status.blacklisted",  # 13
             "datum_status.passive"    ,  # 14
             "datum_status.rejected"   ,  # 15
             "an_depar"                ,  # 16
             "fg_depar"                ,  # 17
             "obsvalue"                ,  # 18
             "final_obs_error"         ,  # 19
             "fg_error"                ,  # 20
             "biascorr_fg"             ,  # 21
             "datum_event1.fg2big@body"   # 22
             ]

# Considered tables & additional sql statement 
tables        =["hdr","desc","body","errstat"]  
extra_ccma_sql="(an_depar is not NULL) AND (fg_depar is not NULL)"


# JARVINEN !
# column name as keys -> attributes 
# CCMA NEEDS  !
#            "obs_name","obstype" ,"codetype"     ,"varno"            ,"coord_ref_1"      ,"sensor","level_range" , 
ccma_keys=["obs_name"  , cols[1]  , cols[2]       ,cols[4]            ,cols[8]            , cols[9],"level_range" ]
gpssol   =["gpssol"    , 1        ,110            ,128                ,None               ,None    ,None ] # CONV 
ship     =["ship"      , 1        ,[21,24,182]    ,[1,39,41,42,58]    ,None               ,None    ,None ] #
synop    =["synop"     , 1        ,[11,14,170]    ,[1,39,41,42,42,58] ,None               ,None    ,None ] #
dribu    =["dribu"     , 4        ,None           ,[1,39,41,42]       ,None               ,None    ,None ] #
ascat    =["ascat"     , 9        ,None           ,None               ,None               ,None    ,None ] #
radar    =["radar"     , 13       ,None           ,[29,195]           ,None               ,None    ,None ] #
airep    =["airep"     , 2        ,None           ,[2,3,4]            ,None               ,None    ,None ] #
temp     =["temp"      , 5        ,None           ,[2,3,4,7]          ,None               ,None    ,None ] #
amsua    =["amsua"     , 7        ,None           ,None               ,[6,9]              ,3       ,[6,9]] # SATEM 
#amsub   =["amsub"     , 7        ,None           ,None               ,None               ,4       ,None]  #
mhs      =["msh"       , 7        ,None           ,None               ,[3,5]              ,15      ,[3,5]] #
iasi     =["iasi"      , 7        ,None           ,None               ,None               ,16      ,None ] #
atms     =["atms"      , 7        ,None           ,None               ,[[7,22],[18,"MAX"]],19      ,[7,22],[18,"MAX"]]#
mwhs2    =["mwhs2"     , 7        ,None           ,None               ,[11,15]            ,73      ,[11,15]]#


obs_list=[ gnss   ,
           synop  ,
           synop_v,
           dribu  ,
           ascat  ,
           radar  ,
           airep  ,
           airep_l,
           temp   ,
           temp_l ,
           amsua  ,
           mhs    ,
           iasi   ,
           atms   ,
           mwhs2 ]

type_   = ObsType ()  
obs_dict=type_.GenDict (  ccma_keys, obs_list )


def CreateDca(  path , sub_base=None     ):
    # Prepare DCA files if not there 
    dbpath  = path 
    db      = OdbObject ( dbpath )
    dbname  = db.GetAttrib()["name"]
    if dbname == "ECMA" and sub_base != None :
       dbpath  =".".join( [path, sub_base ]   )


    if not os.path.isdir ("/".join(  [dbpath , "dca"] )  ):
       print( "No DCA files in {} 'directory'".format(dbpath ) ) 
       env.OdbVars["CCMA.IOASSIGN"]="/".join(  (dbname, "CCMA.IOASSIGN" ) )
       status =    odbDca ( dbpath=dbpath , db=dbname , ncpu=8  )
    else :
       print(  "DCA files generated already in database: '{}'".format( dbname )  )
    return status  


# CCMA  PATH
ccma_path="/hpcperm/cvah/tuning/diagnostics/bgos/odb_in/raw/odb_ccma/CCMA"


# 1- CCMA FOR obstool  (  bash equivalence -> get_ccma=yes  )
ccma=OdbCCMA()
stat=CreateDca(ccma_path)
sql =SqlHandler () 

# ECMA  PATH  & subbases 
# 
ecma_path   ="/hpcperm/cvah/tuning/diagnostics/bgos/odb_in/raw/odb_ecma/ECMA"
ecma_subbase=["conv",
              "radarv",
              "amsua",
              "amsub",
              "atms",
              "iasi",
              "scatt",
              "mwhs2", 
              "seviri"]

# Build queries and fetch odb rows 
for i in range(len(obs_dict[0:2])):
    obs=obs_dict[i]

    #ccma_path = ccma_path+"/"+obs_dict[i]["name"]

    llev=False 
    lev_range= obs["level_range"]
    if lev_range != None :
       llev = True 
    query=sql.BuildQuery(  columns       =cols     ,
                           tables        =tables   ,
                           obs_dict      =obs      ,
                           has_levels    =llev     ,
                           vertco_type   ="height" ,
                           remaining_sql=extra_ccma_sql )

    rows =ccma.FetchByObstype (    dbpath   =ccma_path , 
                                   sql_query=query     ,
                                   sqlfile  =None      ,
                                   pools    =None      ,
                                   verbose  =False     ,
                                   get_header=False)

    for row in rows:        print( row) 

