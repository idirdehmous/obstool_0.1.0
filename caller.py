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



# CCMA  PATH
ccma_path="/hpcperm/cvah/tuning/diagnostics/bgos/odb_in/raw/odb_ccma/CCMA"






# ECMA  PATH 
ecma_path="/hpcperm/cvah/tuning/diagnostics/bgos/odb_in/raw/odb_ecma"
# ECMA.amsua
# ECMA.amsub
# ECMA.atms
# ECMA.conv
# ECMA.iasi
# ECMA.mwhs2
# ECMA.radarv
# ECMA.scatt


# ECMA columns  & tables 
ecma_cols  =["obstype@hdr"  , 
             "codetype@hdr" , 
             "statid@hdr"   , 
             "varno@body"   , 
             "degrees(lat@hdr)", 
             "degrees(lon@hdr)" , 
             "vertco_type@body", 
             "vertco_reference_1@body", 
             "sensor@hdr",
             "date@hdr"        , 
             "time@hdr", 
             "datum_status.active", 
             "datum_status.blacklisted", 
             "datum_status.passive",
             "datum_status.rejected", 
             "an_depar", 
             "fg_depar",
             "obsvalue",  
             "final_obs_error",
             "fg_error", 
             "biascorr_fg", 
             "datum_event1.fg2big@body" ]

# ECMA cols and tables 
ecma_tables=["hdr","desc","body","errstat"]  
ecma_flags ="(an_depar is not NULL) AND (fg_depar is not NULL)"



# CCMA columns & tables 
ccma_cols   =["obstype@hdr",   "codetype@hdr", "statid@hdr","varno@body","degrees(lat@hdr)", 
              "degrees(lon@hdr)","vertco_type@body",       "vertco_reference_1@body",    "sensor@hdr", "date@hdr",
              "time@hdr", "datum_status.active", "datum_status.blacklisted","datum_status.passive",
              "datum_status.rejected","an_depar","fg_depar","obsvalue", "final_obs_error",
              "fg_error","biascorr_fg" ]

ccma_tabs =["hdr,desc,body,errstat"]
ccma_flags="(an_depar is not NULL) AND (datum_event1.fg2big@body = 0) AND (fg_depar is not NULL)"


# CCMA (obstool)
# keys (columns )
keys    =["name"    ,"obstype","codetype"     ,"varno"         ,"level_range" ,"vertco_reference_1" ,"sensor"]
gnss    =["gpssol"  , 1       ,110            ,128             ,None          ,None             ,None]#
synop   =["synop"   , 1       ,[11,14,170,182],None            ,None          ,None             ,None]#
synop_v =["synop_v" , 1       ,[11,14,170,182],[1,42,41,58,39] ,None          ,None             ,None]#
dribu   =["dribu"   , 4       ,None           ,[1,39,41,42]    ,None          ,None             ,None]#
ascat   =["ascat"   , 9       ,None           ,None            ,None          ,None             ,None]#
radar   =["radar"   , 13      ,None           ,[29,195]        ,None          ,None             ,None]#
airep   =["airep"   , 2       ,None           ,[2,3,4]         ,None                            ,None]#
airep_l =["airep_l" , 2       ,None           ,[2,3,4]         ,[25000,35000]                   ,None]#
temp    =["temp"    , 5       ,None           ,[2,3,4,7]       ,None          ,None             ,None]# 
temp_l  =["temp_l"  , 5       ,None           ,[2,3,4,7]       ,[40000,60000] ,None             ,None]#
amsua   =["amsua"   , 7       ,None           ,None            ,None          ,None             ,3 ]#
amsub   =["amsub"   , 7       ,None           ,None            ,None          ,None             ,4 ]#
mhs     =["msh"     , 7       ,None           ,None            ,None          ,None             ,15]#
iasi    =["iasi"    , 7       ,None           ,None            ,None          ,None             ,16]#

# CCMA  Create observation list and attributes 
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
           amsub  , 
           mhs    , 
           iasi]





type_   = ObsType ()  
obs_dict=type_.GenDict (  keys, obs_list )


# Prepare DCA files if not there 
ecma_db      = OdbObject ( ecma_path )
ecma_dbname  = ecma_db.GetAttrib()["name"]


if not os.path.isdir ("/".join(  [ecma_path , "dca"] )  ):
   env.OdbVars["CCMA.IOASSIGN"]="/".join(  (ecma_path, "CCMA.IOASSIGN" ) )
   status = pyodbDca ( dbpath=ecma_path , db=ecma_dbname , ncpu=8  )
else :
    print(  "DCA files already in database:\n{}".format( dbpath )  )

quit()

# 1- CCMA FOR obstool  (  bash equivalence -> get_ccma=yes  )
# Instantiation  ! 
ccma=OdbCCMA()
ecma=OdbECMA()
sql =SqlHandler()


# Build queries and fetch odb rows 
for i in range(len(obs_dict)):
    obs=obs_dict[i]
    llev=False 
    lev_range= obs["level_range"]
    if lev_range != None :
       llev = True 
    query=sql.BuildQuery(columns         =ccma_cols ,
                           tables        =ccma_tabs ,
                           obs_dict      =obs    ,
                           has_levels    =llev  ,
                           vertco_type   ="height" ,
                           remaining_sql=ccma_flags )
    rows =ccma.FetchByObstype (    ccma_path=dbpath, 
                                   sql_query=query ,
                                   sqlfile  =None ,
                                   pools    =None ,
                                   verbose  =False ,
                                   get_header=False)

#    for row in rows:        print( row) 

