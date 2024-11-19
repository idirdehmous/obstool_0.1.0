import os,sys
from   ctypes import cdll , CDLL
import numpy as np

# CREATED MODULES 
sys.path.insert(0,"./modules" )
sys.path.insert(1,"./obstool" )

from odb_glossary import  OdbLexic
from parser       import  StringParser
from environment  import  OdbEnv
from odb_ob       import  OdbObject
from exceptions   import  *
from pool_factory import  PoolSlicer
from obstool.obstype_gen import ObsType  
from obstool.extractor   import OdbCMA 



# What we need as obstype
# GNSS
# SYNOP  TYPE   : 11=surface,14=surface+automatic, 21=ships,24=ships+automatic
#        varno : 39=t2m, 58=rh2m,41=u10m, 42=v10m, 1=z
# RADAR  varno  :  q:29 , DOW:195
# AIREP   ALL + GIVEN LEVELS : vertco_reference_1:  >=25000   <=35000 (T, U , V )
# DRIBU varno: z:1 , 42:v10m  , 41:u10m   , 39:t2m
# TEMP varno   : t:2 , u:3 , v:4 , q:7
# LEVELS       :  vertco_reference_1:  >=40000     <=60000



# PATH
dbpath="/hpcperm/cvah/tuning/diagnostics/bgos/odb_in/raw/odb_ecma"
# ECMA.amsua
# ECMA.amsub
# ECMA.atms
# ECMA.conv
# ECMA.iasi
# ECMA.mwhs2
# ECMA.radarv
# ECMA.scatt

#env.OdbVars["CCMA.IOASSIGN"]="/".join(  (dbpath, "CCMA.IOASSIGN" ) )



#db      = OdbObject ( dbpath )
#db_attr = db.GetAttrib()
#db_type = db_attr["type"]
#db_name = db_attr["name"]





# Cols  AND tables for surface and upper no sat !
ecma_cols=["obstype",
"codetype",
"statid",
"varno",
"degrees(lat)", 
"degrees(lon)",
"vertco_type",
"vertco_reference_1",
"sensor",
"date",
"time",
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
"datum_event1.fg2big@body"]
ecma_tabs  =["hdr","desc","body","errstat"]  
ecma_flags ="(an_depar is not NULL) AND (fg_depar is not NULL)"


# 
ccma_cols   =["statid@hdr",
               "sensor",
                "varno,vertco_reference_1", 
                "degrees(lat@hdr)", 
                "degrees(lon@hdr)",
                "an_depar@body" , 
                "fg_depar@body,obsvalue@body"]
ccma_tabs   =["hdr,body "]
ccma_flags     =" (an_depar is not NULL) AND (datum_event1.fg2big@body = 0) AND (fg_depar is not NULL)"




# CCMA (obstool)
# keys (columns )
keys    =["name"    ,"obstype","codetype"    ,"varno"         ,"level_range" ,"pressure_range" ,"sensor"]

# values 
gnss    =["gpssol"  , 1       , 110          , 128            ,None          ,None             ,None    ]
synop   =["synop"   , 1       ,[11,14,21,24] , None           ,None          ,None             ,None    ]
synop_v =["synop_v" , 1       , [11,14,21,24],[1,42,41,58,39] ,None          ,None             ,None    ]
dribu   =["dribu"   , 4       , None         ,[1,42,41,39]    ,None          ,None             ,None    ]
radar   =["radar"   , 13      , None         ,[29,195]        ,None          ,None             ,None    ]
airep   =["airep"   , 2       ,144           ,None           ,None           ,None    ,None]
airep_l =["airep_l" , 2       ,144           ,None           ,[25000,35000]  ,None    ,None]
temp    =["temp_all", 5       ,None          ,[2,4,7]        ,None           ,None    ,None]
temp_l  =["temp_l"  , 5       ,None          ,[2,4,7]        ,[40000,60000]  ,None    ,None]
amsua   =["amsua"   , 7       ,None          ,None           ,None          ,None              ,3       ]
amsub   =["amsub"   , 7       ,None          ,None           ,None          ,None              ,4       ]
mhs     =["msh"     , 7       ,None          ,None           ,None          ,None              ,15      ]
iasi    =["iasi"    , 7       ,None          ,None           ,None          ,None              ,16      ]

# Create observation list and attributes 
obs_list=[ gnss, synop,synop_v, dribu , radar,airep , airep_l, temp, temp_l , amsua , amsub, mhs, iasi]
type_   = ObsType ()  
obs_dict=type_.GenDict (  keys, obs_list )


# Extract from   ECMA 
# ECMA.amsua
# ECMA.amsub
# ECMA.atms
# ECMA.conv
# ECMA.iasi
# ECMA.mwhs2
# ECMA.radarv
# ECMA.scatt

ecma_obslist =[ "conv",
                "radarv", 
                "scatt" , 
                "amsua", 
                "amsub" , 
                "iasi",
                "atms" , 
                "mwh2"  ]

# Loop over obstypes and queries 
for obstype in ecma_obslist:
    ecma_path = dbpath+"/ECMA."+obstype
    ccma=OdbCMA( ecma_path  )
    for i in range(len(obs_dict)):
        obs=obs_dict[i]
        print( obs)  
        llev=False 
        lev_range= obs["level_range"]
        if lev_range != None :
           llev = True 
           query=ccma.BuildQuery(columns        =ecma_cols ,
                                  tables        =ecma_tabs ,
                                  obs_dict      =obs    ,
                                  has_levels    =llev  ,
                                  vertco_type   ="height" ,
                           
                                  additional_sql=ecma_flags )


           rows =  ccma.FetchByObstype (  sql_query=query,
                                   sqlfile  =None ,
                                   pools    =None ,
                                   verbose  =True ,
                                   get_header=False)

           for row in rows:        print( row) 


# ASCAT  (    HAVE TO CHECK AND CONFIRM  !!!! )
# PARAM :  ??  (  amb_u , amb_v maybe !)
# QUERY : statid , lat , lon , fg_depar , an_depar , obsvalue

