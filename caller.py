import os,sys
from   ctypes import cdll , CDLL
import numpy as np
import geopandas as gpd
import pandas as pd
from pyproj import CRS, Transformer


from datetime import datetime ,timedelta

from scipy.spatial.distance import cdist


# CREATED MODULES 
sys.path.insert(0,"./modules" )
#sys.path.insert(1,"/hpcperm/cvah/tuning/ww_oslo/pyodb_1.1.0/build/lib.linux-x86_64-cpython-310")
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




def ShowError( ):
    os.system( "cat pyodb.stderr"   ) 




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
# FOR obstool  
# columns                            col index shifted by 1 (to match the column No in original bash script )
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
tables        =["hdr","desc","body"]  
extra_ccma_sql="(an_depar is not NULL) AND (fg_depar is not NULL)"


# GET DATA FOR obstool  
# NEEDS CCMA ONLY 
# column name as keys
# "obs_name","obstype" ,"codetype"     ,"varno"        ,"vertco_reference_1" ,"sensor", "level_range"
#ccma_keys =["obs_name", cols[1]  ,cols[2]        ,cols[4]        ,cols[8]   ,cols[9] ,"level_range" ]

ccma_keys=["obs_name","obstype" ,"codetype"     ,"varno"        ,"vertco_reference_1" ,"sensor", "level_range"]
# column value as attribute 
gnss    =["gpssol"    , 1        ,110            ,128            , None     ,None    ,None ]
synop   =["synop"     , 1        ,[11,14,170,182],None           , None     ,None    ,None ]
synop_v =["synop_v"   , 1        ,[11,14,170,182],[1,42,41,58,39], None     ,None    ,None ]
dribu   =["dribu"     , 4        ,None           ,[1,39,41,42]   , None     ,None    ,None ]
ascat   =["ascat"     , 9        ,None           ,None           , None     ,None    ,None ]
radar   =["radar"     , 13       ,None           ,[29,195]       , None     ,None    ,None ]
airep   =["airep"     , 2        ,None           ,[2,3,4]        , None     ,None    ,None ]
airep_l =["airep_l"   , 2        ,None           ,[2,3,4]        , None     ,None    ,[25000,35000] ]
temp    =["temp"      , 5        ,None           ,[2,3,4,7]      , None     ,None    ,None ]
temp_l  =["temp_l"    , 5        ,None           ,[2,3,4,7]      , None     ,None    ,[40000,60000] ]
amsua   =["amsua"     , 7        ,None           ,None           , None     ,3       ,None]
amsub   =["amsub"     , 7        ,None           ,None           , None     ,4       ,None]
mhs     =["msh"       , 7        ,None           ,None           , None     ,15      ,None]
iasi    =["iasi"      , 7        ,None           ,None           , None     ,16      ,None]



# OBS LIST 
obs_list=[  airep ]
type_   = ObsType ()  
selected_obs  =type_.GenDict (  ccma_keys, obs_list )



StartTime = datetime.now()



# CCMA  PATH
cma_date="2024010500"
ccma_path="/hpcperm/cvah/tuning/diags/xp1/odb_in/raw/"+cma_date+"/CCMA"

# 1- CCMA FOR obstool  (  bash equivalence -> get_ccma=yes  )
# INSTANTIATE 
ccma=OdbCCMA()
sql=SqlHandler () 



# START GETTING DATA FOR STATISTICS 


def DupList(l_):
    dlist=  [  j for j in l_  for i in   l_  ]
    return dlist  



def ReshapeList( l_ ):
    slist =  [[ j for j in l_ ] for i in l_ ]
    l0    =  []
    for _  in slist :
        l0=l0+ _

    return l0 



def haversine(lon1, lat1, lon2, lat2) :
    # Radius of Earth in kilometers
    R = 6371.0
    phi1 = np.radians(lat1)
    phi2 = np.radians(lat2)
    delta_phi = np.radians(lat2 - lat1)
    delta_lambda = np.radians(lon2 - lon1)

    a = np.sin(delta_phi / 2)**2 + np.cos(phi1) * np.cos(phi2) * np.sin(delta_lambda / 2)**2
    c = 2 * np.arctan2(np.sqrt(a), np.sqrt(1 - a))
    distance = R * c
    return distance

# GET DATE FROM LOOP 
#date =  "%Y%m%d%H"


# Build queries and fetch odb rows 
#os.environ["ODB_PERMANENT_POOLMASK"]="10"


vstat=[] ; vlat=[] ; vlon=[]  ;    fg_depar_v =[];  an_depar_v=[]
llev=False 
#lev_range= obs["level_range"]
#if lev_range != None :
#   llev = True 


query=sql.BuildQuery(      columns       =cols     ,
                           tables        =tables   ,
                           obs_dict      =selected_obs[0],
                           has_levels    =llev     ,
                           vertco_type   ="height" ,
                           remaining_sql=extra_ccma_sql )

rows =ccma.FetchByObstype (    dbpath   =ccma_path , 
                                   sql_query=query  ,
                                   sqlfile  = None  ,
                                   pools    = None  ,
                                   fmt_float= None  , 
                                   verbose  = False ,
                                   get_header=False )


# python & C  index  
#['obstype@hdr',             0 
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
# 'obsvalue@body'            11

#"statid","lat","lon","an_depar","fg_depar","obsvalue"

# MAKE SURE THAT THE COORDS ARE IN WGS84 

wgs84_crs = CRS("EPSG:4326")
transformer = Transformer.from_crs(wgs84_crs, wgs84_crs, always_xy=True)

coord=[]
nobs=0 


for row in rows:
    obst  = row[0]
    varno = row[3]   
    if obst ==2 and varno== 4 and row[2].strip() == "EU8478":
       nobs =nobs+1 
       st,  lat , lon , an_d , fg_d , obsv =row[2],row[4],row[5],row[9],row[10],row[11]
       coord.append([transformer.transform(lon, lat)[0],transformer.transform(lon, lat)[1] ]   ) 
       vstat.append     (st   )
       fg_depar_v.append(  fg_d )
       an_depar_v.append(  an_d )


StartTime = datetime.now()

matdist = cdist(coord, coord, metric=lambda u, v:    haversine(u[0], u[1], v[0], v[1]))

# DISTANCES MATRIX 
m=matdist.T 

dim1 =list(np.arange( m.shape[0]  ))
dim2 =list(np.arange( m.shape[1]  ))
mdist=list(m.reshape( m.shape[0]*m.shape[1]  ) )


d1  =ReshapeList(  dim1 )
oma1=ReshapeList(   an_depar_v    )
omg1=ReshapeList(   fg_depar_v    )

d2  =DupList    (  dim2 )
oma2=DupList  (   an_depar_v    )
omg2=ReshapeList(   fg_depar_v    )

#lldist=DupList( lDint  ) 


# ADD  an_depar , fg_depar
#Same in R : OA1         OA2        FG1        FG2 (but in lower )
df    = pd.DataFrame({"n1": d1  , "n2": d2      , "dist" :mdist ,  
                                  "oa1":oma1    , "oa2":oma2  , 
                                  "fg1":omg1    , "fg2":omg2  }   )



# AS IN R scripts 
# Spatial separation bin for OmG
bin_int      = 10  # [km] : binning interval for pairs
bin_max_dist = 100 # [km] : maximal distance for the bin_int

# Time separation distance
time_btw_omg = 60  # [+/- min] : time_btw_omg = time between OmG/OmA in pairs
dist         = 10


lDint = list(np.arange(bin_int, bin_max_dist +10 , bin_int ))
cDint = list(np.arange(bin_int, bin_max_dist +10 , bin_int))

subdf =df.query( "dist <=    "+str(bin_max_dist)  )

# Partitions over bins inplace !
# FOR MORE CONTINUOUS DATA , WE CAN GROUP BY dist   
# We use the bins 10 km 

dbin   =[0,1]+lDint
dlabel =[0  ]+cDint 

dbin_serie   =pd.cut(  subdf['dist'], bins=dbin , labels=dlabel, right=True, include_lowest=True, retbins=True )
dbin_col     =dbin_serie[0].to_list()
subdf["dbin"]=dbin_col

#==============================================================
# COV(XY) = E[X*Yt] - E[X]*E[Yt] 
# save: 
# sum[X]; sum[Yt]; sum[X*Yt]; num[X]
#
# COV(OA1,FG2) = E[OA1*FG2] - E[OA1]*E[FG2]
# OA1 = an_depar
# FG2 = fg_depar
# save: 
# sum[OA1]; sum[FG2] ;sum[OA1*FG2]; num[FG2]
#==============================================================
# STD(X) = sqrt( E[X^2] - (E[X])^2 )
# save: 
# sum[FG1]   ; sum[FG2]   ; sum[OA1]
# sum[FG1^2] ; sum[FG2^2] ; sum[OA1^2]
#==============================================================
# COVARIANCE, CORRELATIONS

# Agrregate 
# SUM  :  oa1 , fg1 , fg2 
sum_oa1 = subdf.groupby('dbin')['oa1'].sum().reset_index()
sum_fg1 = subdf.groupby('dbin')['fg1'].sum().reset_index()
sum_fg2 = subdf.groupby('dbin')['fg2'].sum().reset_index()

# SUM  :   fg1*fg2
fg1_xfg2=  subdf["fg1"]*subdf["fg2"]
fg12_dict =  {"fg1xfg2": fg1_xfg2,"dbin": dbin_serie[0]}
fg1fg2    = pd.DataFrame(fg12_dict  )
sqrt_fg   =fg1fg2.groupby( "dbin").sum()


# SUM  :   oa1*fg2
a1_xfg2  =subdf["oa1"]*subdf["fg2"]
a1fg1_dict =  {"oa1xfg2": a1_xfg2,"dbin": dbin_serie[0]}

a1fg2    = pd.DataFrame(a1fg1_dict  )
sqrt_afg2= a1fg2.groupby( "dbin" ).sum()

# NOBS by bin 
nobs    =subdf.groupby( "dbin" )["fg2"].count()

# STD
f11      =subdf["fg1"]*subdf["fg1"]        # FG1 *FG1 
f11_dict  =  {"fg1^2": f11,"dbin": dbin_serie[0]}
_  = pd.DataFrame(f11_dict  )
sqrt_fg11= _.groupby( "dbin" ).sum()


f22       =subdf["fg2"]*subdf["fg2"]        # FG2 *FG2
f22_dict  =  {"fg2^2": f11,"dbin": dbin_serie[0]}
_  = pd.DataFrame(f22_dict  )
sqrt_fg22 = _.groupby( "dbin" ).sum()


a11       =subdf["oa1"]*subdf["oa1"]        # OA1 *OA1
a11_dict  =  {"oa1^2": a11,"dbin": dbin_serie[0]}
_  = pd.DataFrame(a11_dict  )
sqrt_a11  = _.groupby( "dbin" ).sum()


print( sqrt_fg22 , sqrt_a11 )  




#StatTab$FGsqr1 = aggregate(list(FGSQR1  = FG1*FG1)            , list(dist=ldist), FUN=sum )$FGSQR1
#StatTab$FGsqr2 = aggregate(list(FGSQR2  = FG2*FG2)            , list(dist=ldist), FUN=sum )$FGSQR2
#StatTab$Asqr1  = aggregate(list(ASQR1   = OA1*OA1)            , list(dist=ldist), FUN=sum )$ASQR1


#  StatTab$FGsum1 = aggregate(list(FGSUM1  = FG1)                , list(dist=ldist), FUN=sum    )$FGSUM1
#  StatTab$FGsum2 = aggregate(list(FGSUM2  = FG2)                , list(dist=ldist), FUN=sum    )$FGSUM2
#  StatTab$AFGsqr = aggregate(list(AFGSQR  = OA1*FG2)            , list(dist=ldist), FUN=sum    )$AFGSQR
#  StatTab$FGsqr  = aggregate(list(FGSQR   = FG1*FG2)            , list(dist=ldist), FUN=sum    )$FGSQR
#  StatTab$num    = aggregate(list(NOBS    = FG2)                , list(dist=ldist), FUN=length )$NOBS

EndTime = datetime.now()
Duration=EndTime - StartTime
print( "Duration" , Duration   )



ShowError()
