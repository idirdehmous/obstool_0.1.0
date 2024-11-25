import os,sys
from   ctypes import cdll , CDLL
import numpy as np
import geopandas as gpd
import pandas as pd

from pandas import DataFrame 

from pyproj import CRS, Transformer


from datetime import datetime ,timedelta

from scipy.spatial.distance import cdist



original_stderr = sys.stderr
original_stdout = sys.stdout

# Redirect stdout and stderr to the terminal
sys.stdout = sys.__stdout__
sys.stderr = sys.__stderr__


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
obs_list=[ gnss , synop ,dribu , radar , airep, amsua,amsub,mhs, iasi ]
type_   = ObsType ()  
selected_obs  =type_.GenDict (  ccma_keys, obs_list )


# Start run time 
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
                                   verbose  = True ,
                                   get_header=True )



#sys.stdout = original_stdout
#sys.stderr = original_stderr
#print( sys.stdout  )  

quit()
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
vstat=[] ; vlat=[] ; vlon=[]  ;
fg_depar_v =[];
an_depar_v=[]
obs=[]


for row in rows:
    obst  = row[0]
    varno = row[3]   
    if obst ==2 and varno== 4:
       nobs =nobs+1 
#       if nobs >N :
#           break 
       st,  lat , lon , an_d , fg_d , obsv =row[2],row[4],row[5],row[9],row[10],row[11]
       vlat.append(lat)
       vlon.append( lon )
       coord.append([transformer.transform(lon, lat)[0],transformer.transform(lon, lat)[1] ]   ) 
       vstat.append     (st   )
       fg_depar_v.append(fg_d )
       an_depar_v.append( an_d )
       obs.append(  obsv )
StartTime = datetime.now()

# DISTANCES ARE COMPUTED WITH heversine method . THE ACCURACY DIFFERS 
# FROM R (spDists   IMPLEMENTS DeMeeus method )   difference = +/- 50m 
# IF RESULTS DIFFERENTS TEST spDist "C" source code. 
matdist = cdist(coord, coord, metric=lambda u, v:    haversine(u[0], u[1], v[0], v[1]))


d1=[]
d2=[]
for i in range(matdist.shape[0]):
    for j in range(matdist.shape[1]):
        d1.append(i)
        d2.append(j)

# Swapp d1 and d2 to match the same indices in R (  idx -1 )

dfdist = pd.DataFrame(  {"d1"  : d2 , "d2":d1  , 
                      "dist":matdist.reshape(matdist.shape[0]*matdist.shape[1]) }  )
                    
#dfdist = DataFrame( d2,d1,matdist.reshape(matdist.shape[0]*matdist.shape[1]), columns=["d1","d2","dist"]   )


bin_int      = 10  # [km] : binning interval for pairs
bin_max_dist=100
# Time separation distance
time_btw_omg = 60  # [+/- min] : time_btw_omg = time between OmG/OmA in pairs
dist         = 10





ndist_df=  dfdist.query("dist <=  "+str(bin_max_dist) )


data_df = DataFrame(   { "satid":vstat,      "lat":vlat, 
                         "lon":vlon  , "an_depar":an_depar_v ,
                         "fg_depar":fg_depar_v,  "obsvalue":obs}  )

ndist_df['OA1'] = data_df.loc[ndist_df['d1'], 'an_depar'].values
ndist_df['OA2'] = data_df.loc[ndist_df['d2'], 'an_depar'].values
ndist_df['FG1'] = data_df.loc[ndist_df['d1'], 'fg_depar'].values
ndist_df['FG2'] = data_df.loc[ndist_df['d2'], 'fg_depar'].values

lDint = list(np.arange(bin_int, bin_max_dist +10 , bin_int ))
cDint = list(np.arange(bin_int, bin_max_dist +10 , bin_int))

# Partitions over bins inplace !
# FOR MORE CONTINUOUS DATA , WE CAN GROUP BY dist   
# We use the bins 10 km 
dbin    =[0,1]+lDint
ddlabel =[0  ]+cDint

dbin_serie   =pd.cut(  ndist_df['dist'], bins=dbin , labels=ddlabel , right=True, include_lowest=True, retbins=True )
dbin_col     =dbin_serie[0].to_list()
ndist_df["dbin"] = dbin_col
pd.set_option('display.max_rows', None)
print( ndist_df )
quit()

#df_label=dbin.remove(1)
#bin_df=DataFrame( { "dbin": dbin  , "dlabels": df_label  }   )
# FINAL  dataframe cols in R 
# SUMS 
# Asum1   ->  sum OA1
# FGsum1  ->  sum FG1 
# FGsum2  ->  sum FG2

# PRODS 
# AFGsqr  ->  OA1*FG2    
# FGsqr   ->  FG1*FG2
# FGsqr1  ->  FG1*FG1
# FGsqr2  ->  FG2*FG2
# Asqr1   ->  OA1*OA1
# OBS #
# num     -> Nobs 

#  sum(AO1)  sum(FG1)    sum(FG2)
sum_OA1 = ndist_df.groupby('dbin')['OA1'].sum().reset_index()
sum_FG1 = ndist_df.groupby('dbin')['FG1'].sum().reset_index()
sum_FG2 = ndist_df.groupby('dbin')['FG2'].sum().reset_index()      # OK 

#print( sum_OA1 , sum_FG1, sum_FG2 ) 

df_sqrt = ndist_df.grouby("dbin")["OA1", "FG1"]


#df_ =ndist_df["OA1"]*ndist_df["FG2"]
#df_["dbin"]  = ndist_df["dbin"]
print( df_sqrt )
quit()
a1fg1_dict =  {"oa1*fg2": a1_xfg2,"dbin": dbin_serie[0]}

a1fg2      = pd.DataFrame(a1fg1_dict  )
sqrt_afg2= a1fg2.groupby( "dbin" ).sum().reset_index()

print( sqrt_afg2 )


quit()











quit()
dfdist = pd.DataFrame( {"dist":matdist.reshape(matdist.shape[0]*matdist.shape[1]),
                        "OA1" :np.asarray(an_depar_v).reshape( matdist.shape[0]*matdist.shape[1]),
                        "OA2" :np.asarray(an_depar_v).reshape( matdist.shape[0]*matdist.shape[1]),
                        "FG1" :np.asarray(fg_depar_v).reshape( matdist.shape[0]*matdist.shape[1]),
                        "FG2" :np.asarray(fg_depar_v).reshape( matdist.shape[0]*matdist.shape[1])
                        }  ) 


df=dfdist.query(   "dist <= "+str(  bin_max_dist  )  )

lDint = list(np.arange(bin_int, bin_max_dist +10 , bin_int ))
cDint = list(np.arange(bin_int, bin_max_dist +10 , bin_int ))

pd.cut( df['dist'], bins=dbin , labels=dlabel, right=True, include_lowest=True, retbins=True )

print( df ) 

quit()





dim1 =list(np.arange( m.shape[0]  ))
dim2 =list(np.arange( m.shape[1]  ))
mdist=list(m.reshape( m.shape[0]*m.shape[1]  ) )
d1  =ReshapeList(  dim1 )
d2  =DupList    (  dim2 )

df_dist=DataFrame (  { "d1":d1 , "d2":d2 , "dist": mdist   }  )


bin_max_dist=100 #Km
nd_df=df_dist.query(  "dist <= "+str(bin_max_dist))


print(  nd_df  )
quit()
oma1,oma2=an_depar_v, an_depar_v
omg1,omg2=fg_depar_v, fg_depar_v

df_oma1 = DataFrame ({ "d1":d1  , "d2":d2, "dist":mdist,    "FG1":omg1, "OA1":oma1 } )


print( df_oma1  ) 
quit()

bin_max_dist=100 #Km
nd_df=df_dist.query(  "dist <= "+str(bin_max_dist))  


sa1 = pd.Series(an_depar_v)
fg1 = pd.Series(fg_depar_v)
df=pd.concat([sa1, fg1], keys=['oa1', 'fg1'])
print( df )

quit()




oma1,oma2=an_depar_v, an_depar_v
omg1,omg2=fg_depar_v, fg_depar_v

df_oma1 = DataFrame ({ "FG1":omg1, "OA1":oma1 } )


df=df.concat (   )

quit()
df_fa2 = DataFrame ({ "FG2":oma2, "OA2":oma2 } )

df_ = nd_df.merge(df_fa1 ,  left_on='dist',right_on=True)

#df_     = pd.concat([nd_df, df_fa1] ,  ignore_index=True )

#    "FG1":omg1, "FG2":omg2  } ,index=[i for i in range(len(oma1)) ] )
#df_     = pd.concat([nd_df, df_depar], axis=0).reindex(df_depar.index)

print (df_ )
quit()





# Spatial separation bin for OmG
bin_int      = 10  # [km] : binning interval for pairs
bin_max_dist = 100 # [km] : maximal distance for the bin_int

# Time separation distance
time_btw_omg = 60  # [+/- min] : time_btw_omg = time between OmG/OmA in pairs
dist         = 10

lDint = list(np.arange(bin_int, bin_max_dist +10 , bin_int ))
cDint = list(np.arange(bin_int, bin_max_dist +10 , bin_int))

# Partitions over bins inplace !
# FOR MORE CONTINUOUS DATA , WE CAN GROUP BY dist   
# We use the bins 10 km 
dbin   =[0,1]+lDint
dlabel =[0  ]+cDint

ndf=pd.DataFrame( { "OA1":oma1, "OA2":oma2 ,"FG1":omg1, "FG2":omg2  }  )
ndf  ["dist"]=[0,0,0,0,0]

nndf=ndf.query( "dist <="+ str(bin_max_dist)  )

res =pd.cut( nndf['dist'], bins=dbin , labels=dlabel, right=True, include_lowest=True, retbins=True )
print( nndf    )
#print( ndf.query() )
quit()


dict_={ "dist":ddist , "an_depar":oma ,   "fg_depar":omg }
df1=pd.DataFrame(  dict_  )

print( df1 )



quit()
d1  =ReshapeList(  dim1 )
oma1=ReshapeList(   an_depar_v    )
omg1=ReshapeList(   fg_depar_v    )

d2  =DupList    (  dim2 )
oma2=DupList  (   an_depar_v    )
omg2=ReshapeList(   fg_depar_v    )


# ADD  an_depar , fg_depar
#Same in R : OA1         OA2        FG1        FG2 (but in lower )
df    = pd.DataFrame({"n1": d1  , "n2": d2      , "dist" :mdist ,  
                                  "OA1":oma1    , "OA2":oma2  , 
                                  "FG1":omg1    , "FG2":omg2  }   )


print( df)  

quit()
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
# ADD dbin 
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

#    R Terminology :
#    FG1 , FG2  :  fg_depar , fg_depar (T)
#    AO1 , AO2  :  an_depar , an_depar (T)

#    Equivalence in  Deroziers paper :
#    b==background 
#    FG1  = ( D_o - D_b )
#    FG2  = ( D_o - D_b )T
#    AO1  = ( D_o - D_b )
#    AO2  = ( D_o - D_b )T 
#    NEEDED :
#    COV 
#    sum (   D_o - D_b )_t , 
#    sum (   D_o - D_a)   , 
#    sum (  [D_o - D_a] - (D_o - D_b)_t  )
#    nobs = length (D_o - D_b ) 
#    ----------------------------------------------------------
#    STD 
#    sum(  D_o -  D_b )  
#    sum(  D_o -  D_b )_t
#    sum(  D_o -  D_a )
#    sum( [D_o -  D_b]^2] )
#    sum( [D_o -  D_b )_t]^2])
#    sum( [D_o  - D_a )]  ^2])


# Agrregate 
# sum[OA1]; sum[FG2] ;sum[OA1*FG2]; num[FG2]
# USE TH SAME NOTATIONS  ( Not use upper letter for variables in python , it is an exception !)
# SUM  :  OA1 , FG1 , FG2 



# FINAL  dataframe cols in R 
# Asum1     FGsum1     FGsum2     AFGsqr     FGsqr num    FGsqr1  FGsqr2     Asqr1

#  Asum1     FGsum1      FGsum2 
#  sum(AO1)  sum(FG1)    sum(FG2)
sum_OA1 = subdf.groupby('dbin')['OA1'].sum().reset_index()
sum_FG1 = subdf.groupby('dbin')['FG1'].sum().reset_index()
sum_FG2 = subdf.groupby('dbin')['FG2'].sum().reset_index()      # OK 


# AFGsqr      
# AO1*FG2    
#af12_  = subdf["OA1"]*subdf["FG2"]
#af_    = {"OA1*FG2":af12_  ,"dbin": dbin_serie[0]}
#af12   = pd.DataFrame( af_  ).groupby( "dbin" )[ "OA1*FG2" ].sum().reset_index()


# SUM  :   oa1*fg2
a1_xfg2  =subdf["OA1"]*subdf["FG2"]         # OA1*FG2 
a1fg1_dict =  {"oa1*fg2": a1_xfg2,"dbin": dbin_serie[0]}

a1fg2    = pd.DataFrame(a1fg1_dict  )
sqrt_afg2= a1fg2.groupby( "dbin" ).sum().reset_index()

print( sqrt_afg2 )

quit()
ff12    =  subdf["FG1"]*subdf["FG2"]    

fg12_dict   =  {"fg1*fg2": fg1_xfg2,"dbin": dbin_serie[0]}
fg1fg2      =  pd.DataFrame(fg12_dict  )
sqrt_fg12   =  fg1fg2.groupby( "dbin").sum()


# SUM  :   oa1*fg2
a1_xfg2  =subdf["oa1"]*subdf["fg2"]         # OA1*FG2 
a1fg1_dict =  {"oa1*fg2": a1_xfg2,"dbin": dbin_serie[0]}

a1fg2    = pd.DataFrame(a1fg1_dict  )
sqrt_afg2= a1fg2.groupby( "dbin" ).sum()

# NOBS by bin 
nobs    =subdf.groupby( "dbin" )["fg2"].count()









# STD
f11      =subdf["fg1"]*subdf["fg1"]        # FG1 *FG1 
f11_dict  =  {"fg1*fg1": f11,"dbin": dbin_serie[0]}
_  = pd.DataFrame(f11_dict  )
sqrt_fg11= _.groupby( "dbin" ).sum()


f22       =subdf["fg2"]*subdf["fg2"]        # FG2 *FG2
f22_dict  =  {"fg2*fg2": f11,"dbin": dbin_serie[0]}
_  = pd.DataFrame(f22_dict  )
sqrt_fg22 = _.groupby( "dbin" ).sum()


a11       =subdf["oa1"]*subdf["oa1"]        # OA1 *OA1
a11_dict  =  {"oa1*oa1": a11,"dbin": dbin_serie[0]}
_  = pd.DataFrame(a11_dict  )
sqrt_a11  = _.groupby( "dbin" ).sum()

# Results 
# 
# sum_oa1, sum_fg1, sum_fg2 , sqrt_fg12 , sqrt_afg2 , sqrt_fg11, sqrt_fg22, sqrt_a11  , nobs 

# COV 
ag1 = sum_oa1["oa1"].sum()
ag2 = sum_fg1["fg1"].sum()
ag3 = sum_fg2["fg2"].sum()
ag4 = sqrt_afg2["oa1*fg2"].sum()
ag5 = sqrt_fg12["fg1*fg2"].sum()
ag6 = sqrt_fg22 ["fg2*fg2"].sum()

# STD 
ag7 = sqrt_fg11["fg1*fg1"].sum()
ag8 = sqrt_fg22["fg2*fg2"].sum()
ag9 = sqrt_a11 ["oa1*oa1"].sum()



#    print (c ( monSENS$DIST, monSENS$COV.DR.B   ) )
#    print( c(  monSENS$DIST , monSENS$COV.DR.R ) )
#    print( c(  monSENS$DIST ,  monSENS$COV.HL  ) )

# DIST 
#d=[ 5.0000000 ,15.0000000 ,25.0000000, 35.0000000 ,45.0000000, 55.0000000 , 65.0000000 ,75.0000000 ,85.0000000, 95.0000000  ]  
# DR B 
#drb=[3.5011069 , 2.4077607 , 1.5287413  ,1.1799912 , 0.3213617 , 0.3574087 , 0.2883566 ,-3.4877784 , -0.8624630 , 0.3901188 ]

# DR R 
#drr=[1.6579710  ,1.2983750 ,0.6013322  ,0.5480089 , 0.4299339  ,0.4823704  ,1.2023668, -1.6624207 , -0.2441650 , 0.3772984  ]


# HL 
#hl= [ 5.1590779  ,3.7061357 , 2.1300735 , 1.7280002 , 0.7512955 , 0.8397791 , 1.4907235 ,-5.1501990 ,-1.1066280 , 0.7674172  ]


quit()
cor_drb = [3.5011069,  2.4077607 , 1.5287413 , 1.1799912  ,0.3213617 , 0.3574087 ,
 0.2883566, -3.4877784 ,-0.8624630 , 0.3901188   ]

cor_drr =[ 0.44523863 , 0.35252176 , 0.15382195 , 0.17303093 , 0.09762322 , 0.09449228 , 
 0.21576725 ,-0.23032198 ,-0.14715175 , 0.19752809 ]

cor_hl=[ 0.72835047 , 0.52040232 , 0.26770688 , 0.26374859 , 0.08752726 , 0.09209522 , 
 0.15659691 ,-0.37729680 ,-0.27392750 , 0.26169334 ]


quit()
# FOR PLOT 
#cov          = ag6
#cov$COV.HL   = (ag5$FGSQR/ag6$NOBS) - ((ag2$FGSUM1 * ag3$FGSUM2)/(ag6$NOBS)^2)
#cov$COV.DR.B = cov$COV.HL - ((ag4$AFGSQR/ag6$NOBS) - ((ag1$ASUM1 * ag3$FGSUM2)/(ag6$NOBS)^2))
#cov$COV.DR.R = (ag4$AFGSQR/ag6$NOBS) - ((ag1$ASUM1 * ag3$FGSUM2)/(ag6$NOBS)^2)


#  StatTab        = aggregate(list(Asum1   = OA1)                , list(dist=ldist), FUN=sum    )
#  StatTab$FGsum1 = aggregate(list(FGSUM1  = FG1)                , list(dist=ldist), FUN=sum    )$FGSUM1
#  StatTab$FGsum2 = aggregate(list(FGSUM2  = FG2)                , list(dist=ldist), FUN=sum    )$FGSUM2
#  StatTab$AFGsqr = aggregate(list(AFGSQR  = OA1*FG2)            , list(dist=ldist), FUN=sum    )$AFGSQR
#  StatTab$FGsqr  = aggregate(list(FGSQR   = FG1*FG2)            , list(dist=ldist), FUN=sum    )$FGSQR
#  StatTab$num    = aggregate(list(NOBS    = FG2)                , list(dist=ldist), FUN=length )$NOBS



    # COV 
"""    ag1 = aggregate(list(ASUM1  = data$Asum1 ), by=lvar, FUN=sum )
    ag2 = aggregate(list(FGSUM1 = data$FGsum1), by=lvar, FUN=sum )
    ag3 = aggregate(list(FGSUM2 = data$FGsum2), by=lvar, FUN=sum )
    ag4 = aggregate(list(AFGSQR = data$AFGsqr), by=lvar, FUN=sum )
    ag5 = aggregate(list(FGSQR  = data$FGsqr ), by=lvar, FUN=sum )
    ag6 = aggregate(list(NOBS   = data$num   ), by=lvar, FUN=sum )
    # STD
    ag7 = aggregate(list(FGSQR1 = data$FGsqr1   ), by=lvar, FUN=sum )
    ag8 = aggregate(list(FGSQR2 = data$FGsqr2   ), by=lvar, FUN=sum )
    ag9 = aggregate(list(ASQR1  = data$Asqr1    ), by=lvar, FUN=sum )

    # Prepare statistics for plotting
    #---------------------------------
    # Covariance
    cov          = ag6
    cov$COV.HL   = (ag5$FGSQR/ag6$NOBS) - ((ag2$FGSUM1 * ag3$FGSUM2)/(ag6$NOBS)^2)
    cov$COV.DR.B = cov$COV.HL - ((ag4$AFGSQR/ag6$NOBS) - ((ag1$ASUM1 * ag3$FGSUM2)/(ag6$NOBS)^2))
    cov$COV.DR.R = (ag4$AFGSQR/ag6$NOBS) - ((ag1$ASUM1 * ag3$FGSUM2)/(ag6$NOBS)^2)

    # Standard deviation 
    cov$sigmaFG1 = sqrt( (ag7$FGSQR1/ag6$NOBS) - (ag2$FGSUM1/ag6$NOBS)^2 )
    cov$sigmaFG2 = sqrt( (ag8$FGSQR2/ag6$NOBS) - (ag3$FGSUM2/ag6$NOBS)^2 )
    cov$sigmaA1  = sqrt( (ag9$ASQR1/ag6$NOBS) - (ag1$ASUM1/ag6$NOBS)^2 )

    # Correlation
    cov$COR.HL = cov$COV.HL/(cov$sigmaFG1 * cov$sigmaFG2)
    cov$COR.DR.R = cov$COV.DR.R/(cov$sigmaA1 * cov$sigmaFG2)
    cov$COR.DR.B = cov$COV.DR.B/(cov$sigmaFG1 * cov$sigmaFG2)

    return(cov)"""








#OUT: COV.DR (Desroziers), COV.HL (Hollingsworth/Lonnberg)  DIST, EXP
# R plot 
# plot.horiz(     mondata, c("DIST"), c("COV.DR.B", "COV.DR.R","COV.HL"), legx = c("Desroziers.B", "Desroziers.R","HL"), main=paste(" var ",var, sep=""), xlab="Distance [km]", ylab=expression("Covariance [" ~ K^2 ~ "]"), LCOL.EXP=c("black"), LLWD=c(2,2,2), LLTY=c(2,1,3), LPCH=c(17,20,22), INCL0=TRUE, FIRST=1, CORR=FALSE)

"""    ag1 = aggregate(list(ASUM1  = data$Asum1 ), by=lvar, FUN=sum )
    ag2 = aggregate(list(FGSUM1 = data$FGsum1), by=lvar, FUN=sum )
    ag3 = aggregate(list(FGSUM2 = data$FGsum2), by=lvar, FUN=sum )
    ag4 = aggregate(list(AFGSQR = data$AFGsqr), by=lvar, FUN=sum )
    ag5 = aggregate(list(FGSQR  = data$FGsqr ), by=lvar, FUN=sum )
    ag6 = aggregate(list(NOBS   = data$num   ), by=lvar, FUN=sum )
    # STD
    ag7 = aggregate(list(FGSQR1 = data$FGsqr1   ), by=lvar, FUN=sum )
    ag8 = aggregate(list(FGSQR2 = data$FGsqr2   ), by=lvar, FUN=sum )
    ag9 = aggregate(list(ASQR1  = data$Asqr1    ), by=lvar, FUN=sum )"""



#StatTab$FGsqr1 = aggregate(list(FGSQR1  = FG1*FG1)            , list(dist=ldist), FUN=sum )$FGSQR1
#StatTab$FGsqr2 = aggregate(list(FGSQR2  = FG2*FG2)            , list(dist=ldist), FUN=sum )$FGSQR2
#StatTab$Asqr1  = aggregate(list(ASQR1   = OA1*OA1)            , list(dist=ldist), FUN=sum )$ASQR1
#StatTab$FGsum1 = aggregate(list(FGSUM1  = FG1)                , list(dist=ldist), FUN=sum    )$FGSUM1
#StatTab$FGsum2 = aggregate(list(FGSUM2  = FG2)                , list(dist=ldist), FUN=sum    )$FGSUM2
#StatTab$AFGsqr = aggregate(list(AFGSQR  = OA1*FG2)            , list(dist=ldist), FUN=sum    )$AFGSQR
#StatTab$FGsqr  = aggregate(list(FGSQR   = FG1*FG2)            , list(dist=ldist), FUN=sum    )$FGSQR
#StatTab$num    = aggregate(list(NOBS    = FG2)                , list(dist=ldist), FUN=length )$NOBS

EndTime = datetime.now()
Duration=EndTime - StartTime
print( "Duration" , Duration   )



ShowError()
