import os, sys  
import pandas as pd


sys.path.insert(0 ,"./modules" ) 


# pyodb modules 
sys.path.insert(0,'/home/idehmous/Desktop/rmib_dev/github/pyodb_1.1.0/build/lib.linux-x86_64-cpython-39' )


from pyodb_extra  import OdbEnv  
from pyodb_extra  import StringParser 



# INIT ENV ( path to libodb.so, no  /lib/ ! )
odb_dir= os.getenv( "ODB_INSTALL_DIR" )
if odb_dir is not None:
   env= OdbEnv ( odb_dir  , "libodb.so")
   env.InitEnv ()



# Obstool modules 
from setting     import Setting 
from utils       import OdbReader , Rows2Df
from handle_df   import  *
from conv_stats  import DHLStat



# ODB PATH (Should contain CCMA as     ODB_PATH/YYYYMMDDHH/CCMA   )
odb_path="/mnt/HDS_ALD_TEAM/ALD_TEAM/idehmous/depot_tampon/METCOOP_ODB"
odb_type="CCMA"

# Period  
bdate="2024010500"
edate="2024010500"

# Only conv for the moment 
obs_category="conv"

# Var list and other 
cycle_inc = 3 
var_list     =["airep_t"] #,  "airep_u" , "airep_v"] #, "airep_v" , "temp_t" ] #"airepl_t"]



# Instantiate    
st = Setting ()
rd = Rows2Df ()
rr = OdbReader(odb_path , odb_type )


# Set period   
period=st.set_period(  bdate, edate  )

# Check the var list  
st.set_obs_list(  var_list     )

# Collection of DataFrames 
dflist= rr.get_odb_rows (period , var_list, cycle_inc , pbar =True , verbosity =2 )

# Concat 
cdf= rd.PrepDf( dflist )


# Set bin and max distance for diagnostics 
max_dist=  80
bin_dist=  10

# Get the stats by var
# 
for var in var_list:
    dhl= DHLStat ( cdf[var], max_dist = max_dist ,bin_dist = bin_dist   )
    print( dhl.getStatFrame( var )  )


quit()
