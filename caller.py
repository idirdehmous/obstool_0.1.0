import pandas as pd
import multiprocessing as mp  
import os 

import sys 
sys.path.insert(0 ,"./modules" ) 
# pyodb modules 
#sys.path.insert(1 ,"/home/idehmous/Desktop/rmib_dev/github/pyodb_1.2.0/build/lib.linux-x86_64-cpython-39" )
sys.path.insert(0,'/home/idehmous/Desktop/rmib_dev/github/pyodb_1.1.0/build/lib.linux-x86_64-cpython-39' )


from pyodb_extra  import OdbEnv  
from pyodb_extra  import StringParser 

# INIT ENV ( path to libodb.so, no  /lib/ ! )
odb_dir= os.getenv( "ODB_INSTALL_DIR" )
if odb_dir is not None:
   env= OdbEnv ( odb_dir  , "libodb.so")
   env.InitEnv ()
#else:
#  print("Failed to find the path to libodb.so. use 'export ODB_INSTALL_DIR=/where si /bin, include and lib/../' ")



from setting     import Setting 
from utils       import OdbReader , Rows2Df
from handle_df   import  *
from conv_stats  import DHLStat



# ODB PATH (Should contain CCMA as     ODB_PATH/YYYYMMDDHH/CCMA   )
odb_path="/mnt/HDS_ALD_TEAM/ALD_TEAM/idehmous/depot_tampon/METCOOP_ODB"
odb_type="CCMA"
obs_category="conv"
cycle_inc = 3 
var_list     =["airep_t" , "airep_u" , "airep_v"] #, "airep_v" , "temp_t" ] #"airepl_t"]
#var_list     =["templ_t"]


# Set   
st = Setting ()
rd = Rows2Df ()
rr = OdbReader(odb_path , odb_type )

# PERIOD 
bdate="2024010500"
edate="2024010503"

# PERIOD DATES LIST 
period=st.set_period(  bdate, edate  )

# List of concerned obs and varobs ( varobs = obsname_varno , or obsname_sensor )
st.set_obs_list(  var_list     )

dflist= rr.get_odb_rows (period , var_list   , cycle_inc , reprocess_odb=True,chunk_size=None , pbar =True , verbosity =2 )

cdf= rd.PrepDf( dflist )

for var in var_list:
    dhl= DHLStat ( cdf[var], new_max_dist = 80   )
    print( dhl.getCov( var  ) )


#print( df_var ) 
quit()
#from  obstool    import  Setting  
#from  obstool    import  ExtractData
#from  conv_stats import  DHLStat
