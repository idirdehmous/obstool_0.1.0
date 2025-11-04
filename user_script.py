import matplotlib.pyplot as plt
import matplotlib as mpl 
import pandas as pd
import multiprocessing as mp  
import os 

import sys 
sys.path.insert(0 ,"/hpcperm/cvah/tuning/ww_oslo/obstool_mp/modules" ) 
from  obstool    import  Setting  
from  obstool    import  ExtractData
from  conv_stats import  DHLStat



# ODB PATH (Should contain CCMA as     ODB_PATH/YYYYMMDDHH/CCMA   )
odb_path="/hpcperm/cvah/tuning/odbs"  

obs_category="conv"

# SAT OBS types list 
#'amsub'  ,
#'atms'   ,
#'iasi'   ,
#'mwhs'   ,
#'mwhs2'  ,
#'msh'    ,
# obstype =["seviri" ] 


# CONV OBS types list 
# gpssol
# synop  
# dribu  
# airep  
# airepl 
# radar  
# temp   
# templ  

obstype =["airep" , "radar"]



# Init Objects 
st  =Setting ()
ext =ExtractData(odb_path ) 


# PERIOD 
bdate="2024010500"
edate="2024010503"

# PERIOD DATES LIST 
period=st.set_period(  bdate, edate  )

# List of concerned obs and varobs ( varobs = obsname_varno , or obsname_sensor )
varobs , obs_list = st.set_obs_list(obstype     )

# It s possible to use chunks for  huge ODB(s)
ext.get_odb_rows (period, obs_list  ,  reprocess_odb=True , chunk_size=16  , verbosity=2)

quit()


# Proceed to Diagnostics and plots 
#frames = diag.get_frames( period  , 'conv',   obs_list  ,odb_path )

#stat   = diag.dhl_stats ( frames , new_max_dist = 100, new_bin_dist= 10)
#for st in stat:
#    print( st )
