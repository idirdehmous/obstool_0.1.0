import matplotlib.pyplot as plt 
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






def PlotDf ( sdf, varname  ):

    fig, (ax1, ax2 , ax3) =  plt.subplots( 3,1  , figsize=( 10, 13 ) )
    sdf.plot(  x="dist"  , y="COV_HL"    , ax=ax2  , label="COV_HL"   , lw=2)
    sdf.plot(  x="dist"  , y="COV_DR-B"  , ax=ax2  , label="COV_DR-B" , lw=2)
    sdf.plot(  x="dist"  , y="COV_DR-R"  , ax=ax2  , label="COV_DR-R" , lw=2)
    ax2.set_ylabel("Covariance ")
    sdf.plot(  x="dist"  , y="COR_HL"    , ax=ax1  , label="COR_HL"   , lw=2, xlabel="Distance [Km]")
    sdf.plot(  x="dist"  , y="COR_DR-B"  , ax=ax1  , label="COR_DR-B" , lw=2, xlabel="Distance [Km]")
    sdf.plot(  x="dist"  , y="COR_DR-R"  , ax=ax1  , label="COR_DR-R" , lw=2, xlabel="Distance [Km]")

    ax1.set_ylabel("Correlation " )
    ax1.set_xlabel( "Distance [Km]" )
    ax1.set_ylim( -1 ,1 )
    ax1.axhline(y = 0.2, color = 'r', linestyle = '--')

    sdf.plot.bar ( x="dist"   , y="nobs" , ax=ax3 , label="Nobs", color="grey")
    ax3.set_xlabel( "Distance [Km]" )
    #plt.show() 
    plt.savefig(varname+".png")
    #return fig





# ODB PATH (Should contain CCMA as     ODB_PATH/YYYYMMDDHH/CCMA   )
odb_path="/mnt/HDS_ALD_TEAM/ALD_TEAM/idehmous/depot_tampon/METCOOP_ODB"
odb_type="CCMA"

# Period  
bdate="2024010500"
edate="2024011021"

# Only conv for the moment 
obs_category="conv"

# Var list and other 
cycle_inc = 3 
#var_list  =["gpssol" ]
#var_list  =[ "synop_z" , "synop_v", "synop_u" ,"synop_h","synop_t"] 
#var_list  =["dribu_z" , "dribu_t"  , "dribu_u", "dribu_v"] 
var_list  =["airep_t" , "airep_u"  , "airep_v"] 
#var_list  =["radar_rh"] #, "radar_dow"]
#var_list  =["temp_t"  , "temp_u"   , "temp_v" , "temp_q"]



# Set bin and max distance for diagnostics 
max_dist=  100
bin_dist=  10



# Instantiate    
st = Setting ()
gp = GroupDf ()
rd = Rows2Df ()
rr = OdbReader(odb_path , odb_type )


# Set period   
period=st.set_period(  bdate, edate  )

# Check the var list  
st.set_obs_list(  var_list     )

# Collection of pre-selected frames with distances <= max_dist    
dlist= rr.get_odb_rows (period ,var_list, max_dist ,bin_dist,   cycle_inc , pbar =True , verbosity =0 )

# Concat Df 
cdf = rd.PrepDf( dlist )
# Get the stats by var
for var  in var_list:
    dhl= DHLStat ( cdf[var] , max_dist = max_dist ,bin_dist = bin_dist   )
    cov= dhl.getCov( var , inplace=False )
    sig= dhl.getSig( var , inplace=False )
    cor= dhl.getCor( var , inplace=False )
    diag=dhl.getStatFrame(var) 
    print(  diag  )
    PlotDf (  diag , var )


quit()
