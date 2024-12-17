import matplotlib.pyplot as plt 
import pandas as pd  




def PlotDf ( sdf, varname  ):

    fig, (ax1, ax2 , ax3) =  plt.subplots( 3,1  , figsize=( 10, 13 ) )
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
    plt.savefig(varname+".png")

