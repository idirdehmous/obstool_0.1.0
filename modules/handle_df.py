#-*-coding:utf-8 -*- 
import gc  
import pandas as pd 
import numpy  as np 
from   itertools import repeat 

 



__all__=["SplitDf", "ConcatDf", "GroupDf"]


class SplitDf:
    """
        Class :@Split and subset  DF 
    """
    def __init__ (self,dfdist , var , cdtg ,  bdist_max=100  , bin_int=10 , time_int=60 ):
        self.max_dist  = bdist_max  # Maxumum distance for binning in [Km]
        self.bin_int   = bin_int    # Binning interval in   [Km]
        self.time_int  = time_int   # Time between OmG/OmA  pairs in [+/- min]
        self.ndist_df  = dfdist
        self.var       = var  
        self.cdtg      = cdtg  
        return None 


    def _mulDf(sefl,  df1 ,  df2 ):
        df=  np.multiply(df1, df2)
        return df


    def DeparturesDf ( self ):

        ndf= self.ndist_df
        a1f2   =self._mulDf ( ndf.OA1  ,ndf.FG2    ) 
        f1f2   =self._mulDf ( ndf.FG1  ,ndf.FG2    ) 
        f1f1   =self._mulDf ( ndf.FG1  ,ndf.FG1    )
        f2f2   =self._mulDf ( ndf.FG2  ,ndf.FG2    )
        a1a1   =self._mulDf ( ndf.OA1  ,ndf.OA1    )
        a1f1   =self._mulDf ( ndf.OA1  ,ndf.FG1    )

        df_frame = {  "d1"     :ndf.d1   ,
                      "d2"     :ndf.d2   ,
                      "dist"   :ndf.dist ,
                      "OA1"    :ndf.OA1  ,
                      "OA2"    :ndf.OA2  ,
                      "FG1"    :ndf.FG1  ,
                      "FG2"    :ndf.FG2  ,
                      "AFGsqr" :a1f1, 
                      "FGsqr"  :f1f2,
                      "FGsqr1" :f1f1, 
                      "FGsqr2" :f2f2,
                      "Asqr1"  :a1a1  
                   }
        stat_df=pd.DataFrame( df_frame   )
        return stat_df  



    def SubsetDf (self  ):
        max_dist      = self.max_dist
        dist_interval = self.bin_int

        stat_df=self.DeparturesDf()

        dbin   = [0,1]+list(np.arange(self.bin_int, self.max_dist + self.bin_int , self.bin_int ))
        dlabel = [0  ]+list(np.arange(self.bin_int, self.max_dist + self.bin_int , self.bin_int ))

        # DIVIDE BY DIST INTERVALS 
        dfcut  =pd.cut( stat_df['dist'], bins=dbin , labels=dlabel, right=True, include_lowest=True, retbins=True )
        stat_df["dbin"] = dfcut[0]

        # NOBS  & DISTS 
        nobs  = stat_df.groupby     ( "dbin"  , observed=True)["dist"].count()
        ldist = list(stat_df.groupby( "dbin"  , observed=True)["dbin"].groups.keys())
   
        # Col names         
        # sum(AO1)  sum(FG1)    sum(FG2)
        # AFGsqr  ->  OA1*FG2
        # FGsqr   ->  FG1*FG2
        # FGsqr1  ->  FG1*FG1
        # FGsqr2  ->  FG2*FG2
        # Asqr1   ->  OA1*OA1
        oa1_sum  = stat_df.groupby( "dbin"  ,observed=True ) ["OA1"   ].sum().reset_index()
        oa2_sum  = stat_df.groupby( "dbin"  ,observed=True ) ["OA2"   ].sum().reset_index()
        fg1_sum  = stat_df.groupby( "dbin"  ,observed=True) ["FG1"   ].sum().reset_index()
        fg2_sum  = stat_df.groupby( "dbin"  ,observed=True) ["FG2"   ].sum().reset_index()
        a1f2_sqrt= stat_df.groupby( "dbin"  ,observed=True) ["AFGsqr"].sum().reset_index()
        f1f2_sqrt= stat_df.groupby( "dbin"  ,observed=True) ["FGsqr" ].sum().reset_index()
        f1f1_sqrt= stat_df.groupby( "dbin"  ,observed=True) ["FGsqr1"].sum().reset_index()
        f2f2_sqrt= stat_df.groupby( "dbin"  ,observed=True) ["FGsqr2"].sum().reset_index()
        a1a1_sqrt= stat_df.groupby( "dbin"  ,observed=True) ["Asqr1" ].sum().reset_index()

        # Splitted DF by dist intervals 
        var_col=[ self.var   for v in range(len( oa1_sum ) ) ]
        dte_col=[ self.cdtg  for v in range(len( oa1_sum ) ) ]

        frame_={"var"    :var_col      , 
                "date"   :dte_col      ,
                "nobs"   :list(nobs)   ,
                "dist"   :list(ldist ) ,
                "Asum1"  :oa1_sum.OA1  ,
                "FGsum1" :fg1_sum.FG1  ,
                "FGsum2" :fg2_sum.FG2  ,
                "AFGsqr" :a1f2_sqrt.AFGsqr ,
                "FGsqr"  :f1f2_sqrt.FGsqr  ,
                "FGsqr1" :f1f1_sqrt.FGsqr1 ,
                "FGsqr2" :f2f2_sqrt.FGsqr2 ,
                "Asqr1"  :a1a1_sqrt.Asqr1
                     }
        spdf=pd.DataFrame ( frame_)
        
        return spdf 





class ConcatDf:
    """
    Class : @Concat DF 
    """
    def __init__(self ):
        return None 


    def ConcatByDate (self, sub_df  ):
        merged_dict={}
        merged_df  =pd.DataFrame ()
        for k , v in  sub_df.items():
            merged_df    =pd.concat ( v ) 
            merged_dict[k]=merged_df.reset_index().drop(columns =["index"])
        del merged_df
        gc.collect()
        return merged_dict




class GroupDf:
    """
    Class : Group DF  rows
    """
    def __init__(self  ):
        return None 

    def GroupByBins (self, merged_df    , max_dist=100  ,  bin_int =10 ):
        self.max_ndist    = max_dist
        self.bin_ninterval= bin_int

        if max_dist != 100:
           self.max_ndist     =  max_dist
        if bin_int != 10 :
           self.bin_ninterval =  bin_int

        d_bins    = [ int(i) for i in   np.arange(0,self.max_ndist     + self.bin_ninterval    ,self.bin_ninterval )  ]
        d_label   = [ int(i) for i in   np.arange(self.bin_ninterval/2 , self.max_ndist+self.bin_ninterval  -(self.bin_ninterval/2) , self.bin_ninterval)  ]


        # DIVIDE BY DIST INTERVALS 
        cut_series  , used_bins  = pd.cut( merged_df ['dist'], bins=d_bins , labels=d_label, retbins=True  )
        merged_df["mdist"] = cut_series  # Middle bins 

         
        # Needed Columns:Asum1,FGsum1,FGsum2,AFGsqr, FGsqr, FGsqr1, FGsqr2, Asqr
        d1  =merged_df.groupby("mdist", observed=False)["Asum1"  ].sum()
        d2  =merged_df.groupby("mdist", observed=False)["FGsum1" ].sum()
        d3  =merged_df.groupby("mdist", observed=False)["FGsum2" ].sum()
        d4  =merged_df.groupby("mdist", observed=False)["AFGsqr" ].sum()
        d5  =merged_df.groupby("mdist", observed=False)["FGsqr"  ].sum()
        d6  =merged_df.groupby("mdist", observed=False)["FGsqr1" ].sum()
        d7  =merged_df.groupby("mdist", observed=False)["FGsqr2" ].sum()
        d8  =merged_df.groupby("mdist", observed=False)["Asqr1"  ].sum()
        dobs=merged_df.groupby("mdist", observed=False)["nobs"   ].sum()

   
        var=merged_df["var" ].values[0 ]
        dt1=merged_df["date"].values[0 ]
        dt2=merged_df["date"].values[-1]
        
        # Distances 
        dist_list =merged_df ["mdist"]

        # Send the prepared DF to 
        # compute the obstool statistics 
        return d1,d2, d3, d4, d5, d6, d7, d8 , dobs , dist_list , dt1, dt2 , var
