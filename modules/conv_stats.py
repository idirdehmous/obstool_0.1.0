#-*-coding:utf-8 -*- 
import os 
os.environ["NUMEXPR_MAX_THREADS"]="256"

import pandas as pd 
import numpy as np 



class DHLStat:
    """
    Class :  Compute the diffrents statistics
             covariance, correlation and standard deviations 
             from FG1, FG2 , OA1 , OA2 in observation space 
             After the method of 
             Desroziers , Hollingsworth,Lonneberg ( DHL )
    """
    def __init__ (self,df , bdist_max=100  , bin_int=10 , time_int=60 ):

        self.max_dist  = bdist_max  # Maxumum distance for binning in [Km]
        self.bin_int   = bin_int    # Binning interval in [Km]
        self.time_int  = time_int   # Time between OmG/OmA  pairs in [+/- min]
        self.ndist_df  = df 
        return None 

    def _mulDf(sefl,  df1 ,  df2 ):
        df=  np.multiply(df1, df2)
        return df

     
    def departuresDf ( self ):

        ndf   =self.ndist_df.query("dist <= "+str(  self.max_dist  )  )
        a1f2  =self._mulDf ( ndf.OA1  ,ndf.FG2    ) 
        f1f2  =self._mulDf ( ndf.FG1  ,ndf.FG2    ) 
        f1f1  =self._mulDf ( ndf.FG1  ,ndf.FG1    )
        f2f2  =self._mulDf ( ndf.FG2  ,ndf.FG2    )
        a1a1  =self._mulDf ( ndf.OA1  ,ndf.OA1    )

        # AFGsqr  FGsqr num    FGsqr1  FGsqr2    Asqr
        df_frame = {  "d1"     :ndf.d1   ,
                      "d2"     :ndf.d2   ,
                      "dist"   :ndf.dist ,
                      "dbin"   :ndf.dbin ,
                      "OA1"    :ndf.OA1  ,
                      "OA2"    :ndf.OA2  ,
                      "FG1"    :ndf.FG1  ,
                      "FG2"    :ndf.FG2  ,
                      "AFGsqr" :a1f2, 
                      "FGsqr"  :f1f2,
                      "FGsqr1" :f1f1, 
                      "FGsqr2" :f2f2,
                      "Asqr1"  :a1a1  
                   }

        stat_df=pd.DataFrame( df_frame   )
        return stat_df  


    def splitDf (self  ):

        max_dist      = self.max_dist
        dist_interval = self.bin_int

        stat_df=self.departuresDf  (   )
        dbin   = [0,1]+list(np.arange(self.bin_int, self.max_dist + self.bin_int , self.bin_int ))
        dlabel = [0  ]+list(np.arange(self.bin_int, self.max_dist + self.bin_int , self.bin_int ))


        # DIVIDE BY DIST INTERVALS 
        pd.cut( stat_df['dist'], bins=dbin , labels=dlabel, right=True, include_lowest=True, retbins=True )

         # NOBS  & DISTS 
        nobs  = stat_df.groupby( "dbin"  )["dist"].count()
        ldist = list(stat_df.groupby( "dbin"  )["dbin"].groups.keys())

        # sum(AO1)  sum(FG1)    sum(FG2)
        # AFGsqr  ->  OA1*FG2
        # FGsqr   ->  FG1*FG2
        # FGsqr1  ->  FG1*FG1
        # FGsqr2  ->  FG2*FG2
        # Asqr1   ->  OA1*OA1

        oa1_sum  = stat_df.groupby( "dbin"  ) ["OA1"   ].sum().reset_index()
        fg1_sum  = stat_df.groupby( "dbin"  ) ["FG1"   ].sum().reset_index()
        fg2_sum  = stat_df.groupby( "dbin"  ) ["FG2"   ].sum().reset_index()
        a1f2_sqrt= stat_df.groupby( "dbin"  ) ["AFGsqr"].sum().reset_index()
        f1f2_sqrt= stat_df.groupby( "dbin"  ) ["FGsqr" ].sum().reset_index()
        f1f1_sqrt= stat_df.groupby( "dbin"  ) ["FGsqr1"].sum().reset_index()
        f2f2_sqrt= stat_df.groupby( "dbin"  ) ["FGsqr2"].sum().reset_index()
        a1a1_sqrt= stat_df.groupby( "dbin"  ) ["Asqr1" ].sum().reset_index()

        # Splitted DF by dist intervals 
        frame_={ "nobs"  :list(nobs)  ,
                "dist"  :list(ldist ) ,
                "Asum1" :oa1_sum.OA1  ,
                "Fsum1" :fg1_sum.FG1  ,
                "Fsum2" :fg2_sum.FG2  ,
                "AFGsqr":a1f2_sqrt.AFGsqr ,
                "FGsqr" :f1f2_sqrt.FGsqr  ,
                "FGsqr1":f1f1_sqrt.FGsqr1 ,
                "FGsqr2":f2f2_sqrt.FGsqr2 ,
                 "Asqr1" :a1a1_sqrt.Asqr1
                     }
        spdf=pd.DataFrame ( frame_)
        return spdf 

    def GroupDF (self, max_ndist=None  , bin_nint=None ):
        # STATS
        # GET THE STATS IN THE MIDDLE OF THE BIN SQUARE  (the maximum distance and bin interval could be changed !!)

        if max_ndist !=None:
           self.max_ndist =  max_ndist 
           print("New value for maximum distance has been set. dist_max =" , max_ndist )
        else:
           self.max_ndist=self.max_dist  

        if bin_nint !=None:
           self.bin_ninterval =  bin_nint 
           print("New value for maximum distance has been set. bin_interval=" , bin_nint)
        else:
           self.bin_ninterval =  self.bin_int

        d_bins    = [ int(i) for i in   np.arange(0,self.max_ndist     + self.bin_ninterval    ,self.bin_ninterval )  ]
        d_label   = [ int(i) for i in   np.arange(self.bin_ninterval/2 , self.max_ndist+self.bin_ninterval  -(self.bin_ninterval/2) , self.bin_ninterval)  ]

        spdf=self.splitDf ()
   
        # DIVIDE BY DIST INTERVALS 
        sd=pd.cut( spdf['dist'], bins=d_bins , labels=d_label , right=True, include_lowest=False)
        spdf["dist"]=sd

        # DISTANCES
        dist_list =spdf.dist.values [1:]

        # Asum1      Fsum1      Fsum2     AFGsqr      FGsqr      FGsqr1      FGsqr2       Asqr
        d1  =spdf.groupby("dist")["Asum1" ].sum()
        
        d2  =spdf.groupby("dist")["Fsum1" ].sum()
        d3  =spdf.groupby("dist")["Fsum2" ].sum()
        d4  =spdf.groupby("dist")["AFGsqr"].sum()
        d5  =spdf.groupby("dist")["FGsqr" ].sum()
        d6  =spdf.groupby("dist")["FGsqr1"].sum()
        d7  =spdf.groupby("dist")["FGsqr2"].sum()
        d8  =spdf.groupby("dist")["Asqr1" ].sum()
        dobs=spdf.groupby("dist")["nobs"  ].sum()

        return d1,d2, d3, d4, d5, d6, d7, d8 , dobs , dist_list 


    def getCovar  (self , inplace=None, max_dist=None , bin_interval =None   ):


        d1,d2, d3, d4, d5, d6, d7, d8 , dobs , dist_list =self.GroupDF (max_dist  , bin_interval )

        # HL (Holingsworth-LÖnnberg )
        t1 =  np.divide  (d5 ,dobs) 
        t2 =  np.divide  ( np.multiply(d2 ,d3  ) , np.power( dobs, 2 ))
        cov_hl=  np.subtract(t1, t2  )

        # Desroziers B 
        tb1 = np.divide( d4, dobs )
        tb2 = np.divide( np.multiply(d1, d3   ) , np.power(dobs, 2 ) )
        drb_ =np.subtract( tb1, tb2 )
        cov_drB =np.subtract(cov_hl , drb_  )

        # Desroziers R 
        tr1 = np.divide  ( d4 , dobs  )
        tr2 = np.divide  (np.multiply( d1 , d3 ) , np.power(dobs , 2 ) )
        cov_drR = np.subtract(tr1 , tr2 )
        
        if inplace ==True :
           return cov_hl , cov_drB, cov_drR 
        cov={ "dist":dist_list  , "COV_HL":cov_hl , "COV_DR-B":cov_drB  , "COV_DR-R": cov_drR   }
        return cov  


    def getSigma ( self , inplace =None ):

        d1,d2, d3, d4, d5, d6, d7, d8 , dobs , dist_list =self.GroupDF ()

        # SIGMA FG1
        st1=np.divide( d6 , dobs)
        st2=np.divide( d2 , dobs)**2
        sigma_fg1=np.sqrt(  np.subtract(st1, st2 ))
        del st1 , st2 

        # SIGMA FG2 
        st1=np.divide( d7 ,dobs )
        st2=np.divide( d3 ,dobs )**2
        sigma_fg2=np.sqrt( np.subtract(st1, st2 ))
        del st1, st2

        # SIGMA A1
        st1=np.divide( d8 ,dobs )
        st2=np.divide( d1 ,dobs )**2
        sigma_a1=np.sqrt( np.subtract(st1, st2  )) 
        if inplace ==True:
           return sigma_fg1 , sigma_fg2, sigma_a1 
        sigma={ "dist":dist_list  , "sigma_fg1":sigma_fg1 , "sigma_fg2":sigma_fg2  , "sigma_a1": sigma_a1   }
        return sigma  

    def getCorr (self, inplace=None ):


        cov_hl, cov_drB, cov_drR =self.getCovar( inplace =True )
        sfg1 , sfg2 , sa1        =self.getSigma( inplace =True )

        # CORRELATIONS
        cor_hl =np.divide( cov_hl , np.multiply( sfg1 , sfg2  ) )
        cor_drB=np.divide( cov_drB, np.multiply( sfg1 , sfg2  ) )
        cor_drR=np.divide( cov_drR, np.multiply( sa1  , sfg2  ) )
        if inplace ==True:
           return cor_hl , cor_drB , cor_drR 

        return cor_hl , cor_drB  , cor_drR 


    def getStatFrame (self ):
        _,_, _, _, _, _, _, _ , dobs , dist_list=self.GroupDF ()
        nobs=np.asarray(dobs )
        cov_hl, cov_drB, cov_drR     =self.getCovar( inplace =True )
        cor_hl, cor_drB, cor_drR     =self.getCorr ( inplace =True )
        sigma_fg1,sigma_fg2,sigma_a1 =self.getSigma( inplace =True )
       


        drhl_frame={ "dist"     :dist_list    , 
                     "nobs"     :nobs         , 
                     "COV_HL"   :cov_hl       , 
                     "COV_DR-B" :cov_drB      ,
                     "COV_DR-R" :cov_drR      ,
                     "sigma_FG1":sigma_fg1,
                     "sigma_FG2":sigma_fg2,
                     "sigma_a1" :sigma_a1 ,
                     "COR_HL"   :cor_hl   , 
                     "COR_DR-B" :cor_drB  , 
                     "COR_DR-R" :cor_drR
                  }

        stat_frame =pd.DataFrame (   drhl_frame  )
        return stat_frame 
