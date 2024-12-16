#-*- coding :utf-8 -*- 

from   collections import defaultdict 
from   itertools   import product
from   pandas      import DataFrame , cut 
import numpy as np
from itertools import  *  

class DfFromRows:
      def __init__(self):
          return None 



      def BindDist(self , d1 , d2, matdist ):
          # Swap d1 and d2 to match the same indices in R (  idx -1 )
          dfdist = DataFrame(  {
                                 "d1"  : d2 , 
                                 "d2"  : d1 ,
                                 "dist": matdist  }) 
                     
          dfdist['d1']   = dfdist['d1'].astype('uint32')
          dfdist['d2']   = dfdist['d2'].astype('uint32')
          dfdist['dist'] = dfdist['dist'].astype('float32')
          return dfdist 



      def BindDepars (self, data_df , ndist_df ):
             
          # COPY THE DF , AVOID PANDAS WARNING. WORKING ON A SLICED DataFrame
          ndf=ndist_df.copy()

          ndf.loc[:, 'varname'] = data_df.loc[ndf['d1'], 'var'].values
          ndf.loc[:, 'date']= data_df.loc[ndf['d1'],     'date'].values.astype('int32')
          ndf.loc[:, 'OA1'] = data_df.loc[ndf['d1'], 'an_depar'].values.astype('float32')
          ndf.loc[:, 'OA2'] = data_df.loc[ndf['d2'], 'an_depar'].values.astype('float32')
          ndf.loc[:, 'FG1'] = data_df.loc[ndf['d1'], 'fg_depar'].values.astype('float32')
          ndf.loc[:, 'FG2'] = data_df.loc[ndf['d2'], 'fg_depar'].values.astype('float32')
          return ndf  



      def CutDf  (self, ndf   ,  bin_max_dist , bin_int):
          lDint = list(np.arange(bin_int, bin_max_dist +bin_int , bin_int ))
          cDint = list(np.arange(bin_int, bin_max_dist +bin_int , bin_int))

          # Partitions over bins inplace !
          # Binning by  bin_int  Km 
          dbin   =[0,1]+lDint
          dlabel =[0  ]+cDint
          dbin_serie   =cut(  ndf['dist'], bins=dbin , labels=dlabel, right=True, include_lowest=True, retbins=True )
          ndf["dbin"] =dbin_serie[0] 
          return ndf  



      def Rows2Bins(self, stats , 
                         lats , 
                         lons , 
                         an_depar , 
                         fg_depar , 
                         varobs,
                         cdtg ,     bin_max_dist=100 , bin_int=10 ):
          """
          Method: Split the distance/departures matrix into 
                  bins with bins intrvals  
                  default : bin_int          = 10  Km 
                          : maximum distance = 100 Km
          """
          list_df=[]
          df_dict=defaultdict(list)
          for k , v in stats.items():         
                  if k in varobs:  
                     vvar= [ k for i in  range(len(stats[k] )) ] 
                     stat= stats   [k]
                     llat= lats    [k]
                     llon= lons    [k]
                     an_d= an_depar[k]
                     fg_d= fg_depar[k]

                     matdist =    MatrixDist( llon, llat  , k )

                     idx=[]
                     [  idx.append(i)   for i in  product(range(len(llon)) , repeat=2) ]
                     d1=[ i[0] for i in idx ]
                     d2=[ i[1] for i in idx ]

                     dfdist= self.BindDist ( d1 , d2 , matdist    )
                     
                     # THEY CAN HAVE A CONSIDERABLE MEMORY SIZE 
                     # FREE MEMORY 
                     del d1 
                     del d2 
                     del matdist 

                     # SPLIT DF BY BINS (<= 100km by default)
                     ndist_df=  dfdist.query("dist <=  "+str(bin_max_dist) )
                     del dfdist 

                     # Add the other data
                     # Date & varname 
                     d_iter     =repeat ( cdtg , len(llon) )
                     v_iter     =repeat ( k    , len(llon) )
                     dte  =[ int(i) for i in  d_iter   ]
                     var  =[ i for i in  v_iter   ]

                     # Df for departures
                     data_df = DataFrame(   { "date"    :list(dte ) , 
                                              "var"     :list(var)  , 
                                              "statid"  :stat ,      
                                              "lat"     :llat ,
                                              "lon"     :llon , 
                                              "an_depar":an_d,
                                              "fg_depar":fg_d})
                     data_df["date"    ]=data_df["date"    ].astype('int32')

                     # BIND DISTS AND DEPARTURES 
                     ndf= self.BindDepars ( data_df , ndist_df)
                     del data_df 
                     del ndist_df 

                     # Binning
                     cdf= self.CutDf( ndf, bin_max_dist , bin_int)
                     df_dict[k].append( ndf  )
          # RETURNS A LIST OF DICT WITH varname AS KEY !
          return df_dict
