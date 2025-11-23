# -*-coding :utf-8 -*- 
import os , sys  , gc  
import pandas as pd 
import numpy  as np 
from   collections import defaultdict  
import multiprocessing as mp  
from   multiprocessing import Pool , cpu_count,  shared_memory 
from itertools import chain 


sys.path.insert(0,"/home/idehmous/Desktop/rmib_dev/github/pyodb_1.1.0/build/lib.linux-x86_64-cpython-39")

from pyodb_extra  import OdbEnv
env= OdbEnv ("/home/idehmous/Desktop/rmib_dev/github/pkg", "libodb.so")
env.InitEnv ()


# pyodb modules 
from pyodb_extra.odb_ob    import  OdbObject  


env= OdbEnv ("/home/idehmous/Desktop/rmib_dev/github/pkg", "libodb.so")
env.InitEnv ()

# --> NOW pyodb could be imported  !
import pyodb   
from   pyodb   import  odbDict , odbGcdistance
from   pyodb   import  odbConnect , odbClose 
from   pyodb   import  odbDca  



# Obstool, Desroziers & Jarvinen Tools &  modules 
from build_sql       import SqlHandler
from obstype_info    import ObsType
from setting         import Setting  , Conv
from handle_df       import *




__all__=["DataIO", "DCAFiles", "OdbReader", "Rows2Df", "DistMatrix"]






class DataIO:
    """
    Class:  @DataIO : Contains methods to write and read the statstics dataframes  
                      for each cycle 
                      It uses the format 'feather' . 
                                  Fast for I/O 
                                  Compatible with pandas 
    """



    def __init__(self, compression="lz4"):
        # Write frames into feather format file with  either "lz4" or "zstd" compression  
        self.compression = compression  
        if self.compression not in  ["ls4", "zstd"]:
           self.compression = "ls4"    # Fallback to default


    # WRITE method   
    def FlushFrame(self, df,  subdir, cdtg, var, fid=None, verbose =None ):
        """
        Flush a DataFrame to Feather format with lz4 compression algo'.
        Naming based on var, cdtg
        """

        vrb = verbose 

        if df is None or df.empty:
            print("Warning : Empty DataFrame for var={} datatime={}, nothing written".foamt( var , cdtg  ) )
            return None

        # Final directory:  dbpath / subdir / cdtg
        # Write in the  working directory  
        outdir = Path(subdir) / cdtg
        outdir.mkdir(parents=True, exist_ok=True)

        if fid is not None:
            filename = "_".join(  (var,cdtg,fid ) ) +".feather"
        else:
            filename = "_".join(  (var,cdtg))   +".feather"

        filepath = "/".join(  (outdir , filename)   )

        try:
            df.to_feather(filepath, compression=self.compression)
            if vrb in  [2,3]:
               print( "The dataframe has been written into {} for var {} and date/time {}", (filepath, var , cdtg    ))
        except Exception:
               print( "ERROR writing dataframe : var ={} and date/time ={}", ( var , cdtg    ))
               return None
        return str(filepath)




    #  READ  method   
    def ReadFrame(self, dir_  , cdtg, var=None, verbose =None ):
        """
        @ Read all Feather files inside   dir_/cdtg.
              If var is provided, filters only matching files.
        @ Returns a single concatenated DataFrame.
        """

        vrb = verbose 
        directory = Path(dir_) / cdtg
        if not directory.exists():
            if vrb in  [2,3]:
               print( "Directory does not exist: {}".format( directory) )
               return None

        # File pattern
        if var is None:
            files = sorted(directory.glob("*.feather"))
        else:
            files = sorted(directory.glob(f"{var}_{cdtg}*.feather"))

        if not files:
            print( "No feather files found for cdtg={}, var={}".format(cdtg  , var   ))
            return None

        frames = []
        for f in files:
            try:
                df = pd.read_feather(f)
                frames.append(df)
                if vrb in [2,3]:
                   print( "Dataframe loaded with : {} rows)".format( df.shape[0] ))
            except Exception:
                print( "ERROR reading {}".format(f )  )

        if not frames:
            return None

        # Concatenate all chunks
        all_df = pd.concat(frames, axis=0, ignore_index=True)
        print( "TOTAL: {} rows merged".format(  all_df.shape[0] )) 

        return all_df 





class DCAFiles:
    """
    Class :  @Handles, checks and creates the DCA (Direct Access Column )files 
             if they are not already in the ODB 

             @If the DCA files are already there , this class is not called 
    """
    def __init__(self):
        return None

    def CheckDca( self,   dbpath , sub_base=None   , verbose = False   ):
        # Prepare DCA files if not  in ODB 
        db      = OdbObject ( dbpath )
        dbname  = db.GetAttrib()["name"]
        
        if not os.path.isdir ("/".join(  [dbpath , "dca"] )  ):
           if verbose in [2,3 ]:
              print( "No DCA files in {} 'directory'".format(dbpath ) )
           env.OdbVars["CCMA.IOASSIGN"]="/".join(  (dbname, "CCMA.IOASSIGN" ) )
           env.OdbVars["ECMA.IOASSIGN"]="/".join(  (dbname, "ECMA.IOASSIGN" ) )
           status =    odbDca ( dbpath=dbpath , db=dbname , ncpu=8  )
           if status < 0 :
               print("Failed to create DCA files ... \n Another attempt will be done with odbDict !" )
        else :
           if  verbose ==True :
               print("DCA files already in database: '{}'".format( dbname )  )
           else:
               pass 







class OdbReader:
    """
    Class : @Prepare the query according to the user setting (Obs list , period etc)
            The SQL query is sent directly to the ECMA or CCMA ODB .

            @Returns data are as python dictionary 
    """
    def __init__(self , dbpath ,type_  ):
       # Path to the directory containing the ODB(s)
       self.odb_path = dbpath    
       self.odb_type = type_

       if  not os.path.isdir (self.odb_path): 
           print("ODB(s) directory '{}' not found.".format( self.odb_path ))  
           sys.exit(0)

       # Setting 
       self.st    = Setting ()
        
       # DCA Files 
       self.dca_f = DCAFiles()

       # CONV 
       self.conv      = Conv()
       self.conv_obs  = self.conv.conv_obs
       self.cols      = self.conv.cols 
       self.tables    = self.conv.tables
       self.other_sql = self.conv.other_sql
    
       # SQL      
       self.sql   =SqlHandler()

       # DF 
       self.rd    =Rows2Df () 
       self.cnt   =ConcatDf() 

       # return a list of dicts
       self.dlist = defaultdict(list)




    def get_odb_rows  (self,     period      ,      
                                 obs_list    ,
                                 max_dist    ,
                                 bin_dist    ,
                                 cycle_inc  =3, 
                                 chunk_size = None , 
                                 pbar       =False , 
                                 verbosity  =0):
        vrb=verbosity
        if vrb not in [0,1,2,3]:
           print("Min and max verbosity levels: 0 ->  3.  Got :  ", vrb )
           print("Fallback to default value:  verbosity=", vrb  )  


        # Default dict  (Collect dataframes with variable as a key )
        df_vars =  defaultdict(list)

        # Diags period 
        period_interval=  [ min( period ) , max(period)  ]

        # ODB  Paths 
        paths =  [ "/".join( (self.odb_path , prd , self.odb_type )) for prd in period  ]

        # Obs attributes 
        obs_names , codetype , obstype , varno , lrange , vertco ,sensor =self.st.set_obs_list(obs_list  )

        list_dict=[]
        for i, cma_path in enumerate(paths):
            # UPDATE IOASSIGN , ODB_SRCPATH & ODB_DATAPATH
            os.environ["IOASSIGN"]=cma_path+"/IOASSIGN"
            os.environ["ODB_SRCPATH_CCMA"] =cma_path
            os.environ["ODB_DATAPATH_CCMA"]=cma_path
            os.environ["ODB_IDXPATH_CCMA" ]=cma_path

            # Check DCA directory  (if not there they will be created )
            dca_f=DCAFiles()
            dca_f.CheckDca ( cma_path  )

            if vrb == [ 2 , 3]:
               print("ODB PATHS set to  :")
               print("IOASSIGN          :",cma_path+"IOASSIGN" )
               print("ODB_SRCPATH_CCMA  :",cma_path )
               print("ODB_DATAPATH_CCMA :",cma_path )
               print("ODB_IDXPATH_CCMA  :",cma_path )
            
            for jo, obs in enumerate(obs_names) :

                query=self.sql.BuildQuery(  columns       =self.cols      ,
                                                 tables        =self.tables    ,
                                                 obs           =obs            ,
                                                 obstype       =obstype  [jo]  ,
                                                 obsvano       =varno    [jo]  , 
                                                 codetype      =codetype [jo]  ,                                                  
                                                 lrange        =lrange   [jo]  ,
                                                 vertco_type   ="height"       ,
                                                 vertco        =vertco   [jo]  , 
                                                 sensor        =sensor   [jo]  ,
                                                 remaining_sql =self.other_sql )

                nfunc , sql_query = self.sql.CheckQuery( query)
                cdtg =  period [i]
                if vrb in [0, 1, 2, 3]:
                   print( "Process observation   type: {} ODB:   date {} ".format( obs , cdtg   ))                       
                query_file=None   ;
                poolmask = None   ; 
                pool      =None   ; 
                float_fmt = 10    ;   # 10 digits float values  
                verbose = False   ;
                pbar    = False 

                # Progress bar & Verbosity inside pyodb  
                # Progress bar is very useful for huge ODBs
                if vrb in [3  ]: 
                   verbose= True 
                if vrb in [2,3]: 
                   pbar   = True 

                try:
                   rows=   odbDict (cma_path  ,
                                      sql_query , 
                                       nfunc     , 
                                       float_fmt , 
                                       query_file, 
                                       poolmask  , 
                                       pbar      ,
                                       verbose )
                        
                   df_dist = self.rd.DfDist (  rows    , obs, cdtg, max_dist , bin_dist)
                   self.dlist[obs].append( df_dist  )
                except:
                   pyodb.EmptyResultError
                   print( "Warning : SQL request returned no data for variable {} ->  varno={}".format( obs , varno[jo])  )
                   # Append an empty DF  
                   self.dlist[obs].append( pd.DataFrame([] ) )
                   pass        
        return self.dlist  







# The methods belows are ouside classes 
# Since they may be used by multiple processes 
def CreateSharedMfile  (name, size):
    # Delete and recreate the shared memory file if exists 
    # under  /dev/shm 
    try:
        # If exists close and delete 
        old = shared_memory.SharedMemory(name=name)  # unlnk and close 
        old.close()
        old.unlink()
    except FileNotFoundError:
        pass
    return shared_memory.SharedMemory(name=name, create=True, size=size)




def GcdistChunkShared (i0, i1, N):
    """
    @Method :    Worker shared memory .
                 Only receives i0, i1 and global N.
                 Accesses shared memory numpy arrays directly.
    @returns:   The matrix chunked blocks and begin/end index  
    """

    # 
    CreateSharedMfile
    shm_lon = shared_memory.SharedMemory(name="shm_lons")
    shm_lat = shared_memory.SharedMemory(name="shm_lats")

    # Recreate numpy  arrays as buffers 
    lon = np.ndarray((N,), dtype=np.float64, buffer=shm_lon.buf)
    lat = np.ndarray((N,), dtype=np.float64, buffer=shm_lat.buf)

    # Get latlon by chunk indices 
    sub_lon = lon[i0:i1]
    sub_lat = lat[i0:i1]

    # The method for computing the great circle distances is written in C 
    # The C code is more or less same as the one used in R "sp" package   
    # odbGcdistance is a function inside the pyodb package 
    block = odbGcdistance(sub_lon, sub_lat, lon, lat)

    # Close memory buffers handle 
    shm_lon.close()
    shm_lat.close()
    return (i0, i1, block)




class DistMatrix:
    """@Class  : DistMatrix computes the interdistances between all 
                 the latlon pairs  ( for N coordinates > 3000 , the process starts to slow down)

                 Approach to speed up  : Divide the coordinates arrays into chuncks and 
                 compute each block in a sparated process (CPU)

       @Returns: A reconstructed matrix of all distances between lalon pairs. 

    """

    def __init__(self, lons, lats):
        self.lons = np.asarray(lons, dtype=np.float64)  # Init with 64 bits 
        self.lats = np.asarray(lats, dtype=np.float64)
        self.N = len(self.lons)

    def GcdistParallel(self, chunk_size=200, workers=None):
        N = self.N
        lon = self.lons
        lat = self.lats

        # If the number of CPUs is set otherwise get from the machine 
        if workers is None:
           workers = mp.cpu_count()

        # Create shared memory file
        #shm_lons = shared_memory.SharedMemory(create=True, size=lon.nbytes, name="shm_lons")
        #shm_lats = shared_memory.SharedMemory(create=True, size=lat.nbytes, name="shm_lats")

        # More Safe
        shm_lons =CreateSharedMfile ( size=lon.nbytes  , name="shm_lons" )
        shm_lats =CreateSharedMfile ( size=lon.nbytes  , name="shm_lats" )

        # Copy data once
        np.ndarray(lon.shape, dtype=np.float64, buffer=shm_lons.buf)[:] = lon
        np.ndarray(lat.shape, dtype=np.float64, buffer=shm_lats.buf)[:] = lat

        # Chunk ranges 
        chunk_ranges = [(i0, min(i0 + chunk_size, N))  for i0 in range(0, N, chunk_size)]

        # Init the final matrix  
        distmat = np.zeros((N, N), dtype=np.float64)
        try:
            with mp.Pool(processes=workers) as pool:                
                # Use starmap method and give chunk ranges as args 
                results = pool.starmap( GcdistChunkShared   , [(i0, i1, N) for (i0, i1) in chunk_ranges])

            # Fill the matrix by block 
            for (i0, i1, block) in results:
                distmat[i0:i1] = block
        finally:
            # Cleanup and unlink memory
            shm_lons.close()
            shm_lons.unlink()
            shm_lats.close()
            shm_lats.unlink()

        return distmat






class Rows2Df :

    """
    Class:   @Build the dataframes with O-G and O-A departures products. 
             @To speed up the computation of the distances between the 
              latlon pairs , the Great Circle distance function has been 
              wrapped in the C  inside pyodb  module.   

             @The returned DF is a subset of the whole 1st one such as dist<= max_dist
    """

    def __init__(self):
        #self.gc =  gcDistance ()
        return None 



    def DfDist (self , rows ,obs, cdtg,   max_dist ,bin_dist,   verbosity =0 ):
        nn= 200
        #pd.set_option('display.max_rows',  20 )
        vrb=verbosity
        if vrb not in [0,1,2,3]:
            print("WARNING : Min and max verbosity levels: 0 ->  3.  Got :  ", vrb )
            print("Fallback to default value:  verbosity=", vrb  ) 

        if rows is None:
           print("Rows from ODB not available for  var :  {}".format(var ) )
           sys.exit()
        else:
           df_rows = pd.DataFrame(rows )
           
           lats  = np.array(rows["degrees(lat)" ])#[0:nn])
           lons  = np.array(rows["degrees(lon)" ])#[0:nn])
           an_d  = np.array(rows["an_depar@body"])#[0:nn])
           fg_d  = np.array(rows["fg_depar@body"])#[0:nn])
           
           # ARGS order  :  lons1, lats1, lon2, lat2 : Compute distances between latlon pairs 
           d= DistMatrix (lons , lats  )           
           dist = d.GcdistParallel ()
           
           # With pool and chunks = 400  worker =16 , time = 1m20s 
           # With chunks and only nchunks = 200 using the buffrs and shared memory , 
           # the computation is reduced to 10 seconds   ( The most of the process time is wasted in the copy )

           # Reshape to flatten array 
           dist_1d = dist.reshape(len(lats)**2)
           
           # Indices 
           # Optimization : ( Use numpy instead of itertools generators  'product' )
           N = len(lats)
           d1, d2 = np.indices((N, N))
           d1     = d1.ravel().astype(np.int32, copy=False)
           d2     = d2.ravel().astype(np.int32, copy=False)
         
           dist_frame= pd.DataFrame( {"d1"   : d2 ,
                                      "d2"   : d1 ,
                                      "dist" : dist_1d })

           # Subset again 
           df_dist = dist_frame[dist_frame["dist"] <= max_dist  ]
           ndist_copy = df_dist.copy()
           ndist_copy ["OA1"] = an_d[df_dist["d1"].values]
           ndist_copy ["OA2"] = an_d[df_dist["d2"].values]
           ndist_copy ["FG1"] = fg_d[df_dist["d1"].values]
           ndist_copy ["FG2"] = fg_d[df_dist["d2"].values]

           # Optimze a little   32 float rather than 64 bits 
           ndist_copy  = ndist_copy.astype({
                       "d1"  : "int32"  , 
                       "d2"  : "int32"  ,
                       "dist": "float32",
                       "OA1" : "float32", 
                       "OA2" : "float32",
                       "FG1" : "float32", 
                       "FG2" : "float32"
             })
           # Subset  <= max_dist  and cut 
           spl_df =  SplitDf ( ndist_copy  , obs , cdtg , max_dist, bin_dist )
           sub_df =  spl_df.SubsetDf ()

           # Save the df conaining the necessary stats fir the Hollingworth/Loneberg +Desroziers methods 
           # Use pickle  format . It is binary , easy to put a df and has a high compression ratio .
#           if save_df = True:
              
           return sub_df 


    def PrepDf   (self , dlist ):                         
        # Concat df by  vars  
        cnt=  ConcatDf ()
        cdf=  cnt.ConcatByDate (dlist)
        return cdf   

