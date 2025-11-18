# -*- coding:utf-8 -*- 
import os ,sys  

# From pyodb  
from pyodb_extra  import StringParser 


__all__=["SqlHandler"]



class SqlHandler:
    """
    Class : @ Parse the different observation dictionaries and build the 
              SQL query according to obstype, codetype, varno , sensor and level range  
            @ Checks the SQL statement before sending. 
    """
    def __init__(self ):

        # For the moment !
        self.obstool_select=None 

        #self.derozie_select=None
        #self.jarvine_select=None
        return None 


    def BuildQuery(self , **kwarg ):
        self.cols      =",".join(kwarg["columns" ])[1:]
        self.tabs      =",".join(kwarg["tables"  ])
        self.obs_      =kwarg["obs"]
        self.varno     =kwarg["obsvano"]
        self.ctype     =kwarg["codetype"]
        self.obst      =kwarg["obstype"]
        self.levels    =kwarg["lrange"]
        self.vtype     =kwarg["vertco_type"]
        self.vert_coord=kwarg["vertco"]
        self.sensor    =kwarg["sensor"        ]
        self.other     =kwarg["remaining_sql"]

        # Obstool columns and tables 
        self.obstool_select="SELECT  "+self.cols+" FROM " +self.tabs

        where_cond =" "
        obst_cond  =" " 
        var_cond   =" "
        ctype_cond =" "
        level_cond =" "
        sens_cond  =" "

        self.obstool_select = self.obstool_select + where_cond   

        # Check varno  
        if self.varno is not None:
           if isinstance (self.varno , int  ) :
              var_cond = " varno =="+ str(self.varno)
           else:
              print("varno must be integer and not None" )
              sys.exit()
        else:
           var_cond=" "

       
        # Check obstype  
        if self.obst is not None:
           if isinstance (self.obst, int ) :   # It could be list ??    we add it later !
              obst_cond  = " obstype=="+ str(self.obst )
           else:
              print("obstype  must be integer and not None" )
              sys.exit()
        else:
           obst_cond=" "

        
        # Check codetype 
        if self.ctype is not None:
            if isinstance( self.ctype,  list  )  or  isinstance (self.ctype ,  int  ):
               ctype=  [ " codetype =="+str(item)   for item in  self.ctype ] 
               ctype_cond = " OR ".join (ctype)
            else:
               print("codetype  must be integer or list "  )
               sys.exit()
        else:
           ctype_cond=" "


        # Check sensor 
        if self.sensor != None:
            if isinstance( self.ctype,  list  )  or  isinstance (self.ctype ,  int  ):
               sens =  [ " sensor =="+str(item)   for item in  self.sensor ]
               sen_cond  = " OR ".join( sens  )
            else: 
               print("sensor  must be integer or list "  )
        else:
            sens_cond=" " 

        # Check level range 
        if self.levels != None :
          if len (self.levels) <2  or  len( self.levels ) > 2:
             print ( "Level range must have two limites   [l1 , l2], but got argument:", self.levels  )
             sys.exit(0)
          elif not isinstance (self.levels[0], int ) or not isinstance ( self.levels [1], int):
             print( "One or the both level limites in the list  is/are not intger(s)" )
             sys.exit(0)
          elif self.levels[1] < self.levels[0]:
             print( "Bottom level limit can't be greater than top level limit" )
             sys.exit(0)
          else:
             l1 =str(self.levels[0] )
             l2 =str(self.levels[1] )
             level_cond="vertco_reference_1 >="+l1+" AND vertco_reference_1 <= "+l2 
        else:
          level_cond =" "

        # Build the "Where" sql statement 
        cond       = [ var_cond, obst_cond ,ctype_cond , level_cond ]
        cond_list  = [ x.strip() for x in cond if x.strip() != ''   ]

        if len(cond_list):
           where_cond = " AND ".join(  cond_list )


        # Add the other additional conditions 
        #query=" "
        if   self.other !=None and len(where_cond) == 0:
           query=self.select_obstool +" WHERE  "+self.other
        elif self.other !=None and len(where_cond) > 0:
           query=self.obstool_select +" WHERE  "+where_cond+"  AND "+self.other    
        elif self.other ==None and len(where_cond) > 0:
           query=self.obstool_select +" WHERE  "+where_cond 
        return  query  



    def CheckQuery(self  , query   ):
        p      =StringParser()
        nfunc  =p.ParseTokens ( query  )     # N Columns=N pure columns - N functions in the query 
        sql    =p.CleanString ( query  ) 
        return nfunc ,  sql 
