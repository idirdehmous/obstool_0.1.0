#-*- codig:utf-8 -*- 
import os ,sys  
sys.path.insert(  0, "./modules" )
from   ctypes     import cdll , CDLL
from pyodb_extra.environment  import OdbEnv  
from pyodb_extra.parser       import StringParser 





class SqlHandler:
    def __init__(self ):
        return None 
    def BuildQuery(self , **kwarg ):
        self.cols      =",".join(kwarg["columns" ])[1:]
        self.tabs      =",".join(kwarg["tables"  ])
        self.obs_      =kwarg["obs_dict"]
        self.has_level =kwarg["has_levels"]
        self.vtype     =kwarg["vertco_type"]
        self.other     =kwarg["remaining_sql"]
        
        self.obs_name  =self.obs_["obs_name" ]
        self.obst      =self.obs_["obstype" ]
        self.ctype     =self.obs_["codetype"]
        self.varno     =self.obs_["varno"   ]
        self.vertco    =self.obs_["vertco_reference_1"]
        self.levels    =self.obs_["level_range"  ]
        #self.p         =self.obs_["pressure_range"]
        self.sensor    =self.obs_["sensor"        ]

        # NO query has a pressure range 
        # set it to None for the moment 
        self.p  =None 
        self.vertco_sat = None 

        if self.obst != None:
           if isinstance (  self.obst, list) and len(self.obst) > 1 :
              self.otype= [ "obstype =="+str(v) for v in self.obst  ]
              self.obst = " AND ".join( self.otype  )
           else:
              self.obst= "obstype== "+str(self.obst)
        else:
           self.obst =None


        if self.varno != None :
           if isinstance ( self.varno , list ):
              # FOR THE MOMENT WE CONSIDER  ONLY "OR" CONDITION FOR  varno  !
              if len(self.varno) ==1:
                 self.varno="varno== "+str(self.varno[0])
              else:
                 self.varno =[ "varno ==" + str(v) for v in self.varno ]
                 self.varno ="("+ " OR ".join(  self.varno  ) +")"
           elif isinstance ( self.varno , int  ):
              self.varno="varno== "+str(self.varno)
        else:
           self.varno=None
     

        if self.ctype != None: 
           if isinstance (  self.ctype, list) and len(self.ctype) > 1 :
              self.codetype= [ "codetype =="+str(v) for v in self.ctype  ]
              self.type= " OR ".join( self.codetype  )
           else:
              self.type= "codetype== "+str(self.ctype)
        else:
           self.type =None 
           

        if self.sensor != None:
           if isinstance (  self.sensor, list) and len(self.sensor) > 1 :
              self.s     = [ "codetype =="+str(v) for v in self.sensor  ]
              self.sensor= " OR ".join( self.s  )
           else:
              self.sensor=" sensor=="+str(self.sensor)  
        else:
           self.sensor=None 

        if self.has_level ==True :
           if self.vtype != None :
              if self.vtype not in ["height", "pressure", "satem"]:
                 print("Vertical coordinate vertco_type can only be 'height','pressure','satem' but argument :", self.vtype  )
                 sys.exit(0)
                 
           #if self.levels != None :
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

        #if self.has_level:
           if self.p != None :
              if len (self.p) <2  or  len( self.p ) > 2:
                 print ( "Pressure levels range must have two limites   [p1 , p2 ] but got argument:", self.p )
                 sys.exit(0)
              elif not isinstance (self.p[0], int ) or not isinstance ( self.p[1], int):
                 print( "One or the both pressure level limites in the list  is/are not intger(s)" )
                 sys.exit(0)
              else:
                 p1 =str(self.p[0] ) 
                 p2 =str(self.p[1] )
                 press_cond="vertco_reference_2 >="+p1+" AND vertco_reference_2 <= "+ p2 
           else:
              press_cond="  "
        
        
        #if self.has_level:
           if self.vertco != None :
              #print( "ToDo  , split sublist if multiple intervals "  )
              if self.vertco_sat != None:
                 if len (self.vertco_sat) <2  or  len( self.vertco_sat ) > 2:
                    print ( "Satem levels range must have two limites   [p1 , p2 ] but got argument:", self.p )
                    sys.exit(0)
                 elif not isinstance (self.vertco_sat [0], int ) or not isinstance ( self.vertco_sat [1], int):
                    print( "One or the both satallite level limites in the list  is/are not intger(s)" )
                    sys.exit(0)
                 else:
                    vsat1 =str(self.vertco_sat [0] )
                    vsat2 =str(self.vertco_sat [1] )
                    sat_cond="vertco_reference_2 >="+vsat1+" AND vertco_reference_2 <= "+ vsat2
              else:
                 vsat_cond="  "

        select_statement="SELECT  "+self.cols+" FROM " +self.tabs
        where_cond=""
        obs_cond=[self.obst, self.type , self.varno , self.sensor ]
        ii=0 
        for i in range(len(obs_cond )):
            if obs_cond[i] == None :
               pass 
            if obs_cond[i] != None :
               ii=ii+1 
               if ii== 1:
                  where_cond = where_cond+obs_cond[i]
               else:
                  where_cond = where_cond+" AND "+obs_cond[i]
        
        if self.has_level:
           if self.vtype == "height" and  ii==0:              
              where_cond =where_cond +" "+level_cond
           elif self.vtype == "height" and  ii>0:
              where_cond = where_cond+" AND "+level_cond 
            

           if self.vtype =="pressure" and ii==0:
              where_cond  =where_cond +" "+press_cond
           elif self.vtype =="pressure" and ii>0:
              where_cond  =where_cond +" AND "+press_cond


        query=select_statement +" WHERE "+where_cond
        if   self.other !=None and len(where_cond) == 0:
           query=select_statement +" WHERE  "+self.other
        elif self.other !=None and len(where_cond) > 0:
           query=select_statement +" WHERE  "+where_cond+"  AND "+self.other            
        return query   



    def CheckQuery(self  , query   ):
        p      =StringParser()
        nfunc  =p.ParseTokens ( query  )     # N Columns  = N pure columns + N functions in the query 
        sql    =p.CleanString ( query  ) 
        return nfunc ,  sql  

