import sys , os 


class ObsType:
      """
      Class: Contains the  necessary obs list to run 
         obstool , Jarvinen and Desroziers diags 
      """

      def _init__(self):
          # ObsType lists 
          self.conv_obs=[    'gpssol' ,
                        'synop'  ,
                        'dribu'  ,
                        'airep'  ,
                        'airepl' ,
                        'radar'  ,
                        'temp'   ,
                        'templ'  ]
          self.sat_obs = [    'amsua'  ,
                         'amsub'  ,    
                         'atms'   ,   
                         'iasi'   ,
                         'mwhs'   ,
                         'msh'    ,
                         'seviri' ]
          return None 





      def ConvDict(self) :
          # CONVENTIONAL  
          self.obs_conv=[

         {"obs_name"            : "gpssol",
           "obstype"           : 1  ,
           "codetype"          : 110 , 
           "varno"             : 128  ,
           "vertco_reference_1": None,
           "sensor"            : None,
           "level_range"       : None         } ,
 
 
         { "obs_name"          :  "synop",
           "obstype"           : 1 ,
           "codetype"          : [11, 14, 170, 182] ,
           "varno"             : [1, 42, 41, 58, 39],
           "vertco_reference_1": None,
           "sensor"            : None,
           "level_range"       : None          },
 
         { "obs_name"          : "dribu",
           "obstype"           : 4 ,
           "codetype"          : None,
           "varno"             : [1, 39, 41, 42],
           "vertco_reference_1": None,
           "sensor"            : None,
           "level_range"       : None          },
 
         { "obs_name"          : "ascat",
           "obstype"           : 9 ,
           "codetype"          : None,
           "varno"             : None,
           "vertco_reference_1": None,
           "sensor"            : None,
           "level_range"       : None           } ,
 
        {  "obs_name"          : "radar",
           "obstype"           : 13,
           "codetype"          : None,
            "varno"             : [29, 195],
           "vertco_reference_1": None,
           "sensor"            : None,
           "level_range"       : None            },
 
        {  "obs_name"          : "airep",
           "obstype"           :  2 ,
           "codetype"          : None,
           "varno"             : [2, 3, 4] ,
           "vertco_reference_1": None,
           "sensor"            : None,
           "level_range"       : None           },
 
        {  "obs_name"          : "airepl" ,
           "obstype"           : 2 ,
           "codetype"          : None,
           "varno"             : [2, 3, 4],
           "vertco_reference_1": None,
           "sensor"            : None,
           "level_range"       : [25000, 35000] },
 
         { "obs_name"          : "temp",
           "obstype"           : 5,
           "codetype"          : None,
           "varno"             : [2, 3, 4, 7],
           "vertco_reference_1": None,
           "sensor"            : None,
           "level_range"       : None           },
 
         { "obs_name"          : "templ" ,
           "obstype"           : 5 ,
           "codetype"          : None,
           "varno"             : [2, 3, 4, 7] ,
           "vertco_reference_1": None,
           "sensor"            : None,
           "level_range"       : [40000, 60000]} ]
     
          self.varno_dict ={ 128 : "ztd"  ,  # gpssol Zenith total delay 
                       1   :  "z"   ,  # geopotential 
                       42  :  "u"   ,  # 10m wind speed u
                       41  :  "v"   ,  # 10m wind speed v
                       58  :  "h"   ,  # 2 relative humidity
                       39  :  "t"   ,  # 2 meter temperature 
                       2   :  "t"   ,  # T temperature       (upper air)
                       3   :  "u"   ,  # U component of wind (upper air)
                       4   :  "v"   ,  # V component of wind (upper air)
                       7   :  "q"   ,  # specific humidity 
                       29  :  "rh"  ,  # upper rh (radar)
                      195  :  "dw"     # Dopp radial wind 
                      }
          return self.obs_conv  , self.varno_dict
 

      def RenameVarno (self, string_var  ):
          dict_={}
          obslist , dict_            = self.ConvDict()
          #if obs_kind == 'satem':  obslist , _, _, _, dict_   = self.SatDict ()
         

          obsname=[  dobs["obs_name"] for dobs in obslist  ]
          name      =    string_var.split("_")[0]
          vr        =int(string_var.split("_")[1] )
          vname     =name + "_"+ dict_[vr]
          return vname 



      def SelectConv(self, list_   ):
          varobs=[]
          self.list          = list_
          obs_list, var_dict =self.ConvDict()
          for lst in self.list  :
            for k, v in lst.items():
              code =lst["codetype"]
              varno=lst["varno"   ]
              if  code == None and varno == None:
                 if lst["obs_name"] not in varobs:
                    varobs.append( lst["obs_name"] )
                 else:
                    continue              
              elif isinstance( code ,list) and isinstance( varno  ,list):  #  ( list, list)
                  for c in code:
                      for v in varno :
                          obsId = lst["obs_name"]+"_"+var_dict[v]
                          if obsId  not in varobs:
                             varobs.append( obsId   )
              elif isinstance( code , list ) and isinstance( varno  ,int):  # (list,  int
                  for c  in code:
                          obsId = lst["obs_name"]+"_"+var_dict[varno] 
                          if obsId  not in varobs:
                             varobs.append( obsId  )
              elif isinstance( code , int  ) and isinstance( varno  , list ): # (int , list)
                  for v   in varno:
                        obsId =lst["obs_name"]+"_"+var_dict[v]
                        if obsId not in varobs:
                           varobs.append( obsId )
              elif isinstance ( code, int )  and isinstance ( varno , int  ):  # (int , int ) 
                  obsId = lst["obs_name"]+"_"+var_dict[varno]
                  if obsId not in varobs:
                     varobs.append( obsId )
              elif isinstance( code ,list) and varno == None:                 #  (list , None )
                  for c in code:
                      obsId = lst["obs_name"]+"_c"+str(c) 
                      if obsId not in varobs:
                         varobs.append(obsId) 
              elif code ==None and  isinstance( varno  , list ):      # ( None , list ) 
                  for v in varno:
                      obsId =lst["obs_name"]+"_"+var_dict[v]
                      if obsId not in varobs:
                         varobs.append(obsId) 
              elif code ==None and  isinstance( varno  , int ):      # (None , int )
                  obsId = lst["obs_name"]+"_"+var_dict[lst["varno"] ]
                  if obsId not in varobs:
                     varobs.append(  obsId )
          return  varobs  , obs_list

