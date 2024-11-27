import sys , os 
class ObsType:
 """
  Class: Contains the  necessary obs list to run 
         obstool , Jarvinen and Desroziers diags 
         NOT FINISHED !
         MAYBE IT HAS TO SPLITTED INTO DATA CATEGORIES !
 """

 def _init__(self, obs_name=None  ):
     self.name=obs_name 
     return None 
 def ObsDict(self) :
    self.obs_list=[

         {"obs_name"            : "gpssol",
           "obstype"           : 1  ,
           "codetype"          : 110 , 
           "varno"             : 128  ,
           "vertco_reference_1": None,
           "sensor"            : None,
           "level_range"       : None         } ,
 
         { "obs_name"          : "synop",
           "obstype"           : 1  ,
           "codetype"          : [11, 14, 170, 182] ,
           "varno"             : None,
           "vertco_reference_1": None,
           "sensor"            : None,
           "level_range"       : None          },
 
         { "obs_name"          :  "synop_v",
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
 
        {  "obs_name"          : "airep_l",
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
 
         { "obs_name"          : "temp_l",
           "obstype"           : 5 ,
           "codetype"          : None,
           "varno"             : [2, 3, 4, 7] ,
           "vertco_reference_1": None,
           "sensor"            : None,
           "level_range"       : [40000, 60000]},
 
         { "obs_name"          : "amsua",
           "obstype"           : 7 ,
           "codetype"          : None,
           "varno"             : None,
           "vertco_reference_1": None,
           "sensor"            : 3,
           "level_range"       : None     },
 
         { "obs_name"          : "amsub",
           "obstype"           : 7 ,
           "codetype"          : None,
           "varno"             : None,
           "vertco_reference_1": None,
           "sensor"            : 4 ,
           "level_range"       : None     },
 
         { "obs_name"          : "msh",
           "obstype"           :  7 ,
           "codetype"          : None,
           "varno"             : None,
           "vertco_reference_1": None,
           "sensor"            : "15 ",
           "level_range"       : None     },
 
         { "obs_name"          : "iasi",
           "obstype"           : 7,
           "codetype"          : None,
           "varno"             : None,
           "vertco_reference_1": None,
           "sensor"            : 16,
           "level_range"       : None     },
 
         { "obs_name"          : "atms",
           "obstype"           : 7 ,
           "codetype"          : None,
           "varno"             : None,
           "vertco_reference_1": None,
           "sensor"            : 19 ,
           "level_range"       : None     },
 
          {"obs_name"          : "mwhs" ,
           "obstype"           : 7 ,
           "codetype"          : None,
           "varno"             : None,
           "vertco_reference_1": None,
           "sensor"            : 73 ,
           "level_range"       : None      },
 
         { "obs_name"          : "seviri",
           "obstype"           : 7 ,
           "codetype"          : None,
           "varno"             : None,
           "vertco_reference_1": None,
           "sensor"            : 29,
           "level_range"       : None      }   ]
    return self.obs_list


 def UpdateDict():
     """

        Method :Update dictionnary obstype if needed 
        we use a static dictionnary for the moment !
     """
     print("Empty Method !" )
     pass 
     return None


 def SelectObs(self, list_):

     varobs=[]
     self.list    = list_
     for lst in self.list  :
         for k, v in lst.items():
             code =lst["codetype"]
             varno=lst["varno"]

             if  code == None and varno == None:
                 if lst["obs_name"] not in varobs:
                    varobs.append( lst["obs_name"] )
                 else:
                    continue 

             elif isinstance( code ,list) and isinstance( varno  ,list):  #  ( list, list)
                  for c in code:
                      for v in varno :
                          obsId = lst["obs_name"]+"_c"+str(c)+"_v"+str(v) 
                          if obsId  not in varobs:
                             varobs.append( obsId   )


             elif isinstance( code , list ) and isinstance( varno  ,int):  # (list,  int
                  for c  in code:
                          obsId = lst["obs_name"]+"_c"+str(c)+"_v"+str(lst["varno"] ) 
                          if obsId  not in varobs:
                             varobs.append( obsId  )


             elif isinstance( code , int  ) and isinstance( varno  , list ): # (int , list)
                  for v   in varno:
                        obsId =lst["obs_name"]+"_c"+str(lst["codetype"])+"_v"+str(v)
                        if obsId not in varobs:
                           varobs.append( obsId )


             elif isinstance ( code, int )  and isinstance ( varno , int  ):  # (int , int ) 
                  obsId = lst["obs_name"]+"_c"+str(code)+"_v"+str(varno)
                  if obsId not in varobs:
                     varobs.append( obsId )


             elif isinstance( code ,list) and varno == None:                 #  (list , None )
                  for c in code:
                      obsId = lst["obs_name"]+"_c"+str(c) 
                      if obsId not in varobs:
                         varobs.append(obsId) 


             elif code ==None and  isinstance( varno  , list ):      # ( None , list ) 
                  for v in varno:
                      obsId =lst["obs_name"]+"_v"+str(v) 
                      if obsId not in varobs:
                         varobs.append(obsId) 


             elif code ==None and  isinstance( varno  , int ):      # (None , int )
                  obsId = lst["obs_name"]+"_v"+str(lst["varno"] ) 
                  if obsId not in varobs:
                     varobs.append(  obsId )


             elif isinstance( code ,int ) and varno == None:        # (int , None ) 
                  obsId =lst["obs_name"]+"_c"+str(lst["codetype"] ) 
                  varobs.append ( obsId  )



     #for obs in self.attrib:
     #    obs_dict= { k:v  for k , v in  zip ( self.keys, obs  )  }
     #    dict_list.append( obs_dict  )


     return  varobs 





