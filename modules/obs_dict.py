class ObsTypes:
  """
  Class: Contains the  necessary obs list to run 
         obstool , Jarvinen and Desroziers diags 
         NOT FINISHED !
         MAYBE IT HAS TO SPLITTED INTO DATA CATEGORIES !
  """

 def _init__(self, obs_name ):
    
    self.name = obs_name 

    obs_list=[

         {"obs_name"            : "gpssol",
           "obstype"           : 1  ,
           "codetype"          : 110 , 
           "varno"             : 128  ,
           "vertco_reference_1": None,
           "sensor"            : None,
           "level_range"       : None         } ,
 
#         { "obs_name"          : "synop",
#           "obstype"           : 1  ,
#           "codetype"          : [11, 14, 170, 182] ,
#           "varno"             : None,
#           "vertco_reference_1": None,
#           "sensor"            : None,
#           "level_range"       : None          },
 
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
    return None 


    def UpdateDict():
        """

        Method :Update dictionnary obstype if needed 
        we use a static dictionnary for the moment !
        """
        print("Empty Method !" )
        pass 
        return None 

