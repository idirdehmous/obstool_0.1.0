class ObsType:
    def __init__(self):
        return None 


    def GenDict(self,  keys , attrib_list):
        dict_list=[]
        self.keys    =keys
        self.attrib  =attrib_list
        for obs in self.attrib:
            obs_dict= { k:v  for k , v in  zip ( self.keys, obs  )  }
            dict_list.append( obs_dict  )
        return dict_list 



