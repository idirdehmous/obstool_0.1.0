import os,sys
from   ctypes import cdll , CDLL

# CREATED MODULES 
#sys.path.insert(0,"./modules" )

from pyodb_extra.odb_glossary import  OdbLexic
from pyodb_extra.parser       import  StringParser
from pyodb_extra.environment  import  OdbEnv
from pyodb_extra.odb_ob       import  OdbObject
from pyodb_extra.exceptions   import  *
from pyodb_extra.pool_factory import  PoolSlicer

from extractor   import SqlHandler , OdbCCMA , OdbECMA

odb_install_dir=os.getenv( "ODB_INSTALL_DIR" )
env= OdbEnv(odb_install_dir, "libodb.so")
env.InitEnv ()

from pyodb  import   odbDca


class Dca:
    def __init__():
        """
        Possible to move it somewhere !!!
        """
        return None 

    def CreateDca(  path , sub_base=None     ):
        # Prepare DCA files if not there 
        dbpath  = path
        db      = OdbObject ( dbpath )
        dbname  = db.GetAttrib()["name"]
        if dbname == "ECMA" and sub_base != None :
           dbpath  =".".join( [path, sub_base ]   )


        if not os.path.isdir ("/".join(  [dbpath , "dca"] )  ):
           print( "No DCA files in {} 'directory'".format(dbpath ) )
           env.OdbVars["CCMA.IOASSIGN"]="/".join(  (dbname, "CCMA.IOASSIGN" ) )
           status =    odbDca ( dbpath=dbpath , db=dbname , ncpu=8  )
        else :
           print(  "DCA files generated already in database: '{}'".format( dbname )  )
        return status

