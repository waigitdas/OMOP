import SQLLib
import OMOPLib
import os
import pyodbc

#SQLLib.BCPUpload("sct2_Description_Full_CanadianEdition_20200331","C:\\VBNet_Output\\Databases\\Canada InfoWays\\SnomedCT_Canadian_EditionRelease_PRODUCTION_20200331\\Full\\Terminology\\sct2_Description_Full_CanadianEdition_20200331.txt")            #
#SQLLib.CreateBCPXML()

#SQLLib.BCPUpload("sct2_Description_Full_CanadianEdition_20200331","C:\\VBNet_Output\Databases\Canada InfoWays\SnomedCT_Canadian_EditionRelease_PRODUCTION_20200331\Full\Terminology|sct2_Description_Full_CanadianEdition_20200331.txt")            #CreateBCPXML()

SQLLib.LoadOMOPtoSQLServer()
#OMOPLib.LoadOMOPtoNeo4j()
#SQLLib.LoadSNOMEDtoSQLServer()

print("Done!")