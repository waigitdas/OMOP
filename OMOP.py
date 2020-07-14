#######################################################################################
#######################################################################################
##  Code initiated by David A Stumpf, MD, PhD #########################################
##  Professor Emeritus, Northwestern University Feinberg School of Medicine ###########
##  consulting@woodstockHIT.com #######################################################
##  Provisioned via https://github.com/waigitdas/OMOP for the OMOP/OHDSI communities ##
##  Copyright 2020 to protect from unauthorized use ###################################
#######################################################################################
#######################################################################################

# This is the python Start code. 

import SQLLib
import OMOPLib
import os
import pyodbc

#Archival: no longer in use
#SQLLib.BCPUpload("sct2_Description_Full_CanadianEdition_20200331","C:\\VBNet_Output\\Databases\\Canada InfoWays\\SnomedCT_Canadian_EditionRelease_PRODUCTION_20200331\\Full\\Terminology\\sct2_Description_Full_CanadianEdition_20200331.txt")    
#SQLLib.CreateBCPXML()
#SQLLib.BCPUpload("sct2_Description_Full_CanadianEdition_20200331","C:\\VBNet_Output\Databases\Canada InfoWays\SnomedCT_Canadian_EditionRelease_PRODUCTION_20200331\Full\Terminology|sct2_Description_Full_CanadianEdition_20200331.txt") 

SQLLib.LoadOMOPtoSQLServer()
OMOPLib.LoadOMOPtoNeo4j()
SQLLib.LoadSNOMEDtoSQLServer()

print("Done!")