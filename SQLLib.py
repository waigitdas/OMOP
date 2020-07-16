#######################################################################################
#######################################################################################
##  Code initiated by David A Stumpf, MD, PhD #########################################
##  Professor Emeritus, Northwestern University Feinberg School of Medicine ###########
##  consulting@woodstockHIT.com #######################################################
##  Provisioned via https://github.com/waigitdas/OMOP for the OMOP/OHDSI communities ##
##  Copyright 2020 to protect from unauthorized use ###################################
#######################################################################################
#######################################################################################

import pyodbc
import csv
import ast
import Neo4jLib
import OMOPLib
import os
import bcp
from bcp import DataFile

#def ConnStr(db="OMOP"):
#    return "'Driver={SQL Server};' 'Server=DAS2019\DAS2018;' 'Database=" + db + ";' 'Trusted_Connection=yes;'"

def QueryExecute(Q,db="OMOP"):
    #executes query with no return. Used to bulk insert, etc.
    conn = pyodbc.connect(driver='{SQL Server}', server='DAS2020\DAS2020',trusted_connection='yes', database=db)

    try:
        cursor = conn.cursor()
        cursor.execute(Q)
        cursor.commit()
        #print("Bulk Insert ended without exception")
    except Exception as e:
      print(e)

    finally:
        cursor.commit()
        cursor.close()
        #print("Bulk insert had an exception!")

    #for row in cursor:
    #    print(row)

def Query(Q,db="OMOP"):
    #query returns csv file.
    conn = pyodbc.connect(driver='{SQL Server}', server='DAS2020\DAS2020',trusted_connection='yes', database=db)

    cursor = conn.cursor()
    cursor.execute(Q)
    cur=cursor.fetchall()

    c=""
    for i in range(0,len(cursor.description)):
        c = c + cursor.description[i][0]   #.replace("'","").replace(")","").replace(",","").rstrip()
        if i<len(cursor.description)-1:
            c =  c + ","
        else: c=c + "\n"

    for row in cur:
        c = c + str(row).replace("(","").replace(")","") + "\n"

    return c

def QueryWriteCSV(Q,FN,delimiter="|"):
    #query returns csv file.
    fw=open(FN,"w")
    conn = pyodbc.connect('Driver={SQL Server};'
                          'Server=DAS2020\DAS2020;'
                          'Database=OMOP;'
                          'Trusted_Connection=yes;')

    cursor = conn.cursor()
    cursor.execute(Q)
    cur=cursor.fetchall()

    c=""
    for i in range(0,len(cursor.description)):
        c = c + cursor.description[i][0]   #.replace("(","").replace(")","").replace(",","").rstrip()
        if i<len(cursor.description)-1:
            c =  c + delimiter
        else: c=c + "\n"
    fw.write(c)

    for row in cur:
        #c = c + str(row).replace("(","").replace(")","") + "\n"
        #fw.write(c)
        fw.write(str(row).replace("(","").replace(")","").replace(", ",delimiter).replace(",",delimiter).replace("'","") + "\n")
    fw.flush()
    fw.close()


def CreateNodeCSVLoadQuery():
    #one time transition function to create the LOAD CSV queries need to load Neo4j with previously created CSV getDirFiles
    c=

def CreatsNodeCSVLoadQueryString():
    conn = pyodbc.connect('Driver={SQL Server};'
                          'Server=DAS2020\DAS2020;'
                          'Database=OMOP;'
                          'Trusted_Connection=yes;')

    cursor = conn.cursor()
    cursor.execute(SQLQuery)
    data=cursor.fetchall()

    c="["
    for i in range(0,len(cursor.description)):
        c = c + chr(34) + cursor.description[i][0] + chr(34)
        if i<len(cursor.description)-1:
            c =  c + ","
        else: c=c + "]"

    h = ast.literal_eval(c)

   return h

def QueryToNeo4jNodes(NodeLabel,FN,SQLQuery):
    #query returns csv file.
    conn = pyodbc.connect('Driver={SQL Server};'
                          'Server=DAS2020\DAS2020;'
                          'Database=OMOP;'
                          'Trusted_Connection=yes;')

    cursor = conn.cursor()
    cursor.execute(SQLQuery)
    data=cursor.fetchall()

    c="["
    for i in range(0,len(cursor.description)):
        c = c + chr(34) + cursor.description[i][0] + chr(34)
        if i<len(cursor.description)-1:
            c =  c + ","
        else: c=c + "]"

    h = ast.literal_eval(c)


    with open(Neo4jLib.ImportDir + FN + ".csv", 'w', newline='') as f_handle:
        #https://docs.python.org/3/library/csv.html
        writer = csv.writer(f_handle, delimiter='|')
        # Add the header/column names
        writer.writerow(h)
        # Iterate over `data`  and  write to the csv file
        for row in data:
            try:
                writer.writerow(row)
            except:
                x=0 #d nothing

    nq=Neo4jLib.Neo4jCVSLoadFromSQLCursorForNodes(NodeLabel,FN,cursor.description)
    Neo4jLib.UploadWithPeriodicCommit(nq)

def QueryToNeo4jEdges(FN,SQLQuery,CypherLoadStatement):
    #query returns csv file.
    conn = pyodbc.connect('Driver={SQL Server};'
                          'Server=DAS2020\DAS2020;'
                          'Database=OMOP;'
                          'Trusted_Connection=yes;')

    cursor = conn.cursor()
    cursor.execute(SQLQuery)
    data=cursor.fetchall()

    c="["
    for i in range(0,len(cursor.description)):
        c = c + chr(34) + cursor.description[i][0] + chr(34)
        if i<len(cursor.description)-1:
            c =  c + ","
        else: c=c + "]"

    h = ast.literal_eval(c)


    with open(Neo4jLib.ImportDir + FN + ".csv", 'w', newline='') as f_handle:
        #https://docs.python.org/3/library/csv.html
        writer = csv.writer(f_handle, delimiter='|')
        # Add the header/column names
        writer.writerow(h)
        # Iterate over `data`  and  write to the csv file
        for row in data:
            writer.writerow(row)

    nq="USING PERIODIC COMMIT 10000 LOAD CSV WITH HEADERS FROM 'file:///" + FN + ".csv' as line FIELDTERMINATOR '|' " + CypherLoadStatement
    Neo4jLib.UploadWithPeriodicCommit(nq)

#def QueryRowProcess(row):
#    s=""
#    for i in range(0,len(row)):
#        if type(row[i]) is int:
#            q=""
#            s= s + q + str(row[i]) + q
#        else:
#            q=chr(34)
#            s= s + q + row[i] + q
#        if i<len(row)-1:
#                s=  s + ","
#        #else: s = s + "\n"
#    return s



def LoadOMOPtoSQLServer():
    #Create SQL Tables from OMOP uploads
    #https://raw.githubusercontent.com/OHDSI/CommonDataModel/v5.2.2/Sql%20Server/OMOP%20CDM%20ddl%20-%20SQL%20Server.sql
    fr=open("C:\\Data\\Consulting\\MATTER\\Neuropath\\Horizon 2020\\OHDSI\\OMOP Test Data\\SQLTableCreateScript.txt","r")
    t=fr.read()
    QueryExecute(t)
    fr.close

    #read list of csv files and load contents to SQL server
    fl=OMOPLib.getDirFiles("C:\\Data\Consulting\\MATTER\\Neuropath\\Horizon 2020\OHDSI\\OMOP Test Data\\","csv")
    for i in range(0,len(fl)-1):
        fn=OMOPLib.FileNmFromFullPath(fl[i]).replace(".csv","")
        Q= "BULK INSERT " + fn + " FROM '" + fl[i] + "' WITH (FIELDTERMINATOR = '\t', ROWTERMINATOR = '\n' )"
        QueryExecute(Q)
        print(fn)

    Q= "BULK INSERT concept FROM '" + "C:\\Data\Consulting\\MATTER\\Neuropath\\Horizon 2020\OHDSI\\OMOP Test Data\\concept.csv" + "' WITH (FIELDTERMINATOR = '\t', ROWTERMINATOR = '\n' )"
    QueryExecute(Q)


    #create indices
    fr=open("C:\\Data\\Consulting\\MATTER\\Neuropath\\Horizon 2020\\OHDSI\\OMOP Test Data\\SQLIndexCreateScript_DAS.txt","r")
    t=fr.read()
    QueryExecute(t)
    fr.close

def LoadSNOMEDtoSQLServer():
    #CreateSNOMEDSQLTable("sct2_Description_Full_en_US1000124_20200301")
    #CreateSNOMEDSQLTable("sct2_Description_Full_CanadianEdition_20200331")

    #QueryExecute("CREATE INDEX idx_sct2_Description_Full_CanadianEdition_20200331_term ON sct2_Description_Full_CanadianEdition_20200331 (term)",db="Med_Vocab_2020")
    #QueryExecute("CREATE INDEX idx_sct2_Description_Full_CanadianEdition_20200331_conceptId ON sct2_Description_Full_CanadianEdition_20200331 (conceptId)",db="Med_Vocab_2020")

    #QueryExecute("CREATE INDEX idx_sct2_Description_Full_en_US1000124_20200301_conceptId ON sct2_Description_Full_en_US1000124_20200301 (conceptId)",db="Med_Vocab_2020")
    #QueryExecute("CREATE INDEX idx_sct2_Description_Full_en_US1000124_20200301_term ON sct2_Description_Full_en_US1000124_20200301 (term)",db="Med_Vocab_2020")

    Q= "BULK INSERT sct2_Description_Full_en_US1000124_20200301 FROM 'C:\\VBNet_Output\\Databases\\UMLS\\SnomedCT_USEditionRF2_PRODUCTION_20200301T120000Z\\Full\\Terminology\\sct2_Description_Full-en_US1000124_20200301.txt' WITH (FIELDTERMINATOR = '\t', ROWTERMINATOR = '\n' )"
    QueryExecute(Q,"Med_Vocab_2020")

    #Q= "BULK INSERT sct2_Description_Full_CanadianEdition_20200331 FROM 'C:\\VBNet_Output\\Databases\\Canada InfoWays\\SnomedCT_Canadian_EditionRelease_PRODUCTION_20200331\\Full\\Terminology\\sct2_Description_Full_CanadianEdition_20200331.txt' WITH (FIELDTERMINATOR = '\t', ROWTERMINATOR = '\n', codepage='65001')"  #widechar=unicode  DATAFILETYPE='widechar',
    #QueryExecute(Q,"Med_Vocab_2020")

def CreateSNOMEDSQLTable(TblNm,db="Med_Vocab_2020"):
    QueryExecute(Q="CREATE TABLE [dbo].[" + TblNm + "]( [id] [bigint] NULL, [effectiveTime] [bigint] NULL, [active] [int] NULL, [moduleId] [bigint] NULL, [conceptId] [bigint] NULL, [languageCode] [nchar](2) NULL, [typeID] [bigint] NULL, [term] [nchar](254) NULL, [caseSignificanceId] [bigint] NULL)",db=db)

#def CreateBCPXML():   #TblNm, Server="DAS2019\DAS2018"):
#    #conn = pyodbc.connect(driver='{SQL Server}', server='DAS2020\DAS2020',trusted_connection='yes', database='Med_Vocab_2020')
#    #os.system("cmd /k bcp DAS2020\\DAS2020.Med_Vocab_2020.sct2_Description_Full_CanadianEdition_20200331  format nul  -T  -f 'c:\\temp\\xxx.xml' ") #'c:\\temp\\xxx.bcp'")
#    os.system("cmd /k bcp DAS2020.Med_Vocab_2020.sct2_Description_Full_CanadianEdition_20200331 format  -c -t -f c:\\temp\Employee.fmt -T")
#def BCPUpload(SQLTable,CSVtoUpload,delimiter=","):
#    #https://bcp.readthedocs.io/en/latest/
#    #https://pypi.org/project/bcp/
#    conn=conn = bcp.Connection(host='DAS2020\DAS2020', driver='mssql')
#    my_bcp = bcp.BCP(conn)
#    my_file = bcp.DataFile(file_path=CSVtoUpload,delimiter=delimiter )
#    my_bcp.load(input_file=my_file, table=SQLTable)
#    #MSSQLLoad(my_bcp,my_file,SQLTable,batch_size=10000)
#    x=0
