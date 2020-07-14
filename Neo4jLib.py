#######################################################################################
#######################################################################################
##  Code initiated by David A Stumpf, MD, PhD #########################################
##  Professor Emeritus, Northwestern University Feinberg School of Medicine ###########
##  consulting@woodstockHIT.com #######################################################
##  Provisioned via https://github.com/waigitdas/OMOP for the OMOP/OHDSI communities ##
##  Copyright 2020 to protect from unauthorized use ###################################
#######################################################################################
#######################################################################################




from neo4j import GraphDatabase
from py2neo import Graph, Path, Node, Relationship
from pandas import DataFrame
import re
import timer
import time
#import AzureLib
import os, uuid
from azure.storage.blob import BlobServiceClient, BlobClient, ContainerClient

##################################################################
########### Neo4j reference parameters ###########################
##################################################################
ImportDir="C:\\Neo4j\\neo4jDatabases\\database-2389e81d-dcf8-4732-a655-b3d81ed879ec\\installation-3.5.17\\import\\"  #ontology
#ImportDir="C:\\Neo4j\\neo4jDatabases\\database-b74bb205-66fd-41a7-8cf3-be9ec73dea0c\\installation-3.5.7\\import\\"  #clinical
Neo4jTime=0


#FileDir=
#FileDir="C:\\Genealogy\\DNA\Haplotrees\\Neo4j Project\\Inputs\\R-Respository\\"   #"F:\\BigY\\"  #"H:\\BigYFiles\\"
#importDir="C:\\Users\\david\\.Neo4jDesktop\\neo4jDatabases\\database-e3bafe4f-ee69-451e-8327-589f9069e1bd\\installation-3.3.4\\import"
LineFeed=chr(10)
##################################################################
##################################################################
##################################################################

def GetLeaderVM():
    #using Neo4j cluster. Can only write to the Leader Virtual Machine. This is unlikely to change within a given run, so it is set once to avoid recursion error.
    #https://medium.com/neo4j/neo4j-considerations-in-orchestration-environments-584db747dca5
    #https://medium.com/neo4j/querying-neo4j-clusters-7d6fde75b5b4
    #error will occur if you are not using a cluster and the return value will be the existing VM bolt connection
    #also: CALL dbms.cluster.overview   |  CALL dbms.cluster.protocols  |  CALL dbms.cluster.role

    ls=AzureLib.Neo4Server
    try:
        l=CypherBoltQuery("CALL dbms.cluster.routing.getRoutingTable({}) YIELD ttl, servers UNWIND servers as server with server where server.role='WRITE' RETURN server.addresses","csv")
        ls=l.split("\n")
        return "bolt://" + ls[1]
    except:
        return ls

def CreateIndex(Nd,Param,db="azure"):
      #creates index on node property
      CypherBoltQuery("CREATE INDEX ON :" + Nd + "(" + Param + ")","",db=db)
            #'PostToNeo4jServer(U, Q, "", CreateNodes:=False)

def CypherBoltQuery(Q,database="omop",ResultType="",header=1,db="azure",delimiter=","):
    #Neo4j cypher query function
    driver = None
    if db=="azure":
        #print(database)
        driver=GraphDatabase.driver(AzureLib.Neo4Server, auth=(Neo4UserName,Neo4Pswd),database=database )  #"neo4j"))  #AzureLib.Neo4Pswd))
        #print("\n" + Q + "\n")

    else:  #local Neo4j database
        driver=GraphDatabase.driver("bolt://localhost:7687",auth=("neo4j","cns105"))


    QS=chr(34)

    with driver.session(database=database) as session:
        s=""
        QQ=""
        i=0
        #tm=time.time()
        with session.begin_transaction() as tx:
            for record in tx.run(Q):
                if i==0 and header==1 and ResultType!="json":
                    c=0
                    while c<=len(record.keys())-1:
                        s = s + QQ + record.keys()[c] + QQ
                        if c < len(record.keys())-1: s=s + delimiter
                        c=c+1
                    s= s + "\n"
                    i=i+1

                if ResultType=='node':
                    #nothing yet
                    x=y
                elif ResultType=='edge':
                    #nothing yet
                    x=y
                elif ResultType=='GEDCOM':
                    x=y
                elif ResultType=="list":
                    return record
                elif ResultType=="csv":
                    for j in range(0,len(record)):
                        if type(record.values(j)[0]) is int:
                            s = s + str(record.values(j)[0])
                        elif type(record.values(j)[0]) is float:
                            s = s + str(record.values(j)[0])
                        elif type(record.values(j)[0]) is str:
                            s = s + QQ + record.values(j)[0] + QQ
                        #elif type(record.values(j)[0]) is unicode:
                        #    s = s + QQ + record.values(j)[0] + QQ
                        elif type(record.values(j)[0]) is list:
                            k=0
                            while k<len(record.values(j)[0]):
                                s= s + str(record.values(j)[0][k])
                                if k<len(record.values(j)[0])-1:
                                    s = s + ";"
                                k=k+1
                        else:
                            #print("Record type not supported: " + str(type(record.values(j)[0]))+ ": " + record.values(j)[0])
                            s = s + str(record.values(j)[0])
                        if j < len(record)-1: s=s + delimiter

                    s = s + "\n"
                elif ResultType=="fixcsv":
                    for j in range(0,len(record)):
                        if type(record.values(j)[0])is int:
                            s = s + str(record.values(j)[0]);
                        if type(record.values(j)[0])is float:
                            s = s + str(record.values(j)[0]);
                        elif type(record.values(j)[0]) is str:
                            s = s + record.values(j)[0]
                        elif type(record.values(j)[0]) is list:
                            k=0
                            while k<len(record.values(j)[0]):
                                s= s + str(record.values(j)[0][k]).replace("%%%","\n")
                                if k<len(record.values(j)[0])-1:
                                    s = s + ";"
                                k=k+1
                        else:
                            s = s + str(record.values(j)[0])
                        if j < len(record)-1: s=s + delimiter
                        #j=j+1

                    s = s + "\n"
                elif ResultType=='json': #NOT WORKING YET ... close
                    s = s + "{\n"  #start of record
                    for j in range(0,len(record)):

                        if j<len(record):

                            if type(record.values(j)[0])is int:
                                s=s + QS + record.keys()[j] + QS + ": " + QS + record.value(j) + QS ;
                                if j<len(record): s= s+ ",\n"
                                else: s=s+"\n"
                            if type(record.values(j)[0])is float:
                                s=s + QS + record.keys()[j] + QS + ": " + QS + record.value(j) + QS
                                if j<len(record): s= s+ ",\n"
                                else: s=s+"\n"

                            elif type(record.values(j)[0]) is str:
                                s=s + QS + record.keys()[j] + QS + ": " + QS + record.value(j) + QS
                                if j<len(record): s= s+ ",\n"
                                else: s=s+"\n"

                            elif type(record.values(j)[0]) is list:
                                k=0
                                s=s + QS + record.keys()[j] + QS + ": " + "[\n"
                                while k<len(record.values(j)[0]):
                                    s=s + "{" + QS + record.keys()[j][0:3] + QS + ": " + QS + record.value(j)[k] + QS #+ "\n"
                                    #s= s + str(record.values(j)[0][k][0:3]).replace("%%%","\n")
                                    if k<len(record.values(j)[0])-1:
                                        s = s + "},\n"
                                    else: s= s + "}\n"
                                    k=k+1
                                s=s + "]\n"

                            else:
                                s = s + str(record.values(j)[0])
                                #if j<len(record): s= s+ ","
                                #else: s=s+"}"
                    s = s + "}\n"

                elif ResultType=='text':
                    while i <len(record):
                        s = s + record.value(i)  #is integer?

                        if i<len(record)-1:
                            s=s + chr(9)
                        i=i+1
                    return s
                elif ResultType=='item':
                    return record[0]
                else:
                    x=''
                    return ""
            #print(s)
            #Neo4jTime=Neo4jTime + (time.time() - tm)
            #Neo4jQueryCt  = Neo4jQueryCt + 1
            return s  #s.replace('[','').replace(']','')


def CypherBoltQueryJson(Q):  #not mature
    driver = GraphDatabase.driver(AWS.Neo4Server, auth=(AWS.Neo4UserName,AWS.Neo4Pswd))
    with driver.session() as session:
        s=session.run(Q)
        return s


def UploadWithPeriodicCommit(Q,db=""):
    try:
        driver = None
        if db=="azure":
            driver=GraphDatabase.driver(AzureLib.Neo4Server, auth=(AzureLib.Neo4UserName,AzureLib.Neo4Pswd))
        else:  #local Neo4j database
            driver=GraphDatabase.driver("bolt://localhost:7687",auth=("neo4j","cns105"))
        #executes query directly. Cannot use LOAD CSV with a open transaction
        #driver = GraphDatabase.driver(AzureLib.Neo4Server, auth=(AzureLib.Neo4UserName, AzureLib.Neo4Pswd))
        with driver.session() as session:
            session.run(Q)
    except:
        print("Error in UploadWithPeriodicCommit: " + Q)


def CypherBoltQueryWriteResultDirectlyToCSVSavedFile(Q,FN):  #not mature
    f=open(FN,"w")

    driver = GraphDatabase.driver(AWS.Neo4Server, auth=(AWS.Neo4UserName,AWS.Neo4Pswd))
    with driver.session() as session:
        #s=""
        i=0

        with session.begin_transaction() as tx:
            for record in tx.run(Q):
                if i==0:
                    c=0
                    while c<=len(record.keys())-1:
                        f.write(record.keys()[c])
                        if c < len(record.keys())-1: f.write(",")
                        c=c+1
                    f.write("\n")
                    i=i+1

                for j in range(0,len(record)):
                    if type(record.values(j)[0])is int:
                        f.write(str(record.values(j)[0]))
                    if type(record.values(j)[0])is float:
                        f.write(str(record.values(j)[0]))
                    elif type(record.values(j)[0]) is str:
                        f.write(record.values(j)[0])
                    elif type(record.values(j)[0]) is list:
                        k=0
                        while k<len(record.values(j)[0]):
                            f.write(str(record.values(j)[0][k]))
                            if k<len(record.values(j)[0])-1:
                                f.write(";")
                            k=k+1
                    else:
                        s = s + str(record.values(j)[0])
                    if j < len(record)-1: s=s + ","

                s = s + "\n"
        #print(s)
        #return s  #s.replace('[','').replace(']','')

def RecordTypeProcess(v):
    if type(v) is int:
        return chr(34) + str(v) + chr(34)
    elif type(v) is str:
        return chr(34) + v + chr(34)
    elif type(v) is list:
        s = ''
        i=0
        while i < len(v)-1:
            if type(v[i]) is int:
                s = s + chr(34) + str(v[i]) + chr(34)
            else:
                s = s + v[i]
            if i < len(v)-1: s = s + ', '
            i = i + 1
        return chr(34) + s + chr(34)

    else:
        return chr(34) + chr(34)


def CSVList(Items):
    #function that iterates thru submitted string to create text for inserting properties queries producing nodes or edges
    if Items=='': return ""
    s="{"
    ss=Items.split(";")
    i=0
    while i< len(ss):
        sss=ss[i].split(":")
        s= s + sss[0] + ":"
        if sss[1]=="S":
            s = s + "toString("
        elif sss[1]=="I":
            s = s + "toInteger("
        elif sss[1]=="F":
            s = s + "toFloat("

        s = s + 'line.' + sss[0] + ')'
        i=i+1
        if i<len(ss): s = s + ', '


    s=s + "}"
    return s

def AddNodesFromCSV(FN,NodeNm,FldDelimitedList):
    L = CSVList(FldDelimitedList)
    Neo4jLib.UploadWithPeriodicCommit("USING PERIODIC COMMIT 10000 LOAD CSV WITH HEADERS FROM 'file:///" + FN + "' AS line FIELDTERMINATOR '|' merge (n:" + NodeNm +  L + ")")

def AddEdgesFromCSV(FN,ParentNdLabel,ParentNdFld,ParentNdCSVFldWithTo,ChildNdLabel,ChildNdFld,ChildNdCSVFldWithTo,RelName,RelPropertyList):
    L = CSVList(RelPropertyList)
    if L>" ":
        Neo4jLib.CypherBoltQuery("LOAD CSV WITH HEADERS FROM 'file:///" + FN + "' AS line FIELDTERMINATOR '|' match (p:" + ParentNdLabel + "{" +  ParentNdFld + ":" + ParentNdCSVFldWithTo + "}) match (c:" + ChildNdLabel + "{" +  ChildNdFld + ":" + ChildNdCSVFldWithTo + "})  merge (p)-[:" + RelName +   L + "]->(c)" ,"")
    else:
         Neo4jLib.CypherBoltQuery("LOAD CSV WITH HEADERS FROM 'file:///" + FN + "' AS line FIELDTERMINATOR '|' match (p:" + ParentNdLabel + "{" +  ParentNdFld + ":" + ParentNdCSVFldWithTo + "}) match (c:" + ChildNdLabel + "{" +  ChildNdFld + ":" + ChildNdCSVFldWithTo + "})  merge (p)-[:" + RelName +  "]->(c)" ,"")

def IsKitInNeo4jDB(Kit):
    N=CypherBoltQuery("match (k:KitNode{Kit:'" + Kit + "'}) return k.Kit","node")
    if N is None:
        return False
    else:
        return True

def FixNone(S):
    Q=chr(34)
    if S is None:
        return Q + Q
    else: return Q + S.replace(Q,"'") + Q  # re.sub('[^A-Za-z0-9 ]+', '',S)

def PreSignAdd(Q,FN):   #AWS method
   s3 = boto3.client('s3', aws_access_key_id=AWS.AWSAccessKey,aws_secret_access_key=AWS.AWSSecretKey)
   response = s3.generate_presigned_url('get_object', Params={'Bucket': AWS.AWSBucketName,'Key':"import/" + FN},ExpiresIn=3600)  #10 min
   response=response.replace("s3.amazonaws","s3.us-west-2.amazonaws")
   #response=response.replace("/graphdb.gedmatch.com/","/graphdb.gedmatch.com/import/")
   Q =Q.replace("@@",response)
   return Q

def Neo4jCVSLoadFromSQLCursorForNodes(label,fn, cur):
    #cur is a tuple of turples
    cq="USING PERIODIC COMMIT 10000 LOAD CSV WITH HEADERS FROM 'file:///" + fn + ".csv' as line FIELDTERMINATOR '|' "

    cq = cq + "create (n:" + label + "{"

    for i in range (0,len(cur)):
        cq = cq  + cur[i][0] + ":" + GetTypeTo(cur[i][1]) + "line." + cur[i][0]  +")"
        if i<len(cur)-1: cq = cq + ","
    cq= cq + "}) return id(n)"
    return cq


def Neo4jCVSLoadFromSQLCursor(label,fn,cypher, cur):
    #cur is a tuple of turples
    cq="LOAD CSV  WITH HEADERS from 'file:///" + fn + ".csv' delimiter:'|' "
    c=cypher.split("{")

    for i in range(1,len(c),2 ):
        print(c[i])


    for i in range (0,len(cur)):
        cq = cq  + cur[i][0] + ":" + GetTypeTo(cur[i][1]) + "line." + cur[i][0]  +"), "
    cq= cq + "})"
    print(cq)

def GetTypeTo(t):
    x=""
    if t is int:
        x="toInteger("
    elif t is str: x="toString("

    return x
