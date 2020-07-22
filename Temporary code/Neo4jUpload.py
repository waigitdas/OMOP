##################################################################
#### OMOP Code  ##################################
####  David A Stumpf, MD, PhD; Woodsock, IL, USA #################
####  copyright 2020 #############################################
##################################################################
from neo4j import GraphDatabase   #https://pypi.org/project/neo4j-driver/4.0.0a4/
from py2neo import Graph, Path, Node, Relationship
from pandas import DataFrame
import re
import timer
import time
import AzureLib
import os, uuid
from azure.storage.blob import BlobServiceClient, BlobClient, ContainerClient


#one-time loading of neo4j from pre-processed CSV
def OneTimeCode():

    CypherBoltQuery("create (p:Person{person_id:1})")
    CypherBoltQuery("create (p:Provider{provider_id:1})")
    CypherBoltQuery("create (p:CareSite{care_site_id:1})")
    CreateIndex("Person","person_id",db="omop")
    CreateIndex("Provider","provider_id",db="omop")
    CreateIndex("CareSite","care_site_id",db="omop")
    CypherBoltQuery("match (n) delete n")
    print("Index: Person, Provider, Care Site")

    cq="USING PERIODIC COMMIT 10000 LOAD CSV WITH HEADERS FROM 'file:///omop/Person.csv' as line FIELDTERMINATOR '|' create (p:Person{person_id:toInteger(line.person_id),year_of_birth:toInteger(line.year_of_birth),month_of_birth:toInteger(line.month_of_birth),race:toString(line.race),ethnicity:toString(line.ethnicity),state:toString(line.state)})"
    UploadWithPeriodicCommit(cq,"omop")
    print("Person")

    cq="USING PERIODIC COMMIT 10000 LOAD CSV WITH HEADERS FROM 'file:///omop/Provider.csv' as line FIELDTERMINATOR '|' create (p:Provider{provider_id:toInteger(line.provider_id),NPI:toInteger(line.NPI),care_site_id:toInteger(line.care_site_id)})"
    UploadWithPeriodicCommit(cq,"omop")
    print("Provider")

    cq="USING PERIODIC COMMIT 10000 LOAD CSV WITH HEADERS FROM 'file:///omop/CareSite.csv' as line FIELDTERMINATOR '|' create (p:CareSite{care_site_id:toInteger(line.care_site_id),care_site_type:toString(line.care_site_type)})"
    UploadWithPeriodicCommit(cq,"omop")
    print("Care Site")



    UploadWithPeriodicCommit("USING PERIODIC COMMIT 10000 LOAD CSV WITH HEADERS FROM 'file:///omop/PersonProvider.csv' as line FIELDTERMINATOR '|' match (p:Provider{provider_id:toInteger(line.provider_id)}) match (people:Person{person_id:toInteger(line.person_id)}) merge (people)-[r:PersonProvider{visit_ct:toInteger(line.visit_ct)}]->(p)")
    print("PersonProvider")

    UploadWithPeriodicCommit("USING PERIODIC COMMIT 10000 LOAD CSV WITH HEADERS FROM 'file:///omop/ProviderCareSiteEdge.csv' as line FIELDTERMINATOR '|' match (p:Provider{provider_id:toInteger(line.provider_id)}) match (s:CareSite{care_site_id:toInteger(line.care_site_id)}) merge (p)-[r:ProviderCareSite]->(s)")
    print("Provider Care Site")

    cq="USING PERIODIC COMMIT 10000 LOAD CSV WITH HEADERS FROM 'file:///omop/Visit_Occurrence.csv' as line FIELDTERMINATOR '|' create (v:VisitOccurrence{visit_occurrence_id:toInteger(line.visit_occurrence_id),person_id:toInteger(line.person_id),provider_id:toInteger(line.provider_id),care_site_id:toInteger(line.are_site_id),visit_concept_id:toInteger(line.visit_concept_id),visit_start_date:toString(line.visit_start_date),visit_end_date:toString(line.visit_end_date),days:toInteger(line.Days),visit_type:toString(line.visit_type)})"
    UploadWithPeriodicCommit(cq,"omop")
    print("Visit Occurrence")

    CreateIndex("VisitOccurrence","visit_occurrence_id",db='omop')
    CreateIndex("VisitOccurrence","person_id",db='omop')
    CreateIndex("VisitOccurrence","provider_id",db='omop')
    CreateIndex("VisitOccurrence","care_site_id",db='omop')
    print("Visit occurrence indices")

    cq="USING PERIODIC COMMIT 10000 LOAD CSV WITH HEADERS FROM 'file:///omop/VisitSequence.csv' as line FIELDTERMINATOR '|' match (v1:VisitOccurrence{person_id:toInteger(line.person_id),visit_occurrence_id:toInteger(line.fromNode)})  match (v2:VisitOccurrence{person_id:toInteger(line.person_id),visit_occurrence_id:toInteger(line.toNode)}) merge (v1)-[r:visit_sequence]->(v2)"
    UploadWithPeriodicCommit(cq,"omop")
    print("Visit sequence")


    cq="USING PERIODIC COMMIT 10000 LOAD CSV WITH HEADERS FROM 'file:///omop/ConditionOccurrence.csv' as line FIELDTERMINATOR '|' create (c:Condition{condition_concept_id:toInteger(line.condition_concept_id),condition_vocabulary_id:toInteger(line.condition_vocabulary_id),condition_code:toInteger(line.conditin_code),condition:toString(line.condition),Ct:toInteger(line.Ct)})"
    UploadWithPeriodicCommit(cq,"omop")
    print("ConditionOccurrence")

    CreateIndex("Condition","condition_concept_id",db="omop")

    UploadWithPeriodicCommit("USING PERIODIC COMMIT 10000 LOAD CSV WITH HEADERS FROM 'file:///omop/PersonCondition.csv' as line FIELDTERMINATOR '|' match (p:Person{person_id:toInteger(line.person_id)}) match (c:Condition{condition_concept_id:toInteger(line.condition_concept_id)}) merge (p)-[r:PersonCondition{ct:toInteger(line.Ct),startDate:toString(line.startDate)}]->(c)")
    print("Person Condition")

    UploadWithPeriodicCommit("USING PERIODIC COMMIT 10000 LOAD CSV WITH HEADERS FROM 'file:///omop/SharedPatients.csv' as line FIELDTERMINATOR '|' match (p1:Provider{provider_id:toInteger(line.p1)}) match (p2:Provider{provider_id:toInteger(line.p2)}) merge (p1)-[r:SharedPatients{SharedPatients:toInteger(line.SharedPatients)}]-(p2)")
    print("Shared Patients")

    UploadWithPeriodicCommit("USING PERIODIC COMMIT 10000 LOAD CSV WITH HEADERS FROM 'file:///omop/VisitCondition.csv' as line FIELDTERMINATOR '|' match (v:Visit_Occurrence{visit_occurrence_id:toInteger(line.visit_occurrence_id)}) match (c:Condition{condition_concept_id:toInteger(line.condition_concept_id)}) merge (v)-[r:VisitCondition]->(c)")
    print("Visit Condition\nFinisher")


def CreateIndex(Nd,Param,db="azure"):
      #creates index on node property
      try:
          CQ="CREATE INDEX ON :" + Nd + "(" + Param + ")"
          #print("\n" + CQ + "\n")
          CypherBoltQuery(CQ,"",db)

            #'PostToNeo4jServer(U, Q, "", CreateNodes:=False)
      except:
          #do nothing
          exit

def Neo4jBoltQueryAzure(BlobString,BlobFileNm,CQ,ResultType=""):
    AzureLib.DeleteBlob(BlobFileNm,AzureLib.Neo4jBlobConnectionString,Container=AzureLib.Neo4jBlobContainer)
    AzureLib.UploadStringForNeo4jImport(StringToImport=BlobString,StoredFileName= BlobFileNm,BlobConnStr=AzureLib.Neo4jBlobConnectionString,Container=AzureLib.Neo4jBlobContainer)
    h=AzureLib.GetBlobURLwithSAS(BlobFileNm,AzureLib.Neo4jBlobContainer,AzureLib.Neo4jBlobAcctNm, AzureLib.Neo4jBlobAccessKey)
    Q=CQ.replace("FileHere",h)
    print(Q)
    CypherBoltQuery(Q,ResultType)

    #AzureLib.DeleteBlob(BlobFileNm,AzureLib.pythonContainerConnectionString,Container=AzureLib.pythonContaner)
    #AzureLib.UploadStringForNeo4jImport(StringToImport=BlobString,StoredFileName= BlobFileNm,BlobConnStr=AzureLib.pythonContainerConnectionString,Container=AzureLib.pythonContaner)
    #h=AzureLib.GetBlobURLwithSAS(BlobFileNm,"python-neo4j-container","waijupyterdiag", AzureLib.pythonContainerAccessKey)
    #Q=CQ.replace("FileHere",h)
    #print(Q)
    #CypherBoltQuery(Q,ResultType)

def CypherBoltQuery(Q,ResultType="",header=1,db="azure",database="omop",delimiter=","):
    #Neo4j cypher query function
    driver = None
    if db=="azure":
        #print(database)
        driver=GraphDatabase.driver("bolt://104.43.228.191:7687", auth=("neo4j","WHIT!2020das"),database="omop" )  #"neo4j"))  #AzureLib.Neo4Pswd))
        #print("\n" + Q + "\n")

    else:  #local Neo4j database
        driver=GraphDatabase.driver("bolt://localhost:7687",auth=("neo4j","cns105"))


    QS=chr(34)

    with driver.session(database=database) as session:
        s=""
        QQ=""
        i=0
        tm=time.time()
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
            timer.Neo4jTime=timer.Neo4jTime + (time.time() - tm)
            timer.Neo4jQueryCt  = timer.Neo4jQueryCt + 1
            return s  #s.replace('[','').replace(']','')


def CypherBoltQueryJson(Q):  #not mature
    driver = GraphDatabase.driver(AWS.Neo4Server, auth=(AWS.Neo4UserName,AWS.Neo4Pswd))
    with driver.session() as session:
        s=session.run(Q)
        return s


def UploadWithPeriodicCommit(Q,database="omop"):
    try:
        #executes query directly. Cannot use LOAD CSV with a open transaction
        driver = GraphDatabase.driver(AzureLib.Neo4Server, auth=(AzureLib.Neo4UserName,AzureLib.Neo4Pswd),database=database)
        with driver.session(database=database) as session:
            session.run(Q)
    except:
        print("\nError in UploadWithPeriodicCommit: " + Q + "\n")


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
