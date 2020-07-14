#######################################################################################
#######################################################################################
##  Code initiated by David A Stumpf, MD, PhD #########################################
##  Professor Emeritus, Northwestern University Feinberg School of Medicine ###########
##  consulting@woodstockHIT.com #######################################################
##  Provisioned via https://github.com/waigitdas/OMOP for the OMOP/OHDSI communities ##
##  Copyright 2020 to protect from unauthorized use ###################################
#######################################################################################
#######################################################################################



import urllib3  # the lib that handles the url stuff
import glob
import SQLLib
import Neo4jLib

def ReadURLText(U):
    #https://stackoverflow.com/questions/1393324/in-python-given-a-url-to-a-text-file-what-is-the-simplest-way-to-read-the-cont
    http = urllib3.PoolManager()
    response = http.request('GET',U)
    data = response.data.decode('utf-8')
    return data

def getDirFiles(dir, ext):
    s=dir + "*." + ext
    fl=glob.glob(s)
    return fl

def FileNmFromFullPath(Path):
    f1=Path.split("\\")
    f=f1[len(f1)-1]
    return f

def TupleToList(mytuple):
    return [e for e, in mytuple]

def LoadOMOPtoNeo4j():
    #Neo4jLib.CypherBoltQuery("match (n)-[r]-() delete r","",db="")
    Neo4jLib.CypherBoltQuery("match (n) delete n","",db=AzureLib.Neo4jDatabase)
    SQLLib.QueryToNeo4jNodes(NodeLabel="Person",FN="Person",SQLQuery="SELECT per.person_id, per.year_of_birth, per.month_of_birth, per.day_of_birth, con.concept_name as Gender, con1.concept_name as Race, con2.concept_name as Ethnicity, loc.state FROM dbo.person per, OMOP.dbo.concept con, OMOP.dbo.concept con1, OMOP.dbo.concept con2, OMOP.dbo.location loc WHERE per.gender_concept_id=con.concept_id AND per.race_concept_id=con1.concept_id AND per.ethnicity_concept_id=con2.concept_id AND per.location_id=loc.location_id",CypherQuery="create (Person{person_id,year_of_birth,month_of_birth,day_of_birth,Gender,Race,Ethnicity,State})")
    SQLLib.QueryToNeo4jNodes(NodeLabel="Provider",FN="Provider",SQLQuery="Select provider_id, NPI, care_site_id from dbo.provider",CypherQuery="create (Provider{provider_id,NPI,care_site_id})")
    SQLLib.QueryToNeo4jNodes(NodeLabel="CareSite",FN="CareSite",SQLQuery="SELECT s.care_site_id, c.concept_name as care_site_type FROM dbo.care_site s, concept c WHERE s.place_of_service_concept_id=c.concept_id",CypherQuery="create (CareSite{care_site_id,care_site_type})")
    Neo4jLib.CreateIndex("Person","person_id",db=AzureLib.Neo4jDatabase)
    Neo4jLib.CreateIndex("Provider","provider_id",db=AzureLib.Neo4jDatabase)
    Neo4jLib.CreateIndex("CareSite","care_site_id",db=AzureLib.Neo4jDatabase)

    SQLLib.QueryToNeo4jEdges("PersonProvider","SELECT  p.person_id, v.provider_id, pro.NPI,count(*) as visit_ct FROM dbo.person p, visit_occurrence v, OMOP.dbo.provider pro WHERE p.person_id=v.person_id AND v.provider_id=pro.provider_id group by  p.person_id, v.provider_id, pro.NPI","match (p:Provider{provider_id:toInteger(line.provider_id)}) match (people:Person{person_id:toInteger(line.person_id)}) merge (people)-[r:PersonProvider{visit_ct:toInteger(line.visit_ct)}]->(p)")

    SQLLib.QueryToNeo4jEdges("ProviderCareSiteEdge","Select provider_id, care_site_id from dbo.provider","match (p:Provider{provider_id:toInteger(line.provider_id)}) match (s:CareSite{care_site_id:toInteger(line.care_site_id)}) merge (p)-[r:ProviderCareSite]->(s)")

    SQLLib.QueryToNeo4jNodes(NodeLabel="Visit_Occurrence",FN="Visit_Occurrence",SQLQuery="SELECT v.visit_occurrence_id, v.person_id, v.provider_id, v.care_site_id, v.visit_concept_id, v.visit_start_date, v.visit_end_date,datediff(Day,v.visit_start_date, v.visit_end_date) as [Days], c1.concept_name as visit_type, isnull(v.preceding_visit_occurrence_id,0) as preceding_visit_occurrent_id FROM dbo.visit_occurrence v left outer join concept c1 on v.visit_concept_id=c1.concept_id and c1.concept_id>0")
    Neo4jLib.CreateIndex("Visit_Occurrence","visit_occurrence_id",db=AzureLib.Neo4jDatabase)
    Neo4jLib.CreateIndex("Visit_Occurrence","person_id_id",db=AzureLib.Neo4jDatabase)
    Neo4jLib.CreateIndex("Visit_Occurrence","provider_id",db=AzureLib.Neo4jDatabase)
    Neo4jLib.CreateIndex("Visit_Occurrence","care_site_id",db=AzureLib.Neo4jDatabase)

    SQLLib.QueryToNeo4jEdges("VisitSequence","Select v1.person_id,v2.visit_occurrence_id as fromNode,v1.visit_occurrence_id as toNode from dbo.visit_occurrence v1,dbo.visit_occurrence v2 where v1.preceding_visit_occurrence_id=v2.visit_occurrence_id ","match (v1:Visit_Occurrence{person_id:toInteger(line.person_id),visit_occurrence_id:toInteger(line.fromNode)})  match (v2:Visit_Occurrence{person_id:toInteger(line.person_id),visit_occurrence_id:toInteger(line.toNode)}) merge (v1)-[r:visit_sequence]->(v2)")

    SQLLib.QueryToNeo4jNodes(NodeLabel="Condition",FN="ConditionOccurrence",SQLQuery="SELECT c1.concept_id as condition_concept_id,c1.vocabulary_id as condition_vocabulary_id,c1.concept_code as conditin_code,c1.concept_name as condition,count(c.visit_occurrence_id) as Ct FROM dbo.condition_occurrence c left outer join concept c1 on c.condition_concept_id=c1.concept_id where c.condition_concept_id>0 group by c1.concept_id,c1.vocabulary_id,c1.concept_code,c1.concept_name")

    Neo4jLib.CreateIndex("Condition","condition_concept_id",db=AzureLib.Neo4jDatabase)

    SQLLib.QueryToNeo4jEdges("PersonCondition","Select condition_concept_id , person_id,isnull(cast(min(condition_start_date) as char(10)),'') as startDate,count(*) as Ct from dbo.condition_occurrence group by condition_concept_id , person_id","match (p:Person{person_id:toInteger(line.person_id)}) match (c:Condition{condition_concept_id:toInteger(line.condition_concept_id)}) merge (p)-[r:PersonCondition{ct:toInteger(line.Ct),startDate:toString(line.startDate)}]->(c)")


    SQLLib.QueryToNeo4jEdges("SharedPatients","select t1.provider_id as p1,t2.provider_id as p2,count(*) as SharedPatients from (select distinct person_id,provider_id from visit_occurrence) t1,(select distinct person_id,provider_id from visit_occurrence) t2 where t1.person_id=t2.person_id and t1.provider_id<t2.provider_id and t1.provider_id<t2.provider_id group by t1.provider_id,t2.provider_id having count(*)>3","match (p1:Provider{provider_id:toInteger(line.p1)}) match (p2:Provider{provider_id:toInteger(line.p2)}) merge (p1)-[r:SharedPatients{SharedPatients:toInteger(line.SharedPatients)}]-(p2)")

    SQLLib.QueryToNeo4jEdges("VisitCondition","SELECT distinct v.person_id,v.visit_occurrence_id, c.condition_concept_id FROM visit_occurrence v, condition_occurrence c WHERE v.visit_occurrence_id=c.visit_occurrence_id AND v.person_id=c.person_id ","match (v:Visit_Occurrence{visit_occurrence_id:toInteger(line.visit_occurrence_id)}) match (c:Condition{condition_concept_id:toInteger(line.condition_concept_id)}) merge (v)-[r:VisitCondition]->(c)")


    break  #the code below is still in development.
   ##############################################################################
   ###  Ontologies

    SQLLib.QueryToNeo4jNodes(NodeLabel="Concept",FN="Concepts",SQLQuery="SELECT concept_id, concept_name, domain_id, vocabulary_id, concept_class_id, standard_concept, concept_code, valid_start_date, valid_end_date, invalid_reason FROM dbo.concept")

    Neo4jLib.CreateIndex("Concept","concept_id",db=AzureLib.Neo4jDatabase)
    Neo4jLib.CreateIndex("Concept","concept_name",db=AzureLib.Neo4jDatabase)

    c=SQLLib.Query("Select relationship_id from dbo.concept_relationship where relationship_id>'ICD9P - SNOMED eq' group by relationship_id order by relationship_id")
    cs=c.split("\n")

    for i in range(1,len(cs)):
       css=cs[i].rstrip().replace(" ","_").replace(",","").replace("'","").replace("-","to").replace("/","_")  # "—") #last relace used MDASH
       SQLLib.QueryWriteCSV("SELECT concept_id_1, concept_id_2, min(replace(valid_start_date,'-','')) as valid_start_date, max(replace(valid_end_date,'-','')) as valid_end_date,count(*) as ct FROM dbo.concept_relationship where relationship_id=" + cs[i].replace(",","") + " group by concept_id_1, concept_id_2 ",Neo4jLib.ImportDir + css + ".csv")
       #c = c.replace("'","").replace(", ","|").replace(",","|")
       #fw=open(Neo4jLib.ImportDir + css + ".csv","w")
       #fw.write(c)
       #fw.close()
    CQ="USING PERIODIC COMMIT 10000 LOAD CSV WITH HEADERS FROM 'file:///" + css + ".csv' as line FIELDTERMINATOR '|' match (c1:Concept{concept_id:toInteger(line.concept_id_1)}) match (c2:Concept{concept_id:toInteger(line.concept_id_2)}) merge (c1)-[:" + css + "{startDate:toString(line.valid_start_date),endDate:toString(line.valid_end_date),ct:toInteger(line.ct)}]->(c2)"
    Neo4jLib.UploadWithPeriodicCommit(CQ,db=AzureLib.Neo4jDatabase)

    SQLLib.QueryToNeo4jEdges("VisitCondition","Select * from dbo.concept_relationship","match (v:Visit_Occurrence{visit_occurrence_id:toInteger(line.visit_occurrence_id)}) match (c:Condition{condition_concept_id:toInteger(line.condition_concept_id)}) merge (v)-[r:VisitCondition]->(c)")
    x=0
