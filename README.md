# OMOP
OMOP: python code to migrate OMOP data to Neo4j

OMOP is a data standard for clinical records designed to support relational database harmonization across institutions. It is used primarily by research projects in which each institutional participant maintains their own data; that is, it is a distributed system.

Relational databases have limitations. This repository contains assets that support the migration of OMOP data to Neo4j, a native graph database. It uses Neo4j version 4.x which supports multiple database with a single install with individual management of user access and roles. This could support multi-institutional research projects. Neo4j does enable queries across databases, which could facilitate analytics of the reseach data. 

A sample Neo4j database is available at ______________. It runs on the Azure Cloud.

DATA SOURCE

The data used to develop this code is available at https://drive.google.com/file/d/18EjMxyA6NsqBo9eed_Gab1ESHWPxJygz/view. It is simulated data designed for other purposes and thus does not present a very cogent output. But it is useful in development of the Neo4j platform.

CONTENTS OF THE REPOSITORY

Python code is utilized to parse OMOP files and upload them to a Microsoft SQL Server database. It then queries this database, transforms the results to csv files, and imports the csv into Neo4j.  


IMPLEMENTATION

The python code was written using Visual Studio and Atom. You can ignore the VS files if you are running it with other tools. The python start file is OMOP.py and can be used to call code for the capabilities desired. 

The SQL Server is located on your local PC. It is populated using SQLLib.LoadOMOPtoSQLServer(). You must change the directory variables to reflect your local directory structure. The code then steps through the following:
<ul>
<li>Creates the SQLServer tables using https://github.com/waigitdas/OMOP/blob/master/Data%20setup/SQLIndexCreateScript_DAS.txt.
<li>Interates through the CSV files with the OMOP data and uploads the contents to SLQServer. These files must all be in one directory. The files themselves are obtained from the sourse mentioned above. You can substitute your own files. The code will delete prior information in each SQLServer table before re-populating it with the data you provide  
</ul>

The code is written for Azure. You must have Neo4j version v4.x installed and create a database named "omop". You will need to set your specific Azure environment variables in the AzureLib.py, including 
<ul>Neo4Server ="bolt://{your IP}:7687"  
<li>Neo4UserName = "neo4j"
<li>Neo4Pswd ="{your password}"
<li>Neo4jDatabase="omop" or other name us may choose.
<li>AzureBlob="{your Azure container}" 
</ul>

The code OMOPLib.LoadOMOPtoNeo4j() will then populate Neo4j with the following steps:
<ul>
  <li>Creates "dummy" nodes and setup up indices that accelerate later uploads. These nodes and any pre-existing nodes or relationships are then deleted. 
  <li>Next the various clinical dataset are uploaded in a specific sequence as nodes and edges. The sequence is important because creating edges requires the source and target node are already in place. Also, some properties are set based on queries requiring the nodes and edges being pre-populated. 
    <ul>
      <li>Persons (nodes)
      <li>Providers (nodes)
      <li>Care sites (nodes)
      <li>PersonProvider (edge): this links patients to the provider(s)
      <li>ProviderCareSite (edge): this links providers to care sites
      <li>Visit_Occurrence (node)
      <li>VisitSequence (edge): links visits for a patient in sequential order based on their date
      <li>Condition (node)
      <li>PersonCondition (edge): links patients to their condition. The edge properties are the start date and number of times it was recorded. Obtaining these properties is a major reason for using an intermediary SQL Server database.
       <li>SharedPatients (edge): this edge links two providers and has a property SharedPatients, which is the number of distinct patients they share.
       <li>VisitCondition (edge): this links each Visit_Occurrence to the conditions recorded for the visit.  
    </ul>
</ul>  


