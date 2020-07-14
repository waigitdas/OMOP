# OMOP
OMOP: python code to migrate OMOP data to Neo4j

OMOP is a data standard for clinical records designed to support relational database harmonization across institutions. It is used primarily by research projects in which each institutional participant maintains their own data; that is, it is a distributed system.

Relational databases have limitations. This repository contains assets that support the migration of OMOP data to Neo4j, a native graph database. It uses Neo4j version 4.x which supports multiple database with a single install with individual management of user access and roles. This could support multi-institutional research projects. Neo4j does enable queries across databases, which could facilitate analytics of the reseach data. 

A sample Neo4j database is available at ______________. It runs on the Azure Cloud.

CONTENTS OF THE REPOSITORY

Python code is utilized to parse OMOP files and upload them to a Microsoft SQL Server database. It then queries this database, transforms the results to csv files, and imports the csv into Neo4j.  

The data used for this example is from the ____________. It is simulated data designed for other purposes and thus does not present a very cogent output. But it is useful in development of the Neo4j platform.

