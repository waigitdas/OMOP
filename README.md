# OMOP
OMOP: python code to migrate OMOP data to Neo4j

OMOP is a data standard for clinical records designed to support relational database harmonization across institutions. It is used primarily by research projects in which each institutional participant maintains their own data; that is, it is a distributed system.

Relational databases have limitations. This repository contains assets that support the migration of OMOP data to Neo4j, a native graph database. It uses Neo4j version 4.x which supports multiple database with a single install with individual management of user access and roles. This could support multi-institutional research projects. Neo4j does enable queries across databases, which could facilitate analytics of the reseach data.


