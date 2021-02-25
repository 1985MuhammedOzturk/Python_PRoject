# ETL - working project

## Objectives 

In this working project, we'll implement a simple **ETL package** using the ETL exmaple that professor Dr. J.Ferreira gave us. 

ETL stands for Extraction-Transform-Load, and, in practice, means the code we are about to write must:

* Extract data from two specific files;
* Transform the original data to serve our goals;
* Load the transformed data into a spatial database (postgres - postgis). 

For Installing please:

1. Create a folder in the following path: C:\EDMUNDO\CODE\
2. Paste the zip file and unzip in the above path
3. Read the requirements file
4. You need to have installed PostgreSQL and its extension Postgis
5. Change the parameters in the config file "01.yml" for setting  your database in postgresql

for testing the results of the ETL you can choose SQL Statements from the file "queries_project.sql" and run them in PgAdmin 