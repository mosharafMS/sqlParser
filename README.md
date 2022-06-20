# Synapse SQL Parser

[![Build and deploy dotnet core app to Azure Function App - synapseQueryParserfunc](https://github.com/mosharafMS/sqlParser/actions/workflows/master_synapsequeryparserfunc.yml/badge.svg)](https://github.com/mosharafMS/sqlParser/actions/workflows/master_synapsequeryparserfunc.yml)

[![Deploy to nuget](https://github.com/mosharafMS/sqlParser/actions/workflows/publish_nuget.yml/badge.svg)](https://github.com/mosharafMS/sqlParser/actions/workflows/publish_nuget.yml)

## Why:

You want to get insights about tables usage including

- DML operations (Update, Delete, Insert) on the Synapse SQL Pools tables
- Select operations on the table
- Which tables joined with which other tables
  - The most frequent tables to be joined together
- Information about grouped by columns, aggregated columns...etc. 





## How:

Depending on the `Microsoft.SqlServer.TransactSql.ScriptDom` namespace in the `Microsoft.SqlServer.DacFX` package from SQL Server team to do the core parsing. 

The core library is implemented as a class library project compiled against .NET Standard 2.1 and published to Nuget at https://www.nuget.org/packages/SynapseQueryParserKernel

The library is wrapped in two interfaces 
 - Console app
 - REST API implemented as Azure Function app with http trigger

To use the library, you can 
- Reference it in synapse .net spark notebook and use it directly
- Call the REST API

## End to end process
1) Store Synapse queries into permanent store like 
    - Query Store (preferred)
    - Diagnostics logs in storage account
    - ....open to more scenarios if requested
2) Load data into spark dataframe
    - In the case of Query Store, I use pipeline to incremental load into Synapse SQL table (can be replaced with SQL DB table) then use notebook to load it into dataframe
3) To process the queries, two options implemented
    - If possible to deploy the REST API and call it then it can be used by spark or other tools. However not many organizations would be open to deploy another REST API. 
    - Reference the [nuget package](https://www.nuget.org/packages/SynapseQueryParserKernel) in a .Net Spark notebook and call the library directly by using Spark UDF
  Both implementations are added in the /Notebooks folder

## Project:

- QueryParserKernel: The class library that has the core functionality
- SqlParser: Console app to quick test, uses the QueryParserKernel
- SynapseQueryParser: Azure Function App with Open API to expose the functionality as http endpoint
- Notebooks folder to have the sample notebooks for using the library 
