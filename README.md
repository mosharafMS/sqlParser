# Synapse SQL Parser

[![Build and deploy dotnet core app to Azure Function App - synapseQueryParserfunc](https://github.com/mosharafMS/sqlParser/actions/workflows/master_synapsequeryparserfunc.yml/badge.svg)](https://github.com/mosharafMS/sqlParser/actions/workflows/master_synapsequeryparserfunc.yml)


## Why:

You want to get insights about tables usage including

- DML operations (Update, Delete, Insert) on the Synapse SQL Pools tables
- Select operations on the table
- Which tables joined with which other tables
  - The most frequent tables to be joined together 





## How:

Depending on the `Microsoft.SqlServer.TransactSql.ScriptDom` namespace in the `Microsoft.SqlServer.DacFX` package from SQL Server team to do the core parsing. Solution built as a class library project referenced by Console app to use local or HTTP API implemented as Azure function to use it from any client. 

The main client is a spark notebook that uses *SynapseML* to call the HTTP API with the queries in a dataframe in parallel then parse the results and save them on spark table to be accessible from both spark and SQL Serverless Pool. 







## Project:

- QueryParserKernel: The class library that has the core functionality
- SqlParser: Console app to quick test, uses the QueryParserKernel
- SynapseQueryParser: Azure Function App with Open API to expose the functionality as http endpoint
