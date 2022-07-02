# Synapse Query Analytics

[![Build and deploy dotnet core app to Azure Function App - synapseQueryParserfunc](https://github.com/mosharafMS/sqlParser/actions/workflows/master_synapsequeryparserfunc.yml/badge.svg)](https://github.com/mosharafMS/sqlParser/actions/workflows/master_synapsequeryparserfunc.yml)

[![Deploy to nuget](https://github.com/mosharafMS/sqlParser/actions/workflows/publish_nuget.yml/badge.svg)](https://github.com/mosharafMS/sqlParser/actions/workflows/publish_nuget.yml)

## Why:

You want to get insights about Synapse SQL tables' usage including

- DML operations (Update, Delete, Insert) on the Synapse SQL Pools tables
- Select operations on the table
- Which tables joined with which other tables
  - The most frequent tables to be joined together
- Information about grouped by columns, aggregated columns...etc. 



## Demo video
[Synapse Query Analytics](https://youtu.be/JS6apfe5Lbs)

## How:

The core of the solution is a Synapse SQL Parser library. It's built depending on the `Microsoft.SqlServer.TransactSql.ScriptDom` namespace in the `Microsoft.SqlServer.DacFX` package from SQL Server team to do the core parsing. 

The core library is implemented as a class library project compiled against .NET Standard 2.1 and published to Nuget at [SynapseQueryParserKernel](https://www.nuget.org/packages/SynapseQueryParserKernel)

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
  Both implementations are added in the [/Notebooks](/Notebooks/) folder

## Permissions on the SQL Pools
The pipeline that reads the query store queries from the SQL Pool uses this query

```sql
SELECT txt.query_sql_text,txt.statement_sql_handle,qry.query_id,qry.object_id,qry.is_internal_query,qry.last_execution_time,SUM(count_executions) AS count_executions
FROM sys.query_store_query_text txt
INNER JOIN sys.query_store_query qry
    ON Qry.query_text_id = txt.query_text_id
JOIN sys.query_store_plan pln
    ON qry.query_id=pln.query_id
JOIN sys.query_store_runtime_stats runstate
ON pln.plan_id=runstate.plan_id
GROUP BY txt.query_sql_text,txt.statement_sql_handle,qry.query_id,qry.object_id,qry.is_internal_query,qry.last_execution_time; 
```

The permission needed to run this query is only ***VIEW DATABASE STATE*** on the SQL Pool user database. 

This sample code shows how to grant it

```sql
-- In master
CREATE LOGIN sqlQueryStoreReader WITH PASSWORD='<YourStr0ngP@s$w0rd>'


-- In the user database
CREATE USER sqlQueryStoreReader FROM LOGIN sqlQueryStoreReader

GRANT VIEW DATABASE STATE TO sqlQueryStoreReader;
```

## Project:

- QueryParserKernel: The class library that has the core functionality
- SqlParser: Console app to quick test, uses the QueryParserKernel
- SynapseQueryParser: Azure Function App with Open API to expose the functionality as http endpoint
- Notebooks folder to have the sample notebooks for using the library 
