// See https://aka.ms/new-console-template for more information
using Microsoft.SqlServer.Dac.Model;
using Microsoft.SqlServer.TransactSql.ScriptDom;
using System.Diagnostics;
using System.IO;
using System.Text.Json;

string sqlFile = "sqlScripts.txt";

// get textReader object
string strTSQL=File.ReadAllText(sqlFile);

JoinsVisitor joinsVisitor = new JoinsVisitor();
var output = joinsVisitor.ProcessVisitor(strTSQL);
Console.WriteLine(output);





Console.WriteLine(".........done...........");
Console.ReadKey(true);



public class JoinsVisitor : TSqlConcreteFragmentVisitor
{
    private List<String> _tables = new List<String>();
    private List<String> _joinColumns = new List<String>();
    private Dictionary<String, String> _tableAliases = new Dictionary<String, String>();
   
    private class joinModel
    {
        public List<string> JoinedTables { get; set; }
        public List<string> JoinedColumns { get; set; }
    }

    public string ProcessVisitor(string strSQL)
    {
        IList<ParseError> errors = new List<ParseError>();
        TSql160Parser parser = new TSql160Parser(false, SqlEngineType.SqlAzure);

        using (TextReader reader = new StringReader(strSQL))
        {
            TSqlScript sqlFragments = (TSqlScript)parser.Parse(reader, out errors);
            foreach (ParseError err in errors)
            {
                Console.WriteLine(err.Message);
            }

            // walk the tree now
            sqlFragments.Accept(this);
        }

        joinModel outObject = new joinModel();
        outObject.JoinedTables = _tables;
        outObject.JoinedColumns = _joinColumns;

        var outstring = JsonSerializer.Serialize<joinModel>(outObject);

        return outstring;
    }

   public override void ExplicitVisit(QualifiedJoin node)
    {

        var first = node.FirstTableReference;
        var second = node.SecondTableReference;
        var searchCondition = node.SearchCondition;
        //loop until the type of first and second is NamedTableReference (in case of multi tables joins)
        while (first.GetType() == typeof(QualifiedJoin))
        {
            var localSecond= ((QualifiedJoin)first).SecondTableReference;
            var localSearchCondition = ((QualifiedJoin)first).SearchCondition;
            first = ((QualifiedJoin)first).FirstTableReference;
            
          if(localSecond.GetType() == typeof(NamedTableReference))
            {
                _GetTableName((NamedTableReference)localSecond);
            }

            if (localSearchCondition.GetType() == typeof(BooleanComparisonExpression))
            {
                _GetJoinColumns((BooleanComparisonExpression)localSearchCondition);
            }

        }
        

        if (first.GetType() == typeof(NamedTableReference))
        {
            _GetTableName((NamedTableReference)first);
        }


        if (second.GetType() == typeof(NamedTableReference))
        {
            _GetTableName((NamedTableReference)second);
        }

        if (searchCondition.GetType() == typeof(BooleanComparisonExpression))
        {
            _GetJoinColumns((BooleanComparisonExpression)searchCondition);
        }

        // after finishing all tables and columsn, map aliases
        _mapTableAliases(ref _joinColumns);


        //Console.WriteLine("----Tables-----");
        //foreach (string t in _tables)
        //{
        //    Console.WriteLine(t);
        //}

        //Console.WriteLine("----Columns-----");
        //foreach (String c in _joinColumns)
        //{
        //    Console.WriteLine(c);
        //}

    }

    private void _GetTableName(NamedTableReference table)
    {
        var schemaName = table.SchemaObject.SchemaIdentifier != null ? table.SchemaObject.SchemaIdentifier.Value : "dbo";
        var tableName = table.SchemaObject.BaseIdentifier.Value;
        if(tableName != null)
        {
            _tables.Add(schemaName + "." + tableName);
            // add the table alias to search for it when adding the columns
           _tableAliases[table.Alias.Value]=schemaName + "." + tableName;
        }
    }

    private void _GetJoinColumns(BooleanComparisonExpression searchCondition)
    {
        var firstCol = searchCondition.FirstExpression as ColumnReferenceExpression;
        var secondCol = searchCondition.SecondExpression as ColumnReferenceExpression;
        if(firstCol != null && secondCol != null)
        {
            if (firstCol.MultiPartIdentifier.Count > 1)
            {
                var tableAlias=firstCol.MultiPartIdentifier.Identifiers[0].Value;
                var colName = firstCol.MultiPartIdentifier.Identifiers[1].Value;
                _joinColumns.Add(tableAlias + "." + colName);
            }
            else
                _joinColumns.Add(firstCol.MultiPartIdentifier.Identifiers[0].Value);

            if(secondCol.MultiPartIdentifier.Count >1 )
            {
                var tableAlias = secondCol.MultiPartIdentifier.Identifiers[0].Value;
                var colName= secondCol.MultiPartIdentifier.Identifiers[1].Value;
                _joinColumns.Add(tableAlias + "." + colName);
            }
            else
                _joinColumns.Add(secondCol.MultiPartIdentifier.Identifiers[0].Value);

        }
    }

    private void _mapTableAliases(ref List<String> columns)
    {
        for (int i=0;i<columns.Count;i++)
        {
            // if column has .
            if(columns[i].IndexOf('.') != -1)
            {
                var colName = columns[i].Split('.')[1];
                var alias = columns[i].Split('.')[0];
                if (_tableAliases.ContainsKey(alias))
                {
                    var table = _tableAliases[alias];
                    columns[i] = table + "." + colName;
                }
            }

        }
    }


}




