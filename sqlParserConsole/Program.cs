// See https://aka.ms/new-console-template for more information
using Microsoft.SqlServer.Dac.Model;
using Microsoft.SqlServer.TransactSql.ScriptDom;
using System.Diagnostics;
using System.IO;
using System.Text.Json;

string sqlFile = "sqlScripts.txt";

// get textReader object
string strTSQL=File.ReadAllText(sqlFile);

SynapseVisitor joinsVisitor = new SynapseVisitor();
var output = joinsVisitor.ProcessVisitor(strTSQL);
Console.WriteLine(output);





Console.WriteLine(".........done...........");
Console.ReadKey(true);



public class SynapseVisitor : TSqlConcreteFragmentVisitor
{

    private Dictionary<String, String> _tableAliases = new Dictionary<String, String>();

    private SynapseQueryModel _synapsequerymodel;

    public SynapseVisitor()
    {
        _synapsequerymodel = new SynapseQueryModel();

    }

    private class SynapseQueryModel
    {
        public SynapseQueryModel()
        {
            JoinedTables = new List<String>();
            JoinedColumns = new List<String>();
            CopyStatementFrom = new List<String>();
            CopyStatementInto = new List<String>();
            IsCopyStatement = false;
            IsSelectStatement = false;
            IsInsertStatement = false;
            IsBulkInsertStatement = false;
            IsUpdateStatement = false;
            IsDeleteStatement = false;
            InsertStatmentTargets = new List<string>();
            DeleteStatementTargets = new List<string>();
        }
        public List<string> JoinedTables { get; set; }
        public List<string> JoinedColumns { get; set; }

        public bool IsSelectStatement { get; set; }

        public bool IsInsertStatement { get; set; }

        public bool IsBulkInsertStatement { get; set; }

        public bool IsUpdateStatement { get; set; }

        public bool IsDeleteStatement { get; set; }

        public bool IsCopyStatement { get; set; }

        public List<string> CopyStatementFrom { get; set; }

        public List<string> CopyStatementInto  { get; set; }

        public List<string> InsertStatmentTargets { get; set; }

        public List<string> DeleteStatementTargets { get; set; }

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

        var outstring = JsonSerializer.Serialize<SynapseQueryModel>(_synapsequerymodel);

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
                _synapsequerymodel.JoinedTables.Add(_GetTableName((NamedTableReference)localSecond));
            }

            if (localSearchCondition.GetType() == typeof(BooleanComparisonExpression))
            {
                _GetJoinColumns((BooleanComparisonExpression)localSearchCondition);
            }

        }
        

        if (first.GetType() == typeof(NamedTableReference))
        {
            _synapsequerymodel.JoinedTables.Add(_GetTableName((NamedTableReference)first));
        }


        if (second.GetType() == typeof(NamedTableReference))
        {
            _synapsequerymodel.JoinedTables.Add(_GetTableName((NamedTableReference)second));
        }

        if (searchCondition.GetType() == typeof(BooleanComparisonExpression))
        {
            _GetJoinColumns((BooleanComparisonExpression)searchCondition);
        }

        // after finishing all tables and columsn, map aliases
        _mapTableAliases(ref _synapsequerymodel);

    }

    public override void ExplicitVisit(CopyStatement node)
    {
        _synapsequerymodel.IsCopyStatement = true;

        IList<StringLiteral> copyFrom = node.From;
        if(copyFrom != null)
        { 
            foreach (StringLiteral s in copyFrom)
            {
                _synapsequerymodel.CopyStatementFrom.Add(s.Value);
            }
        }

        var tableName = _GetTableName(node.Into);
        if (tableName != null)
        {
            _synapsequerymodel.CopyStatementInto.Add(tableName);
        }

        base.ExplicitVisit(node);
    }

    public override void ExplicitVisit(SelectStatement node)
    {
        _synapsequerymodel.IsSelectStatement = true;
        base.ExplicitVisit(node);
    }

    public override void ExplicitVisit(InsertStatement node)
    {
        _synapsequerymodel.IsInsertStatement = true;
        InsertSpecification insertSpecs = node.InsertSpecification as InsertSpecification;
        if (insertSpecs != null)
        {
            if (insertSpecs.Target is NamedTableReference targetTable)
            {
                var tableName = _GetTableName(targetTable);
                _synapsequerymodel.InsertStatmentTargets.Add(tableName);
            }

        }
        base.ExplicitVisit(node);
    }

    public override void ExplicitVisit(InsertBulkStatement node)
    {
        _synapsequerymodel.IsBulkInsertStatement = true;
        base.ExplicitVisit(node);
    }

    public override void ExplicitVisit(DeleteStatement node)
    {
        _synapsequerymodel.IsDeleteStatement = true;
        DeleteSpecification deleteSpecs=node.DeleteSpecification as DeleteSpecification;
        if(deleteSpecs != null)
        {
            if (deleteSpecs.Target is NamedTableReference targetTable)
            {
                var tableName = _GetTableName(targetTable);
                _synapsequerymodel.DeleteStatementTargets.Add(tableName);
            }
        }
        base.ExplicitVisit(node);
    }

    private string _GetTableName(NamedTableReference table)
    {
        var schemaName = table.SchemaObject.SchemaIdentifier != null ? table.SchemaObject.SchemaIdentifier.Value : "dbo";
        var tableName = table.SchemaObject.BaseIdentifier.Value;
        if (tableName != null)
        {
            // add the table alias to search for it when adding the columns
            if(table.Alias != null)
                _tableAliases[table.Alias.Value] = schemaName + "." + tableName;
            return (schemaName + "." + tableName);
        }
        else
            return null;
    }

    private string _GetTableName(SchemaObjectName schemaObject)
    {
        var schemaName = schemaObject.SchemaIdentifier != null ? schemaObject.SchemaIdentifier.Value : "dbo";
        var tableName = schemaObject.BaseIdentifier.Value;
        if (tableName != null)
        {
            return (schemaName + "." + tableName);
        }
        else
            return null;
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
                _synapsequerymodel.JoinedColumns.Add(tableAlias + "." + colName);
            }
            else
                _synapsequerymodel.JoinedColumns.Add(firstCol.MultiPartIdentifier.Identifiers[0].Value);

            if(secondCol.MultiPartIdentifier.Count >1 )
            {
                var tableAlias = secondCol.MultiPartIdentifier.Identifiers[0].Value;
                var colName= secondCol.MultiPartIdentifier.Identifiers[1].Value;
                _synapsequerymodel.JoinedColumns.Add(tableAlias + "." + colName);
            }
            else
                _synapsequerymodel.JoinedColumns.Add(secondCol.MultiPartIdentifier.Identifiers[0].Value);

        }
    }

    private void _mapTableAliases(ref SynapseQueryModel model)
    {
        List<string> columns = model.JoinedColumns;
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




