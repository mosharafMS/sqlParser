using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Microsoft.SqlServer.TransactSql.ScriptDom;
using System.Threading.Tasks;

using System.IO;
using System.Security.Cryptography;

namespace QueryParserKernel
{

    [Serializable]
    public class SynapseVisitor : TSqlConcreteFragmentVisitor
    {

        private Dictionary<String, String> _tableAliases = new Dictionary<String, String>();

        private SynapseQueryModel _synapsequerymodel;

        public SynapseVisitor()
        {
            _synapsequerymodel = new SynapseQueryModel();

        }


        public SynapseQueryModel ProcessVisitor(string strSQL)
        {
            IList<ParseError> errors = new List<ParseError>();
            TSql160Parser parser = new TSql160Parser(false, SqlEngineType.SqlAzure);


            TSqlScript sqlFragments = null;
            using (TextReader reader = new StringReader(strSQL))
            {
                sqlFragments = (TSqlScript)parser.Parse(reader, out errors);
                foreach (ParseError error in errors)
                {
                    _synapsequerymodel.Errors.Add(String.Format("Error in line {0}, column {1}, message: {2}", error.Line.ToString(), error.Column.ToString(), error.Message.ToString()));
                }
                // walk the tree now
                sqlFragments.Accept(this);
            }

            //remove the comments before returning back to sqlcommand
            strSQL = String.Join(
                String.Empty,
                sqlFragments.ScriptTokenStream
                .Where(x => x.TokenType != TSqlTokenType.MultilineComment)
                .Where(x => x.TokenType != TSqlTokenType.SingleLineComment)
                .Select(x => x.Text));


            _synapsequerymodel.SQLCommand = strSQL;

            _synapsequerymodel.Hash = HashString(strSQL);

            return _synapsequerymodel;
        }

        private string HashString(string strSQL)
        {
            //create SHA hash object
            using (SHA256 sha256 = SHA256.Create())
            {
                byte[] hashedBytes = sha256.ComputeHash(Encoding.UTF8.GetBytes(strSQL));
                StringBuilder hex = new StringBuilder(hashedBytes.Length * 2);
                foreach (byte b in hashedBytes)
                    hex.AppendFormat("{0:x2}", b);
                return hex.ToString();
            }
        }

        public override void ExplicitVisit(QualifiedJoin node)
        {

            var first = node.FirstTableReference;
            var second = node.SecondTableReference;
            var searchCondition = node.SearchCondition;
            //loop until the type of first and second is NamedTableReference (in case of multi tables joins)
            while (first.GetType() == typeof(QualifiedJoin))
            {
                var localSecond = ((QualifiedJoin)first).SecondTableReference;
                var localSearchCondition = ((QualifiedJoin)first).SearchCondition;
                first = ((QualifiedJoin)first).FirstTableReference;

                if (localSecond.GetType() == typeof(NamedTableReference))
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
            if (copyFrom != null)
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
            var querySpecs = node.QueryExpression as QuerySpecification;
            var fromClause = querySpecs?.FromClause as FromClause;
            var tables = fromClause?.TableReferences;
            if (tables != null && tables.Count > 0 && tables[0].GetType() == typeof(NamedTableReference))
            {
                foreach (var table in tables)
                {
                    var tableName = _GetTableName((NamedTableReference)table);
                    if (tableName != null)
                    {
                        _synapsequerymodel.SelectStatementFrom.Add(tableName);
                    }
                }
            }
            
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
                    _synapsequerymodel.InsertStatementTargets.Add(tableName);
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
            DeleteSpecification deleteSpecs = node.DeleteSpecification as DeleteSpecification;
            if (deleteSpecs != null)
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
                if (table.Alias != null)
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
            if (firstCol != null && secondCol != null)
            {
                if (firstCol.MultiPartIdentifier.Count > 1)
                {
                    var tableAlias = firstCol.MultiPartIdentifier.Identifiers[0].Value;
                    var colName = firstCol.MultiPartIdentifier.Identifiers[1].Value;
                    _synapsequerymodel.JoinedColumns.Add(tableAlias + "." + colName);
                }
                else
                    _synapsequerymodel.JoinedColumns.Add(firstCol.MultiPartIdentifier.Identifiers[0].Value);

                if (secondCol.MultiPartIdentifier.Count > 1)
                {
                    var tableAlias = secondCol.MultiPartIdentifier.Identifiers[0].Value;
                    var colName = secondCol.MultiPartIdentifier.Identifiers[1].Value;
                    _synapsequerymodel.JoinedColumns.Add(tableAlias + "." + colName);
                }
                else
                    _synapsequerymodel.JoinedColumns.Add(secondCol.MultiPartIdentifier.Identifiers[0].Value);

            }
        }

        private void _mapTableAliases(ref SynapseQueryModel model)
        {
            List<string> columns = model.JoinedColumns;
            for (int i = 0; i < columns.Count; i++)
            {
                // if column has .
                if (columns[i].IndexOf('.') != -1)
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
}