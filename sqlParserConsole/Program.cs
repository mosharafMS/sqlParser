// See https://aka.ms/new-console-template for more information
using Microsoft.SqlServer.Dac.Model;
using Microsoft.SqlServer.TransactSql.ScriptDom;
using System.Diagnostics;
using System.IO;
using System.Text.Json;
using QueryParserKernel;

string sqlFile = "sqlScripts.txt";

// get textReader object
string strTSQL=File.ReadAllText(sqlFile);

SynapseVisitor joinsVisitor = new SynapseVisitor();
SynapseQueryModel model = joinsVisitor.ProcessVisitor(strTSQL);
var outstring = JsonSerializer.Serialize<SynapseQueryModel>(model);
Console.WriteLine(outstring);





Console.WriteLine(".........done...........");
Console.ReadKey(true);






