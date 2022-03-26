using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace QueryParserKernel
{
    public class SynapseQueryModel
    {
        public SynapseQueryModel()
        {
            Command = string.Empty;
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
            InsertStatementTargets = new List<string>();
            DeleteStatementTargets = new List<string>();
            Errors = new List<string>();
            Hash = String.Empty;
        }
        public string Command { get; set; }
        public List<string> JoinedTables { get; set; }
        public List<string> JoinedColumns { get; set; }
        public bool IsSelectStatement { get; set; }
        public bool IsInsertStatement { get; set; }
        public bool IsBulkInsertStatement { get; set; }
        public bool IsUpdateStatement { get; set; }
        public bool IsDeleteStatement { get; set; }
        public bool IsCopyStatement { get; set; }
        public List<string> CopyStatementFrom { get; set; }
        public List<string> CopyStatementInto { get; set; }
        public List<string> InsertStatementTargets { get; set; }
        public List<string> DeleteStatementTargets { get; set; }
        public List<string> Errors { get; set; }

        public string Hash { get; set; }

    }
}
