using Microsoft.Azure.WebJobs.Extensions.OpenApi.Core.Abstractions;
using Microsoft.Azure.WebJobs.Extensions.OpenApi.Core.Enums;
using Microsoft.OpenApi.Models;
using Microsoft.SqlServer.TransactSql.ScriptDom;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;


namespace SynapseQueryParser
{
    public class OpenApiConfigurationOptions : IOpenApiConfigurationOptions
    {
        public OpenApiInfo Info { get; set; } = new OpenApiInfo()
        {
            Version = "1.0.0",
            Title = "Synapse SQL Query Analytics",
            Description = "HTTP APIs that parse and analyze Synapse SQL",
            Contact = new OpenApiContact()
            {
                Name = "Mohamed Sharaf",
                Email = "Mohamed.Sharaf@Microsoft.com",
                Url = new Uri("https://github.com/mosharafMS/sqlParser/issues"),
            },
            License = new OpenApiLicense()
            {
                Name = "MIT",
                Url = new Uri("http://opensource.org/licenses/MIT"),
            }
        };
        private bool forcehttp=false;
        private bool forcehttps = false;
        private bool includerequestinghostname = false;
        public List<OpenApiServer> Servers { get; set; } = new List<OpenApiServer>();

        public OpenApiVersionType OpenApiVersion { get; set; } = OpenApiVersionType.V2;
        public bool IncludeRequestingHostName { get => includerequestinghostname; set => includerequestinghostname=value; }
        public bool ForceHttp { get => forcehttp; set => forcehttp=value; }
        public bool ForceHttps { get => forcehttps ; set => forcehttps=value; }
    }
}
