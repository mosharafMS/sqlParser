using System.IO;
using System.Net;
using System.Threading.Tasks;
using System.Web.Http;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Azure.WebJobs;
using Microsoft.Azure.WebJobs.Extensions.Http;
using Microsoft.Azure.WebJobs.Extensions.OpenApi.Core.Attributes;
using Microsoft.Azure.WebJobs.Extensions.OpenApi.Core.Enums;
using Microsoft.Extensions.Logging;
using Microsoft.OpenApi.Models;
using Newtonsoft.Json;
using System.Text;
using System;
using QueryParserKernel;
using Microsoft.WindowsAzure.Storage.Table;

namespace SynapseQueryParser
{
    public class Parse
    {
        private readonly ILogger<Parse> _logger;

        

        public Parse(ILogger<Parse> log)
        {
            _logger = log;
        }

        [FunctionName("Parse")]
        [OpenApiOperation(operationId: "Run")]
        [OpenApiSecurity("function_key", SecuritySchemeType.ApiKey, Name = "code", In = OpenApiSecurityLocationType.Query)]
        [OpenApiRequestBody("application/json", typeof(RequestBodyModel), Description ="Request Body should have only one input, command which includes the SQL Statement to parse")]
        [OpenApiResponseWithBody(statusCode: HttpStatusCode.OK, contentType: "text/plain", bodyType: typeof(string), Description = "The OK response")]
        public async Task<IActionResult> Run(
            [HttpTrigger(AuthorizationLevel.Function, "post", Route = null)] HttpRequest req, [Table("SynapseQueries",Connection = "AzureWebJobsStorage")] IAsyncCollector<TableStorageItem> tableCollector)
        {
            _logger.LogInformation("HTTP trigger function processed a request.");
            SynapseQueryModel model = null;
            string responseMessage = null;

            try
            {
                responseMessage = null;

                string requestBody = await new StreamReader(req.Body).ReadToEndAsync();
                dynamic data = JsonConvert.DeserializeObject(requestBody);
  
                string? sqlCommand = data?.command;

                if (string.IsNullOrEmpty(sqlCommand))
                {
                    responseMessage = "This HTTP triggered function executed successfully. Pass a command in the request body for a response.";
                    
                    return new BadRequestObjectResult(responseMessage);
                }

                _logger.LogInformation("SQLCommand is not null or empty...proceed to parsing");
                //process
                SynapseVisitor joinsVisitor = new SynapseVisitor();
                model = joinsVisitor.ProcessVisitor(sqlCommand);
                _logger.LogInformation("Parse done...serialize");
                //serialize the model 
                responseMessage = JsonConvert.SerializeObject(model);

                _logger.LogInformation("Save the query in table storage for future reference and enhancements");
                //save the query
                TableStorageItem tableStorageItem = new TableStorageItem();
                tableStorageItem.PartitionKey = model.Hash;
                tableStorageItem.RowKey = Guid.NewGuid().ToString();
                tableStorageItem.Text = responseMessage;

                await tableCollector.AddAsync(tableStorageItem);


                if (model.Errors.Count > 0)
                {
                    _logger.LogError("Errors array is not empty. throwing exception");
                    _logger.LogError($"SqlCommand hash: {model.Hash}");
                    return new BadRequestObjectResult(responseMessage);
                }
                else
                    return new OkObjectResult(responseMessage);
            }
            catch (Exception e)
            {
                _logger.LogError(e.Message);
                _logger.LogError($"SqlCommand hash: {model!.Hash}");
                model!.Errors.Add(e.Message);
                responseMessage = JsonConvert.SerializeObject(model);
                return new BadRequestObjectResult(responseMessage);
            }
            
        }
    }

    public class RequestBodyModel
    {
        public string command { get; set; }
        
    }


    public class TableStorageItem: TableEntity
    {
        public string Text { get; set; }
    }

}

