from urllib.request import Request
import requests

# function to call rest API with code authentication
def call_rest_api(url, key, sqlCommand):
    # set headers
    headers={'Content-Type': 'application/json', 'x-functions-key': key}
    data=sqlCommand
    response= requests.request(method="POST", url=url, data=data, headers=headers)
    return response.content.decode()



url='https://synapsequeryparserfunc.azurewebsites.net/api/parse'
key=''
sqlcommand="select * from synapse.dbo.test"
response = call_rest_api(url, key, sqlcommand)
print(response)


