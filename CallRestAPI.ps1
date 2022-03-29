[CmdletBinding()]
param (
    [Parameter(Mandatory=$true)]
    $url,
    [Parameter(Mandatory=$true)]
    $AuthKey,
    [Parameter(Mandatory=$true)]
    $SQLCommand
)

$body=$SQLCommand

# add the $AuthKey as header value for x-functions-key header
$headers = @{
    "x-functions-key" = $AuthKey
    "Content-Type" = "application/json"
}

# Call rest API
$response = Invoke-RestMethod -Uri $url -Method Post -Headers $headers -Body $body

# Convert $response to json and write to output
$response | ConvertTo-Json | Write-Host