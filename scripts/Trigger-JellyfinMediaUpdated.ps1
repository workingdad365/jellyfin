[CmdletBinding()]
param(
    [Parameter(Position = 0, Mandatory = $true, ValueFromRemainingArguments = $true)]
    [string[]]$Path,

    [Parameter()]
    [string]$ApiKey = $env:JELLYFIN_API_KEY,

    [Parameter()]
    [string]$ServerUrl = $(if ([string]::IsNullOrWhiteSpace($env:JELLYFIN_SERVER_URL)) { 'http://localhost:8096' } else { $env:JELLYFIN_SERVER_URL }),

    [Parameter()]
    [ValidateSet('Created', 'Modified', 'Deleted')]
    [string]$UpdateType = 'Created',

    [Parameter()]
    [switch]$SkipCertificateCheck
)

Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'

if ($Path.Count -eq 0) {
    throw 'At least one path must be provided.'
}

if ([string]::IsNullOrWhiteSpace($ApiKey)) {
    throw 'ApiKey is missing. Provide -ApiKey or set the JELLYFIN_API_KEY environment variable.'
}

$trimmedServerUrl = $ServerUrl.TrimEnd('/')
$uri = "$trimmedServerUrl/Library/Media/Updated"

$updates = @(
    foreach ($singlePath in $Path) {
        if ([string]::IsNullOrWhiteSpace($singlePath)) {
            continue
        }

        [PSCustomObject]@{
            Path = $singlePath
            UpdateType = $UpdateType
        }
    }
)

if ($updates.Count -eq 0) {
    throw 'No valid non-empty paths were provided.'
}

$body = @{
    Updates = $updates
} | ConvertTo-Json -Depth 4

$headers = @{
    'X-Emby-Token' = $ApiKey
}

$requestParams = @{
    Method = 'Post'
    Uri = $uri
    Headers = $headers
    ContentType = 'application/json'
    Body = $body
}

if ($SkipCertificateCheck) {
    $requestParams.SkipCertificateCheck = $true
}

Write-Host ("Sending media update for {0} path(s) to {1}" -f $updates.Count, $uri)

try {
    $null = Invoke-RestMethod @requestParams
    Write-Host 'Done. Jellyfin accepted the update request.'
}
catch {
    $message = $_.Exception.Message
    throw ("Request failed: {0}" -f $message)
}
