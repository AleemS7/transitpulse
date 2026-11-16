# Azure Functions and Databricks Apps install from requirements.txt; regenerate it from uv.lock after
# changing dependencies in collector/ or api/.
$ErrorActionPreference = 'Stop'
$PSNativeCommandUseErrorActionPreference = $true

$root = Split-Path -Parent $PSScriptRoot
foreach ($project in 'collector', 'api') {
    Push-Location (Join-Path $root $project)
    try {
        uv export --format requirements-txt --no-dev --no-hashes --no-emit-project --quiet -o requirements.txt
    }
    finally {
        Pop-Location
    }
}
