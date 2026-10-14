# Azure Functions installs from requirements.txt; regenerate it from uv.lock after changing dependencies.
$ErrorActionPreference = 'Stop'
$PSNativeCommandUseErrorActionPreference = $true

Push-Location (Join-Path (Split-Path -Parent $PSScriptRoot) 'collector')
try {
    uv export --format requirements-txt --no-dev --no-hashes --no-emit-project --quiet -o requirements.txt
}
finally {
    Pop-Location
}
