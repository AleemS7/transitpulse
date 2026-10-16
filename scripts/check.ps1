# Runs every local check. CI runs the same script with -SkipBundle (bundle validation needs workspace sign-in).
param([switch]$SkipBundle)

$ErrorActionPreference = 'Stop'
$PSNativeCommandUseErrorActionPreference = $true

$root = Split-Path -Parent $PSScriptRoot

function Step($name, [scriptblock]$body) {
    Write-Host "==> $name" -ForegroundColor Cyan
    & $body
}

# PySpark needs Java 17+; prefer an installed JDK 17 over an older java on PATH.
if ($IsWindows -and -not $env:JAVA_HOME) {
    $jdk = Get-ChildItem 'C:\Program Files\Microsoft\jdk-17*', 'C:\Program Files\Eclipse Adoptium\jdk-17*' -Directory -ErrorAction SilentlyContinue |
        Sort-Object Name | Select-Object -Last 1
    if ($jdk) { $env:JAVA_HOME = $jdk.FullName }
}

# winget installs the Databricks CLI outside PATH until a new shell starts.
if ($IsWindows -and -not (Get-Command databricks -ErrorAction SilentlyContinue)) {
    $cli = Get-ChildItem "$env:LOCALAPPDATA\Microsoft\WinGet\Packages\Databricks.DatabricksCLI*\databricks.exe" -ErrorAction SilentlyContinue |
        Select-Object -First 1
    if ($cli) { $env:PATH = "$($cli.DirectoryName);$env:PATH" }
}

Push-Location $root
try {
    Step 'pipeline: sync (locked)' { uv sync --locked --quiet }
    Step 'pipeline: lint' { uv run ruff check src tests }
    Step 'pipeline: format' { uv run ruff format --check src tests }
    Step 'pipeline: types' { uv run mypy }
    Step 'pipeline: sql lint' { uv run sqlfluff lint src sql }
    Step 'pipeline: tests (local Spark)' { uv run pytest }
    Step 'pipeline: dependency audit' { uv run pip-audit --progress-spinner off }
}
finally {
    Pop-Location
}

Push-Location (Join-Path $root 'collector')
try {
    Step 'collector: sync (locked)' { uv sync --locked --quiet }
    Step 'collector: lint' { uv run ruff check . }
    Step 'collector: format' { uv run ruff format --check . }
    Step 'collector: types' { uv run mypy }
    Step 'collector: tests' { uv run pytest }
    Step 'collector: requirements.txt matches uv.lock' {
        $expected = uv export --format requirements-txt --no-dev --no-hashes --no-emit-project --quiet |
            Where-Object { $_ -notmatch '^#' }
        $actual = Get-Content requirements.txt | Where-Object { $_ -notmatch '^#' }
        if (Compare-Object $expected $actual) { throw 'requirements.txt is stale; run scripts/export-requirements.ps1' }
    }
    Step 'collector: dependency audit' { uv run pip-audit --progress-spinner off }
}
finally {
    Pop-Location
}

Step 'infra: bicep build' { az bicep build --file (Join-Path $root 'infra/collector.bicep') --stdout | Out-Null }
if (-not $SkipBundle) {
    Step 'bundle: validate' {
        Push-Location $root
        try { databricks bundle validate --target dev | Out-Null } finally { Pop-Location }
    }
}

Write-Host 'ALL CHECKS PASSED' -ForegroundColor Green
