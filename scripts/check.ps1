# Runs every local check. CI runs the same steps.
$ErrorActionPreference = 'Stop'
$PSNativeCommandUseErrorActionPreference = $true

$root = Split-Path -Parent $PSScriptRoot

function Step($name, [scriptblock]$body) {
    Write-Host "==> $name" -ForegroundColor Cyan
    & $body
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

Write-Host 'ALL CHECKS PASSED' -ForegroundColor Green
