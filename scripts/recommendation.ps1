#Requires -Version 7.0
param(
    [string]$Handoff = (Join-Path $PSScriptRoot '..\..\database'),
    [string]$RunId = 'backfill-50x1000-20261109',
    [switch]$CacheArtwork,
    [switch]$InstallQueryModel
)
$ErrorActionPreference = 'Stop'
$PSNativeCommandUseErrorActionPreference = $true
$handoffPath = (Resolve-Path -LiteralPath $Handoff).Path
foreach ($file in @('absa_database.parquet', 'build_config.json')) {
    if (!(Test-Path -LiteralPath (Join-Path $handoffPath $file) -PathType Leaf)) {
        throw "Handoff file missing: $file"
    }
}

Push-Location (Join-Path $PSScriptRoot '..')
try {
    if ($InstallQueryModel) {
        docker compose run --rm --no-deps --entrypoint python worker -m pip install --disable-pip-version-check --no-deps --target .runtime/query-python -r requirements-query.txt
        docker compose run --rm --no-deps --entrypoint python worker -m src.query_model
    }
    docker compose run --rm --no-deps -v "${handoffPath}:/handoff:ro" --entrypoint python worker -m src.recommendation_build --run-id $RunId
    docker compose up -d --wait recommendation dashboard
    if ($CacheArtwork) {
        docker compose run --rm --no-deps --entrypoint python worker -m src.recommendation_art $RunId
    }
    Write-Output 'Recommendation UI http://localhost:18082/'
    Write-Output 'Pipeline UI http://localhost:18081/'
} finally {
    Pop-Location
}
