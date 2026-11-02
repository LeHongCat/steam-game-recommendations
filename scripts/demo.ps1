param([switch]$SkipBuild)
$ErrorActionPreference = 'Stop'
Set-Location -LiteralPath (Split-Path -Parent $PSScriptRoot)

function Invoke-Compose {
    param([string[]]$Arguments)
    & docker compose @Arguments
    if ($LASTEXITCODE -ne 0) { throw "Docker Compose failed with exit code $LASTEXITCODE" }
}

if (-not $SkipBuild) { Invoke-Compose -Arguments @('build') }
Invoke-Compose -Arguments @('up', '-d', 'kafka', 'airflow')
Invoke-Compose -Arguments @('stop', 'consumer')
Invoke-Compose -Arguments @('run', '--rm', 'worker', 'all')
Invoke-Compose -Arguments @('run', '--rm', '--entrypoint', 'python', 'worker', '-m', 'unittest', 'discover', '-s', 'tests', '-v')
$publishedJson = & docker compose run --rm --entrypoint python worker -m src.streaming publish
if ($LASTEXITCODE -ne 0) { throw 'Kafka publisher failed' }
Write-Output $publishedJson
$published = $publishedJson | ConvertFrom-Json
if ($published.published -gt 0) {
    Invoke-Compose -Arguments @('run', '--rm', '--entrypoint', 'python', 'worker', '-m', 'src.streaming', 'consume', '--count', "$($published.published)", '--timeout-seconds', '90')
}
Invoke-Compose -Arguments @('run', '--rm', '--entrypoint', 'python', 'worker', '-m', 'src.streaming', 'export')
Invoke-Compose -Arguments @('up', '-d', 'consumer')
Invoke-Compose -Arguments @('exec', 'airflow', 'airflow', 'dags', 'trigger', 'steam_api_batch')
Invoke-Compose -Arguments @('exec', 'airflow', 'airflow', 'dags', 'unpause', 'steam_api_batch')
Write-Output 'Open http://localhost:18080 and inspect the steam_api_batch DAG.'
