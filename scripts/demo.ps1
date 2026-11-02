param(
    [switch]$SkipBuild,
    [ValidateRange(30, 1800)][int]$TimeoutSeconds = 300
)

Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'
Set-Location -LiteralPath (Split-Path -Parent $PSScriptRoot)

function Invoke-Compose {
    param([string[]]$Arguments)
    $output = & docker compose @Arguments
    if ($LASTEXITCODE -ne 0) {
        throw "docker compose $($Arguments -join ' ') failed (exit $LASTEXITCODE).`n$output"
    }
    return $output
}

if (-not $SkipBuild) {
    Invoke-Compose -Arguments @('build')
}

Write-Host 'Starting Kafka and Airflow...'
Invoke-Compose -Arguments @('up', '-d', '--wait', '--wait-timeout', "$TimeoutSeconds", 'kafka', 'airflow')
Invoke-Compose -Arguments @('run', '--rm', '--entrypoint', 'python', 'worker', '-m', 'src.streaming', 'init')
Invoke-Compose -Arguments @('up', '-d', 'consumer')

$deadline = [DateTime]::UtcNow.AddSeconds($TimeoutSeconds)
do {
    $dags = Invoke-Compose -Arguments @('exec', '-T', 'airflow', 'airflow', 'dags', 'list', '--output', 'json') | ConvertFrom-Json
    $dag = $dags | Where-Object { $_.dag_id -eq 'steam_api_batch' }
    if ($dag) { break }
    if ([DateTime]::UtcNow -ge $deadline) { throw 'Airflow did not load steam_api_batch before timeout.' }
    Start-Sleep -Seconds 2
} while ($true)

$runId = 'demo-' + [Guid]::NewGuid().ToString('N')
Invoke-Compose -Arguments @('exec', '-T', 'airflow', 'airflow', 'dags', 'unpause', 'steam_api_batch')
Invoke-Compose -Arguments @('exec', '-T', 'airflow', 'airflow', 'dags', 'trigger', '--run-id', $runId, 'steam_api_batch')
Write-Host "Waiting for Airflow run $runId..."

$deadline = [DateTime]::UtcNow.AddSeconds($TimeoutSeconds)
do {
    $runs = Invoke-Compose -Arguments @('exec', '-T', 'airflow', 'airflow', 'dags', 'list-runs', 'steam_api_batch', '--output', 'json') | ConvertFrom-Json
    $run = $runs | Where-Object { $_.run_id -eq $runId }
    if ($run -and $run.state -eq 'success') { break }
    if ($run -and $run.state -eq 'failed') { throw "Airflow run $runId failed; inspect http://localhost:18080." }
    if ([DateTime]::UtcNow -ge $deadline) { throw "Airflow run $runId did not finish before timeout." }
    Start-Sleep -Seconds 2
} while ($true)

$reportDir = Join-Path (Get-Location) 'reports\steam_api'
New-Item -ItemType Directory -Path $reportDir -Force | Out-Null
ConvertTo-Json -InputObject @($runs) -Depth 10 | Set-Content -LiteralPath (Join-Path $reportDir 'AIRFLOW_RUNS.json') -Encoding utf8

Write-Host 'Waiting for the consumer and checking batch/stream consistency...'
Invoke-Compose -Arguments @('run', '--rm', '--entrypoint', 'python', 'worker', '-m', 'src.reporting', '--wait-seconds', "$TimeoutSeconds")
Write-Host 'Demo complete. Airflow: http://localhost:18080'
