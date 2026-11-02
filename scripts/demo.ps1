param([int]$Reviews = 5000, [int]$Events = 200)
$ErrorActionPreference = 'Stop'
Set-Location -LiteralPath (Split-Path -Parent $PSScriptRoot)

function Invoke-Compose {
    param([string[]]$Arguments)
    & docker compose @Arguments
    if ($LASTEXITCODE -ne 0) { throw "Docker Compose failed with exit code $LASTEXITCODE" }
}

Invoke-Compose -Arguments @('build')
Invoke-Compose -Arguments @('up', '-d', 'kafka', 'airflow')
Invoke-Compose -Arguments @('run', '--rm', 'worker', 'all', '--limit', "$Reviews")
Invoke-Compose -Arguments @('run', '--rm', '--entrypoint', 'python', 'worker', '-m', 'unittest', 'discover', '-s', 'tests', '-v')
Invoke-Compose -Arguments @('run', '--rm', '--entrypoint', 'python', 'worker', '-m', 'src.streaming', 'init')
foreach ($round in 1..2) {
    Write-Output "Kafka replay round $round"
    Invoke-Compose -Arguments @('run', '--rm', '--entrypoint', 'python', 'worker', '-m', 'src.streaming', 'produce', '--count', "$Events")
    Invoke-Compose -Arguments @('run', '--rm', '--entrypoint', 'python', 'worker', '-m', 'src.streaming', 'consume', '--count', "$Events")
    Invoke-Compose -Arguments @('run', '--rm', '--entrypoint', 'python', 'worker', '-m', 'src.streaming', 'export')
}
Write-Output 'Open http://localhost:18080 and trigger the steam_batch DAG to verify orchestration.'
