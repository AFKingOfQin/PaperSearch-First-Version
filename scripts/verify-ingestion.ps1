# AI-generated code (Codex): run isolated Kafka/MySQL recovery trials and stop their services.
$ErrorActionPreference = 'Stop'
Push-Location (Join-Path $PSScriptRoot '..')
$project = 'papersearch-m3-' + [guid]::NewGuid().ToString('N').Substring(0, 12)
$tests = (Resolve-Path -LiteralPath 'tests').Path
function Invoke-Compose {
    Write-Host "docker compose -p $project --profile core --profile test $args"
    & docker compose -p $project --profile core --profile test @args
    if ($LASTEXITCODE -ne 0) { throw "Compose $($args[0]) exited $LASTEXITCODE" }
}
function Test-Ingestion($stage) {
    Invoke-Compose run --rm --no-deps -v "${tests}:/verify:ro" --entrypoint python cli -u /verify/integration/kafka_recovery.py $stage "/artifacts/$project"
}
try {
    & .\.venv\Scripts\papersearch.exe preflight
    if ($LASTEXITCODE -ne 0) { throw 'Storage preflight failed.' }
    Invoke-Compose build cli
    Invoke-Compose up -d --wait --wait-timeout 180 mysql kafka
    # Fresh schemas allow baseline and fault runs to share the exact same Kafka log.
    $sql = foreach ($scenario in 'baseline','before','after') {
        "CREATE DATABASE m3_$scenario; GRANT ALL ON m3_$scenario.* TO 'papersearch'@'%';"
    }
    $sql | docker compose -p $project exec -T mysql sh -c 'export MYSQL_PWD=$MYSQL_ROOT_PASSWORD; exec mysql -uroot'
    if ($LASTEXITCODE -ne 0) { throw 'Could not prepare isolated test databases' }
    Test-Ingestion setup
    Invoke-Compose stop mysql
    Test-Ingestion outage
    Invoke-Compose up -d --wait --wait-timeout 180 mysql
    Test-Ingestion recover
    Write-Output "PASS M3 Kafka recovery ($project)"
} finally {
    try { Invoke-Compose stop mysql kafka } finally { Pop-Location }
}
