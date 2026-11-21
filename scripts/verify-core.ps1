# AI-generated code (Codex): Phase 0 checks against isolated, persistent test volumes.
$ErrorActionPreference = 'Stop'
Push-Location (Join-Path $PSScriptRoot '..')
function Invoke-Compose {
    & docker compose --project-name papersearch-m0 --profile core --profile test @args
    if ($LASTEXITCODE -ne 0) { throw "Compose failed: $($args[0]) (exit $LASTEXITCODE)" }
}
try {
    & .\.venv\Scripts\python.exe -m papersearch.cli doctor
    if ($LASTEXITCODE -ne 0) { throw 'Host or Linux engine check failed.' }
    & .\.venv\Scripts\python.exe -m papersearch.cli preflight
    if ($LASTEXITCODE -ne 0) { throw 'Storage preflight failed.' }
    Invoke-Compose config --quiet
    Invoke-Compose build cli
    Invoke-Compose up -d --wait --wait-timeout 180 mysql kafka
    $mysqlBefore = Invoke-Compose run --rm --no-deps cli smoke mysql
    $mysqlBefore
    Invoke-Compose run --rm --no-deps cli smoke kafka
    Invoke-Compose run --rm --no-deps --entrypoint alembic cli current

    $topics = '/opt/kafka/bin/kafka-topics.sh'
    $topic = 'papersearch.m0.volume-check'
    Invoke-Compose exec -T -e 'KAFKA_HEAP_OPTS=-Xms16m -Xmx96m' kafka $topics --bootstrap-server kafka:9092 --create --if-not-exists --topic $topic --partitions 3 --replication-factor 1

    # Only replace this verification project's containers. Both named volumes retain their data.
    Invoke-Compose up -d --force-recreate --wait --wait-timeout 180 mysql kafka
    $mysqlAfter = Invoke-Compose run --rm --no-deps cli smoke mysql
    $mysqlAfter
    if ($mysqlBefore -ne $mysqlAfter) { throw 'MySQL server identity changed after recreation.' }
    Invoke-Compose run --rm --no-deps cli smoke kafka
    Invoke-Compose exec -T -e 'KAFKA_HEAP_OPTS=-Xms16m -Xmx96m' kafka $topics --bootstrap-server kafka:9092 --describe --topic $topic

    foreach ($service in 'mysql', 'kafka') {
        $container = Invoke-Compose ps -q $service
        & docker inspect --format '{{.Name}} cpu_nano={{.HostConfig.NanoCpus}} memory_bytes={{.HostConfig.Memory}}' $container
        if ($LASTEXITCODE -ne 0) { throw "Could not read limits for $service." }
    }
    Write-Output 'PASS M0 service checks: same MySQL server UUID and Kafka topic after container replacement.'
} finally {
    Pop-Location
}
