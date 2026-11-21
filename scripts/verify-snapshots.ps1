# AI-generated code (Codex): verify fixture derivatives and search package recovery on isolated services.
$ErrorActionPreference = 'Stop'
Push-Location (Join-Path $PSScriptRoot '..')
$project = 'papersearch-m4-' + [guid]::NewGuid().ToString('N').Substring(0, 12)
$tests = (Resolve-Path -LiteralPath 'tests').Path
function Invoke-Compose {
    Write-Host "docker compose -p $project --profile core --profile snapshot $args"
    & docker compose -p $project --profile core --profile snapshot @args
    if ($LASTEXITCODE -ne 0) { throw "Compose $($args[0]) exited $LASTEXITCODE" }
}
try {
    & .\.venv\Scripts\papersearch.exe preflight
    if ($LASTEXITCODE -ne 0) { throw 'Storage preflight failed.' }
    Invoke-Compose build snapshot
    Invoke-Compose up -d --wait --wait-timeout 180 mysql kafka
    Invoke-Compose run --rm --no-deps --entrypoint alembic snapshot upgrade head
    Invoke-Compose run --rm --no-deps --entrypoint alembic snapshot current
    Invoke-Compose run --rm --no-deps -v "${tests}:/verify:ro" --entrypoint python snapshot -u /verify/integration/snapshot_recovery.py "/artifacts/$project"
    Invoke-Compose restart mysql
    Invoke-Compose up -d --wait --wait-timeout 180 mysql
    Invoke-Compose run --rm --no-deps snapshot snapshot --root "/artifacts/$project/store" verify
    Write-Output "PASS M4 snapshot publication and recovery ($project)"
} finally {
    try { Invoke-Compose stop mysql kafka } finally { Pop-Location }
}
