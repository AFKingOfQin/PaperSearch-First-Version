# AI-generated code (Codex): verify durable state across a real MySQL restart.
$ErrorActionPreference = 'Stop'
Push-Location (Join-Path $PSScriptRoot '..')
$project = 'papersearch-m2-' + [guid]::NewGuid().ToString('N').Substring(0, 12)
$tests = (Resolve-Path -LiteralPath 'tests').Path
function Invoke-Compose {
    Write-Host "docker compose -p $project --profile core --profile test $args"
    & docker compose -p $project --profile core --profile test @args
    if ($LASTEXITCODE -ne 0) { throw "Compose $($args[0]) exited $LASTEXITCODE" }
}
function Test-Storage($stage) {
    Invoke-Compose run --rm --no-deps -v "${tests}:/verify:ro" --entrypoint python cli /verify/integration/mysql_state.py $stage "/artifacts/$project"
}
try {
    & .\.venv\Scripts\papersearch.exe preflight
    if ($LASTEXITCODE -ne 0) { throw 'Storage preflight failed.' }
    Invoke-Compose build cli
    Invoke-Compose up -d --wait --wait-timeout 180 mysql
    Invoke-Compose run --rm --no-deps --entrypoint alembic cli upgrade head
    Invoke-Compose run --rm --no-deps --entrypoint alembic cli current
    Test-Storage before
    Invoke-Compose restart mysql
    Invoke-Compose up -d --wait --wait-timeout 180 mysql
    Test-Storage after
    Write-Output "PASS M2 MySQL persistence and recovery ($project)"
} finally {
    try { Invoke-Compose stop mysql } finally { Pop-Location }
}
