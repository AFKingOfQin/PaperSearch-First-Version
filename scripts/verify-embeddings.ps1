# AI-generated verification (Codex): exercise real CPU worker recovery on an isolated MySQL database.
$ErrorActionPreference = 'Stop'
Push-Location (Join-Path $PSScriptRoot '..')
$project = 'papersearch-m6-check-' + [guid]::NewGuid().ToString('N').Substring(0, 8)
$tests = (Resolve-Path -LiteralPath 'tests').Path
function Invoke-Compose {
    & docker compose -p $project --profile core --profile embedding @args
    if ($LASTEXITCODE) { throw "Compose $($args[0]) exited $LASTEXITCODE" }
}
try {
    Invoke-Compose build embedding
    Invoke-Compose up -d --wait mysql
    Invoke-Compose run --rm --no-deps --entrypoint alembic embedding upgrade head
    Invoke-Compose run --rm --no-deps -v "${tests}:/verify:ro" --entrypoint python embedding -u /verify/integration/semantic_recovery.py "/artifacts/$project"
} finally {
    try { Invoke-Compose stop mysql } finally { Pop-Location }
}
