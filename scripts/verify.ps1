# Project verify command for Windows PowerShell 5.1.
# Usage: scripts\verify.ps1 [-Full] [-Down]
param(
    [switch]$Full,
    [switch]$Down
)
$ErrorActionPreference = 'Stop'
Set-Location (Join-Path $PSScriptRoot '..')
$env:MSYS_NO_PATHCONV = '1'

function Banner([string]$n, [string]$text) {
    Write-Host ''
    Write-Host "=== STEP ${n}: $text ==="
}
function Check([string]$what) {
    if ($LASTEXITCODE -ne 0) {
        throw "FAILED: $what (exit code $LASTEXITCODE)"
    }
}

Banner 1 'docker info'
docker info | Out-Null
Check 'docker info'
Write-Host 'docker is reachable'

Banner 2 'CR check (no CR in scripts, mvnw, docker, infra)'
$crFiles = @()
foreach ($p in 'scripts', 'mvnw', 'docker', 'infra') {
    if (Test-Path $p) {
        foreach ($f in (Get-ChildItem -Path $p -Recurse -File)) {
            $bytes = [System.IO.File]::ReadAllBytes($f.FullName)
            if ($bytes -contains 13) { $crFiles += $f.FullName }
        }
    }
}
if ($crFiles.Count -gt 0) {
    $crFiles | ForEach-Object { Write-Host $_ }
    throw 'FAILED: files contain CR characters'
}
Write-Host 'no CR characters found'

Banner 3 '.\mvnw.cmd -B verify'
& .\mvnw.cmd -B verify
Check 'mvnw verify'

Banner 4 'KafkaTemplate only in outbox packages and KafkaConfig'
$offenders = @()
foreach ($d in (Get-ChildItem -Directory)) {
    $src = Join-Path $d.FullName 'src\main\java'
    if (Test-Path $src) {
        $hits = Get-ChildItem -Path $src -Recurse -Filter *.java |
            Select-String -Pattern 'KafkaTemplate' -List |
            Where-Object { $_.Path -notmatch '[\/]outbox[\/]' -and $_.Path -notmatch 'KafkaConfig' }
        foreach ($h in $hits) { $offenders += $h.Path }
    }
}
if ($offenders.Count -gt 0) {
    $offenders | ForEach-Object { Write-Host $_ }
    throw 'FAILED: KafkaTemplate used outside outbox/KafkaConfig'
}
Write-Host 'no KafkaTemplate offenders'

Banner 5 'docker compose up -d and check-topics'
docker compose up -d
Check 'docker compose up'
bash scripts/check-topics.sh
Check 'check-topics.sh'

if ($Full) {
    Banner 6 'build jars, compose apps + obs profiles up (restaurant-service x3)'
    & .\mvnw.cmd -B -q package -DskipTests
    Check 'mvnw package'
    docker compose --profile apps --profile obs up -d --build --scale restaurant-service=3
    Check 'compose up apps'

    Banner 7 'wait for stack'
    bash scripts/wait-for-stack.sh
    Check 'wait-for-stack.sh'

    Banner 8 'consumer groups'
    bash scripts/check-consumer-groups.sh restaurant-service 3
    Check 'check-consumer-groups.sh'

    Banner 9 'e2e tests'
    & .\mvnw.cmd -B -Pe2e verify -pl e2e-tests
    Check 'mvnw e2e'
}

if ($Down) {
    Banner 'down' 'docker compose down'
    docker compose --profile apps --profile obs down
    Check 'docker compose down'
}

Write-Host ''
Write-Host 'VERIFY OK'
