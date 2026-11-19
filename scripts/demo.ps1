# Demo scenarios for the food-delivery saga (PowerShell 5.1). Everything goes through the gateway.
# Usage: scripts\demo.ps1 happy|reject|nodriver|cancel|poison
# Needs the Compose stack: docker compose --profile apps --profile obs up -d --build --scale restaurant-service=3
param(
    [Parameter(Position = 0)]
    [string]$Scenario
)
$ErrorActionPreference = 'Stop'
Set-Location (Join-Path $PSScriptRoot '..')
$env:MSYS_NO_PATHCONV = '1'

$GW = if ($env:GATEWAY_BASE) { $env:GATEWAY_BASE } else { 'http://localhost:8000' }
$Customer = '99999999-9999-9999-9999-999999999999'
$Pasta = '11111111-1111-1111-1111-111111111111'
$NightGrill = '11111111-1111-1111-1111-111111111112'
$Margherita = '22222222-2222-2222-2222-000000000001'
$Drivers = 1..5 | ForEach-Object { "33333333-3333-3333-3333-00000000000$_" }
$MaxWait = 60
$script:Oid = ''
$script:Corr = ''

function Step([string]$text) { Write-Host ''; Write-Host "--> $text" }
function Fail([string]$text) { throw "DEMO FAILED: $text" }

# Returns the parsed JSON body, or $null on any error.
function Get-Json([string]$path) {
    try { return Invoke-RestMethod -Uri "$GW$path" -Method Get -TimeoutSec 10 } catch { return $null }
}
# POST without a body. Returns the HTTP status code (0 when the request fails).
function Post-Code([string]$path) {
    try {
        $r = Invoke-WebRequest -Uri "$GW$path" -Method Post -TimeoutSec 10 -UseBasicParsing
        return [int]$r.StatusCode
    } catch {
        if ($_.Exception.Response) { return [int]$_.Exception.Response.StatusCode }
        return 0
    }
}
function Patch-Driver([string]$id, [string]$status) {
    try {
        Invoke-RestMethod -Uri "$GW/drivers/$id" -Method Patch -ContentType 'application/json' `
            -Body "{`"status`":`"$status`"}" -TimeoutSec 10 | Out-Null
    } catch { }
}
function Patch-AllDrivers([string]$status) { foreach ($d in $Drivers) { Patch-Driver $d $status } }

function Current([string]$kind) {
    $r = Get-Json "/$kind/$($script:Oid)"
    if ($r) { return [string]$r.status }
    return ''
}
# Polls for at most $MaxWait seconds until /<kind>/<orderId> has the wanted status.
function Wait-For([string]$what, [string]$kind, [string]$want) {
    $t = 0
    $got = ''
    while ($t -lt $MaxWait) {
        $got = Current $kind
        if ($got -eq $want) { Write-Host "    $what = $got (after ${t}s)"; return }
        Start-Sleep -Seconds 1
        $t++
    }
    Fail "$what is '$got', wanted '$want' after ${MaxWait}s"
}
function Show-History {
    $h = Get-Json "/orders/$($script:Oid)/history"
    if ($h) { Write-Host ("    " + (($h | ForEach-Object { $_.toStatus }) -join ' > ')) }
}

function Test-Gateway {
    try {
        Invoke-WebRequest -Uri "$GW/restaurants" -Method Get -TimeoutSec 5 -UseBasicParsing | Out-Null
    } catch {
        Fail "gateway $GW/restaurants is not answering. Start the stack: docker compose --profile apps --profile obs up -d --build --scale restaurant-service=3, then bash scripts/wait-for-stack.sh"
    }
}

function New-Order([string]$restaurantId) {
    $body = @{
        customerId      = $Customer
        restaurantId    = $restaurantId
        items           = @(@{ menuItemId = $Margherita; quantity = 2; unitPrice = 12.50 })
        deliveryAddress = '5 Demo Road'
    } | ConvertTo-Json -Depth 5
    $resp = Invoke-RestMethod -Uri "$GW/orders" -Method Post -ContentType 'application/json' -Body $body -TimeoutSec 10
    $script:Oid = [string]$resp.orderId
    $script:Corr = [string]$resp.correlationId
    if (-not $script:Oid) { Fail 'POST /orders did not return an orderId' }
    Write-Host "    orderId       = $($script:Oid)"
    Write-Host "    correlationId = $($script:Corr)"
    Write-Host "    Jaeger trace  = http://localhost:16686/trace/$($script:Corr.Replace('-', ''))"
}

function Run-Happy {
    Step '1. Place an order at Pasta Place (POST /orders)'
    New-Order $Pasta
    Step '2. Restaurant accepts, delivery assigns a driver'
    Wait-For 'ticket' 'tickets' 'ACCEPTED'
    Wait-For 'delivery' 'deliveries' 'ASSIGNED'
    Step '3. Kitchen finishes the food (POST /tickets/{id}/ready)'
    Write-Host "    HTTP $(Post-Code "/tickets/$($script:Oid)/ready")"
    Wait-For 'delivery' 'deliveries' 'READY_FOR_PICKUP'
    Step '4. Driver picks up (POST /deliveries/{id}/pickup)'
    Write-Host "    HTTP $(Post-Code "/deliveries/$($script:Oid)/pickup")"
    Wait-For 'order' 'orders' 'OUT_FOR_DELIVERY'
    Step '5. Driver delivers (POST /deliveries/{id}/deliver)'
    Write-Host "    HTTP $(Post-Code "/deliveries/$($script:Oid)/deliver")"
    Wait-For 'order' 'orders' 'DELIVERED'
    Step 'Order history'
    Show-History
}

function Run-Reject {
    Step '1. Place an order at Night Grill (seeded as closed)'
    New-Order $NightGrill
    Step '2. Restaurant rejects with CLOSED. The order ends REJECTED (terminal, no OrderCancelled)'
    Wait-For 'order' 'orders' 'REJECTED'
    Step 'Order history'
    Show-History
}

function Run-NoDriver {
    try {
        Step '1. PATCH all 5 drivers OFFLINE'
        Patch-AllDrivers 'OFFLINE'
        Step '2. Place an order at Pasta Place'
        New-Order $Pasta
        Step '3. Delivery fails (NO_DRIVER_AVAILABLE), order service cancels, restaurant cancels the ticket'
        Wait-For 'order' 'orders' 'CANCELLED'
        Wait-For 'ticket' 'tickets' 'CANCELLED'
        Wait-For 'delivery' 'deliveries' 'FAILED'
        Step 'Order history'
        Show-History
    } finally {
        Step 'Restoring all drivers to AVAILABLE'
        Patch-AllDrivers 'AVAILABLE'
    }
}

function Run-Cancel {
    Step '1. Place an order at Pasta Place'
    New-Order $Pasta
    Wait-For 'delivery' 'deliveries' 'ASSIGNED'
    Wait-For 'order' 'orders' 'DRIVER_ASSIGNED'
    Step '2. Customer cancels (POST /orders/{id}/cancel). Expect 200'
    Write-Host "    HTTP $(Post-Code "/orders/$($script:Oid)/cancel")"
    Wait-For 'order' 'orders' 'CANCELLED'
    Wait-For 'ticket' 'tickets' 'CANCELLED'
    Wait-For 'delivery' 'deliveries' 'CANCELLED'
    Step '3. Cancel again. Expect 409'
    Write-Host "    HTTP $(Post-Code "/orders/$($script:Oid)/cancel")"
    Step 'Order history'
    Show-History
}

function Run-Poison {
    if (-not (Get-Command docker -ErrorAction SilentlyContinue)) { Fail 'docker is not on PATH' }
    $key = "demo-poison-$([DateTimeOffset]::UtcNow.ToUnixTimeSeconds())"
    $topic = 'order.events.restaurant-service.dlt'
    $kb = '/opt/kafka/bin'
    Step "1. Produce the non-JSON record 'this-is-not-json' to order.events with key $key"
    "${key}:this-is-not-json" | docker compose exec -T kafka "$kb/kafka-console-producer.sh" `
        --bootstrap-server localhost:9092 --topic order.events `
        --property parse.key=true --property key.separator=:
    if ($LASTEXITCODE -ne 0) { Fail 'producer failed' }
    Step "2. Look for it on $topic (poison goes straight to the DLT, no retry hops)"
    $t = 0
    $found = $null
    while ($t -lt $MaxWait -and -not $found) {
        $out = docker compose exec -T kafka "$kb/kafka-console-consumer.sh" `
            --bootstrap-server localhost:9092 --topic $topic --from-beginning `
            --property print.key=true --timeout-ms 8000 2>$null
        $found = $out | Where-Object { $_ -like "$key*" } | Select-Object -First 1
        if (-not $found) { $t += 10 }
    }
    if (-not $found) { Fail "record with key $key not found on $topic after ${MaxWait}s" }
    Write-Host "    found on the DLT: $found"
    Step '3. The retry topic stays empty for this key'
    $out = docker compose exec -T kafka "$kb/kafka-console-consumer.sh" `
        --bootstrap-server localhost:9092 --topic order.events.restaurant-service.retry-0 --from-beginning `
        --property print.key=true --timeout-ms 5000 2>$null
    if ($out | Where-Object { $_ -like "$key*" }) { Write-Host '    UNEXPECTED: key found on retry-0' }
    else { Write-Host '    not on retry-0 (as expected)' }
    Write-Host '    The ERROR log line: docker compose logs restaurant-service | Select-String ALERT'
}

switch ($Scenario) {
    'happy'    { Test-Gateway; Run-Happy }
    'reject'   { Test-Gateway; Run-Reject }
    'nodriver' { Test-Gateway; Run-NoDriver }
    'cancel'   { Test-Gateway; Run-Cancel }
    'poison'   { Run-Poison }
    default    { Write-Host 'usage: scripts\demo.ps1 happy|reject|nodriver|cancel|poison'; exit 2 }
}
Write-Host ''
Write-Host "DEMO '$Scenario' OK"
