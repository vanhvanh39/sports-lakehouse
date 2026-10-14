# Hàm dùng chung cho các script thí nghiệm
$ErrorActionPreference = "Stop"
$Root = Resolve-Path (Join-Path $PSScriptRoot "../..")
Set-Location $Root

function Compose { param([string[]]$A) & docker compose @A; if ($LASTEXITCODE -ne 0) { throw "docker compose $($A -join ' ') lỗi" } }
function Tool { param([string[]]$A) Compose (@("--profile", "tools", "run", "--rm", "tools") + $A) }
function ToolDetached { param([string]$Name, [string[]]$A) Compose (@("--profile", "tools", "run", "-d", "--rm", "--name", $Name, "tools") + $A) }
function NowMs { [DateTimeOffset]::UtcNow.ToUnixTimeMilliseconds() }
function Log-Fault {
    param([string]$Target, [string]$Action, [string]$Note = "")
    $dir = Join-Path $Root "data/experiments/e3"
    if (-not (Test-Path $dir)) { New-Item -ItemType Directory -Path $dir | Out-Null }
    $line = '{"ts_ms": ' + (NowMs) + ', "target": "' + $Target + '", "action": "' + $Action + '", "note": "' + $Note + '"}'
    # ghi UTF-8 không BOM để Python đọc được
    [System.IO.File]::AppendAllText((Join-Path $dir "faults.jsonl"), $line + "`n")
    Write-Host ("[{0}] {1} {2} {3}" -f (Get-Date -Format "HH:mm:ss"), $Action.ToUpper(), $Target, $Note) -ForegroundColor Magenta
}
function Wait-Seconds { param([int]$S, [string]$Why = "") Write-Host "  ... chờ $S giây $Why"; Start-Sleep -Seconds $S }
