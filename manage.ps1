<#
.SYNOPSIS
  Lệnh tiện ích cho Sports Lakehouse trên Windows (PowerShell).

.EXAMPLE
  .\manage.ps1 setup            # tạo .env và thư mục data
  .\manage.ps1 up               # build + khởi động cụm
  .\manage.ps1 up -Ui           # kèm Kibana và kafka-ui
  .\manage.ps1 status           # trạng thái + địa chỉ các giao diện
  .\manage.ps1 download         # tải dữ liệu StatsBomb + Metrica
  .\manage.ps1 replay-sb --competition 43 --season 106 --matches 4 --speed 10
  .\manage.ps1 replay-tracking --game 1 --speed 100
  .\manage.ps1 query --preset standings
  .\manage.ps1 gold             # chạy job gold ngay (full)
  .\manage.ps1 exp e5           # chạy thí nghiệm với tham số mặc định (e1, e2, e3, e5, e6, e8)
  .\manage.ps1 test             # chạy unit test
  .\manage.ps1 logs spark-pipeline
  .\manage.ps1 restart collector   # tạo lại container -> nạp cấu hình mới trong .env
  .\manage.ps1 smoke            # kiểm tra nhanh toàn bộ luồng sau khi up
  .\manage.ps1 down             # dừng cụm (giữ dữ liệu)
  .\manage.ps1 reset            # XÓA toàn bộ dữ liệu lakehouse, checkpoint, Elasticsearch
#>
param([string]$Command = "help")

$ErrorActionPreference = "Stop"
Set-Location -Path $PSScriptRoot
# Tham số còn lại được chuyển nguyên cho lệnh Python bên dưới (vd. --speed 10)
$Rest = @($args)
$Ui = $Rest -contains "-Ui"
$Volumes = $Rest -contains "-Volumes"
$Rest = @($Rest | Where-Object { $_ -ne "-Ui" -and $_ -ne "-Volumes" })

function Invoke-Compose {
    param([string[]]$ComposeArgs)
    & docker compose @ComposeArgs
    if ($LASTEXITCODE -ne 0) { throw "docker compose $($ComposeArgs -join ' ') thất bại (mã $LASTEXITCODE)" }
}

function Invoke-Tool {
    param([string[]]$ToolArgs)
    $all = @("--profile", "tools", "run", "--rm", "tools") + $ToolArgs
    Invoke-Compose $all
}

function Ensure-Setup {
    if (-not (Test-Path ".env")) {
        Copy-Item ".env.example" ".env"
        Write-Host "Đã tạo .env từ .env.example (sửa APIFOOTBALL_KEY nếu muốn chạy live)." -ForegroundColor Yellow
    }
    foreach ($d in @("data", "data/metrics", "data/experiments", "data/experiments/e3", "data/experiments/results")) {
        if (-not (Test-Path $d)) { New-Item -ItemType Directory -Path $d | Out-Null }
    }
}

function Show-Urls {
    Write-Host ""
    Write-Host "Giao diện:" -ForegroundColor Cyan
    Write-Host "  Grafana (dashboard)       http://localhost:3000   (admin / admin)"
    Write-Host "  Spark master              http://localhost:8080"
    Write-Host "  Spark UI của pipeline     http://localhost:4040"
    Write-Host "  MinIO console             http://localhost:9001   (minioadmin / minioadmin123)"
    Write-Host "  Prometheus                http://localhost:9090"
    Write-Host "  Elasticsearch             http://localhost:9200"
    Write-Host "  Mock API-Football         http://localhost:8000/docs  |  /_admin/scenarios"
    Write-Host "  Schema Registry           http://localhost:8081/subjects"
    if ($Ui) {
        Write-Host "  Kibana                    http://localhost:5601"
        Write-Host "  kafka-ui                  http://localhost:8085"
    }
}

switch ($Command) {
    "setup" { Ensure-Setup; Write-Host "Xong." }
    "up" {
        Ensure-Setup
        $args2 = @()
        if ($Ui) { $args2 += @("--profile", "ui") }
        # build trước (chỉ init và spark-master có "build"), sau đó mới tạo container
        Invoke-Compose @("build")
        $args2 += @("up", "-d")
        Invoke-Compose $args2
        Show-Urls
        Write-Host ""
        Write-Host "Pipeline Spark cần ~1-2 phút để khởi động. Xem log: .\manage.ps1 logs spark-pipeline" -ForegroundColor Yellow
    }
    "down" {
        $a = @("--profile", "ui", "--profile", "tools", "down")
        if ($Volumes) { $a += "-v" }
        Invoke-Compose $a
    }
    "reset" {
        $ok = Read-Host "Xóa TOÀN BỘ dữ liệu (lakehouse, checkpoint, Elasticsearch, Grafana)? Gõ YES để tiếp tục"
        if ($ok -eq "YES") {
            Invoke-Compose @("--profile", "ui", "--profile", "tools", "down", "-v")
            foreach ($f in @("data/state", "data/metrics")) { if (Test-Path $f) { Remove-Item -Recurse -Force $f } }
            Write-Host "Đã xóa. Chạy lại: .\manage.ps1 up"
        }
    }
    "status" { Invoke-Compose @("ps"); Show-Urls }
    "logs" {
        $svc = if ($Rest.Count -gt 0) { $Rest } else { @("spark-pipeline") }
        Invoke-Compose (@("logs", "-f", "--tail", "200") + $svc)
    }
    "restart" {
        # Tạo lại container (không chỉ "docker compose restart") để nạp lại biến môi trường từ .env
        if ($Rest.Count -lt 1) { throw "Cần tên dịch vụ, vd: .\manage.ps1 restart collector" }
        Invoke-Compose (@("up", "-d", "--no-deps", "--force-recreate") + $Rest)
    }
    "download" { Ensure-Setup; Invoke-Tool (@("python", "-m", "sportslake.tools.download_data") + $Rest) }
    "replay-sb" { Invoke-Tool (@("python", "-m", "sportslake.ingestion.replay_statsbomb") + $Rest) }
    "replay-tracking" { Invoke-Tool (@("python", "-m", "sportslake.ingestion.replay_tracking") + $Rest) }
    "inject" { Invoke-Tool (@("python", "-m", "sportslake.ingestion.inject") + $Rest) }
    "query" { Invoke-Tool (@("python", "-m", "sportslake.serving.query") + $Rest) }
    "admin" { Invoke-Tool (@("python", "-m", "sportslake.tools.admin") + $Rest) }
    "gold" {
        $a = if ($Rest.Count -gt 0) { $Rest } else { @("--mode", "full", "--tracking") }
        Invoke-Compose (@("exec", "jobs", "python3", "-m", "sportslake.batch.job_runner", "gold") + $a)
    }
    "spark-sql" { Invoke-Compose (@("exec", "jobs", "python3", "-m", "sportslake.batch.job_runner", "tool", "--") + $Rest) }
    "mock-restart" {
        # Tạo lại các trận giả lập bắt đầu ngay bây giờ, vd: .\manage.ps1 mock-restart 4 2 10
        $n = if ($Rest.Count -gt 0) { $Rest[0] } else { "3" }
        $gap = if ($Rest.Count -gt 1) { $Rest[1] } else { "2" }
        $speed = if ($Rest.Count -gt 2) { $Rest[2] } else { "10" }
        Invoke-RestMethod -Method Post -Uri "http://localhost:8000/_admin/restart?matches=$n&gap_min=$gap&speed=$speed&delay_s=30" | ConvertTo-Json -Depth 5
        Invoke-RestMethod -Method Post -Uri "http://localhost:8000/_admin/reset_quota" | Out-Null
        Invoke-Compose @("up", "-d", "--no-deps", "--force-recreate", "collector")
    }
    "test" { Invoke-Tool @("python", "-m", "pytest", "-q", "tests") }
    "smoke" { Invoke-Tool (@("python", "-m", "sportslake.tools.smoke") + $Rest) }
    "exp" {
        if ($Rest.Count -lt 1) { throw "Cần tên thí nghiệm: e1 | e2 | e3 | e5 | e6 | e8" }
        $name = $Rest[0]
        $script = Join-Path $PSScriptRoot "scripts/experiments/$name.ps1"
        if (-not (Test-Path $script)) { throw "Không có $script" }
        # Muốn đổi tham số thì gọi trực tiếp, vd: .\scripts\experiments\e3.ps1 -RunId e3b
        & $script
    }
    default {
        Get-Help $PSCommandPath -Detailed | Out-String | Write-Host
    }
}