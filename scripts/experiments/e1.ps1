<#
  E1 - Độ trễ theo từng khâu (RQ1).
  Chế độ mock: dùng luồng Mock API đang chạy (collector), đo các trận vừa diễn ra.
  Chế độ replay: phát lại StatsBomb ở tốc độ x1 và x10 trong một khoảng thời gian rồi đo.

  .\scripts\experiments\e1.ps1                         # cả mock và replay x1, x10
  .\scripts\experiments\e1.ps1 -Mode mock -SinceMin 60
#>
param([string]$Mode = "all", [int]$SinceMin = 60, [int]$ReplaySeconds = 600)
. (Join-Path $PSScriptRoot "_common.ps1")

if ($Mode -in @("all", "mock")) {
    Write-Host "== E1 / Mock API: tạo 3 trận mới ở tốc độ x10 và chờ kết thúc (~15 phút) ==" -ForegroundColor Cyan
    Invoke-RestMethod -Method Post -Uri "http://localhost:8000/_admin/restart?matches=3&gap_min=1&speed=10&delay_s=20" | Out-Null
    Invoke-RestMethod -Method Post -Uri "http://localhost:8000/_admin/reset_quota" | Out-Null
    Compose @("up", "-d", "--no-deps", "--force-recreate", "collector")
    Wait-Seconds 960 "cho các trận giả lập kết thúc"
    Tool @("python", "-m", "sportslake.experiments.e1_latency", "--source", "mock", "--since-min", "$SinceMin", "--label", "mock_x10")
}
if ($Mode -in @("all", "replay")) {
    foreach ($speed in @(1, 10)) {
        Write-Host "== E1 / Replay StatsBomb x$speed trong $ReplaySeconds giây ==" -ForegroundColor Cyan
        $suffix = "e1x$speed" + (Get-Date -Format "HHmm")
        Tool @("python", "-m", "sportslake.ingestion.replay_statsbomb", "--all-downloaded", "--matches", "4", "--parallel", "4",
               "--speed", "$speed", "--only-key", "--id-suffix", $suffix, "--max-seconds", "$ReplaySeconds")
        Wait-Seconds 30 "cho pipeline xử lý xong"
        $mins = [int]($ReplaySeconds / 60) + 3
        Tool @("python", "-m", "sportslake.experiments.e1_latency", "--source", "statsbomb", "--since-min", "$mins", "--label", "replay_x$speed")
    }
}
Write-Host "Kết quả: data/experiments/results/e1_latency*.{csv,json,png}" -ForegroundColor Green
