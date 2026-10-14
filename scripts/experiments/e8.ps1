<#
  E8 - Kiểm tra tính đúng bằng time travel: so sánh VERSION AS OF trước/sau hiệu chỉnh.
  Nên chạy sau khi đã có vài trận Mock (có VAR hủy bàn và sửa tên cầu thủ).
  .\scripts\experiments\e8.ps1
  .\scripts\experiments\e8.ps1 -MinutesAgo 20 -MatchPrefix 900
#>
param([int]$MinutesAgo = 15, [string]$MatchPrefix = "9")
. (Join-Path $PSScriptRoot "_common.ps1")
Tool @("python", "-m", "sportslake.experiments.e8_time_travel", "--minutes-ago", "$MinutesAgo", "--match-prefix", $MatchPrefix)
Write-Host "Minh họa bằng Spark SQL (VERSION AS OF):" -ForegroundColor Cyan
Compose @("exec", "jobs", "python3", "-m", "sportslake.batch.job_runner", "tool", "--", "history", "silver.match_events")
Write-Host "Kết quả: data/experiments/results/e8_*.{csv,json}" -ForegroundColor Green
