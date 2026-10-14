<#
  E5 - Bộ lập lịch thích ứng so với lịch cố định (RQ3). Mô phỏng offline trên lịch thi đấu thật.
  .\scripts\experiments\e5.ps1
  .\scripts\experiments\e5.ps1 -Budget 100 -ProviderDelayS 20
#>
param([int]$Budget = 100, [int]$ProviderDelayS = 20)
. (Join-Path $PSScriptRoot "_common.ps1")
Tool @("python", "-m", "sportslake.experiments.e5_scheduler_sim", "--budget", "$Budget", "--provider-delay-s", "$ProviderDelayS")
Write-Host "Kết quả: data/experiments/results/e5_scheduler_*.csv, e5_scheduler.png" -ForegroundColor Green
