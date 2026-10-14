<#
  E6 - Chất lượng phát hiện diễn biến nổi bật (RQ4): V1 luật, V2 chấm điểm, V1+V2.
  .\scripts\experiments\e6.ps1
#>
param([string]$Thresholds = "3,4,5,6,7,8,9,10,12")
. (Join-Path $PSScriptRoot "_common.ps1")
Tool @("python", "-m", "sportslake.experiments.e6_highlight_eval", "--thresholds", $Thresholds)
Write-Host "Kết quả: data/experiments/results/e6_*.csv, e6_precision_recall.png" -ForegroundColor Green
