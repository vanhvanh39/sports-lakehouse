<#
  E3 - Chịu lỗi, đúng-một-lần và xử lý hiệu chỉnh (RQ2).
  Bơm luồng có đáp án (trùng, sửa, thu hồi, đảo thứ tự, bản ghi lỗi) trong 5 phút,
  trong lúc đó lần lượt: dừng 1 broker Kafka, khởi động lại Spark worker (mất executor),
  giết driver pipeline. Sau đó so sánh bảng silver với đáp án và đo thời gian phục hồi.

  .\scripts\experiments\e3.ps1
  .\scripts\experiments\e3.ps1 -RunId e3b -DurationS 300
#>
param([string]$RunId = ("e3_" + (Get-Date -Format "MMddHHmm")), [int]$DurationS = 300, [int]$Events = 600)
. (Join-Path $PSScriptRoot "_common.ps1")

Write-Host "== E3 run $RunId ==" -ForegroundColor Cyan
$name = "e3-inject-$RunId"
ToolDetached $name @("python", "-m", "sportslake.ingestion.inject", "--run-id", $RunId, "--events", "$Events", "--duration-s", "$DurationS")
Wait-Seconds 45 "(luồng bình thường)"

Log-Fault "kafka2" "fail" "docker stop"
Compose @("stop", "kafka2")
Wait-Seconds 60 "(cụm chỉ còn 2/3 broker, min.insync.replicas=2 vẫn ghi được)"
Compose @("start", "kafka2")
Log-Fault "kafka2" "recover" "docker start"
Wait-Seconds 30

Log-Fault "spark-worker" "fail" "docker restart (mất mọi executor)"
Compose @("restart", "spark-worker")
Log-Fault "spark-worker" "recover" "worker đã khởi động lại"
Wait-Seconds 45

Log-Fault "spark-pipeline" "fail" "docker kill (giết driver)"
Compose @("kill", "spark-pipeline")
Wait-Seconds 20
Compose @("start", "spark-pipeline")
Log-Fault "spark-pipeline" "recover" "docker start"

Write-Host "Chờ luồng bơm kết thúc..."
& docker wait $name | Out-Null
Wait-Seconds 120 "cho pipeline xử lý phần còn lại"
Tool @("python", "-m", "sportslake.experiments.e3_verify", "--run-id", $RunId, "--mock")
Write-Host "Kết quả: data/experiments/results/e3_*.csv" -ForegroundColor Green
