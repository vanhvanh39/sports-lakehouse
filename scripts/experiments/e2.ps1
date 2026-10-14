<#
  E2 - Thông lượng và khả năng mở rộng.
  Mỗi cấu hình: tạo lại topic match.tracking với P partition, khởi động lại pipeline với C core,
  phát lại dữ liệu vị trí Metrica (25 Hz) ở tốc độ S, rồi tổng hợp tiến độ vi lô.

  .\scripts\experiments\e2.ps1
  .\scripts\experiments\e2.ps1 -Partitions 3,12 -Cores 1,3 -Speeds 200,1000
  Muốn hơn 3 core: tăng SPARK_WORKER_CORES trong .env hoặc: docker compose up -d --scale spark-worker=2
#>
param([int[]]$Partitions = @(3, 6, 12), [int[]]$Cores = @(1, 2, 3), [int[]]$Speeds = @(200, 1000), [int]$Workers = 4)
. (Join-Path $PSScriptRoot "_common.ps1")

foreach ($p in $Partitions) {
    foreach ($c in $Cores) {
        foreach ($s in $Speeds) {
            $run = "p$($p)_c$($c)_x$($s)_" + (Get-Date -Format "HHmmss")
            Write-Host "== E2 $run ==" -ForegroundColor Cyan
            Tool @("python", "-m", "sportslake.tools.admin", "recreate-topic", "match.tracking", "$p")
            $env:RUN_ID = $run
            $env:TRACKING_RUN_ID = $run
            $env:PIPELINE_CORES_MAX = "$c"
            Compose @("up", "-d", "--no-deps", "--force-recreate", "spark-pipeline")
            Wait-Seconds 75 "cho pipeline khởi động"
            # hậu tố id = RUN_ID để đếm đúng số bản ghi vị trí của lần chạy này trong bảng Delta
            Tool @("python", "-m", "sportslake.ingestion.replay_tracking", "--game", "1", "--speed", "$s", "--workers", "$Workers", "--id-suffix", $run)
            Wait-Seconds 45 "cho Spark xử lý hết phần tồn"
            Tool @("python", "-m", "sportslake.experiments.e2_throughput", "--run-id", $run, "--partitions", "$p", "--cores", "$c", "--speed", "$s")
        }
    }
}
Remove-Item Env:RUN_ID, Env:TRACKING_RUN_ID, Env:PIPELINE_CORES_MAX -ErrorAction SilentlyContinue
Compose @("up", "-d", "--no-deps", "--force-recreate", "spark-pipeline")
Tool @("python", "-m", "sportslake.experiments.e2_throughput", "--report")
Write-Host "Kết quả: data/experiments/results/e2_throughput.csv, e2_throughput.png" -ForegroundColor Green
