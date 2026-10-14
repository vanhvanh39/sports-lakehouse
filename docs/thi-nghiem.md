# Hướng dẫn chạy thí nghiệm (tư liệu cho Chương 4)

Mọi kết quả được ghi vào `data/experiments/results/`. Trước khi chạy, cụm phải đang hoạt động (`.\manage.ps1 up`) và đã tải dữ liệu (`.\manage.ps1 download`). Riêng E5 và E6 chỉ cần dữ liệu đã tải, không cần cụm.

**Trước mọi thí nghiệm cần cụm**, chạy `.\manage.ps1 smoke --wait-mock 20` và chỉ tiếp tục khi tất cả các mục đều ĐẠT. Nếu luồng cơ bản chưa đúng thì số đo của thí nghiệm cũng không có ý nghĩa.

Thứ tự gợi ý khi thời gian gấp: **E5 → E6 → E3 → E1 → E8 → E2**. Bốn thí nghiệm đầu trả lời đủ RQ1–RQ4.

---

## E1. Độ trễ theo từng khâu (RQ1)
**Lệnh:** `.\scripts\experiments\e1.ps1` (hoặc `-Mode mock` / `-Mode replay`)

**Cách làm:**
- Mock x10: tạo 3 trận mới, chờ kết thúc, sau đó đo.
- Replay StatsBomb x1 và x10: mỗi tốc độ chạy 10 phút, chỉ phát các sự kiện quan trọng.

**Cách đo:** mỗi sự kiện mang đủ mốc thời gian (xem `docs/kien-truc.md` §3). Thời điểm commit lấy từ Change Data Feed (`_commit_timestamp`), thời điểm vào Elasticsearch lấy từ `es_indexed_ts`, thời điểm gửi cảnh báo lấy từ index `alerts`. Chỉ tính các sự kiện chưa từng bị sửa.

**Đầu ra:** `e1_latency.csv` (p50/p95/p99 theo khâu và nhãn), `e1_latency_<nhãn>.png` (boxplot).

**Gợi ý phân tích:**
- Ở chế độ Mock, khâu `source→ingest` chiếm phần lớn độ trễ. Đây là độ trễ do *chu kỳ gọi API* cộng với độ trễ nhà cung cấp (mặc định 20 giây trận), lớn hơn nhiều so với Kafka và Spark.
- Ở chế độ Replay, khâu đó gần bằng 0. Độ trễ còn lại chủ yếu đến từ `kafka→spark` (chờ vi lô 2 giây cộng thời gian vi lô) và `spark→commit` (MERGE INTO).
- Thử đổi `TRIGGER_EVENTS_SECONDS` trong `.env` (1, 2, 5) để thấy đánh đổi giữa độ trễ và số file nhỏ.

## E2. Thông lượng và khả năng mở rộng
**Lệnh:** `.\scripts\experiments\e2.ps1 -Partitions 3,6,12 -Cores 1,2,3 -Speeds 200,1000`

Mỗi cấu hình làm các bước sau:
1. Tạo lại topic `match.tracking` với P partition.
2. Khởi động lại pipeline với `spark.cores.max = C` (mỗi executor 1 core).
3. Phát lại trận Metrica 1 (3,3 triệu bản ghi vị trí) ở tốc độ S.
4. Tổng hợp tiến độ các vi lô của truy vấn `bronze_tracking`.

**Đầu ra:** `e2_throughput.csv` và `e2_throughput.png`. Hai đơn vị được đo **tách biệt**:
- *khung hình/giây* (`frames_per_s_*`): số thông điệp Kafka Spark đọc vào. Một thông điệp là một khung hình chứa vị trí bóng và khoảng 22 cầu thủ;
- *bản ghi vị trí/giây* (`positions_per_s_*`): số dòng thực sự được ghi ra `bronze.tracking_positions` sau bước explode. Con số này được đếm trực tiếp trong bảng Delta theo `match_id` của lần chạy (hậu tố = RUN_ID) và đối chiếu với `sink.numOutputRows` của Spark.

Mỗi đơn vị được tính theo hai cách:
- `_busy`: số dòng chia tổng thời gian các vi lô có dữ liệu, cho biết năng lực xử lý;
- `_span`: số dòng chia khoảng từ vi lô đầu tới vi lô cuối, cho biết thông lượng thực tế.

Các cột khác:
- `positions_complete`: số vị trí ghi ra có khớp số vị trí bên phát hay không (kiểm tra không mất dữ liệu khi tải cao);
- `trigger_ms_p50`, `lag_max_frames`, `keeps_up`.

**Lưu ý:**
- Muốn dùng hơn 3 core, tăng `SPARK_WORKER_CORES` hoặc chạy `docker compose up -d --scale spark-worker=2`.
- Nếu `frames_per_s_span` gần bằng `producer_frames_per_s`, Spark đang theo kịp và điểm nghẽn nằm ở phía phát (Python). Khi đó tăng `--workers` hoặc dùng `--speed 0` để phát nhanh nhất có thể. Năng lực thật của Spark lúc này là cột `_busy`.
- Thời gian MERGE INTO của bảng silver nằm trong `data/metrics/batch_metrics.jsonl` (cột `merge_s`). Để có số liệu này, chạy thêm `replay-sb --speed 1000` khi đo.

## E3. Chịu lỗi, đúng-một-lần, xử lý hiệu chỉnh (RQ2)
**Lệnh:** `.\scripts\experiments\e3.ps1` (khoảng 10 phút)

**Kịch bản:**
1. Bộ bơm `inject` gửi 600 sự kiện trong 5 phút. Trong đó khoảng 30% bị gửi trùng, 15% bị sửa, 10% bị thu hồi, 30% các cặp sửa/thu hồi đến sai thứ tự, 2% là bản ghi lỗi.
2. Trong lúc đang bơm, lần lượt gây ba sự cố:
   - `docker stop kafka2` trong 60 giây;
   - khởi động lại `spark-worker` (mất toàn bộ executor);
   - `docker kill spark-pipeline` (giết driver) rồi khởi động lại sau 20 giây.

**Kiểm chứng** (`e3_verify`):
- So sánh bảng silver với đáp án: số sự kiện mất, trùng, sai trạng thái thu hồi, sai giá trị sau khi sửa, và phần trăm trạng thái cuối đúng.
- Số bản ghi lỗi gửi đi so với số bản ghi DLQ nhận được.
- Số thông điệp ở bronze (gồm cả bản trùng) so với số dòng ở silver. Chênh lệch cho thấy bước loại trùng hoạt động.
- Các trận Mock đã kết thúc: so sánh với `/_admin/truth`, tức tập sự kiện cuối cùng sau VAR và sau khi sửa tên.
- Thời gian phục hồi: từ lúc dịch vụ được khởi động lại tới vi lô đầu tiên có dữ liệu (đọc `faults.jsonl` và `streaming_progress.jsonl`).

**Đầu ra:** `e3_correctness.csv`, `e3_mock_truth.csv`, `e3_recovery.csv`.

## E5. Lịch gọi thích ứng (RQ3), mô phỏng offline
**Lệnh:** `.\scripts\experiments\e5.ps1 -Budget 100 -ProviderDelayS 20`

Dùng lịch thi đấu thật (giờ bóng lăn, thời điểm từng bàn thắng, thẻ, thay người) của các giải đã tải. Mỗi ngày thi đấu được mô phỏng riêng với ngân sách 100 request. Lớp `Scheduler` trong mô phỏng chính là lớp mà collector dùng. So sánh ba chính sách:
- thích ứng;
- cố định với khoảng cách chia đều vừa khít ngân sách;
- cố định 60 giây.

**Đầu ra:** `e5_scheduler_summary.csv`, `e5_scheduler_per_day.csv`, `e5_scheduler.png`.

Kết quả mẫu và cách diễn giải: xem README §5.

## E6. Chất lượng phát hiện nổi bật (RQ4), offline
**Lệnh:** `.\scripts\experiments\e6.ps1`

Bộ phát hiện chạy tuần tự trên dữ liệu StatsBomb, bằng đúng đoạn mã pipeline dùng, kể cả quy tắc chặn V2 ngay sau V1. Không có bước gộp cảnh báo riêng cho đánh giá.

**Nhãn:** bàn thắng, phản lưới, thẻ đỏ, phạt đền, cơ hội lớn (xG ≥ 0,3), gộp các nhãn trong vòng 30 giây. Một cảnh báo khớp một nhãn nếu nằm trong khoảng [−30 s, +120 s] quanh nhãn đó.

**Tách tập:**
- Ngưỡng V2 được **chọn** trên tập điều chỉnh: World Cup 2022, 64 trận. Tiêu chí là F1 của cấu hình triển khai V1+V2.
- Kết quả chính được **báo cáo** trên tập kiểm tra tách biệt: Euro 2024, 51 trận.
- Có thể đổi bằng `--tune-comp` và `--test-comp`. Nếu chỉ có một giải, dữ liệu được tách 50/50 theo hash `match_id`.

**Đầu ra:** `e6_highlight_main.csv` (kết quả trên tập kiểm tra), `e6_highlight_v2_sweep.csv`, `e6_highlight.json`, `e6_precision_recall.png`.

**Hạn chế cần ghi trong báo cáo:**
- Nhãn được suy ra từ chính dữ liệu sự kiện, nên V1 gần như đúng tuyệt đối với các loại sự kiện nó dựa vào.
- Đánh giá offline đo chất lượng *quyết định*, không đo độ trễ khi chạy thật. Độ trễ cảnh báo thực tế do E1 đo (`nguồn→cảnh báo`).
- Đây vẫn là phương pháp hợp lý khi không có bộ nhãn "highlight" do con người gán.

## E8. Kiểm tra tính đúng bằng time travel
**Lệnh:** `.\scripts\experiments\e8.ps1 -MinutesAgo 15` (nên chạy sau khi đã có vài trận Mock)

**Cách làm:** so sánh `silver.match_events` tại phiên bản cách đây N phút (`VERSION AS OF`) với phiên bản hiện tại. Mỗi bản ghi được phân loại thành: không đổi, bị sửa, bị thu hồi, thêm sau. Với từng sự kiện từng bị sửa, giá trị lúc mới ghi được đọc lại bằng time travel tại đúng phiên bản ghi đầu tiên, rồi đặt cạnh giá trị hiện tại.

**Đầu ra:** `e8_time_travel.csv` và `e8_time_travel_v<N>.json`.

Có thể minh họa thêm bằng Spark SQL: `.\manage.ps1 spark-sql time-travel silver.match_events --version <N>`.

---

## Số liệu giám sát dùng cho báo cáo
- `data/metrics/streaming_progress.jsonl`: tiến độ từng vi lô của mọi truy vấn (tốc độ, thời gian, lag).
- `data/metrics/batch_metrics.jsonl`: thời gian MERGE INTO, số bản ghi DLQ, số cảnh báo trong mỗi vi lô.
- `data/metrics/maintenance.jsonl`: OPTIMIZE/VACUUM (số file trước và sau).
- `data/metrics/collector_polls_<chế độ>.jsonl`: từng request API kèm trọng số lịch.
- Grafana *Giám sát pipeline*: chụp màn hình để đưa vào báo cáo.
