# Thiết kế hệ thống (tư liệu cho Chương 3 của báo cáo)

## 1. Luồng dữ liệu

```
API-Football / Mock API ──(bộ lập lịch thích ứng)──► collector ──┐
StatsBomb (replay) ─────────────────────────────────────────────┼──► Kafka (3 broker, KRaft, RF=3, min.insync=2)
Metrica 25 Hz (replay) ─────────────────────────────────────────┘        topics: match.events | state | stats | tracking
                                                                                   │  Avro + Schema Registry
                                       ┌───────────────────────────────────────────┘
                                       ▼
                      Spark Structured Streaming (MỘT ứng dụng, 9 truy vấn; job gold chạy riêng)
     bronze_raw ─► bronze.kafka_raw (append)          bronze_tracking ─► bronze.tracking_positions (append)
     silver_events ─► kiểm tra chất lượng ─► match.dlq
                   └► cảnh báo V1 ─► MERGE INTO silver.match_events ─► chỉ số kịch tính ─► cảnh báo V2 ─► silver.alerts
     alerts_outbox : Change Data Feed của silver.alerts ─► match.alerts
     silver_state / silver_stats ─► MERGE INTO silver.match_state / match_stats
     es_* : Change Data Feed của silver ─► Elasticsearch
                                       │
         job gold (theo lịch, đọc CDF) ─► gold.team_match_summary, standings, team_form, player_summary, player_distance
                                       │
     Grafana (Elasticsearch + Prometheus)   DuckDB (truy vấn ad-hoc trên Delta)   alerter (Telegram / webhook)
```

## 2. Các quyết định thiết kế chính

### 2.1. Suy ra sự kiện từ ảnh chụp trạng thái (`ingestion/differ.py`)
Mỗi lần gọi, API-Football trả về *toàn bộ* danh sách sự kiện hiện có của trận và các sự kiện không có id. Collector gán cho mỗi sự kiện một **khóa tự nhiên** gồm (trận, loại, đội, phút, phút bù, thứ tự trong nhóm trùng) và đặt `event_id = sha1(khóa)`. Nội dung để phát hiện thay đổi được băm từ cả id lẫn tên cầu thủ/kiến tạo, nên nguồn chỉ sửa tên mà giữ id vẫn được nhận ra. Khóa cố ý không chứa tên cầu thủ: khi nguồn sửa tên người ghi bàn, sự kiện vẫn giữ nguyên `event_id` và được sinh ra một bản `UPSERT` mới với `seq` lớn hơn, thay vì bị coi là một sự kiện khác. Sự kiện có ở ảnh chụp trước nhưng mất ở ảnh chụp sau sinh ra `RETRACT`. Nếu bàn thắng vẫn còn trong danh sách nhưng có thêm sự kiện "Var – Goal cancelled", bàn thắng đó cũng bị thu hồi. Trạng thái của differ được lưu ra đĩa, nên khi collector khởi động lại vẫn phát hiện được các lần thu hồi.

### 2.2. Bộ lập lịch thích ứng (`ingestion/scheduler.py`)
Một lần gọi `/fixtures?live=all` lấy được mọi trận đang diễn ra, nên chi phí tính theo *thời gian* chứ không theo *số trận*. Thuật toán gồm các bước:

1. Dự đoán dòng thời gian còn lại của từng trận và gán trọng số cho từng phút: nghỉ giữa hiệp = 0; hiệp 1 = 1,0; hiệp 2 = 1,1; từ phút 75 = 1,3; từ phút 75 khi tỷ số sát nút = 1,5; hiệp phụ = 1,5.
2. Tính W = tổng số phút có trọng số còn lại (lấy hợp theo thời gian của các trận) và B = ngân sách còn lại − dự phòng − các request bắt buộc (chốt trận, thống kê).
3. Khoảng cách giữa hai lần gọi = (W / B) / trọng số hiện tại, kẹp trong khoảng [60 / giới hạn mỗi phút, 10 phút trận].
4. Khi tất cả trận đang nghỉ giữa hiệp, hẹn lần gọi tiếp theo đúng lúc hiệp 2 dự kiến bắt đầu.
5. Khi chỉ còn phần dự phòng, chỉ dùng nó để chốt kết quả: gọi `/fixtures?id=` một lần khi trận rời danh sách live.

Các trọng số trên được hiệu chỉnh bằng mô phỏng E5 trên lịch thi đấu thật.

### 2.3. Đúng-một-lần hiệu dụng
| Lớp | Cơ chế |
|---|---|
| Producer → Kafka | `enable.idempotence=true`, `acks=all`, topic RF = 3, `min.insync.replicas` = 2 |
| Kafka → Spark | Offset lưu trong checkpoint; sau sự cố, vi lô được chạy lại từ đúng offset |
| Spark → Delta | Mỗi `MERGE INTO` là một giao dịch ACID. Chạy lại cùng vi lô không làm thay đổi gì, vì điều kiện cập nhật là `s.seq > t.seq` |
| Bản trùng trong luồng | `dropDuplicatesWithinWatermark(event_id, seq, op)` với watermark 10 phút, sau đó lấy bản mới nhất cho mỗi `event_id` trong vi lô |

**Xóa logic (bia mộ) thay cho DELETE.** Nếu xóa vật lý khi nhận `RETRACT`, một bản `UPSERT` cũ đến muộn (do đảo thứ tự) sẽ chèn lại sự kiện đã bị hủy. Vì vậy `RETRACT` chỉ đặt `is_deleted = true` và giữ lại `seq`; nếu `RETRACT` đến trước `UPSERT` thì vẫn chèn một dòng bia mộ. Các bản ghi có `seq` nhỏ hơn không thể ghi đè hay "hồi sinh" sự kiện. Gold và dashboard lọc `is_deleted = false`. Phần kiểm thử `tests/test_merge_semantics.py` chứng minh luồng có trùng, sửa, thu hồi và đảo thứ tự luôn hội tụ về đúng đáp án.

**Cảnh báo không trùng, không mất:** xem §2.3b.

### 2.3b. Không mất dữ liệu khi lỗi hoặc khởi động lại (sau góp ý review)
| Điểm có thể mất dữ liệu | Cách xử lý |
|---|---|
| Collector đã ghi nhớ một sự kiện nhưng thông điệp chưa tới Kafka thì tiến trình chết hoặc broker lỗi | Mỗi lượt gọi API là một "giao dịch" (`Collector._transaction`). Collector chụp lại bộ nhớ differ, gửi thông điệp, rồi chờ `flush` tới khi Kafka xác nhận hết (tối đa 330 s, lớn hơn `message.timeout.ms`). Chỉ khi không có lỗi mới lưu trạng thái ra đĩa. Nếu có lỗi, bộ nhớ được hoàn tác, lượt sau phát hiện lại và gửi lại; MERGE phía Spark là idempotent nên gửi lại không sai. Lần chốt trận chưa tới Kafka được đưa vào hàng đợi để chốt lại. |
| Driver chết sau MERGE nhưng trước khi phát cảnh báo | Cảnh báo được **lưu bền trước** vào `silver.alerts` (MERGE theo `alert_id` tất định), theo thứ tự: (1) V1, (2) MERGE sự kiện, (3) V2, (4) trạng thái bộ phát hiện. Kafka được cấp dữ liệu bởi truy vấn riêng `alerts_outbox`, đọc CDF của `silver.alerts` (outbox pattern, ít nhất một lần). Alerter bỏ trùng theo `alert_id`. Chạy lại vi lô ở bất kỳ điểm nào cũng không mất và không nhân đôi cảnh báo. |
| Ghi Elasticsearch lỗi một phần | Lỗi tạm thời (429, 5xx, mất kết nối) còn sót sau khi thử lại sẽ làm vi lô **thất bại** và Spark chạy lại từ checkpoint. Lỗi vĩnh viễn của từng tài liệu (4xx) được ghi vào `data/metrics/es_rejected.jsonl`. Mọi tài liệu đều có `_id` ổn định (`event_id`, `match_id`, `match_id|batch_id` cho lịch sử kịch tính) nên ghi lại không sinh bản sao. |
| Gold incremental bỏ sót thay đổi chỉ có ở bảng thống kê | Job gold theo dõi phiên bản và CDF của cả ba bảng `match_events`, `match_state`, `match_stats`. |
| Áp lại cùng một RETRACT | Điều kiện là `s.seq > t.seq OR (s.seq = t.seq AND NOT t.is_deleted)`. RETRACT cùng `seq` thắng UPSERT, còn áp lại một RETRACT đã áp dụng không thay đổi gì, kể cả `version_count`. Test `test_replayed_batches_change_nothing` chạy lại mọi vi lô hai lần và so sánh. |

### 2.4. Một nơi ghi duy nhất cho mỗi bảng
Bảng Delta nằm trên S3 (MinIO). Với S3, Delta 3.x chỉ bảo đảm an toàn khi các lần ghi đồng thời vào một bảng đến từ cùng một driver (`S3SingleDriverLogStore`). Hệ thống vì thế được thiết kế như sau:
- mọi bảng silver chỉ được ghi bởi driver pipeline, kể cả OPTIMIZE và VACUUM định kỳ (luồng `Maintenance` trong cùng driver);
- trong driver, mọi thao tác ghi silver đi qua một khóa chung, kèm cơ chế thử lại khi gặp lỗi xung đột;
- job gold là nơi duy nhất ghi bảng gold;
- Elasticsearch, DuckDB và các script thí nghiệm chỉ đọc.

### 2.5. Change Data Feed
Mọi bảng đều bật `delta.enableChangeDataFeed`. CDF được dùng cho ba việc: (1) đồng bộ Elasticsearch theo thời gian thực; (2) job gold chế độ `incremental` chỉ tính lại các trận có thay đổi kể từ phiên bản đã xử lý lần trước; (3) thí nghiệm E1 lấy thời điểm commit của từng sự kiện qua `_commit_timestamp`.

### 2.6. Chỉ số kịch tính (`analytics/excitement.py`)
- **V1 (luật):** cảnh báo ngay khi có bàn thắng, phản lưới, thẻ đỏ, phạt đền hoặc VAR.
- **V2 (chấm điểm):** điểm = Σ w(e)·exp(−Δt/180 s) trên cửa sổ 10 phút trận, nhân với hệ số bối cảnh. Trọng số: bàn thắng 5; phạt đền 4; thẻ đỏ 3,5; VAR 3; cú sút 0,6 + 8·xG (+0,7 nếu trúng đích). Hệ số bối cảnh: +0,3 từ phút 75, +0,5 hiệp phụ, +0,3 nếu tỷ số sát nút sau phút 60. Cảnh báo được phát khi điểm vượt ngưỡng (mặc định 7, chọn trên tập điều chỉnh của E6) theo chiều đi lên, có trễ hồi (chỉ sẵn sàng lại khi điểm < 60% ngưỡng) và thời gian chờ 3 phút. V2 không phát nếu V1 vừa báo cùng trận trong 60 giây trận trước đó, để tránh báo hai lần cho một khoảnh khắc.

Pipeline và đánh giá offline (E6) dùng chung đúng một đoạn mã này.

### 2.7. Lựa chọn so với đề cương
| Đề cương | Triển khai | Lý do |
|---|---|---|
| HDFS/MinIO | MinIO (S3A) | Nhẹ, ổn định trên Docker Desktop. Mô hình đối tượng S3 cũng là lựa chọn phổ biến cho lakehouse |
| Trino hoặc DuckDB | DuckDB + thư viện `deltalake` | Không cần Hive metastore. Truy vấn trực tiếp file Delta, hỗ trợ time travel |
| Airflow hoặc cron | `batch/job_runner.py` (vòng lặp gọi `spark-submit`) | Tiết kiệm khoảng 1,5 GB RAM so với Airflow, nhưng vẫn ghi nhật ký từng lần chạy |
| Kibana/Grafana | Grafana (chính), Kibana (tùy chọn `-Ui`) | Một công cụ hiển thị được cả số liệu Prometheus và dữ liệu Elasticsearch |

## 3. Lược đồ dữ liệu

- **Kafka (Avro):** `schemas/match_event.avsc`, `match_state.avsc`, `match_stats.avsc`, `tracking_frame.avsc`. Định dạng Confluent (5 byte đầu: magic + schema id). Schema Registry đặt chế độ tương thích BACKWARD. Pipeline lấy lược đồ mới nhất khi khởi động.
- **Delta:** định nghĩa trong `streaming/tables.py` và `batch/gold_job.py`. Mỗi sự kiện ở silver mang đủ mốc thời gian cho phân tích độ trễ: `source_ts`, `ingest_ts`, `kafka_ts`, `processed_ts`, `first_seen_ts`, `updated_ts`, cùng `version_count` và `batch_id`.

## 4. Tài nguyên mặc định (máy 16 GB)
Kafka 3 × 384 MB heap; Schema Registry 256 MB; Elasticsearch 768 MB; Spark worker 4 core / 4 GB; pipeline 2 executor × 768 MB (`PIPELINE_CORES_MAX=2`) cùng driver 1 GB; job gold 1 executor × 768 MB. Tổng khoảng 9–10 GB khi tất cả cùng chạy.
