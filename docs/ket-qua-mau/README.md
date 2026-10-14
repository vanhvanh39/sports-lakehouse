# Kết quả mẫu E5, E6: cách tái lập

Các file trong thư mục này được sinh ra bởi đúng mã nguồn trong repo. Thứ tự tạo lại:

```powershell
# 1. Tải đúng hai giải đã dùng (toàn bộ trận, không giới hạn): 64 trận World Cup 2022 + 51 trận Euro 2024
.\manage.ps1 download --sb-seasons 43:106,55:282 --max-matches 0 --statsbomb-only

# 2. Chạy hai thí nghiệm offline (không cần cụm Spark/Kafka)
.\manage.ps1 exp e5        # tham số mặc định: --budget 100 --provider-delay-s 20
.\manage.ps1 exp e6        # mặc định: tune = competition 43, test = competition 55, ngưỡng 3..12
```

Kết quả nằm trong `data/experiments/results/` và phải trùng với các file ở đây. Cả hai thí nghiệm đều tất định: không dùng số ngẫu nhiên.

Đầu vào là dữ liệu StatsBomb Open Data tải từ nhánh `master` của https://github.com/statsbomb/open-data (tải ngày 14/10/2026). Nếu StatsBomb cập nhật dữ liệu sau ngày đó, con số có thể lệch nhẹ. Khi viết báo cáo, hãy ghi rõ ngày tải.

| File | Nội dung |
|---|---|
| `e5_scheduler_summary.csv` | Trung bình theo ngày của mỗi chính sách lập lịch (45 ngày thi đấu) |
| `e5_scheduler_per_day.csv` | Kết quả từng ngày: số request, độ trễ p50/p95, sự kiện bỏ lỡ |
| `e5_scheduler.png` | Biểu đồ so sánh |
| `e6_highlight_main.csv` | V1, V2, V1+V2 trên **tập kiểm tra** (Euro 2024) với ngưỡng chọn từ tập điều chỉnh |
| `e6_highlight_v2_sweep.csv` | Quét ngưỡng trên tập điều chỉnh (dùng để chọn) và tập kiểm tra (chỉ tham khảo) |
| `e6_highlight.json` | Toàn bộ kết quả, cách tách tập, ngưỡng được chọn |
| `e6_precision_recall.png` | Đường precision/recall trên tập kiểm tra |
