# Kết quả thu thập 50 game Steam

Lượt `backfill-50x1000-20261109` hoàn tất lúc **22:20:51 ngày 09/11/2026 (UTC+7)**. Đây là số đo của lượt chạy local qua Docker.

## Dữ liệu

Chọn 50 game gốc đủ review tiếng Anh từ Steam Charts và lưu kết quả sàng lọc. Review được lấy theo thứ tự cập nhật gần nhất qua Steam API. [Steam Charts](https://store.steampowered.com/charts/mostplayed) · [Steamworks Reviews API](https://partner.steamgames.com/doc/webapi/IUserReviewsService)

| Chỉ số | Kết quả |
|---|---:|
| Game trong catalog xuất | 50 |
| Review thô | 55.000 |
| Review bị loại hoặc trùng trong lượt crawl | 0 |
| Review duy nhất trong Parquet | 50.000 |
| Review mỗi game | 1.000 |
| Recommend / không recommend | 36.486 / 13.514 |
| Review rỗng giữ lại làm feedback | 216 |
| Kiểm tra batch đạt | 11/11 |

## Kafka

| Chỉ số | Kết quả |
|---|---:|
| Event mới được ACK | 55.000 |
| Tốc độ publish | 578,04 event/s |
| Lag trước / sau consume | 55.000 / 0 |
| Outbox chờ gửi sau publish | 0 |
| Tốc độ consume tính trên toàn lượt | 83,64 event/s |
| Kiểm tra batch/stream đạt | 7/7 |

Consumer ghi tuần tự ở đoạn đầu với 38,91 event/s, rồi tiếp tục từ offset đã commit với 8 luồng ghi và đạt 218,07 event/s ở đoạn còn lại. Có 96 offset được đọc lại khi tiếp tục. Receipt theo offset giữ một bản cho mỗi vị trí. Tốc độ toàn lượt dùng 55.000 offset đã xử lý chia cho 657,58 giây consume qua hai đoạn.

## Thời gian từng bước

| Bước | Giây |
|---|---:|
| Chọn game và kiểm tra lượng review | 55,48 |
| Crawl và lưu Bronze | 565,74 |
| Làm sạch Silver | 5,21 |
| Tổng hợp Gold | 1,06 |
| Kiểm tra batch và ghi outbox | 87,95 |
| Xuất Parquet theo mục tiêu | 1,47 |
| Publish Kafka và cập nhật outbox | 95,19 |
| Consume tuần tự đến khi dừng | 492,91 |
| Consume song song sau khi tiếp tục | 164,75 |
| Dựng Silver stream từ receipt | 324,32 |
| Đối chiếu Silver batch và stream | 18,69 |

File Parquet, `summary.json` và `kafka.json` nằm trong `data/steam_api/exports/backfill-50x1000-20261109/`. Log từng lần thử và bằng chứng chọn game nằm trong `reports/runtime/runs/backfill-50x1000-20261109/`.
