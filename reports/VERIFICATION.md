# Kết quả crawl Steam API ngày 2 tháng 11 năm 2026

Đã chạy trực tiếp hai endpoint Steam catalog và reviews, lưu raw JSON, chuẩn hóa Parquet, gửi qua Kafka và chạy DAG Airflow. Số liệu dưới đây là dữ liệu API thật trong cửa sổ demo, không phải kết quả mô hình recommendation.

| Game | App ID | Reviews | Recommend | Không recommend |
|---|---:|---:|---:|---:|
| Counter-Strike 2 | 730 | 200 | 145 | 55 |
| Dota 2 | 570 | 200 | 136 | 64 |
| Stardew Valley | 413150 | 200 | 198 | 2 |
| Cyberpunk 2077 | 1091500 | 200 | 180 | 20 |
| ELDEN RING | 1245620 | 200 | 179 | 21 |
| Tổng | | 1.000 | 838 | 162 |

Phạm vi request: tiếng Anh, Updated filter, tối đa 200 review/game, 100 review/page, catalog US/USD. Đây là cửa sổ có giới hạn, không phải toàn bộ lịch sử Steam.

## Dữ liệu đã crawl và làm sạch

Catalog có tên, mô tả, developer/publisher, tag/category ID, free flag và lựa chọn mua/giá. Reviews có ID thật, tác giả, text, nhãn recommend, created/updated timestamps, playtime và các cờ nguồn có sẵn. Raw được giữ nguyên trong Bronze, có metadata request và SHA-256.

Đã chuẩn hóa Unicode, newline và ký tự điều khiển lỗi; mô tả game sang plain text; Unix time sang timestamp UTC; ID sang string; thiếu optional playtime/vote/cờ giữ null. Giữ cả 162 review không recommend. Không có dòng bị quarantine trong lượt crawl thật; có 3 review rỗng được giữ với `absa_eligible=false`, còn 997 review có text cho ABSA.

Catalog game miễn phí có giá base game bằng 0, không lấy gói nâng cấp trả phí làm giá game. Currency USD được ghi là suy từ request context US/USD, không giả là trường currency có trong response catalog.

## Pipeline đã kiểm chứng

| Kiểm tra thực tế | Kết quả |
|---|---|
| Lượt crawl đầu | 5 game, 1.000 reviews, 1.000 review upserts trong outbox |
| Hai lượt Airflow tiếp theo | Cùng 1.000 review, phiên bản không đổi, mỗi lượt tạo 0 event mới |
| Quality gate | 11/11 đạt |
| Kafka publisher | 1.000 phiên bản đã ACK, 0 pending |
| Kafka consumer | Nhận 1.000, accepted 1.000, quarantine 0 |
| Silver stream | 1.000 phiên bản, 1.000 review ID mới nhất |
| Airflow | Một scheduled run và một manual run thành công, đủ 5 task |
| Bộ kiểm tra tự động | 9/9 đạt |

Các test tự động dùng API-shaped fixtures để kiểm tra pagination/resume, API 429, negative feedback, missing optional data, bản trùng, review được cập nhật, checksum nguồn, publish failure và duplicate Kafka delivery. Test cập nhật review là tình huống kiểm soát; các lượt API live quan sát không có thay đổi phiên bản trong cửa sổ kiểm chứng.

## Bằng chứng và cách tái kiểm tra

- [RESULTS.json](steam_api/RESULTS.json): số liệu theo game, từng poll, outbox và đối chiếu batch/stream.
- [FIRST_CRAWL.json](steam_api/FIRST_CRAWL.json): endpoint, request parameters, checkpoint, raw checksum và quality gate lượt đầu.
- [LATEST_CRAWL.json](steam_api/LATEST_CRAWL.json): manifest lượt thành công mới nhất.
- [AIRFLOW_RUNS.json](steam_api/AIRFLOW_RUNS.json): trạng thái các run thực tế.
- [ENVIRONMENT.json](steam_api/ENVIRONMENT.json): Python, Airflow, PyArrow và Kafka client.

Raw chứa dữ liệu tác giả nên được giữ local trong `data/steam_api/`, không đẩy lên GitHub. Bộ báo cáo chỉ có aggregate, metadata request và đường dẫn lineage; không chứa nội dung review hoặc Steam ID tác giả.

```powershell
docker compose run --rm --entrypoint python worker -m unittest discover -s tests -v
docker compose exec airflow airflow dags list-runs steam_api_batch --output json
docker compose run --rm --entrypoint python worker scripts/collect_api_report.py
```

Chưa benchmark throughput/p95 hoặc thử kill process chính xác giữa ACK/commit. Kafka một broker và filesystem local phục vụ demo môn học. ABSA, recommendation model và full-history backfill thuộc bước mở rộng.
