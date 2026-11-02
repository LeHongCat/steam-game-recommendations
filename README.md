# Steam API Recommendation Data Platform

Pipeline lấy dữ liệu trực tiếp từ Steam API cho ba phần Data Sources, Ingestion & Streaming và Storage.

```text
Steam catalog + reviews API
  → Airflow crawl có phân trang và checkpoint
  → Bronze JSON gốc + metadata request + SHA-256
  → Silver Parquet games/reviews/interactions
  → Quality gate + SQLite outbox
  → Kafka review_upsert
  → Consumer lưu Bronze Kafka rồi commit offset
  → Silver stream review_versions và reviews_latest
```

[Kế hoạch ba phần](docs/PLAN_TODAY.md) · [Schema và quy tắc làm sạch](docs/DATA_CONTRACT.md) · [Kết quả chạy API thật](reports/VERIFICATION.md)

## Phạm vi crawl

Cấu hình trong `config/steam.json`: Counter-Strike 2, Dota 2, Stardew Valley, Cyberpunk 2077 và ELDEN RING; mỗi lần lấy tối đa 200 review tiếng Anh cập nhật gần nhất cho mỗi game, 100 review/trang. Có thể sửa App ID và giới hạn trong file này.

- Catalog: tên, mô tả, developer/publisher, tag/category ID, free-to-play, các lựa chọn mua và giá theo ngữ cảnh US/USD.
- Reviews: review ID thật, tác giả, nội dung, nhãn recommend, ngôn ngữ, thời gian tạo/cập nhật, playtime và các cờ nguồn có sẵn.
- Không thu thập click/wishlist/purchase event. `interactions` là feedback được suy ra từ cùng review.

Đây là crawl có giới hạn cho môn học. Manifest ghi `coverage=bounded_latest_updates` và lý do dừng từng game; không khẳng định đã lấy toàn bộ Steam hay không bỏ sót cập nhật giữa hai lần poll. Ngôn ngữ và số review làm mẫu có selection bias khi đánh giá recommendation.

## Chạy demo

Yêu cầu Docker Desktop với Linux containers. Trong thư mục repository:

```powershell
./scripts/demo.ps1
```

Hoặc chạy từng bước:

```powershell
docker compose build
docker compose up -d kafka airflow
docker compose run --rm worker all
docker compose run --rm --entrypoint python worker -m src.streaming publish
docker compose run --rm --entrypoint python worker -m src.streaming consume --count 1000 --timeout-seconds 90
docker compose run --rm --entrypoint python worker -m src.streaming export
```

`--count 1000` phù hợp lần crawl đầu mặc định. Nếu publisher báo số khác, dùng đúng số `published`. Khi không có message mới, consumer bounded trả exit code 2 do timeout, không tự tạo event để đủ số lượng.

Để consumer tự xử lý các lần poll tiếp theo:

```powershell
docker compose up -d consumer
docker compose exec airflow airflow dags list-import-errors
docker compose exec airflow airflow dags trigger steam_api_batch
docker compose exec airflow airflow dags unpause steam_api_batch
docker compose exec airflow airflow dags list-runs steam_api_batch --output json
```

Airflow tại `http://localhost:18080`; DAG `steam_api_batch` chạy `@hourly`, có 5 task: `crawl_steam → silver → gold → quality_gate → publish_review_changes`.

Mỗi lần poll mới bắt đầu từ review cập nhật gần nhất. Nếu task crawl lỗi, Airflow retry cùng run ID và tiếp tục từ page/cursor đã lưu. CLI có thể resume bằng `worker all --run-id <run_id>`; phải giữ nguyên config của run đó. Không chạy CLI ghi snapshot đồng thời với DAG; DAG được giới hạn một active run.

## Lưu trữ và độ tin cậy

Tất cả dữ liệu nằm trong `data/steam_api/`:

```text
bronze/api/<run_id>/      JSON phản hồi nguyên byte và request metadata
bronze/kafka/<topic>/     event receipt theo partition/offset
silver/<run_id>/          games reviews interactions Parquet
silver/stream/            review_versions và reviews_latest Parquet
gold/<run_id>/            thống kê review theo game
manifests/               cursor, checksum, config, row counts, coverage
quarantine/              record không hợp lệ và lý do
state.sqlite             accepted review revisions và Kafka outbox
latest.json              snapshot đã qua quality gate
```

Chỉ phiên bản review mới được quan sát hoặc thay đổi nội dung/nhãn/thời gian cập nhật/playtime-at-review mới sinh event. Votes và lifetime playtime thay đổi riêng không phát event mới. Outbox đánh dấu sent sau ACK; có thể giao lại sau crash, consumer dedup bằng event ID ổn định theo review ID và revision. Đây là at-least-once delivery, không phải exactly-once end-to-end.

Raw JSON giữ nguyên; review text được chuẩn hóa Unicode, newline và ký tự điều khiển. Không xóa phủ định, emoji, review âm hay tự gán sentiment aspect. Review rỗng được giữ với `absa_eligible=false`. Silver gộp snapshot trước với poll hiện tại để giữ các review không nằm trong cửa sổ poll mới; không phát hiện xóa review chỉ từ việc nó biến mất khỏi cửa sổ này.

## Kiểm tra và vận hành

```powershell
docker compose run --rm --entrypoint python worker -m unittest discover -s tests -v
docker compose logs --tail 30 consumer
Get-Content data/steam_api/latest.json
docker compose down
```

`down` dừng riêng demo, giữ volume và dữ liệu. Airflow standalone, Kafka một broker và local filesystem phục vụ demo môn học. Chưa triển khai ABSA/model recommendation, Spark/Hadoop hoặc Iceberg.

Compose đặt DNS công khai cho riêng container pipeline vì DNS mặc định của môi trường thử nghiệm không resolve được Steam. Không thay DNS của máy. Chỉ dùng hostname HTTPS hợp lệ, không tắt kiểm tra chứng chỉ. Các cổng demo bind loopback.

Review endpoint được mô tả trong [Steamworks IUserReviewsService](https://partner.steamgames.com/doc/webapi/IUserReviewsService); catalog dùng `IStoreBrowseService/GetItems/v1` trên chính Steam API, đã kiểm tra bằng phản hồi live. Catalog endpoint có ít tài liệu công khai hơn nên adapter kiểm tra schema trước khi nhận dữ liệu.
