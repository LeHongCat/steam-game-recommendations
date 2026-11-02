# Steam Game Recommendation Data Platform

Demo ba tầng đầu của hệ thống Steam Game Recommendation: Data Sources, Ingestion & Streaming và Storage. Dữ liệu thật từ UCSD đi qua Airflow batch, Kafka replay và Bronze/Silver/Gold. Kế hoạch theo từng phần và checklist nghiệm thu nằm trong [docs/PLAN_TODAY.md](docs/PLAN_TODAY.md).

Schema và quy tắc bàn giao nằm trong [docs/DATA_CONTRACT.md](docs/DATA_CONTRACT.md). Có thể chạy phần lớn demo bằng `./scripts/demo.ps1`; sau đó trigger DAG trong Airflow để xác nhận orchestration.

Kết quả chạy thực tế và các số liệu nghiệm thu: [reports/VERIFICATION.md](reports/VERIFICATION.md).

Yêu cầu: Docker Desktop đang chạy Linux containers. Demo dùng project Compose riêng `steam-recsys-demo`, Kafka tại `localhost:19092`, Airflow tại `http://localhost:18080`. Airflow standalone và quyền admin tự động chỉ phục vụ demo local, cổng chỉ bind loopback.

## Chạy batch

```powershell
docker compose build
docker compose up -d kafka airflow
docker compose run --rm worker all --limit 5000
```

Script tự tải hai file nguồn nếu chưa có. Có thể đặt file tải sẵn vào `data/downloads/`. Dữ liệu tải về, Parquet, state và log không được commit lên Git.

Sau khi Airflow khởi động, mở `http://localhost:18080`, bật và trigger DAG `steam_batch`. DAG gồm `ingest_sources → silver → gold → quality_gate`. Chạy CLI chỉ xác minh pipeline Python; cần xem run thành công trong Airflow để xác minh orchestration.

```powershell
docker compose exec airflow airflow dags list-import-errors
docker compose exec airflow airflow dags trigger steam_batch
docker compose exec airflow airflow dags unpause steam_batch
docker compose exec airflow airflow dags list-runs steam_batch
```

## Chạy Kafka replay

```powershell
docker compose run --rm --entrypoint python worker -m src.streaming init
docker compose run --rm --entrypoint python worker -m src.streaming produce --count 200
docker compose run --rm --entrypoint python worker -m src.streaming consume --count 200
docker compose run --rm --entrypoint python worker -m src.streaming export
```

Chạy lại ba lệnh produce/consume/export để kiểm tra dedup. Sự kiện là review feedback lịch sử được replay, có `source_mode=replay`; không phải click mới từ Steam. Kafka receipt lưu theo partition và offset; offset chỉ commit sau khi lưu bền vững. Silver dedup theo `event_id`.

## Kiểm tra và đọc kết quả

```powershell
docker compose run --rm --entrypoint python worker -m unittest discover -s tests -v
Get-Content data/latest.json
Get-Content reports/runtime/stream-export.json
```

File dữ liệu chính:

- `data/bronze/files/<sha256>/`: gzip gốc và lineage trong manifest.
- `data/silver/<snapshot_id>/`: `games.parquet`, `reviews.parquet`, `interactions.parquet`.
- `data/gold/<snapshot_id>/game_statistics.parquet`: thống kê mô tả.
- `data/silver/stream/review_feedback.parquet`: event sau dedup.
- `data/manifests/`, `data/metadata.sqlite`: số dòng, source checksum và run metadata.
- `data/quarantine/`, `reports/runtime/`: dữ liệu lỗi và báo cáo kiểm tra.

Để dừng riêng demo và giữ dữ liệu/volume:

```powershell
docker compose down
```

## Dữ liệu và giới hạn

Nguồn: [UCSD Steam Video Game and Bundle Data](https://cseweb.ucsd.edu/~jmcauley/datasets.html#steam_data), file V1 reviews và V2 catalog. Dùng App ID để join; user key chỉ có nghĩa trong namespace nguồn. Khi sử dụng cho báo cáo nghiên cứu cần trích dẫn bài phù hợp được UCSD nêu trên trang nguồn và kiểm tra điều kiện sử dụng; repository này không tái phân phối dataset.

Mặc định lấy 5.000 review đầu để kiểm chứng kỹ thuật, có selection bias. Giữ cả positive/negative feedback; không suy luận rating 1–5. Ngày thiếu năm giữ null, không tự đoán. Metadata giá giữ nguyên `price_raw`. Các thống kê Gold không tự bảo đảm đúng thời điểm cho đánh giá recommendation. Các bảng interaction được suy ra từ review, không phải nguồn hành vi độc lập.

Kafka một broker, Airflow standalone, filesystem local và SQLite metadata là cấu hình demo. ABSA, recommendation model, API serving, Spark/Hadoop, object storage và Iceberg là phần mở rộng sau khi pipeline đạt nghiệm thu.

API review live nếu triển khai tiếp: [Steamworks IUserReviewsService](https://partner.steamgames.com/doc/webapi/IUserReviewsService). Airflow dùng [TaskFlow API](https://airflow.apache.org/docs/apache-airflow/3.3.2/tutorial/taskflow.html); cơ chế Kafka cần hiểu theo [delivery semantics](https://kafka.apache.org/40/design/design/).
