# Vận hành và phát triển

## Chạy demo

Từ thư mục repository, dùng PowerShell 7 và Docker Desktop Linux containers:

```powershell
.\scripts\demo.ps1
```

Script chờ Kafka/Airflow sẵn sàng, tạo topic, bật consumer, trigger một DAG run có ID riêng, chờ `success`, rồi chờ batch và stream khớp nhau. Script không chạy thêm pipeline CLI song song với DAG và không giả định phải có đúng 1.000 message mới.

```powershell
.\scripts\demo.ps1 -SkipBuild -TimeoutSeconds 600
docker compose logs --tail 50 airflow consumer
docker compose down
```

`down` giữ dữ liệu và volume. Airflow ở `http://localhost:18080`; chỉ cổng loopback được mở cho demo local.

## Cấu hình crawl

Sửa [config/steam.json](../config/steam.json) trước một run mới.

| Trường | Ý nghĩa |
|---|---|
| `app_ids` | Watchlist App ID nguyên dương, không trùng |
| `language` | Steam language code hoặc `all` |
| `max_reviews_per_game` | Trần review mỗi game trong một poll |
| `page_size` | Số dòng request, từ 1 đến 100 |
| `request_interval_seconds` | Khoảng cách tối thiểu giữa request |
| `timeout_seconds`, `max_attempts` | Timeout mỗi request và số lần thử |
| `country_code`, `currency` | Adapter hiện hỗ trợ cặp `US`/`USD` |

Sửa watchlist không tự xóa dữ liệu đã thu thập. Snapshot tích lũy các review đã quan sát. Không đổi config khi resume cùng run ID.

## Điều khiển Airflow

```powershell
docker compose exec airflow airflow dags list-import-errors
docker compose exec airflow airflow dags trigger steam_api_batch
docker compose exec airflow airflow dags list-runs steam_api_batch --output json
docker compose exec airflow airflow dags pause steam_api_batch
docker compose exec airflow airflow dags unpause steam_api_batch
```

DAG pause sẽ dừng lịch mới. Trước khi chạy pipeline thủ công, cần chờ run đang hoạt động hoàn tất.

## Chạy từng bước khi debug

```powershell
docker compose run --rm worker crawl --run-id debug-001
docker compose run --rm worker clean --run-id debug-001
docker compose run --rm worker aggregate --run-id debug-001
docker compose run --rm worker verify --run-id debug-001
docker compose run --rm --entrypoint python worker -m src.streaming publish
```

Resume crawl lỗi bằng `worker all --run-id debug-001`. Nếu báo base snapshot đã thay đổi, bắt đầu run ID mới. Run cũ vẫn được giữ để audit.

Consumer service xử lý các event đã phát. Có thể chạy export lại từ receipt đã lưu:

```powershell
docker compose run --rm --entrypoint python worker -m src.streaming export
docker compose run --rm --entrypoint python worker -m src.reporting --wait-seconds 120
```

Báo cáo được sinh từ dữ liệu đo được và có timestamp. Nếu stream chưa đồng bộ, reporter chờ hoặc trả lỗi; không ghi đè báo cáo đạt bằng số liệu chưa hoàn tất. Script demo cũng lưu trạng thái DAG vào `reports/steam_api/AIRFLOW_RUNS.json`.

## Test và format

```powershell
docker compose run --rm --entrypoint python worker -m unittest discover -s tests -v
python -m pip install -r requirements-dev.txt
ruff check src dags scripts tests
ruff format --check src dags scripts tests
```

Ruff dùng cấu hình trong `pyproject.toml`. Dữ liệu chạy, cache và file tạm đã được loại khỏi Git.

## Chẩn đoán lỗi

| Triệu chứng | Kiểm tra |
|---|---|
| Không kết nối Docker | Docker Desktop đã chạy Linux containers chưa |
| Airflow không healthy | `docker compose logs --tail 100 airflow`; kiểm tra import error |
| Steam timeout hoặc 429 | Xem task log và request metadata; giảm tần suất nếu cần |
| Quarantine tăng | Mở file trong `data/steam_api/quarantine/` để xem lý do và page nguồn |
| Có outbox pending | Xem task `publish_changes` và log Kafka; publish lại sau khi broker sẵn sàng |
| Báo cáo chưa khớp stream | Xem log consumer; reporter chỉ kiểm chứng khi các event đã được xử lý |

Compose dùng DNS công khai trong riêng container pipeline vì DNS mặc định của môi trường thử nghiệm không resolve được Steam. Có thể chỉnh `dns` trong `compose.yaml` theo mạng của bạn; HTTPS vẫn kiểm tra chứng chỉ.
