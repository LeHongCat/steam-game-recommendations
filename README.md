# Steam Game Recommendation — Data Pipeline

Thu thập thông tin game và review trực tiếp từ Steam API, làm sạch dữ liệu và lưu để phục vụ bước recommendation. Repository triển khai ba phần: **Data Sources**, **Ingestion & Streaming** và **Storage**.

```text
Steam API → Airflow → Bronze JSON → Silver/Gold Parquet → Kafka → Silver stream
```

## Chạy nhanh

Cần **Docker Desktop** đang chạy Linux containers và **PowerShell 7**.

```powershell
git clone https://github.com/LeHongCat/steam-game-recommendations.git
cd steam-game-recommendations
.\scripts\demo.ps1
```

Script dựng môi trường, chạy DAG, chờ consumer xử lý xong và kiểm tra dữ liệu batch với stream. Khi thành công:

- Mở Airflow tại [localhost:18080](http://localhost:18080), DAG `steam_api_batch`.
- Đọc kết quả trong [reports/VERIFICATION.md](reports/VERIFICATION.md).
- Dữ liệu nằm tại `data/steam_api/`, không được commit lên Git.

Lần chạy sau có thể dùng `.\scripts\demo.ps1 -SkipBuild`. Dừng các service bằng `docker compose down`; dữ liệu vẫn được giữ.

## Crawl những gì

| Dữ liệu | Nội dung |
|---|---|
| Game | App ID, tên, mô tả, developer/publisher, tag/category ID, free flag và giá |
| Review | Review ID thật, tác giả, nội dung, recommend, ngôn ngữ, thời gian tạo/cập nhật, playtime và vote |

Cấu hình mặc định theo dõi CS2, Dota 2, Stardew Valley, Cyberpunk 2077 và ELDEN RING. Mỗi poll lấy **tối đa 200 review tiếng Anh cập nhật gần nhất mỗi game**, 100 review/trang. Sửa App ID và giới hạn tại [config/steam.json](config/steam.json).

Đây là cửa sổ có giới hạn; tổng số review đã lưu có thể tăng qua nhiều poll. Pipeline chưa bảo đảm lấy hết lịch sử hoặc mọi cập nhật giữa hai lần poll. `interactions` được suy ra từ review; nguồn này không cung cấp log click hay giao dịch mua.

## Làm sạch những gì

- Giữ JSON gốc ở Bronze; chuẩn hóa Unicode, newline và ký tự điều khiển lỗi cho Silver.
- Chuyển Unix time sang UTC, chuẩn hóa ID và hash SteamID tác giả.
- Gộp theo review ID, giữ phiên bản cập nhật mới hơn; dữ liệu sai cấu trúc vào quarantine.
- Giữ review tiêu cực. Review rỗng vẫn giữ feedback và có `absa_eligible=false`.
- Dữ liệu optional thiếu hoặc sai kiểu được giữ `null`. Giá catalog dùng context US/USD.

Review mới được quan sát hoặc thay đổi phiên bản sẽ tạo event Kafka. Poll lại cùng phiên bản không tạo event mới. Kafka consumer lưu receipt trước khi commit offset và loại trùng theo event ID.

## Cấu trúc chính

| Đường dẫn | Vai trò |
|---|---|
| `src/steam_api.py` | HTTP client, phân trang và checkpoint |
| `src/normalization.py` | Chuẩn hóa game và review |
| `src/pipeline.py` | Tạo Silver/Gold và kiểm tra chất lượng |
| `src/events.py`, `src/state.py` | Review revision, event contract và outbox |
| `src/streaming.py` | Kafka publisher, consumer và bảng stream |
| `dags/steam_api_batch.py` | DAG chạy mỗi giờ |

## Tài liệu và kiểm tra

- [Kiến trúc](docs/ARCHITECTURE.md): luồng dữ liệu và trách nhiệm từng thành phần.
- [Data contract](docs/DATA_CONTRACT.md): schema, khóa và ý nghĩa các trường.
- [Vận hành](docs/OPERATIONS.md): cấu hình, chạy thủ công, test và xử lý lỗi.
- [Báo cáo kiểm chứng](reports/VERIFICATION.md): snapshot có thời điểm ghi nhận, không phải trạng thái live.

```powershell
docker compose run --rm --entrypoint python worker -m unittest discover -s tests -v
```

Bản triển khai dùng Kafka một broker, Airflow standalone và filesystem local cho demo môn học. ABSA và mô hình recommendation là bước tiếp theo.
