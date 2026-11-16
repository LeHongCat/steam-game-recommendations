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

## Dashboard demo

### Gợi ý game

Chạy với `database/` nằm cạnh repository và snapshot Steam 50 game đã xuất:

```powershell
.\scripts\recommendation.ps1 -InstallQueryModel -CacheArtwork
```

Mở [User](http://localhost:18082/) và chọn một trong ba hồ sơ người chơi. Bấm một câu mẫu như **“Fast-paced FPS, great graphics”** hoặc **“Lái xe tải xuyên châu Âu”**, hoặc gõ yêu cầu tự do. Bấm **Why this game?** trên một thẻ để xem lý do, cách tính điểm và trích dẫn review. Mở [Admin](http://localhost:18082/admin) ở tab khác với PIN demo `2468` để kiểm tra điểm đã ghi và thao tác trên kết quả. Xem [kịch bản demo hai màn hình](docs/RECOMMENDATION.md#demo-hai-man-hinh) và [FastAPI Swagger](http://localhost:18082/docs).

### Pipeline Steam và Kafka

```powershell
docker compose up -d dashboard
```

Mở [localhost:18081](http://localhost:18081) và chọn lần chạy. Dashboard đọc tiến độ từng game, nhật ký các bước, throughput Kafka, consumer lag và kết quả đối chiếu batch với stream. Xem [cách thu thập 50 game × 1.000 review](docs/OPERATIONS.md#backfill-va-dashboard).

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
| `src/backfill.py` | Crawl theo lô, xuất Parquet theo mục tiêu và đo Kafka |
| `src/dashboard.py`, `ui/` | Dashboard chỉ đọc từ checkpoint và báo cáo local |
| `dags/steam_api_batch.py` | DAG chạy mỗi giờ |
| `src/recommendation_build.py`, `src/recommendation_handoff.py` | Kiểm tra bộ bàn giao ABSA, đánh giá E1–E5, ghi `serving.sqlite` |
| `src/recommendation.py`, `src/recommendation_query.py`, `src/query_model.py` | Xếp hạng hybrid, phân tích câu nhập và model truy vấn ONNX |
| `src/recommendation_api.py`, `src/recommendation_activity.py`, `ui/recommendation/` | FastAPI, phiên demo, nhật ký tìm kiếm và giao diện User/Admin |
| `src/recommendation_art.py`, `src/recommendation_labels.py` | Cache ảnh Steam và nhãn hiển thị |

`.runtime/query-python` chứa dependency của model truy vấn do `scripts/recommendation.ps1 -InstallQueryModel` cài; không xóa thư mục này khi service recommendation đang chạy.

## Tài liệu và kiểm tra

- [Kiến trúc](docs/ARCHITECTURE.md): luồng dữ liệu và trách nhiệm từng thành phần.
- [Data contract](docs/DATA_CONTRACT.md): schema, khóa và ý nghĩa các trường.
- [Vận hành](docs/OPERATIONS.md): cấu hình, chạy thủ công, test và xử lý lỗi.
- [Báo cáo kiểm chứng](reports/VERIFICATION.md): snapshot có thời điểm ghi nhận, không phải trạng thái live.
- [Kết quả backfill 50 game](reports/BACKFILL_50X1000.md): số dòng, thời gian từng bước và số đo Kafka từ lượt chạy thực tế.
- [Kết quả recommendation](reports/RECOMMENDATION.md): phép thử E1–E5 và độ trễ HTTP local.

```powershell
docker compose run --rm --entrypoint python worker -m unittest discover -s tests -v
```

Bản triển khai dùng Kafka một broker, Airflow standalone và filesystem local cho demo môn học. Demo recommendation kết hợp hồ sơ người dùng, dữ liệu ABSA Phase 1 và model truy vấn ngữ nghĩa chạy trên CPU.
