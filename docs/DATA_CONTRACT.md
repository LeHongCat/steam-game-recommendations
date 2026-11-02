# Contract dữ liệu bàn giao

Contract v1 mô tả dữ liệu mà ba tầng đầu cung cấp cho Processing. Các đường dẫn dưới đây tính từ thư mục project. Đọc `data/latest.json` để chọn snapshot đã vượt quality gate, sau đó lấy file theo ID đó. Dữ liệu không có thời gian hợp lệ vẫn được giữ, có trạng thái rõ ràng.

## Catalog game

`data/silver/<snapshot_id>/games.parquet`, một dòng cho một `item_id`.

| Trường | Kiểu Arrow | Ý nghĩa |
|---|---|---|
| item_id | string | Steam App ID, bắt buộc và duy nhất |
| title | string | title, fallback sang app_name |
| genres | list string | Danh sách genres; thiếu là danh sách rỗng |
| tags | list string | Danh sách tags; thiếu là danh sách rỗng |
| developer | string nullable | Giữ giá trị nguồn |
| publisher | string nullable | Giữ giá trị nguồn |
| price_raw | string | Giá nguyên nghĩa nguồn; chưa khẳng định currency |
| source | string | ucsd_steam_catalog_v2 |

Không dùng tên game làm khóa. Catalog lịch sử V2 không tự bảo đảm trạng thái bán, giá hay mô tả tại thời điểm từng review V1. Không giả định file này có description, ảnh hoặc nhãn loại game/DLC đầy đủ.

## Review

`data/silver/<snapshot_id>/reviews.parquet`, một dòng cho một review key trong nguồn.

| Trường | Kiểu Arrow | Ý nghĩa |
|---|---|---|
| review_id | string | Khóa sinh ổn định từ namespace, source user, item và posted_raw |
| user_id | string | Namespace ucsd_v1 cộng SHA-256 source user key |
| item_id | string | Khóa ngoại sang catalog |
| review_text | string | Nội dung review đã trim, giữ cả review tiêu cực |
| recommended | boolean | Nhãn recommend của nguồn, không phải rating sao |
| event_time | string nullable | ISO 8601 có timezone theo quy ước lưu trữ |
| timestamp_precision | string | day_assumed_utc hoặc unknown |
| posted_raw | string | Thời gian text gốc để audit |
| absa_eligible | boolean | Có review text không rỗng |
| source | string | ucsd_steam_reviews_v1 |

Nếu nguồn không có review ID thì đây là khóa suy ra, không được gọi là Steam recommendation ID thật. Cùng user/item/ngày có thể bị gom theo quy tắc này; số bản bị gom được ghi trong manifest. Giữ Bronze để có thể thay quy tắc nhận dạng sau khi profile sâu hơn.

Ngày thiếu năm không được suy ra từ năm tải file hoặc năm hiện tại. Chỉ dùng dòng có `event_time` khi cần temporal split. Dữ liệu dạng ngày không cung cấp độ chính xác đến giờ/phút của sự kiện gốc.

## Feedback từ review

`data/silver/<snapshot_id>/interactions.parquet`, một dòng feedback cho mỗi review sạch.

| Trường | Kiểu Arrow | Ý nghĩa |
|---|---|---|
| event_id | string | Bằng source review ID trong demo |
| user_id | string | Cùng namespace với reviews |
| item_id | string | Cùng App ID với catalog |
| event_type | string | review_positive hoặc review_negative |
| event_time | string nullable | Theo review nguồn |
| source_review_id | string | Lineage đến review |
| source_mode | string | historical |

Đây là một cách biểu diễn khác của cùng tín hiệu review. Không cộng như hai hành vi độc lập khi đã dùng reviews. Với implicit collaborative filtering, người phụ trách model cần định nghĩa rõ cách dùng positive và negative; không coi mọi dòng là positive.

## Kafka feedback replay

Topic `steam.review-feedback.v1`; contract máy đọc được tại `contracts/review-feedback-v1.schema.json`. Bảng Silver stream ở `data/silver/stream/review_feedback.parquet` có cùng các trường payload.

| Trường | Kiểu | Ý nghĩa |
|---|---|---|
| schema_version | integer | 1 |
| event_id | UUID string | Logical ID ổn định qua các lần replay |
| review_id | string | Source review key |
| user_id | string | Source namespace user key |
| item_id | string | Steam App ID |
| recommended | boolean | Feedback gốc |
| event_time | ISO string hoặc null | Thời gian lịch sử |
| produced_at | ISO string có timezone | Thời điểm phát replay |
| source_mode | string | replay |

Bronze Kafka bổ sung `topic`, `partition`, `offset`, `ingested_at` và giữ `payload`. Silver gom logical event trùng; Bronze giữ receipt của từng offset. Consumer group `steam-bronze-v1` commit sau khi lưu batch, cung cấp at-least-once receipt; không tuyên bố exactly-once end-to-end.

## Gold mô tả

`data/gold/<snapshot_id>/game_statistics.parquet` có `item_id`, `review_count`, `positive_count`, `negative_count`, `positive_ratio`, `snapshot_id`. Đây là kết quả tổng hợp toàn bộ review trong mẫu snapshot, chưa cắt theo thời điểm train. Không dùng làm feature quá khứ mà không tổng hợp lại theo cutoff.

## Contract dành cho phần Processing bổ sung

Các bảng dưới đây là thiết kế bàn giao, chưa có prediction/model output trong bản demo:

- `absa_outputs`: review_id, item_id, user_id, aspect, opinion_term, sentiment, sentiment_score, confidence nullable, model_version, processed_at, source_snapshot_id.
- `item_aspect_profiles`: item_id, aspect, quality_score, mention_count, model_version, source_snapshot_id, feature_cutoff_time.
- `user_aspect_profiles`: user_id, aspect, preference_score, importance nullable, mention_count, model_version, source_snapshot_id, feature_cutoff_time.

Không điền sẵn neutral cho aspect chưa được nhắc đến; không dùng recommended để gán sentiment cho từng aspect. Người phụ trách Processing định nghĩa thang điểm, schema model và cutoff trước khi ghi các bảng này.
