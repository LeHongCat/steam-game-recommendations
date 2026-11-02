# Contract Steam API v2

Đọc `data/steam_api/latest.json` để lấy run đã qua quality gate. Mọi file Silver batch nằm trong `data/steam_api/silver/<run_id>/`. ID là string để tránh mất độ chính xác của SteamID/review ID 64 bit. Thời gian Silver là Arrow `timestamp[us, tz=UTC]`.

## Catalog game

`games.parquet`, một dòng cho mỗi Steam App ID.

| Nhóm trường | Kiểu | Ý nghĩa |
|---|---|---|
| item_id, title, description | string | App ID, tên, mô tả plain text |
| developers, publishers | list string | Danh sách tên từ API |
| tag_ids, category_ids | list int64 | Steam IDs; không tự quy đổi thành tên genre |
| is_free | boolean | Free-to-play |
| price_minor_units | int64 nullable | Giá trong đơn vị nhỏ của tiền tệ; 0 cho game miễn phí |
| price_currency | string nullable | USD trong config US/USD của adapter |
| price_currency_source | string nullable | request_context; không giả là field currency có trong response |
| purchase_option_name, purchase_options_json | string nullable | Lựa chọn giá được chọn và các lựa chọn gốc |
| country_code | string | Ngữ cảnh Store request |
| observed_at | UTC timestamp | Thời điểm lấy catalog |
| source | string | steam_api |

Với game trả phí, dùng `best_purchase_option` khi có; thiếu thì giá null. Với game free-to-play, giữ giá 0 kể cả API có gói nâng cấp trả phí. Giá là quan sát lúc crawl, không bảo đảm giá tại ngày review.

## Reviews

`reviews.parquet`, một dòng cho mỗi review ID thật của Steam. Snapshot hợp nhất các review đã quan sát, giữ bản có updated time mới hơn.

| Trường | Kiểu | Quy tắc |
|---|---|---|
| review_id | string | recommendationid từ Steam, không sinh từ user/ngày |
| user_id | string | steam: cộng SHA-256 SteamID tác giả |
| item_id | string | App ID của review request, phải có trong catalog |
| review_text | string | NFC Unicode, newline LF, bỏ ký tự điều khiển lỗi và trim |
| recommended | boolean | voted_up, giữ cả false |
| language | string | Phải khớp ngôn ngữ request, trừ cấu hình all |
| created_at, updated_at | UTC timestamp | Từ Unix timestamp; updated >= created |
| observed_at | UTC timestamp | Thời điểm lấy page nguồn |
| playtime_at_review_minutes | int64 nullable | Playtime tại lúc viết review |
| playtime_forever_minutes | int64 nullable | Lifetime playtime được API trả ở lúc crawl |
| votes_up, votes_funny | int64 nullable | Vote quan sát lúc crawl |
| steam_purchase, received_for_free, refunded | boolean nullable | Cờ từ API; không tạo purchase event |
| absa_eligible | boolean | false nếu text rỗng; feedback vẫn được giữ |
| source_page | string | Đường dẫn page Bronze để audit |
| source | string | steam_api |

Không xóa phủ định, emoji hoặc review âm. Không xóa stopwords/stemming trong tầng ingestion. ID/nhãn/ngôn ngữ/timestamp không hợp lệ được quarantine cùng page và vị trí dòng; thiếu optional playtime/vote/cờ giữ null. Không suy luận missing playtime thành 0.

## Feedback suy ra từ review

`interactions.parquet`: event_id=`review:<review_id>`, source_review_id, user_id, item_id, event_type=`review_positive` hoặc `review_negative`, event_time=created_at, source=`steam_api_review`.

Đây là biểu diễn khác của cùng review, không phải hành vi độc lập. Không cộng hai lần khi đã dùng review; không biến mọi dòng thành positive interaction. Click, wishlist và giao dịch mua nằm ngoài nguồn hiện tại.

## Kafka upsert

Topic `steam.reviews.upserts.v2`; contract máy đọc được trong `contracts/review-upsert-v2.schema.json`.

- schema_version=2, event_type=review_upsert, source_mode=api_poll.
- review_id và item_id khớp review payload; source_run_id liên kết manifest.
- review chứa toàn bộ row đã chuẩn hóa, timestamp biểu diễn ISO 8601 trong JSON.
- revision là SHA-256 của identity, text, recommended, language, created/updated và playtime-at-review. Votes, lifetime playtime và observed_at không làm thay đổi revision riêng.
- event_id là UUID ổn định từ review ID và revision. Broker key là review ID.
- emitted_at là lúc tạo outbox event, khác created_at/updated_at của Steam và ingested_at của Kafka consumer.

Lần đầu quan sát review tạo upsert dù review đó đã được đăng từ trước. Poll lại cùng phiên bản không tạo event mới. Khi review thay đổi, ID review giữ nguyên, revision và event ID thay đổi. Outbox và accepted version được commit cùng transaction; sent chỉ được đánh dấu sau broker ACK.

`silver/stream/review_versions.parquet` giữ một dòng mỗi event ID. `reviews_latest.parquet` giữ một dòng mỗi review ID, chọn theo updated_at rồi observed_at. Raw Kafka receipt vẫn giữ topic/partition/offset; retry delivery có thể tạo thêm receipt mà không nhân logical version.

## Gold và bàn giao ABSA

`gold/<run_id>/game_statistics.parquet`: item_id, review_count, positive_count, negative_count, positive_ratio, run_id. Đây là thống kê toàn snapshot, không tự dùng làm feature lịch sử trong temporal split.

Nhóm Processing bổ sung sau, chưa có prediction trong demo:

- absa_outputs: review_id, review_revision, item_id, aspect, opinion_term, sentiment, score, confidence nullable, model_version, source_run_id, processed_at.
- user/item aspect profiles: user_id hoặc item_id, aspect, score, mention_count, feature_cutoff_time, source_run_id, model_version.

Nên dùng review ID + revision để chỉ chạy lại ABSA cho review mới hoặc được chỉnh sửa. Không dùng recommended để gán sentiment cho từng aspect; không coi aspect không xuất hiện là neutral. Khi đánh giá theo thời gian, cần xét updated_at của phiên bản review và observed_at của metadata để tránh dùng thông tin tương lai.
