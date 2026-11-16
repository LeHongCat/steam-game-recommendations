# Recommendation và UI demo

## Chạy local

Từ repository, chạy PowerShell 7 với Docker Desktop Linux containers:

```powershell
.\scripts\recommendation.ps1 -Handoff D:\DATA\BigData\database -InstallQueryModel -CacheArtwork
```

Mặc định script dùng `../database` và bản export `backfill-50x1000-20261109`. Chọn snapshot khác bằng `-RunId` khi bộ bàn giao được tạo từ đúng snapshot đó. `-InstallQueryModel` tải tokenizer và ONNX có kiểm tra SHA-256 để chạy trên CPU. Chạy lại có thể bỏ cờ này. `-CacheArtwork` lấy ảnh công khai từ CDN Steam. Mở [User](http://localhost:18082/), [Admin](http://localhost:18082/admin), [API](http://localhost:18082/docs) hoặc [Pipeline](http://localhost:18081/).

<a id="demo-hai-man-hinh"></a>

## Demo hai màn hình

1. Mở [màn hình User](http://localhost:18082/) và đăng nhập bằng User ID **1**, **2** hoặc **3**. Nhập nhu cầu, chọn thêm game đã thích rồi bấm **Find games**. Xếp hạng dùng lịch sử review tích cực, game đã mở qua Steam trong các lượt demo trước và lựa chọn hiện tại.
2. Mở **Score and review evidence** để xem điểm thành phần, công thức rerank, điểm cuối và trích đoạn review. Bấm **Open on Steam** để ghi click hoặc **Dismiss** để ghi dismiss.
3. Mở [màn hình Admin](http://localhost:18082/admin) ở tab khác, nhập PIN **2468**. Trong **Searches**, mở lượt tìm và bấm **Calculation** ở cột **Score**. Admin đọc phép tính đã lưu khi API trả kết quả. Với lượt tìm trước khi bổ sung tính năng này, hệ thống có điểm cuối đã lưu.

Phiên Admin và User có cookie riêng. Mã `2468` dành cho demo localhost, có thể đổi bằng cách đặt `STEAM_DEMO_ADMIN_PIN` rồi chạy `docker compose up -d recommendation`. Lượt tìm của các hồ sơ demo được lưu trong `events.sqlite`. Biểu đồ game đã xem đếm một lần cho mỗi cặp lượt tìm–game có mở chi tiết hoặc click ra Steam. Chỉ số hiển thị đếm các thẻ đã vào khung nhìn. Lượt tìm được thống kê từ lúc tính năng lưu User ID được bật.

## Sử dụng bộ bàn giao ABSA

- Aspect là một chủ đề được nhắc trong review, ví dụ Graphics, Story hoặc Gameplay. Phase 1 dùng `SovetskySoyuz110/Category_and_Polarity_ABSA_with_Game_Domain_FlanT5` để gán aspect và polarity. `build_config.json` ghi chế độ chấm điểm sentiment `deberta`.
- `absa_sentences.parquet` nối câu nguồn với prediction theo review ID, chỉ số câu và nhãn. **Score and review evidence** hiển thị trích đoạn ngắn kèm nhãn polarity do ABSA dự đoán.
- `item_dict.json`, `user_dict.json` và `games.parquet` được đối chiếu với profile tính từ prediction và catalog Steam. Cache inference JSONL được đối chiếu theo khóa câu và nhãn. Kết quả kiểm tra nằm trong `handoff_audit.json` và màn hình Admin **ABSA data and coverage**.
- Admin có thể chọn một game để xem số review được xử lý, số review nhắc từng khía cạnh và phân bố polarity. User thấy các khía cạnh thường nhắc trong lịch sử review của hồ sơ demo.

## Dữ liệu và đặc trưng

- `absa_database.parquet` mang dự đoán theo review. Import kiểm tra review ID, App ID, user ID đã băm, nội dung, nhãn recommend và thời gian với Parquet crawl. `build_config.json` cung cấp phiên bản model ABSA. Phase 1 ghi model ID và để trống revision.
- Gold lưu từng prediction và profile user/game vào `data/recommendation/gold/<run_id>/`. Với mỗi review và aspect, pipeline gộp các dự đoán lặp, chuẩn hóa điểm có dấu bằng `tanh`, giảm trọng số theo tuổi review ở item profile và làm mượt theo support. Mỗi game cần ít nhất 10 review nhắc aspect để aspect đó tham gia xếp hạng. Review trống vẫn dùng được cho implicit feedback.
- `serving.sqlite` lưu catalog, profile và snapshot đánh giá. `events.sqlite` ghi input và kết quả đúng tại thời điểm trả lời của các lượt demo, kèm model version và thao tác. UI gửi impression khi ít nhất 25% thẻ game xuất hiện trong viewport, hoặc khi người dùng trực tiếp thao tác trên thẻ.
- Nhật ký import, đánh giá và publish feature ở `reports/runtime/runs/recommend-<run_id>/steps/`. Hash của hai Parquet đầu vào nằm trong report API.

## Xếp hạng

E1 tính popularity bằng tỷ lệ recommend có làm mượt. E2 dùng item-to-item collaborative filtering từ cặp game được cùng người dùng recommend. E3 thêm độ tương đồng từ description, tag và category. E4 thêm điểm chất lượng các aspect có trong profile người dùng. E5 rerank thêm theo aspect chọn ở UI hoặc theo tần suất aspect trong lịch sử. Khi cả lịch sử và lựa chọn đều trống, API xếp theo popularity. API loại game người dùng đã chọn hoặc đã review khỏi danh sách gợi ý.

1. Tính điểm gốc bằng tổng `điểm thành phần × trọng số hiệu lực` trên các thành phần dùng được. Trọng số xuất phát từ cấu hình của snapshot và được chuẩn hóa về tổng 1.
2. Với aspect trong query, cộng `0.25 × coverage × (quality − 0.5)` rồi giới hạn điểm trong [0, 1]. `quality` là trung bình sentiment đã đưa về [0, 1]. `coverage` là tỷ lệ trọng số aspect có ít nhất 10 review. Nếu không có query, dùng `0.80 × điểm gốc + 0.20 × quality × coverage` khi có đủ dữ liệu aspect. Điều chỉnh nhịp độ nhanh áp dụng tiếp theo nếu có bằng chứng.
3. Khi chạy model query, đổi cosine similarity sang `relevance = clamp₀₋₁((cosine − 0.70) / 0.25)`. Điểm cuối là `điểm trước đó × (1 − w) + relevance × w`. Dùng `w = 0.30` khi có tag hoặc aspect nhận diện được và `w = 0.85` khi tìm hoàn toàn theo ngữ nghĩa.
4. Sắp kết quả theo điểm cuối giảm dần. Khi bằng điểm, ưu tiên popularity rồi App ID. Trả tối đa K game. API ghi nguyên breakdown vào lịch sử để Admin đối chiếu.

## Tìm game bằng câu nhập tự do

Nhập **“I want a fast-paced FPS with beautiful graphics”** hoặc **“Lái xe tải chở hàng xuyên châu Âu”**. Model `intfloat/multilingual-e5-small` so sánh câu nhập với title, description và Steam tags trong catalog. Bộ nhận diện tiêu chí rõ ràng hỗ trợ tiếng Việt và tiếng Anh. Nếu chỉ dùng ngữ nghĩa, API giữ các game gần điểm cao nhất khi similarity đủ mạnh. Khi model vắng mặt, các tag và aspect nhận diện được vẫn hoạt động. [Multilingual E5](https://huggingface.co/intfloat/multilingual-e5-small)

- **FPS**, **RPG**, **thế giới mở** và các thể loại được hỗ trợ lọc theo tag trong snapshot Steam. Với FPS, hệ thống dùng 5 tag đầu từ Bronze khi còn catalog gốc, để giảm game chỉ được gắn FPS như tag phụ. Dùng **hoặc** giữa hai thể loại để chấp nhận một trong hai. Cụm trực tiếp như **không kinh dị** loại game mang tag Horror.
- **Đồ họa**, **gameplay**, **cốt truyện** và các khía cạnh khác dùng điểm sentiment từ ABSA Phase 1 để xếp hạng. Mở **Điểm và bằng chứng** để xem điểm và số review nhắc đến khía cạnh.
- **Nhịp độ nhanh** được ưu tiên khi game có tag Fast-Paced hoặc ít nhất hai review có câu nhắc `fast-paced` hay `high-octane` và được ABSA gắn nhãn gameplay tích cực. UI báo khi kết quả thiếu bằng chứng. Khi catalog thiếu game khớp bộ lọc, API trả danh sách rỗng.

API nhận trường tùy chọn `query`, tối đa 400 ký tự, trong `POST /recommend`. Phản hồi có `query_intent`, tóm tắt `ranking` và `scoring` theo game. Để trống query để dùng hồ sơ, game đã thích và aspect chọn trực tiếp. Tags phản ánh snapshot lúc crawl. [Steam Tags](https://partner.steamgames.com/doc/store/tags)

## Đánh giá và giới hạn

- Split theo `updated_at` của phiên bản review đã thu thập. Chọn trọng số trên validation, sau đó tính E1–E5 trên cùng test, mỗi user một game chưa review được recommend. ABSA profile và CF chỉ fit trên review trước cutoff. Catalog dùng metadata quan sát lúc crawl, vì chưa có bản catalog lịch sử.
- Đây là đánh giá hồi cứu trên mẫu 50 game được chọn. Mỗi game có 1.000 review trong snapshot. Tập test chỉ gồm 25 người có đủ tương tác lịch sử, nên chênh lệch điểm cần được kiểm tra trên cohort lớn hơn.
- Bộ bàn giao chứa prediction. Để tính Macro-F1 của ABSA cần thêm review hoặc câu có nhãn gold. Notebook Phase 2 mô tả thêm suy luận ABSA từ yêu cầu tự do và đánh giá leave-one-out.
- Các số liệu thời gian trên UI đo xử lý gợi ý trong API trước khi ghi log. Kiểm tra HTTP đầu-cuối được báo cáo riêng trong [kết quả demo](../reports/RECOMMENDATION.md).
