# Kết quả recommendation Steam

Lượt chạy ngày 16/11/2026 dùng snapshot `backfill-50x1000-20261109` và model xếp hạng `steam-hybrid-v1`.

| Chỉ số dữ liệu | Số lượng |
|---|---:|
| Game | 50 |
| Review | 50.000 |
| Người dùng đã băm ID | 48.940 |
| Review có kết quả xử lý Phase 1 | 49.784 |
| Review có ít nhất một aspect | 44.525 |
| Lượt nhắc aspect | 140.747 |
| Nhãn aspect khác nhau | 25 |

## Đánh giá theo thời gian

Train có 37.500 review, validation 7.500 và test 5.000. Validation có 58 người dùng đúng tiêu chí, test có 25. Mỗi người dùng được đánh giá với một game chưa review mà họ recommend trong cửa sổ kiểm tra. Mốc tách theo giờ Việt Nam là 04:42 ngày 08/11 và 08:30 ngày 09/11/2026.

| Cấu hình | Recall@10 | NDCG@10 | MAP@10 | Coverage |
|---|---:|---:|---:|---:|
| E1 Popularity | 0,1200 | 0,0698 | 0,0550 | 22% |
| E2 Collaborative filtering | 0,2800 | 0,1575 | 0,1191 | 90% |
| E3 CF + content | 0,2800 | 0,1535 | 0,1138 | 86% |
| E4 CF + content + ABSA | 0,2800 | 0,1633 | 0,1282 | 86% |
| E5 E4 + aspect reranking | 0,2800 | 0,1574 | 0,1208 | 80% |

E4 đạt NDCG@10 và MAP@10 cao nhất trong lượt test này. E5 giữ Recall@10 ở 0,28 và giảm NDCG@10 so với E4. Mẫu chỉ có 25 người dùng đủ điều kiện. Phép thử dùng phiên bản review và metadata catalog thu được lúc crawl. Bộ bàn giao cần nhãn gold để đo Macro-F1 cho ABSA.

## Kiểm tra phục vụ

API local trả thành công 40/40 request Top-6 với 5 request đồng thời. Độ trễ HTTP do client đo trên máy demo đạt p50 **401,54 ms**, p95 **630,61 ms**. Phép đo gồm truyền HTTP, đọc kết quả và ghi nhật ký request phía máy chủ.

Kiểm tra trình duyệt đã xác nhận danh sách Top-6, lọc game đã chọn, bằng chứng aspect, ẩn gợi ý và bảng E1–E5. UI đã chạy trên màn hình desktop và viewport 390 px.

File số liệu JSON nằm ở `data/recommendation/gold/backfill-50x1000-20261109/evaluation.json` và `data/recommendation/http-verification.json`. Xem [hướng dẫn chạy và định nghĩa metric](../docs/RECOMMENDATION.md).

## Kiểm tra truy vấn ngữ nghĩa và Top-K

Kiểm tra model `intfloat/multilingual-e5-small` ONNX trên CPU qua API local, cùng snapshot 50 game. Hai câu đầu dùng tìm kiếm ngữ nghĩa và cùng xếp Euro Truck Simulator 2 đầu tiên.

| Query | Game hợp lệ | Trả về | Game đầu và điểm cuối |
|---|---:|---:|---|
| Drive trucks across Europe and deliver cargo | 1 | 1 | Euro Truck Simulator 2 · 0.7229 |
| Lái xe tải chở hàng xuyên châu Âu | 1 | 1 | Euro Truck Simulator 2 · 0.4732 |
| Tôi muốn chơi game FPS, đồ họa đẹp, gameplay nhanh | 10 | 6 | Left 4 Dead 2 · 0.7904 |
| xyz | 0 | 0 | — |
| learn calculus integrals | 0 | 0 | — |

Với câu FPS, hai game đầu có cùng điểm khi đổi K từ 6 sang 2. User và Admin hiển thị cùng phép tính đã lưu cho lượt tìm. Phép thử trên một số câu minh họa chưa đủ để đo độ chính xác truy vấn trên từng ngôn ngữ.
