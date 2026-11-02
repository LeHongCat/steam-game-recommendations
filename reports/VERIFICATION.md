# Kết quả nghiệm thu demo ngày 2 tháng 11 năm 2026

Đã chạy thực tế trên Docker Desktop với Kafka 4.3.1, Airflow 3.3.2 và Python 3.12.14. Đây là bằng chứng ba tầng Data Sources, Ingestion & Streaming và Storage; chưa đánh giá mô hình recommendation.

| Hạng mục | Kết quả |
|---|---|
| Nguồn thật | UCSD Steam reviews V1 và catalog V2 |
| Catalog nguồn | 32.135 object; 32.131 game sạch; 1 bản trùng; 3 dòng loại |
| Review đọc cho demo | 5.000 |
| Review sạch | 4.517 |
| Review trùng được gom | 27 |
| Review không khớp điều kiện user/item/feedback | 456 đưa vào quarantine |
| Review sạch thiếu thời gian xác định được năm | 1.005, giữ event_time=null |
| Quality gate | 9/9 kiểm tra đạt |
| Boundary integration tests | 4/4 đạt |
| Airflow | Một run scheduled và hai run manual thành công |
| Kafka vòng 1 | Gửi 200, nhận 200, Silver 200 logical event |
| Kafka vòng 2 | Gửi lại 200, nhận thêm 200, Bronze 400 receipt, Silver vẫn 200 |
| Payload sai schema | Nhận 1, quarantine 1, accepted 0 |
| Consumer mới sau commit | Không nhận lại event cũ trong cửa sổ kiểm tra 3 giây |

Đối chiếu review: `5.000 = 4.517 + 27 + 456`. Đối chiếu catalog: `32.135 = 32.131 + 1 + 3`.

Snapshot đã kiểm tra: `ea77ea508512995a`. Lượt Airflow cuối cùng: `manual__2026-11-02T10:06:48.731281+00:00`, trạng thái `success`. Các timestamp trong báo cáo JSON là UTC; ngày trình bày theo múi giờ Asia/Saigon.

Bốn integration test kiểm tra dữ liệu lỗi/trùng/âm và thiếu năm, chạy lại giữ snapshot và metadata duy nhất, phát hiện sửa Bronze bằng checksum, cùng dedup event và schema guard. Kiểm tra Kafka và Airflow dùng service thật, không thay bằng mock trong các số ở bảng trên.

Lệnh kiểm tra resume cố ý yêu cầu 1 message khi không còn message mới: consumer trả exit code 2 do hết thời gian, `received=0`. Wrapper kiểm chứng coi đây là kết quả mong đợi; không dùng exit code đó để khẳng định lỗi ingest.

Các file bằng chứng:

- `SOURCE_PROFILE.json`: profile trên file nguồn đầy đủ.
- `BATCH_MANIFEST.json`: nguồn, checksum, cấu hình, row count và trạng thái snapshot.
- `BATCH_QUALITY.json`: các quality check và số lượng bản ghi.
- `AIRFLOW_RUNS.json`: trạng thái DAG thực tế.
- `STREAM_ROUND2.json`: consumer nhận 200 event vòng 2.
- `STREAM_REPLAY.json`: 400 receipt và 200 logical event sau dedup.
- `STREAM_QUARANTINE.json`: payload lỗi được giữ riêng.
- `STREAM_RESUME.json`: kết quả đọc tiếp từ committed offset.
- `ENVIRONMENT.json`: phiên bản runtime và thư viện.

Giới hạn của bằng chứng: chưa thử kill consumer đúng lúc ghi/commit, chưa benchmark throughput hoặc latency p95, chưa có high availability. Kết quả resume ở đây xác nhận checkpoint qua hai tiến trình consumer nối tiếp; không thay cho thử nghiệm crash recovery đầy đủ. Catalog và thời gian thiếu cần xử lý thêm trước khi đánh giá recommendation theo temporal split.
