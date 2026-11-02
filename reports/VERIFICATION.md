# Kết quả kiểm chứng Steam API

Sinh lúc **02/11/2026 23:03:31 (UTC+7)** từ dữ liệu đã lưu. Đây là snapshot kiểm chứng, không phải trạng thái live.

Run gần nhất: `airflow-20b0805d5b87563f95c250b98419665c`. Phạm vi: `bounded_latest_updates`.

## Dữ liệu trong snapshot

| Game | App ID | Review | Recommend | Không recommend |
|---|---:|---:|---:|---:|
| Cyberpunk 2077 | 1091500 | 254 | 231 | 23 |
| ELDEN RING | 1245620 | 212 | 190 | 22 |
| Stardew Valley | 413150 | 215 | 212 | 3 |
| Dota 2 | 570 | 229 | 151 | 78 |
| Counter-Strike 2 | 730 | 347 | 252 | 95 |

Tổng **1257 review**, gồm **7 review rỗng** được giữ với `absa_eligible=false`.

## Kiểm tra

- Quality gate: 11/11 đạt.
- Kafka: 1260 phiên bản đã ACK, 0 phiên bản chờ gửi.
- Silver stream: 1260 phiên bản, 1257 review ID.
- Đối chiếu batch và stream: 7/7 kiểm tra đạt.

## Các lần poll

| Run | Review quan sát | Event mới |
|---|---:|---:|
| `api-live-20261102-01` | 1000 | 1000 |
| `airflow-cc546d9d76ef5a0992a561a10a84f596` | 1000 | 0 |
| `airflow-91c6d7d9546c52838f2bda54804c2f2a` | 1000 | 0 |
| `airflow-20b0805d5b87563f95c250b98419665c` | 1000 | 260 |

## File đối chiếu

- [RESULTS.json](steam_api/RESULTS.json): số liệu và từng điều kiện kiểm tra.
- [FIRST_CRAWL.json](steam_api/FIRST_CRAWL.json): manifest lần crawl đầu.
- [LATEST_CRAWL.json](steam_api/LATEST_CRAWL.json): manifest của snapshot được báo cáo.
- [AIRFLOW_RUNS.json](steam_api/AIRFLOW_RUNS.json): trạng thái DAG đã xuất bằng script demo.
- [ENVIRONMENT.json](steam_api/ENVIRONMENT.json): phiên bản môi trường tạo báo cáo.

Cách sinh lại: xem [hướng dẫn vận hành](../docs/OPERATIONS.md). Kết quả test phần mềm được kiểm tra riêng; báo cáo này không suy ra chất lượng recommendation.
