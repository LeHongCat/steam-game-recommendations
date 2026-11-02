"""Collect reproducible evidence and render the report from the same measured data."""

import argparse
import platform
import time
from importlib.metadata import version
from zoneinfo import ZoneInfo

import pyarrow.parquet as pq

from src import state, storage
from src.events import revision
from src.streaming import TOPIC


class ReportNotReadyError(RuntimeError):
    """A batch or its stream output has not completed yet."""


def _read_stream(name: str) -> list[dict]:
    path = storage.DATA / "silver/stream" / f"{name}.parquet"
    if not path.exists():
        raise ReportNotReadyError("Consumer has not written the stream tables yet.")
    return pq.read_table(path).to_pylist()


def _outbox_status() -> tuple[set[str], int, dict[str, int]]:
    with storage.database() as db:
        sent = {
            row[0] for row in db.execute("SELECT event_id FROM outbox WHERE sent_at IS NOT NULL")
        }
        pending = db.execute("SELECT COUNT(*) FROM outbox WHERE sent_at IS NULL").fetchone()[0]
        by_run = dict(db.execute("SELECT run_id, COUNT(*) FROM outbox GROUP BY run_id"))
    return sent, pending, by_run


def _game_counts(games: list[dict], reviews: list[dict]) -> list[dict]:
    counts = {
        game["item_id"]: {
            "item_id": game["item_id"],
            "title": game["title"],
            "reviews": 0,
            "positive": 0,
            "negative": 0,
        }
        for game in games
    }
    for review in reviews:
        entry = counts[review["item_id"]]
        entry["reviews"] += 1
        entry["positive" if review["recommended"] else "negative"] += 1
    return list(counts.values())


def collect_report() -> tuple[dict, dict, dict]:
    run_id = state.latest_run_id()
    if run_id is None:
        raise ReportNotReadyError("No completed Steam API snapshot. Run the Airflow DAG first.")
    latest = storage.read_manifest(run_id)
    complete = sorted(
        (storage.read_json(path) for path in (storage.DATA / "manifests").glob("*.json")),
        key=lambda value: value["started_at"],
    )
    complete = [value for value in complete if value["status"] == "completed"]
    base = storage.DATA / "silver" / run_id
    games = pq.read_table(base / "games.parquet").to_pylist()
    reviews = pq.read_table(base / "reviews.parquet").to_pylist()
    versions = _read_stream("review_versions")
    stream_latest = _read_stream("reviews_latest")
    sent, pending, by_run = _outbox_status()
    checks = {
        "latest_quality_gate": all(latest["checks"].values()),
        "no_pending_outbox": pending == 0,
        "all_sent_versions_consumed": sent == {row["event_id"] for row in versions},
        "stream_matches_batch_review_ids": {row["review_id"] for row in reviews}
        == {row["review_id"] for row in stream_latest},
        "stream_matches_batch_content": {row["review_id"]: revision(row) for row in reviews}
        == {row["review_id"]: row["revision"] for row in stream_latest},
        "api_source_only": all(row["source"] == "steam_api" for row in reviews + games),
        "api_events_only": all(row["source_mode"] == "api_poll" for row in versions),
    }
    report = {
        "recorded_at": storage.now(),
        "source": "steam_api",
        "coverage": latest["coverage"],
        "first_run_id": complete[0]["run_id"],
        "latest_run_id": run_id,
        "games": _game_counts(games, reviews),
        "reviews_in_latest_snapshot": len(reviews),
        "empty_review_text": sum(not row["absa_eligible"] for row in reviews),
        "quality_checks_passed": sum(latest["checks"].values()),
        "quality_checks_total": len(latest["checks"]),
        "polls": [
            {
                "run_id": value["run_id"],
                "reviews_observed": value["counts"]["review_rows_observed"],
                "new_events_enqueued": value["new_events_enqueued"],
                "outbox_versions": by_run.get(value["run_id"], 0),
            }
            for value in complete
        ],
        "kafka": {
            "topic": TOPIC,
            "sent_versions": len(sent),
            "pending_versions": pending,
            "bronze_receipts": len(list((storage.DATA / "bronze/kafka" / TOPIC).glob("*/*.json"))),
            "unique_review_versions": len(versions),
            "latest_review_ids": len(stream_latest),
        },
        "checks": checks,
    }
    return report, complete[0], latest


def _markdown(report: dict) -> str:
    local_time = report["recorded_at"].astimezone(ZoneInfo("Asia/Ho_Chi_Minh"))
    lines = [
        "# Kết quả kiểm chứng Steam API",
        "",
        f"Sinh lúc **{local_time:%d/%m/%Y %H:%M:%S} (UTC+7)** từ dữ liệu đã lưu. Đây là snapshot kiểm chứng, không phải trạng thái live.",
        "",
        f"Run gần nhất: `{report['latest_run_id']}`. Phạm vi: `{report['coverage']}`.",
        "",
        "## Dữ liệu trong snapshot",
        "",
        "| Game | App ID | Review | Recommend | Không recommend |",
        "|---|---:|---:|---:|---:|",
    ]
    for game in report["games"]:
        title = game["title"].replace("|", "\\|").replace("\n", " ")
        lines.append(
            f"| {title} | {game['item_id']} | {game['reviews']} | {game['positive']} | {game['negative']} |"
        )
    lines.extend(
        [
            "",
            f"Tổng **{report['reviews_in_latest_snapshot']} review**, gồm **{report['empty_review_text']} review rỗng** được giữ với `absa_eligible=false`.",
            "",
            "## Kiểm tra",
            "",
            f"- Quality gate: {report['quality_checks_passed']}/{report['quality_checks_total']} đạt.",
            f"- Kafka: {report['kafka']['sent_versions']} phiên bản đã ACK, {report['kafka']['pending_versions']} phiên bản chờ gửi.",
            f"- Silver stream: {report['kafka']['unique_review_versions']} phiên bản, {report['kafka']['latest_review_ids']} review ID.",
            f"- Đối chiếu batch và stream: {sum(report['checks'].values())}/{len(report['checks'])} kiểm tra đạt.",
            "",
            "## Các lần poll",
            "",
            "| Run | Review quan sát | Event mới |",
            "|---|---:|---:|",
        ]
    )
    for poll in report["polls"]:
        lines.append(
            f"| `{poll['run_id']}` | {poll['reviews_observed']} | {poll['new_events_enqueued']} |"
        )
    lines.extend(
        [
            "",
            "## File đối chiếu",
            "",
            "- [RESULTS.json](steam_api/RESULTS.json): số liệu và từng điều kiện kiểm tra.",
            "- [FIRST_CRAWL.json](steam_api/FIRST_CRAWL.json): manifest lần crawl đầu.",
            "- [LATEST_CRAWL.json](steam_api/LATEST_CRAWL.json): manifest của snapshot được báo cáo.",
            "- [AIRFLOW_RUNS.json](steam_api/AIRFLOW_RUNS.json): trạng thái DAG đã xuất bằng script demo.",
            "- [ENVIRONMENT.json](steam_api/ENVIRONMENT.json): phiên bản môi trường tạo báo cáo.",
            "",
            "Cách sinh lại: xem [hướng dẫn vận hành](../docs/OPERATIONS.md). Kết quả test phần mềm được kiểm tra riêng; báo cáo này không suy ra chất lượng recommendation.",
            "",
        ]
    )
    return "\n".join(lines)


def write_report(report: dict, first: dict, latest: dict) -> None:
    if not all(report["checks"].values()):
        failed = ", ".join(name for name, passed in report["checks"].items() if not passed)
        raise ReportNotReadyError(f"Pipeline has not converged: {failed}")
    destination = storage.ROOT / "reports/steam_api"
    storage.atomic_json(destination / "RESULTS.json", report)
    storage.atomic_json(destination / "FIRST_CRAWL.json", first)
    storage.atomic_json(destination / "LATEST_CRAWL.json", latest)
    storage.atomic_json(
        destination / "ENVIRONMENT.json",
        {
            "python": platform.python_version(),
            "airflow": version("apache-airflow"),
            "pyarrow": version("pyarrow"),
            "kafka_python": version("kafka-python"),
        },
    )
    storage.atomic_bytes(
        storage.ROOT / "reports/VERIFICATION.md", _markdown(report).encode("utf-8")
    )


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--wait-seconds", type=float, default=0)
    args = parser.parse_args()
    if args.wait_seconds < 0:
        parser.error("--wait-seconds must be nonnegative")
    deadline = time.monotonic() + args.wait_seconds
    while True:
        try:
            report, first, latest = collect_report()
            write_report(report, first, latest)
            break
        except ReportNotReadyError as error:
            if time.monotonic() >= deadline:
                parser.exit(2, f"{error}\n")
            time.sleep(min(2, max(0, deadline - time.monotonic())))
    print(
        f"Verified {len(report['games'])} games and {report['reviews_in_latest_snapshot']} reviews."
    )
    print("Report: reports/VERIFICATION.md")


if __name__ == "__main__":
    main()
