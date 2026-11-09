const stepNames = {
  select_games: "Chọn game từ Steam Charts",
  crawl: "Crawl và lưu Bronze",
  clean: "Làm sạch Silver",
  aggregate: "Tổng hợp Gold",
  verify_batch: "Kiểm tra batch",
  export_dataset: "Xuất bộ review Parquet",
  publish: "Gửi event Kafka",
  consume: "Nhận event và commit offset",
  export_stream: "Tạo Silver stream",
  verify_stream: "Đối chiếu batch và stream",
};

const checkNames = {
  bronze_checksums: "Checksum các trang Bronze khớp",
  collection_complete: "Crawl đã hoàn tất",
  requested_games_available: "Có đủ game trong danh sách",
  nonempty_reviews: "Có dữ liệu review",
  unique_review_ids: "Review ID duy nhất",
  valid_item_references: "Review và feedback trỏ đúng game",
  review_reconciliation: "Khớp số dòng crawl, trùng và bị loại",
  one_feedback_per_review: "Mỗi review có một feedback",
  valid_utc_timestamps: "Timestamp UTC hợp lệ",
  gold_reconciliation: "Gold khớp số review trong Silver",
  binary_feedback_preserved: "Giữ cả hai nhãn recommend",
  latest_quality_gate: "Batch đạt kiểm tra chất lượng",
  no_pending_outbox: "Outbox đã gửi hết",
  all_sent_versions_consumed: "Đã nhận đủ event được ACK",
  stream_matches_batch_review_ids: "Batch và stream khớp review ID",
  stream_matches_batch_content: "Batch và stream khớp phiên bản review",
  api_source_only: "Batch dùng dữ liệu Steam API",
  api_events_only: "Stream dùng event từ Steam API",
};

const states = {running: "Đang chạy", success: "Hoàn tất", failed: "Lỗi", pending: "Chờ", cancelled: "Đã dừng"};
const labels = {
  ...stepNames,
  lag_before: "Đo lag trước khi nhận",
  lag_resume: "Đo lag khi tiếp tục",
  lag_after: "Đo lag sau khi nhận",
};
const counts = new Intl.NumberFormat("vi-VN", {maximumFractionDigits: 2});
const clock = new Intl.DateTimeFormat("vi-VN", {timeStyle: "medium", timeZone: "Asia/Ho_Chi_Minh"});
const date = new Intl.DateTimeFormat("vi-VN", {dateStyle: "short", timeStyle: "short", timeZone: "Asia/Ho_Chi_Minh"});
const $ = (id) => document.getElementById(id);
const number = (value) => value == null ? "—" : counts.format(value);
const unit = (value, suffix) => value == null ? "—" : `${number(value)} ${suffix}`;
const text = (id, value) => { $(id).textContent = value; };

function element(tag, content, className) {
  const node = document.createElement(tag);
  if (content != null) node.textContent = content;
  if (className) node.className = className;
  return node;
}

function renderGames(data) {
  const large = data.games.length > 10;
  $("game-search-box").hidden = !large;
  const query = large ? $("game-search").value.trim().toLocaleLowerCase() : "";
  const games = data.games.filter((game) => `${game.title} ${game.app_id}`.toLocaleLowerCase().includes(query));
  text("game-count", query ? `${games.length}/${data.games.length} game` : unit(data.games.length, "game"));
  text("game-description", data.target
    ? `Mục tiêu ${number(data.target)} review hợp lệ mỗi game. Trần lấy thô ${number(data.raw_limit)}.`
    : `Lượt poll tối đa ${number(data.raw_limit)} review mỗi game.`);
  const list = $("game-list");
  const scroll = list.scrollTop;
  list.classList.toggle("large", large);
  list.replaceChildren(...games.map((game) => {
    const completed = game.export.exported != null;
    const node = element("article", null, `game ${completed ? "complete" : ""}`);
    const header = element("div", null, "game-title");
    header.append(element("span", game.title), element("span", number(game.export.exported ?? game.rows)));
    const progress = element("progress");
    progress.max = completed ? data.target : data.raw_limit;
    progress.value = completed ? game.export.exported : game.rows;
    progress.setAttribute("aria-label", `${game.title} ${completed ? "đã xuất" : "đã crawl"}`);
    const detail = completed
      ? `${number(game.export.positive)} recommend · ${number(game.export.negative)} không recommend`
      : `${unit(game.pages, "trang")} · ${game.done ? "Đã lấy xong" : game.pages ? "Đang lấy" : "Chờ lấy"}`;
    node.append(header, progress, element("p", `App ${game.app_id} · ${detail}`, "game-meta"));
    return node;
  }));
  list.scrollTop = scroll;
  if (!games.length) list.append(element("p", "Không có game khớp từ khóa.", "muted"));
}

function renderSteps(data) {
  const latest = data.attempts.at(-1);
  const batchDone = data.status === "completed";
  const verified = data.steps.verify_stream?.status === "success" || (batchDone && !data.target && latest?.status !== "running");
  let phase = "pending";
  if (["failed", "cancelled"].includes(latest?.status)) phase = latest.status;
  else if (verified) phase = "success";
  else if (data.status === "collecting" || latest?.status === "running") phase = "running";
  $("phase").className = `pill ${phase}`;
  text("phase", states[phase]);
  const rows = Object.entries(stepNames).filter(([key]) => key !== "select_games" || data.steps[key]);
  $("steps").replaceChildren(...rows.map(([key, label], index) => {
    const record = data.steps[key];
    const fallback = batchDone && ["crawl", "clean", "aggregate", "verify_batch"].includes(key) ? "success"
      : key === "crawl" && data.status === "collecting" ? "running" : "pending";
    const status = record?.status ?? fallback;
    const item = element("li");
    const name = element("span", null, "step-name");
    name.append(element("span", String(index + 1).padStart(2, "0"), "step-number"), element("span", label));
    item.append(name, element("span", unit(record?.duration_seconds, "s"), "step-duration"),
      element("span", states[status], `pill ${status}`));
    return item;
  }));
}

function renderKafka(data) {
  const publish = data.steps.publish?.metrics ?? {};
  const consume = data.steps.consume?.metrics ?? {};
  const before = data.steps.lag_before?.metrics;
  const resume = data.steps.lag_resume?.metrics;
  const after = data.steps.lag_after?.metrics;
  const offsets = after ?? resume ?? before;
  text("published", number(publish.published));
  text("publish-rate", unit(publish.events_per_second, "event/s khi gửi"));
  text("lag", number(offsets?.lag));
  text("lag-detail", after ? "Sau khi consume" : resume ? "Khi tiếp tục consume" : before ? "Trước khi consume" : "Chờ đo trên broker");
  const rows = [
    ["Event chờ trong outbox sau publish", number(publish.pending)],
    ["Tốc độ publish", unit(publish.events_per_second, "event/s")],
    ["Offset consumer xử lý", number(data.kafka.offsets_processed ?? consume.received)],
    ["Tốc độ consume", unit(data.kafka.consume_events_per_second ?? consume.events_per_second, "event/s")],
    ["Consumer lag trước → sau", `${number(before?.lag)} → ${number(after?.lag)}`],
    ["Receipt Bronze Kafka trong snapshot", number(data.kafka.bronze_receipts)],
    ["Lượt giao trùng đã gộp", number(data.steps.export_stream?.metrics?.duplicate_deliveries_collapsed)],
  ];
  $("kafka-metrics").replaceChildren(...rows.map(([name, value]) => {
    const row = element("div");
    row.append(element("dt", name), element("dd", value));
    return row;
  }));
  $("offsets").replaceChildren(...(offsets ? [
    element("p", `Group ${offsets.group}`, "technical"),
    ...offsets.partitions.map((row) => element("p", `Partition ${row.partition} · offset commit ${number(row.committed_offset)} · log end ${number(row.end_offset)}`)),
  ] : []));
}

function renderChecks(data) {
  const groups = [["Batch", data.batch_checks], ["Stream", data.stream_checks]];
  const outcomes = groups.flatMap(([, checks]) => Object.values(checks));
  const clean = data.counts;
  text("clean-counts", clean.unique_reviews_in_poll == null ? "" : [
    unit(clean.unique_reviews_in_poll, "ID hợp lệ trong lượt crawl"),
    unit(clean.duplicate_review_rows, "dòng trùng"),
    unit(clean.rejected_reviews, "dòng bị loại"),
  ].join(" · "));
  text("check-count", outcomes.length ? `${outcomes.filter(Boolean).length}/${outcomes.length} đạt` : "Chờ kiểm tra");
  $("checks").replaceChildren(...groups.flatMap(([group, checks]) => [
    element("p", group, "check-group"),
    ...Object.entries(checks).map(([key, passed]) => {
      const row = element("div", null, `check ${passed ? "" : "failed"}`);
      row.append(element("span", checkNames[key] ?? key), element("span", passed ? "Đạt" : "Lỗi"));
      return row;
    }),
    ...(Object.keys(checks).length ? [] : [element("p", "Chưa có kết quả", "muted")]),
  ]));
}

function renderLogs(data) {
  const rows = data.attempts.map((record) => {
    const line = element("div", null, "log-entry");
    const time = element("time", clock.format(new Date(record.started_at)));
    time.dateTime = record.started_at;
    const message = [labels[record.step] ?? record.step, states[record.status], record.note ?? record.error].filter(Boolean).join(" · ");
    line.append(time, element("span", message, "log-message"));
    return line;
  });
  $("logs").replaceChildren(...data.incidents.map((incident) => {
    const line = element("div", null, "log-entry incident");
    line.append(element("span", "Gián đoạn", "log-label"), element("span", incident.detail, "log-message"));
    return line;
  }), ...rows);
  if (!$("logs").children.length) $("logs").append(element("p", "Lần chạy chưa có nhật ký từng bước.", "muted"));
}

function render(data) {
  displayed = data;
  if (JSON.stringify(data.runs) !== $("run").dataset.runs) {
    $("run").replaceChildren(...data.runs.map((run) => new Option(run, run)));
    $("run").dataset.runs = JSON.stringify(data.runs);
  }
  $("run").value = data.run_id ?? "";
  $("results").hidden = !data.run_id || data.missing;
  $("notice").hidden = !$("results").hidden;
  if ($("results").hidden) {
    text("notice", data.missing ? "Không tìm thấy lần chạy đã chọn." : "Chưa có dữ liệu Steam. Hãy chạy pipeline để bắt đầu.");
    return;
  }
  text("run-caption", `Bắt đầu ${date.format(new Date(data.started_at))} · Ngôn ngữ Steam ${data.language}`);
  text("raw", number(data.games.reduce((total, game) => total + game.rows, 0)));
  text("pages", unit(data.games.reduce((total, game) => total + game.pages, 0), "trang review đã lưu"));
  text("exported", number(data.exported));
  text("target", data.target ? `Mục tiêu ${number(data.target * data.games.length)} review` : "Xuất riêng cho lượt backfill");
  renderGames(data);
  renderSteps(data);
  renderKafka(data);
  renderChecks(data);
  renderLogs(data);
}

let selected = new URLSearchParams(location.search).get("run") ?? "";
let displayed;
let timer;
let request;

async function refresh() {
  clearTimeout(timer);
  request?.abort();
  const current = new AbortController();
  request = current;
  try {
    const response = await fetch(`/api/status?${new URLSearchParams({run: selected})}`, {signal: current.signal});
    if (!response.ok) throw new Error(`HTTP ${response.status}`);
    render(await response.json());
    $("connection").className = "connection live";
    text("connection", "Đang cập nhật");
  } catch (error) {
    if (error.name === "AbortError") return;
    $("connection").className = "connection";
    text("connection", "Mất kết nối");
    $("notice").hidden = false;
    text("notice", "Không đọc được trạng thái. Dashboard sẽ tự thử lại.");
  } finally {
    if (request === current) timer = setTimeout(refresh, 4000);
  }
}

$("run").addEventListener("change", () => {
  selected = $("run").value;
  $("game-search").value = "";
  history.replaceState(null, "", `?${new URLSearchParams({run: selected})}`);
  refresh();
});
$("game-search").addEventListener("input", () => renderGames(displayed));
refresh();
