const stepNames = {
  select_games: "Select games from Steam Charts",
  crawl: "Crawl and store Bronze data",
  clean: "Clean Silver data",
  aggregate: "Aggregate Gold data",
  verify_batch: "Validate batch",
  export_dataset: "Export reviews as Parquet",
  publish: "Publish Kafka events",
  consume: "Consume events and commit offsets",
  export_stream: "Build Silver stream",
  verify_stream: "Reconcile batch and stream",
};

const checkNames = {
  bronze_checksums: "Bronze page checksums match",
  collection_complete: "Collection complete",
  requested_games_available: "All requested games available",
  nonempty_reviews: "Reviews collected",
  unique_review_ids: "Unique review IDs",
  valid_item_references: "Valid game references",
  review_reconciliation: "Collected, duplicate, and rejected counts reconcile",
  one_feedback_per_review: "One feedback entry per review",
  valid_utc_timestamps: "Valid UTC timestamps",
  gold_reconciliation: "Gold and Silver review counts match",
  binary_feedback_preserved: "Positive and negative recommendations retained",
  latest_quality_gate: "Batch passes quality checks",
  no_pending_outbox: "Outbox fully published",
  all_sent_versions_consumed: "All acknowledged events consumed",
  stream_matches_batch_review_ids: "Batch and stream review IDs match",
  stream_matches_batch_content: "Batch and stream review versions match",
  api_source_only: "Batch sourced from Steam API",
  api_events_only: "Stream sourced from Steam API",
};

const states = {running: "Running", success: "Complete", failed: "Failed", pending: "Pending", cancelled: "Cancelled"};
const labels = {
  ...stepNames,
  lag_before: "Measure lag before consumption",
  lag_resume: "Measure lag on resume",
  lag_after: "Measure lag after consumption",
};
const counts = new Intl.NumberFormat("en-US", {maximumFractionDigits: 2});
const clock = new Intl.DateTimeFormat("en-US", {timeStyle: "medium", timeZone: "Asia/Ho_Chi_Minh"});
const date = new Intl.DateTimeFormat("en-US", {dateStyle: "short", timeStyle: "short", timeZone: "Asia/Ho_Chi_Minh"});
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
    ? `Target ${number(data.target)} valid reviews per game. Raw limit ${number(data.raw_limit)}.`
    : `Polling up to ${number(data.raw_limit)} reviews per game.`);
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
    progress.setAttribute("aria-label", `${game.title} ${completed ? "exported" : "collected"}`);
    const detail = completed
      ? `${number(game.export.positive)} positive · ${number(game.export.negative)} negative`
      : `${unit(game.pages, "pages")} · ${game.done ? "Collected" : game.pages ? "Collecting" : "Pending"}`;
    node.append(header, progress, element("p", `App ${game.app_id} · ${detail}`, "game-meta"));
    return node;
  }));
  list.scrollTop = scroll;
  if (!games.length) list.append(element("p", "No games match the search.", "muted"));
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
  text("publish-rate", unit(publish.events_per_second, "events/s published"));
  text("lag", number(offsets?.lag));
  text("lag-detail", after ? "After consumption" : resume ? "On resume" : before ? "Before consumption" : "Waiting for broker measurement");
  const rows = [
    ["Pending outbox events after publish", number(publish.pending)],
    ["Publish throughput", unit(publish.events_per_second, "event/s")],
    ["Consumer offsets processed", number(data.kafka.offsets_processed ?? consume.received)],
    ["Consume throughput", unit(data.kafka.consume_events_per_second ?? consume.events_per_second, "event/s")],
    ["Consumer lag before → after", `${number(before?.lag)} → ${number(after?.lag)}`],
    ["Receipt Bronze Kafka trong snapshot", number(data.kafka.bronze_receipts)],
    ["Duplicate deliveries merged", number(data.steps.export_stream?.metrics?.duplicate_deliveries_collapsed)],
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
    unit(clean.unique_reviews_in_poll, "valid review IDs in this crawl"),
    unit(clean.duplicate_review_rows, "duplicate rows"),
    unit(clean.rejected_reviews, "rejected rows"),
  ].join(" · "));
  text("check-count", outcomes.length ? `${outcomes.filter(Boolean).length}/${outcomes.length} passed` : "Awaiting checks");
  $("checks").replaceChildren(...groups.flatMap(([group, checks]) => [
    element("p", group, "check-group"),
    ...Object.entries(checks).map(([key, passed]) => {
      const row = element("div", null, `check ${passed ? "" : "failed"}`);
      row.append(element("span", checkNames[key] ?? key), element("span", passed ? "Pass" : "Failed"));
      return row;
    }),
    ...(Object.keys(checks).length ? [] : [element("p", "No results yet", "muted")]),
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
    line.append(element("span", "Interruption", "log-label"), element("span", incident.detail, "log-message"));
    return line;
  }), ...rows);
  if (!$("logs").children.length) $("logs").append(element("p", "This run has no step logs.", "muted"));
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
    text("notice", data.missing ? "Selected run not found." : "No Steam data yet. Run the pipeline to get started.");
    return;
  }
  text("run-caption", `Started ${date.format(new Date(data.started_at))} · Steam language ${data.language}`);
  text("raw", number(data.games.reduce((total, game) => total + game.rows, 0)));
  text("pages", unit(data.games.reduce((total, game) => total + game.pages, 0), "review pages saved"));
  text("exported", number(data.exported));
  text("target", data.target ? `Target ${number(data.target * data.games.length)} review` : "Dataset export available for backfills");
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
    text("connection", "Updating");
  } catch (error) {
    if (error.name === "AbortError") return;
    $("connection").className = "connection";
    text("connection", "Disconnected");
    $("notice").hidden = false;
    text("notice", "Could not load run status. Retrying automatically.");
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
