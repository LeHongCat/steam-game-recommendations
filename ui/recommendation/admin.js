const byId = (id) => document.getElementById(id);
const number = new Intl.NumberFormat("en-US");
const dates = new Intl.DateTimeFormat("en-US", {
  day: "2-digit", month: "2-digit", hour: "2-digit", minute: "2-digit",
  second: "2-digit", timeZone: "Asia/Ho_Chi_Minh",
});
const EVENT_NAMES = {
  impression: "Impression", detail: "Opened score and review evidence",
  click: "Opened Steam", dismiss: "Dismissed",
};
const INPUT_NAMES = {query: "Query", games: "Selected games", aspects: "Aspects", history: "History"};
const PAGE_SIZE = 20;
let offset = 0;
let signedIn = false;
let selectedSearch = null;
let overviewVersion = 0;
let absa = null;

function element(tag, value, className) {
  const node = document.createElement(tag);
  if (value != null) node.textContent = value;
  if (className) node.className = className;
  return node;
}

function formatTime(value) {
  return dates.format(new Date(value));
}

async function api(path, body) {
  const response = await fetch(path, body === undefined ? {cache: "no-store"} : {
    method: "POST", headers: {"Content-Type": "application/json"}, body: JSON.stringify(body),
  });
  const data = await response.json();
  if (!response.ok) {
    const error = new Error(typeof data.detail === "string" ? data.detail : "The request failed.");
    error.status = response.status;
    throw error;
  }
  return data;
}

function showError(message = "") {
  byId("admin-error").textContent = message;
  byId("admin-error").hidden = !message;
}

function showSession(authenticated) {
  signedIn = authenticated;
  byId("admin-login").hidden = signedIn;
  byId("admin-dashboard").hidden = !signedIn;
  byId("admin-logout").hidden = !signedIn;
  if (!signedIn) {
    overviewVersion += 1;
    offset = 0;
    closeInspect();
    absa = null;
    byId("absa-browser").open = false;
    byId("absa-content").hidden = true;
    byId("activity-rows").replaceChildren();
  }
}

function renderChart(id, entries) {
  if (!entries.length) {
    byId(id).replaceChildren(element("li", "No activity in this period.", "small muted"));
    return;
  }
  const max = entries[0].count;
  byId(id).replaceChildren(...entries.map(({label, count}) => {
    const row = element("li");
    const text = element("div", null, "bar-label");
    text.append(element("span", label), element("strong", number.format(count)));
    const track = element("div", null, "bar-track");
    const fill = element("div", null, "bar-fill");
    fill.style.width = (100 * count / max) + "%";
    track.setAttribute("aria-hidden", "true");
    track.append(fill);
    row.append(text, track);
    return row;
  }));
}

function emptyRow(columns, message) {
  const row = element("tr");
  const cell = element("td", message, "muted");
  cell.colSpan = columns;
  row.append(cell);
  return row;
}

function renderOverview(data) {
  const {summary, charts, searches, total} = data;
  const metrics = [
    ["Searches", summary.searches], ["Active users", summary.users],
    ["Game impressions", summary.impressions],
    ["Review evidence views", summary.details], ["Steam clicks", summary.clicks],
  ];
  byId("admin-stats").replaceChildren(...metrics.map(([label, value]) => {
    const stat = element("div", null, "stat");
    stat.append(element("strong", number.format(value)), element("span", label, "muted small"));
    return stat;
  }));
  renderChart("chart-queries", charts.queries);
  renderChart("chart-criteria", charts.criteria);
  renderChart("chart-selected-games", charts.selected_games);
  renderChart("chart-viewed-games", charts.viewed_games);

  const rows = searches.map((row) => {
    const tr = element("tr");
    const input = element("td", null, "search-query");
    input.append(element("p", row.query || row.source.map(source => INPUT_NAMES[source]).join(" + ")));
    if (row.selected_games.length) {
      input.append(element("p", row.selected_games.map(game => game.title).join(", "), "muted small"));
    }
    const actions = element("td");
    const button = element("button", "Open", "chip");
    button.type = "button";
    button.dataset.requestId = row.request_id;
    button.setAttribute("aria-label", "Open search by User " + row.user_id + " at " + formatTime(row.at));
    actions.append(button);
    tr.append(element("td", formatTime(row.at)), element("td", "User " + row.user_id),
      input, element("td", number.format(row.result_count)), element("td", number.format(row.view_count)), actions);
    return tr;
  });
  byId("activity-rows").replaceChildren(...(rows.length ? rows : [
    emptyRow(6, "No searches yet. Submit a request from the User demo to get started."),
  ]));
  byId("ledger-count").textContent = total
    ? number.format(offset + 1) + "–" + number.format(offset + searches.length) + " / " + number.format(total)
    : "0 searches";
  byId("previous-page").disabled = offset === 0;
  byId("next-page").disabled = offset + PAGE_SIZE >= total;
  byId("refreshed").textContent = "Updated at " + formatTime(Date.now()) + " · Vietnam time";
}

function renderSearch(search) {
  const {inputs, results, events} = search;
  const resultRows = byId("result-rows");
  const expanded = new Set(Array.from(resultRows.querySelectorAll("details[open]"),
    (details) => details.dataset.scoreKey));
  byId("inspect-meta").textContent = "User " + search.user_id + " · " + formatTime(search.at)
    + " · " + search.model_version;
  const values = [
    ["Query", search.query],
    ["Selected games", inputs.selected_games.map(game => game.title).join(", ")],
    ["Positive Steam review history", inputs.steam_history.map(game => game.title).join(", ")],
    ["Steam clicks before search", inputs.click_history.map(game => game.title).join(", ")],
    ["Criteria", inputs.criteria.join(", ")],
    ["Ranking", inputs.ranking ? scoringView.summary(inputs.ranking) : "Requested K " + inputs.top_k],
    ["Query model", inputs.query_intent?.query_model?.id],
  ];
  byId("search-inputs").replaceChildren(...values.map(([label, value]) => {
    const row = element("p");
    row.append(element("strong", label), element("span", value || "—"));
    return row;
  }));
  const eventsByItem = new Map();
  for (const event of events) {
    if (!eventsByItem.has(event.item_id)) eventsByItem.set(event.item_id, []);
    eventsByItem.get(event.item_id).push(event);
  }
  const rows = results.map((row) => {
    const tr = element("tr");
    const game = element("td");
    game.append(element("strong", row.title), element("p", row.explanation, "small muted"));
    if (row.matched_aspects.length) {
      game.append(element("p", row.matched_aspects.map(aspect => aspect.label + " · "
        + number.format(aspect.support) + " reviews").join(" · "), "small"));
    }
    const actions = element("td");
    const chips = element("div", null, "badges");
    for (const event of eventsByItem.get(row.item_id) ?? []) {
      chips.append(element("span", EVENT_NAMES[event.event], "badge event-badge"));
    }
    if (!chips.children.length) chips.append(element("span", "—", "muted"));
    actions.append(chips);
    const scoreCell = element("td");
    const details = element("details", null, "admin-score");
    details.dataset.scoreKey = search.request_id + "/" + row.item_id;
    details.open = expanded.has(details.dataset.scoreKey);
    details.append(element("summary", scoringView.format(row.score) + " · Calculation"),
      scoringView.breakdown(row.scoring));
    scoreCell.append(details);
    tr.append(element("td", row.rank), game, scoreCell, actions);
    return tr;
  });
  resultRows.replaceChildren(...(rows.length ? rows : [emptyRow(4, "This search returned no games.")]));
  byId("event-timeline").replaceChildren(...(events.length ? events.map(event =>
    element("li", formatTime(event.at) + " · " + EVENT_NAMES[event.event] + " · " + event.title, "small")
  ) : [element("li", "No interactions recorded.", "small muted")]));
}

function closeInspect() {
  selectedSearch = null;
  byId("search-inspect").hidden = true;
}

async function inspect(requestId, reveal = false) {
  selectedSearch = requestId;
  const data = await api("/api/admin/searches/" + encodeURIComponent(requestId));
  if (!signedIn || selectedSearch !== requestId) return;
  renderSearch(data);
  byId("search-inspect").hidden = false;
  if (reveal) byId("search-inspect").focus();
}

async function refresh() {
  if (!signedIn) return;
  const version = ++overviewVersion;
  const params = new URLSearchParams({hours: byId("filter-period").value, limit: PAGE_SIZE, offset});
  if (byId("filter-user").value) params.set("user_id", byId("filter-user").value);
  const data = await api("/api/admin/activity?" + params);
  if (!signedIn || version !== overviewVersion) return;
  renderOverview(data);
  if (selectedSearch) await inspect(selectedSearch);
}

function handleError(error) {
  if (error.status === 401) showSession(false);
  showError(error.message);
}

function renderGameAspects() {
  const game = absa?.games.find(item => item.item_id === byId("absa-game").value);
  if (!game) return;
  byId("absa-coverage").textContent = number.format(game.processed) + "/" + number.format(game.reviews)
    + " reviews analyzed with ABSA · " + number.format(game.reviews_with_aspects) + " reviews with aspects";
  byId("absa-rows").replaceChildren(...game.aspects.map(aspect => {
    const row = element("tr");
    const values = [aspect.label, number.format(aspect.support),
      ...["POS", "NEU", "NEG"].map(key => number.format(aspect.counts[key] ?? 0))];
    row.append(...values.map(value => element("td", value)));
    return row;
  }));
}

async function loadAbsa() {
  if (!signedIn || absa || !byId("absa-browser").open) return;
  byId("absa-status").textContent = "Loading Phase 1 audit…";
  const data = await api("/api/admin/absa");
  if (!signedIn) return;
  if (!data.audit) {
    byId("absa-status").textContent = "Rebuild the snapshot to display the Phase 1 audit.";
    return;
  }
  absa = data;
  byId("absa-status").textContent = "Snapshot " + data.run_id;
  const audit = data.audit;
  const ratio = (left, right) => number.format(left) + " / " + number.format(right);
  const values = [
    ["Phase 1 aspect and polarity model", data.model],
    ["Phase 1 sentiment scoring mode", data.score_mode ?? "Unavailable"],
    ["Reviews analyzed with ABSA", ratio(data.processed, data.reviews)],
    ["Reviews with at least one aspect", number.format(data.reviews_with_aspects)],
    ["Aspect mentions", number.format(data.mentions)],
    ["Predictions matched to Phase 1 sentences and labels", audit.sentences ? ratio(audit.sentences.verified_mentions, data.mentions) : "—"],
    ["Sentences matched to the prediction cache", audit.cache && audit.sentences ? ratio(audit.cache.consistent_sentences, audit.sentences.rows) : "—"],
    ["Catalog entries matched", audit.catalog ? ratio(audit.catalog.matched, audit.catalog.rows) : "—"],
    ["Game profiles matched", audit.items ? ratio(audit.items.matched, audit.items.rows) : "—"],
    ["User profiles matched", audit.users ? ratio(audit.users.matched, audit.users.rows) : "—"],
  ];
  byId("absa-audit").replaceChildren(...values.map(([label, value]) => {
    const row = element("div");
    row.append(element("dt", label), element("dd", value));
    return row;
  }));
  byId("absa-game").replaceChildren(...data.games.map(game => new Option(game.title, game.item_id)));
  byId("absa-content").hidden = false;
  renderGameAspects();
}

byId("admin-login-form").addEventListener("submit", async event => {
  event.preventDefault();
  showError();
  const button = byId("admin-login-button");
  button.disabled = true;
  try {
    await api("/api/admin/login", {pin: byId("admin-pin").value});
    byId("admin-login-form").reset();
    showSession(true);
    await refresh();
  } catch (error) {
    handleError(error);
  } finally {
    button.disabled = false;
  }
});
byId("admin-logout").addEventListener("click", async () => {
  try {
    await api("/api/admin/logout", {});
    showSession(false);
    showError();
    byId("admin-pin").focus();
  } catch (error) {
    handleError(error);
  }
});
byId("activity-rows").addEventListener("click", event => {
  const button = event.target.closest("[data-request-id]");
  if (button) inspect(button.dataset.requestId, true).catch(handleError);
});
byId("close-inspect").addEventListener("click", closeInspect);
for (const id of ["filter-user", "filter-period"]) {
  byId(id).addEventListener("change", () => { offset = 0; closeInspect(); refresh().catch(handleError); });
}
byId("previous-page").addEventListener("click", () => { offset = Math.max(0, offset - PAGE_SIZE); refresh().catch(handleError); });
byId("next-page").addEventListener("click", () => { offset += PAGE_SIZE; refresh().catch(handleError); });
byId("refresh").addEventListener("click", () => { showError(); refresh().catch(handleError); });
byId("absa-browser").addEventListener("toggle", () => loadAbsa().catch(handleError));
byId("absa-game").addEventListener("change", renderGameAspects);

api("/api/admin/session").then(({authenticated}) => {
  showSession(authenticated);
  return refresh();
}).catch(handleError);
setInterval(() => {
  if (!document.hidden && !byId("activity-rows").contains(document.activeElement)
      && !byId("result-rows").contains(document.activeElement)) refresh().catch(handleError);
}, 8000);
