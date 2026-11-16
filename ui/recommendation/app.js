const byId = (id) => document.getElementById(id);
const number = new Intl.NumberFormat("en-US");
const decimal = new Intl.NumberFormat("en-US", { maximumFractionDigits: 4 });
const dates = new Intl.DateTimeFormat("en-US", {
  dateStyle: "short", timeStyle: "short", timeZone: "Asia/Ho_Chi_Minh",
});
const seeds = new Map();
const aspects = new Set();
let activeUser = null;
let observer;
let requestNumber = 0;

function element(tag, text, className) {
  const node = document.createElement(tag);
  if (text != null) node.textContent = text;
  if (className) node.className = className;
  return node;
}

async function api(path, body, keepalive = false) {
  const options = body ? {
    method: "POST", headers: { "Content-Type": "application/json" },
    body: JSON.stringify(body), keepalive,
  } : { cache: "no-store" };
  const response = await fetch(path, options);
  const value = await response.json();
  if (!response.ok) {
    const error = new Error(typeof value.detail === "string" ? value.detail : "The request failed.");
    error.status = response.status;
    throw error;
  }
  return value;
}

function pageError(message = "") {
  byId("page-error").textContent = message;
  byId("page-error").hidden = !message;
}

function showProfile() {
  const liked = activeUser?.liked_games ?? [];
  const clicks = activeUser?.recent_clicks ?? [];
  const reviewAspects = activeUser?.review_aspects ?? [];
  byId("profile-note").hidden = !liked.length;
  byId("profile-note").textContent = "Recommended on Steam · " + liked.slice(0, 3).join(", ");
  byId("recent-clicks").hidden = !clicks.length;
  byId("recent-clicks").textContent = "Recently opened · " + clicks.join(", ");
  byId("review-aspects").hidden = !reviewAspects.length;
  byId("review-aspects").textContent = "Common aspects in your past reviews · " + reviewAspects.map(aspect => aspect.label).join(", ");
}

function clearSearch() {
  requestNumber += 1;
  observer?.disconnect();
  byId("preferences").reset();
  seeds.clear();
  aspects.clear();
  renderSeeds();
  for (const chip of byId("aspects").children) chip.setAttribute("aria-pressed", "false");
  byId("games").replaceChildren();
  byId("result-count").textContent = "";
  byId("query-interpretation").hidden = true;
  byId("result-status").hidden = false;
  byId("result-status").textContent = "Enter a request or choose games you like to get started.";
  byId("submit").disabled = !activeUser;
  byId("query-submit").disabled = !activeUser;
}

function showSession(user) {
  activeUser = user;
  byId("user-login").hidden = Boolean(user);
  byId("user-area").hidden = !user;
  byId("user-label").textContent = user ? "User " + user.id : "";
  clearSearch();
  showProfile();
  if (user) route();
}

function renderSeeds() {
  byId("seeds").replaceChildren(...Array.from(seeds, ([id, title]) => {
    const chip = element("button", title + " ×", "chip");
    chip.type = "button";
    chip.setAttribute("aria-label", "Remove " + title);
    chip.addEventListener("click", () => { seeds.delete(id); renderSeeds(); });
    return chip;
  }));
  for (const option of byId("game-picker").options) {
    if (option.value) option.disabled = seeds.has(option.value) || seeds.size >= 5;
  }
}

function price(game) {
  if (game.is_free) return "Free to play";
  if (game.price_minor_units == null || !game.price_currency) return "Check the Steam price";
  return new Intl.NumberFormat("en-US", { style: "currency", currency: game.price_currency })
    .format(game.price_minor_units / 100);
}

async function logEvent(requestId, event, itemId) {
  try {
    const payload = { request_id: requestId, event, item_ids: [itemId] };
    if (event !== "impression") await api("/feedback", { ...payload, event: "impression" }, true);
    await api("/feedback", payload, true);
    if (event === "click") {
      void api("/api/user/session").then(({user}) => {
        if (user && user.id === activeUser?.id) {
          activeUser = user;
          showProfile();
        }
      }).catch(() => {});
    }
    return true;
  } catch (error) {
    pageError("Could not record feedback. " + error.message);
    return false;
  }
}

function card(row, requestId) {
  const game = row.game;
  const article = element("article", null, "card");
  article.dataset.itemId = row.item_id;
  const cover = element("div", null, "cover");
  const image = element("img");
  image.src = game.image_url;
  image.alt = "";
  image.loading = "lazy";
  image.addEventListener("error", () => image.remove(), { once: true });
  cover.append(image, element("span", "#" + row.rank));

  const body = element("div", null, "card-body");
  const title = element("div");
  title.append(element("h3", game.title), element("p", price(game), "card-meta"));
  body.append(title, element("p", row.explanation, "reason"));
  if (row.matched_tags?.length || row.matched_aspects.length) {
    const chips = element("div", null, "badges");
    for (const tag of row.matched_tags ?? []) chips.append(element("span", tag.label, "badge tag"));
    for (const match of row.matched_aspects.slice(0, 3)) {
      chips.append(element("span", match.label + " · " + number.format(match.support) + " reviews", "badge"));
    }
    body.append(chips);
  }
  const details = element("details");
  details.append(element("summary", "Score and review evidence · " + scoringView.format(row.score)));
  details.append(scoringView.breakdown(row.scoring));
  if (row.matched_aspects.length) {
    const list = element("ul", null, "aspect-evidence");
    for (const match of row.matched_aspects) {
      list.append(element("li", match.label + " · " + number.format(match.support)
        + " reviews · sentiment " + decimal.format(match.quality)
        + " · priority weight " + number.format(match.priority_weight)));
    }
    details.append(element("h4", "Review aspects used in reranking"), list);
  }
  const evidence = element("div", null, "review-evidence");
  details.append(evidence);
  details.addEventListener("toggle", () => {
    if (details.open) {
      void logEvent(requestId, "detail", row.item_id);
      void loadEvidence(row, evidence);
    }
  });
  body.append(details);

  const footer = element("footer");
  const link = element("a", "Open on Steam ↗");
  link.href = game.store_url;
  link.target = "_blank";
  link.rel = "noopener noreferrer";
  link.addEventListener("click", () => { void logEvent(requestId, "click", row.item_id); });
  const dismiss = element("button", "Dismiss", "quiet");
  dismiss.type = "button";
  dismiss.addEventListener("click", async () => {
    dismiss.disabled = true;
    if (await logEvent(requestId, "dismiss", row.item_id)) article.remove();
    else dismiss.disabled = false;
  });
  footer.append(link, dismiss);
  body.append(footer);
  article.append(cover, body);
  return article;
}

async function loadEvidence(row, panel) {
  if (panel.dataset.state) return;
  panel.dataset.state = "loading";
  panel.textContent = "Loading review excerpts…";
  const params = new URLSearchParams();
  row.matched_aspects.slice(0, 3).forEach(aspect => params.append("aspect", aspect.aspect));
  try {
    const data = await api("/api/user/games/" + encodeURIComponent(row.item_id) + "/aspects?" + params);
    const names = {POS: "Positive", NEG: "Negative", NEU: "Neutral"};
    panel.replaceChildren(element("h4", "Steam review excerpts"),
      element("p", number.format(data.reviews_processed) + " reviews analyzed with ABSA", "muted"));
    for (const aspect of data.aspects) {
      const section = element("section");
      section.append(element("h4", aspect.label), element("p",
        ["POS", "NEU", "NEG"].map(key => names[key] + " " + number.format(aspect.counts[key] ?? 0)).join(" · "), "muted"));
      for (const example of aspect.examples) {
        const quote = element("blockquote");
        quote.append(element("strong", names[example.sentiment]), document.createTextNode(example.text));
        section.append(quote);
      }
      panel.append(section);
    }
    if (!data.aspects.length) panel.append(element("p", "No review excerpts are available for this aspect.", "muted"));
    panel.dataset.state = "loaded";
  } catch {
    panel.textContent = "Could not load review excerpts. Reopen this panel to retry.";
    delete panel.dataset.state;
  }
}

function renderQueryIntent(intent) {
  const panel = byId("query-interpretation");
  panel.hidden = !intent || intent.status === "unrecognized";
  if (panel.hidden) return;

  const labels = [
    ...(intent.query_model ? [["Multilingual E5 query match", "badge filter"]] : []),
    ...intent.required_tag_groups.map((group) => ["Require " + group.map((tag) => tag.label).join(" or "), "badge filter"]),
    ...intent.excluded_tags.map((tag) => ["Exclude " + tag.label, "badge filter"]),
    ...intent.aspects.filter((aspect) => aspect.available).map((aspect) => ["Prefer " + aspect.label, "badge"]),
    ...intent.preferred_tags.map((tag) => ["Prefer " + tag.label, "badge"]),
  ];
  byId("query-criteria").replaceChildren(...labels.map(([text, style]) => element("span", text, style)));
  const notes = [];
  if (intent.query_model) {
    notes.push(`${intent.query_model.id} compares the request with Steam catalog text.`);
  } else {
    notes.push("Using the recognized tags and aspects because the query model is unavailable.");
  }
  if (intent.preferred_tags_missing_in_results.length) {
    const names = intent.preferred_tags_missing_in_results.map((tag) => tag.label.toLowerCase());
    notes.push("The results have limited evidence for " + names.join(", ") + ".");
  }
  const missingAspects = intent.aspects.filter((aspect) => !aspect.available).map((aspect) => aspect.label);
  if (missingAspects.length) notes.push("The review dataset has no ABSA data for " + missingAspects.join(", ") + ".");
  byId("query-note").textContent = notes.join(" ");
  byId("query-note").hidden = !notes.length;
}

async function recommend() {
  const version = ++requestNumber;
  byId("submit").disabled = true;
  byId("query-submit").disabled = true;
  pageError();
  byId("result-status").textContent = "Ranking games…";
  byId("result-status").hidden = false;
  try {
    const result = await api("/api/user/recommend", {
      liked_items: Array.from(seeds.keys()),
      aspects: Array.from(aspects),
      query: byId("query").value.trim(),
      top_k: Number(byId("top-k").value),
    });
    if (version !== requestNumber) return;
    byId("result-count").textContent = scoringView.summary(result.ranking);
    renderQueryIntent(result.query_intent);
    let message = "";
    if (result.query_intent?.status === "unrecognized") {
      message = "This request has no confident match in the catalog. Try adding a genre, a theme, or a review aspect.";
    } else if (!result.recommendations.length) {
      message = "No games in the catalog match these filters.";
    } else if (result.mode === "popularity") {
      message = "Ranked by the share of positive recommendations in the review sample.";
    }
    byId("result-status").textContent = message;
    byId("result-status").hidden = !byId("result-status").textContent;
    observer?.disconnect();
    const cards = result.recommendations.map((row) => card(row, result.request_id));
    byId("games").replaceChildren(...cards);
    const visibility = new IntersectionObserver((entries) => {
      for (const entry of entries) {
        if (entry.intersectionRatio >= 0.25) {
          visibility.unobserve(entry.target);
          void logEvent(result.request_id, "impression", entry.target.dataset.itemId);
        }
      }
    }, { threshold: 0.25 });
    observer = visibility;
    cards.forEach((node) => observer.observe(node));
  } catch (error) {
    if (version === requestNumber) {
      if (error.status === 401) showSession(null);
      pageError(error.message);
      byId("result-status").hidden = true;
    }
  } finally {
    if (version === requestNumber) {
      byId("submit").disabled = false;
      byId("query-submit").disabled = false;
    }
  }
}

async function evaluation() {
  const data = await api("/api/metrics");
  const report = data.evaluation;
  const stats = [
    ["Games in the catalog", data.games], ["Steam reviews", data.reviews],
    ["ABSA-processed reviews", data.absa_processed], ["Test users", report.test_users],
  ];
  byId("stats").replaceChildren(...stats.map(([label, count]) => {
    const box = element("div", null, "stat");
    box.append(element("strong", number.format(count)), element("span", label, "muted small"));
    return box;
  }));
  byId("evaluation-rows").replaceChildren(...report.results.map((row) => {
    const tr = element("tr");
    tr.append(element("td", row.id + " · " + row.label));
    for (const key of ["recall", "ndcg", "map", "coverage"]) {
      tr.append(element("td", row[key] == null ? "—" : decimal.format(row[key])));
    }
    return tr;
  }));
  byId("split").textContent = "Train / validation / test · "
    + [report.train_reviews, report.validation_reviews, report.test_reviews].map(number.format).join(" / ")
    + " reviews · split dates " + [report.train_end_exclusive, report.validation_end_exclusive]
      .map((value) => dates.format(new Date(value))).join(" and ");
  byId("evaluation-note").textContent = "The target is a held-out game the user recommended. "
    + "Evaluation uses the catalog at crawl time. Measuring ABSA Macro-F1 requires human labels.";
  const values = [
    ["Recent requests sampled", data.sample_requests],
    ["API ranking latency P50 (ms)", data.latency_ms.p50],
    ["API ranking latency P95 (ms)", data.latency_ms.p95],
    ["Requests with no results", data.empty_results],
    ["Impression", data.feedback.impression ?? 0],
    ["Steam clicks", data.feedback.click ?? 0],
    ["Dismiss", data.feedback.dismiss ?? 0],
  ];
  byId("serving-metrics").replaceChildren(...values.map(([label, value]) => {
    const row = element("div");
    row.append(element("dt", label), element("dd", value == null ? "—" : decimal.format(value)));
    return row;
  }));
  byId("versions").textContent = data.model_version + " · " + data.run_id + " · " + data.absa_model;
}

function route() {
  if (!activeUser) return;
  const showEvaluation = ["#evaluation", "#danh-gia"].includes(location.hash);
  byId("browse").hidden = showEvaluation;
  byId("evaluation").hidden = !showEvaluation;
  const active = byId(showEvaluation ? "evaluation-tab" : "browse-tab");
  const inactive = byId(showEvaluation ? "browse-tab" : "evaluation-tab");
  active.setAttribute("aria-current", "page");
  inactive.removeAttribute("aria-current");
  if (showEvaluation) evaluation().catch((error) => pageError(error.message));
}

async function start() {
  const [catalog, session] = await Promise.all([api("/api/catalog"), api("/api/user/session")]);
  for (const game of catalog.games) byId("game-picker").add(new Option(game.title, game.item_id));
  for (const { id, label } of catalog.aspects) {
    const chip = element("button", label, "chip");
    chip.type = "button";
    chip.setAttribute("aria-pressed", "false");
    chip.addEventListener("click", () => {
      if (aspects.has(id)) aspects.delete(id);
      else if (aspects.size < 6) aspects.add(id);
      chip.setAttribute("aria-pressed", String(aspects.has(id)));
    });
    byId("aspects").append(chip);
  }
  byId("game-picker").addEventListener("change", (event) => {
    const select = event.target;
    if (select.value && seeds.size < 5) seeds.set(select.value, select.selectedOptions[0].text);
    select.value = "";
    renderSeeds();
  });
  byId("preferences").addEventListener("submit", (event) => { event.preventDefault(); void recommend(); });
  byId("query-examples").addEventListener("click", (event) => {
    const example = event.target.closest("[data-query]");
    if (example) {
      byId("query").value = example.dataset.query;
      byId("preferences").requestSubmit();
    }
  });
  byId("clear-preferences").addEventListener("click", () => { pageError(); clearSearch(); });
  byId("user-login-form").addEventListener("submit", async (event) => {
    event.preventDefault();
    const button = byId("user-login-button");
    button.disabled = true;
    pageError();
    try {
      const {user} = await api("/api/user/login", {user_id: Number(byId("user-id").value)});
      showSession(user);
      byId("query").focus();
    } catch (error) {
      pageError(error.message);
    } finally {
      button.disabled = false;
    }
  });
  byId("user-logout").addEventListener("click", async () => {
    try {
      await api("/api/user/logout", {});
      pageError();
      showSession(null);
      byId("user-id").focus();
    } catch (error) {
      pageError(error.message);
    }
  });
  window.addEventListener("hashchange", route);
  showSession(session.user);
}

start().catch((error) => {
  pageError(error.message);
  byId("result-status").hidden = true;
});
