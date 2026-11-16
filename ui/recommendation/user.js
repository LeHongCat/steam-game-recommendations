const $ = (id) => document.getElementById(id);
const number = new Intl.NumberFormat("en-US");
const state = {
  user: null,
  catalog: null,
  titles: new Map(),
  liked: new Map(),
  aspects: new Set(),
  k: 6,
  query: "",
  rows: new Map(),
  requestId: null,
  version: 0,
  observer: null,
};
const SIGNALS = {
  popularity: ["Steam review popularity", "#7aa6c2"],
  cf: ["Players with similar taste", "#9be09a"],
  content: ["Similar tags and description", "#4fb3a6"],
  absa: ["Aspects you care about", "#e8c873"],
};
const STEPS = {
  query_aspects: "Aspects in your request",
  priority_aspects: "Aspects you usually mention",
  fast_pace: "Fast-paced evidence",
  semantic_query: "Match with your description",
};
const TONE = {POS: ["Positive", "pos", "var(--positive)"], NEU: ["Neutral", "neu", "var(--mixed)"], NEG: ["Negative", "neg", "var(--negative)"]};

function el(tag, text, className) {
  const node = document.createElement(tag);
  if (text != null) node.textContent = text;
  if (className) node.className = className;
  return node;
}

async function api(path, body, keepalive = false) {
  const options = body
    ? {method: "POST", headers: {"Content-Type": "application/json"}, body: JSON.stringify(body), keepalive}
    : {cache: "no-store"};
  const response = await fetch(path, options);
  const value = await response.json().catch(() => ({}));
  if (!response.ok) {
    const error = new Error(typeof value.detail === "string" ? value.detail : "The request failed.");
    error.status = response.status;
    throw error;
  }
  return value;
}

function showError(message = "") {
  $("error").textContent = message;
  $("error").hidden = !message;
}

const tone = (quality) => (1 + quality) / 2;
const toneColor = (value) => value >= 0.6 ? "var(--positive)" : value >= 0.45 ? "var(--mixed)" : "var(--negative)";
const toneWord = (value) => value >= 0.6 ? "praise" : value >= 0.45 ? "are mixed on" : "criticize";

function price(game) {
  if (game.is_free) return "Free to play";
  if (game.price_minor_units == null || !game.price_currency) return "Price on Steam";
  return new Intl.NumberFormat("en-US", {style: "currency", currency: game.price_currency})
    .format(game.price_minor_units / 100);
}

/* ---------- Plain-language explanation built from the API's structured scoring ---------- */

function reasons(row) {
  const list = [];
  const semantic = row.scoring.adjustments.find((step) => step.kind === "semantic_query");
  if (semantic) {
    if (semantic.relevance >= 0.5) list.push("Closely matches what you described");
    else if (semantic.relevance > 0) list.push("Partly matches what you described");
  }
  if (row.matched_tags.length) list.push("Tagged " + row.matched_tags.map((tag) => tag.label).join(", ") + " on Steam");
  const praised = row.matched_aspects.filter((aspect) => tone(aspect.quality) >= 0.6);
  if (praised.length) {
    list.push("Reviewers praise its " + praised.slice(0, 2).map((aspect) => aspect.label.toLowerCase()).join(" and "));
  }
  if (row.pace_review_support) list.push(`${row.pace_review_support} positive reviews mention fast pacing`);
  if (Math.max(row.signals.cf ?? 0, row.signals.content ?? 0) >= 0.5) list.push("Similar to games you like");
  if (!list.length) list.push(`${Math.round(row.signals.popularity * 100)}% of sampled Steam reviews recommend it`);
  return list;
}

/* ---------- Session and profiles ---------- */

function renderProfiles() {
  $("profiles").replaceChildren(...state.catalog.profiles.map((profile) => {
    const id = Number(profile.id.replace("demo-", ""));
    const button = el("button", null, "profile");
    button.type = "button";
    const list = el("ul");
    profile.liked_games.slice(0, 4).forEach((title) => list.append(el("li", title)));
    button.append(el("small", "Steam reviewer"), el("strong", "User " + id), list,
      el("span", "Use this profile →", "go"));
    button.addEventListener("click", () => signIn(id, button));
    return button;
  }));
}

async function signIn(id, button) {
  button.disabled = true;
  showError();
  try {
    const {user} = await api("/api/user/login", {user_id: id});
    showSession(user);
  } catch (error) {
    showError(error.message);
  } finally {
    button.disabled = false;
  }
}

function showSession(user) {
  state.user = user;
  $("login").hidden = Boolean(user);
  $("app").hidden = !user;
  $("session").hidden = !user;
  if (!user) return;
  $("session-label").textContent = "User " + user.id;
  resetFilters(false);
  $("query").value = "";
  void search("");
}

/* ---------- Refine controls ---------- */

function renderLiked() {
  $("liked").replaceChildren(...Array.from(state.liked, ([id, title]) => {
    const pill = el("button", title, "pill removable");
    pill.type = "button";
    pill.setAttribute("aria-label", "Remove " + title);
    pill.addEventListener("click", () => { state.liked.delete(id); renderLiked(); rerun(); });
    return pill;
  }));
  $("liked-count").textContent = state.liked.size;
  $("liked-count").hidden = !state.liked.size;
  $("liked-input").disabled = state.liked.size >= 5;
  updateReset();
}

function renderAspects() {
  for (const pill of $("aspects").children) pill.setAttribute("aria-pressed", String(state.aspects.has(pill.dataset.id)));
  $("aspect-count").textContent = state.aspects.size;
  $("aspect-count").hidden = !state.aspects.size;
  updateReset();
}

function updateReset() {
  $("reset").hidden = !(state.liked.size || state.aspects.size || state.k !== 6 || state.query);
}

function resetFilters(render = true) {
  state.liked.clear();
  state.aspects.clear();
  state.k = 6;
  state.query = "";
  for (const button of document.querySelectorAll("[data-k]")) button.setAttribute("aria-pressed", String(button.dataset.k === "6"));
  renderLiked();
  renderAspects();
  if (render) { $("query").value = ""; void search(""); }
}

function togglePanel(button, panel) {
  const open = button.getAttribute("aria-expanded") !== "true";
  for (const [other, otherPanel] of [["toggle-liked", "liked-panel"], ["toggle-aspects", "aspect-panel"]]) {
    $(other).setAttribute("aria-expanded", "false");
    $(otherPanel).hidden = true;
  }
  button.setAttribute("aria-expanded", String(open));
  $(panel).hidden = !open;
  if (open && panel === "liked-panel") $("liked-input").focus();
}

let rerunTimer;
function rerun() {
  clearTimeout(rerunTimer);
  rerunTimer = setTimeout(() => search(state.query), 250);
}

/* ---------- Search and results ---------- */

async function search(query) {
  const version = ++state.version;
  state.query = query.trim();
  updateReset();
  showError();
  const submit = document.querySelector("#search .primary");
  submit.disabled = true;
  $("grid").classList.add("loading");
  try {
    const result = await api("/api/user/recommend", {
      query: state.query,
      liked_items: Array.from(state.liked.keys()),
      aspects: Array.from(state.aspects),
      top_k: state.k,
    });
    if (version === state.version) render(result);
  } catch (error) {
    if (version !== state.version) return;
    if (error.status === 401) showSession(null);
    showError(error.message);
  } finally {
    if (version === state.version) {
      submit.disabled = false;
      $("grid").classList.remove("loading");
    }
  }
}

function heading(result) {
  if (state.query) return [`Results for “${state.query}”`, ""];
  const picks = state.liked.size || state.aspects.size;
  const liked = state.user.liked_games.slice(0, 3).join(", ");
  return [
    picks ? "Picked from your selections" : "Picked for you",
    liked ? `Based on User ${state.user.id}’s positive Steam reviews of ${liked}.` : "Ranked by Steam review popularity.",
  ];
}

function renderIntent(intent, rows) {
  const box = $("understood");
  const notes = [];
  box.replaceChildren();
  box.hidden = true;
  if (!intent) return notes;
  const chips = [];
  for (const group of intent.required_tag_groups) chips.push([group.map((tag) => tag.label).join(" or "), "must"]);
  for (const tag of intent.excluded_tags) chips.push(["No " + tag.label, "not"]);
  for (const tag of intent.preferred_tags) chips.push([tag.label, ""]);
  for (const aspect of intent.aspects.filter((item) => item.available)) chips.push([aspect.label, ""]);
  if (intent.query_model) chips.push(["Description match · E5", ""]);
  if (chips.length) {
    box.append(el("span", "We looked for"));
    chips.forEach(([text, kind]) => box.append(el("span", text, "criterion " + kind)));
    box.hidden = false;
  }
  if (intent.status === "approximate") {
    notes.push("No game clearly matches this description. These are the closest games by description, adjusted for your profile.");
  } else if (intent.status === "unrecognized") {
    notes.push("The request could not be interpreted (the query model is not installed), so these picks come from your profile. Try a genre such as FPS, RPG or racing.");
  }
  if (rows.length && intent.preferred_tags_missing_in_results.length) {
    notes.push("Little evidence for " + intent.preferred_tags_missing_in_results.map((tag) => tag.label.toLowerCase()).join(", ") + " among these games.");
  }
  const missing = intent.aspects.filter((aspect) => !aspect.available).map((aspect) => aspect.label.toLowerCase());
  if (missing.length) notes.push("Reviews in this sample say too little about " + missing.join(", ") + " to rank by it.");
  return notes;
}

function render(result) {
  const [title, sub] = heading(result);
  $("results-title").textContent = title;
  $("results-sub").textContent = sub;
  $("results-sub").hidden = !sub;
  const {returned, eligible_candidates: eligible} = result.ranking;
  $("meta").textContent = returned ? `Top ${returned} of ${number.format(eligible)} eligible games · ${Math.round(result.latency_ms)} ms` : "";
  const notes = renderIntent(result.query_intent, result.recommendations);
  $("notice").textContent = notes.join(" ");
  $("notice").hidden = !notes.length;

  state.requestId = result.request_id;
  state.rows = new Map(result.recommendations.map((row) => [row.item_id, row]));
  state.observer?.disconnect();
  if (!result.recommendations.length) {
    const empty = el("div", null, "empty");
    empty.append(el("p", "No game in the 50-game catalog meets every condition in this request."));
    const clear = el("button", "Clear the search", "secondary");
    clear.type = "button";
    clear.style.marginTop = "12px";
    clear.addEventListener("click", () => resetFilters());
    empty.append(clear);
    $("grid").replaceChildren(empty);
    return;
  }
  const cards = result.recommendations.map((row) => card(row, result.request_id));
  $("grid").replaceChildren(...cards);
  const requestId = result.request_id;
  state.observer = new IntersectionObserver((entries) => {
    for (const entry of entries) {
      if (entry.intersectionRatio >= 0.25) {
        state.observer.unobserve(entry.target);
        void logEvent(requestId, "impression", entry.target.dataset.itemId);
      }
    }
  }, {threshold: 0.25});
  cards.forEach((node) => state.observer.observe(node));
}

function cover(game, className) {
  const box = el("div", null, className);
  box.append(el("span", game.title, "cover-fallback"));
  const image = el("img");
  image.src = game.image_url;
  image.alt = "";
  image.loading = "lazy";
  image.addEventListener("error", () => image.remove(), {once: true});
  box.append(image);
  return box;
}

function meter(label, value, color, display) {
  const row = el("div", null, "signal");
  const bar = el("div", null, "signal-bar");
  const fill = el("span");
  fill.style.width = Math.round(Math.max(0, Math.min(1, value)) * 100) + "%";
  fill.style.background = color;
  bar.append(fill);
  row.append(el("span", label), bar, el("b", display));
  return row;
}

function card(row, requestId) {
  const game = row.game;
  const article = el("article", null, "card");
  article.dataset.itemId = row.item_id;
  const top = cover(game, "cover");
  top.append(el("span", "#" + row.rank, "rank"));
  const dismiss = el("button", "×", "dismiss");
  dismiss.type = "button";
  dismiss.title = "Not interested";
  dismiss.setAttribute("aria-label", "Not interested in " + game.title);
  dismiss.addEventListener("click", () => dismissGame(row.item_id, requestId));
  top.append(dismiss);

  const body = el("div", null, "card-body");
  const head = el("div", null, "card-title");
  head.append(el("h2", game.title), el("span", price(game), "price"));
  body.append(head, el("p", reasons(row).slice(0, 2).join(" · "), "reason"));

  if (row.matched_aspects.length) {
    const signals = el("div", null, "signals");
    for (const aspect of row.matched_aspects.slice(0, 3)) {
      const value = tone(aspect.quality);
      signals.append(meter(aspect.label, value, toneColor(value), Math.round(value * 100)));
    }
    signals.title = "Review sentiment per aspect, 0–100, from Phase 1 ABSA";
    body.append(signals);
  }
  if (row.matched_tags.length) {
    const tags = el("div", null, "tags");
    row.matched_tags.forEach((tag) => tags.append(el("span", tag.label, "tag")));
    body.append(tags);
  }

  const foot = el("div", null, "card-foot");
  const score = el("div", null, "score");
  const ring = el("span", String(Math.round(row.score * 100)), "score-ring");
  ring.style.setProperty("--value", Math.round(row.score * 100));
  score.append(ring, el("span", "score"));
  const why = el("button", "Why this game?", "why");
  why.type = "button";
  why.addEventListener("click", () => openDrawer(row.item_id));
  foot.append(score, why);
  body.append(foot);
  article.append(top, body);
  return article;
}

async function logEvent(requestId, event, itemId) {
  try {
    const payload = {request_id: requestId, event, item_ids: [itemId]};
    if (event !== "impression") await api("/feedback", {...payload, event: "impression"}, true);
    await api("/feedback", payload, true);
    return true;
  } catch (error) {
    showError("Could not record feedback. " + error.message);
    return false;
  }
}

async function dismissGame(itemId, requestId) {
  if (!(await logEvent(requestId, "dismiss", itemId))) return;
  document.querySelector(`.card[data-item-id="${CSS.escape(itemId)}"]`)?.remove();
  if ($("drawer").open) $("drawer").close();
}

/* ---------- Detail drawer ---------- */

function section(title, ...children) {
  const box = el("section", null, "drawer-section");
  if (title) box.append(el("h3", title));
  box.append(...children);
  return box;
}

function scoreParts(row) {
  const parts = row.scoring.components.filter((part) => part.weight > 0 && part.value != null);
  const total = parts.reduce((sum, part) => sum + part.contribution, 0) || 1;
  const stack = el("div", null, "stack");
  const legend = el("div", null, "legend");
  for (const part of parts) {
    const [label, color] = SIGNALS[part.signal] ?? [part.signal, "#888"];
    const piece = el("span");
    piece.style.width = (part.contribution / total) * 100 + "%";
    piece.style.background = color;
    piece.title = label;
    stack.append(piece);
    const line = el("div");
    const swatch = el("i");
    swatch.style.background = color;
    line.append(swatch, el("span", `${label} · weight ${Math.round(part.weight * 100)}%`), el("b", part.contribution.toFixed(3)));
    legend.append(line);
  }
  const steps = el("div", null, "steps");
  const base = el("div");
  base.append(el("span", "Base score"), el("b", row.scoring.base_score.toFixed(3)));
  steps.append(base);
  for (const step of row.scoring.adjustments) {
    const line = el("div");
    const delta = step.after - step.before;
    line.append(el("span", STEPS[step.kind] ?? step.kind), el("b", (delta >= 0 ? "+" : "−") + Math.abs(delta).toFixed(3)));
    steps.append(line);
  }
  const final = el("div");
  final.append(el("span", "Final score"), el("b", row.scoring.final_score.toFixed(3)));
  steps.append(final);
  const calc = el("details", null, "calc");
  calc.append(el("summary", "Show the exact calculation"), scoringView.breakdown(row.scoring));
  return [stack, legend, steps, calc];
}

async function loadReviews(row, container) {
  container.replaceChildren(el("p", "Loading review excerpts…", "muted small"));
  const params = new URLSearchParams();
  row.matched_aspects.slice(0, 3).forEach((aspect) => params.append("aspect", aspect.aspect));
  try {
    const data = await api(`/api/user/games/${encodeURIComponent(row.item_id)}/aspects?${params}`);
    container.replaceChildren(el("p", `${number.format(data.reviews_processed)} reviews analysed by aspect (Phase 1 ABSA).`, "muted small"));
    for (const aspect of data.aspects) {
      const box = el("div", null, "aspect");
      const head = el("div", null, "aspect-head");
      const total = ["POS", "NEU", "NEG"].reduce((sum, key) => sum + (aspect.counts[key] ?? 0), 0) || 1;
      head.append(el("strong", aspect.label), el("span", `${number.format(aspect.support)} reviews · ${Math.round(100 * (aspect.counts.POS ?? 0) / total)}% positive`));
      const split = el("div", null, "split");
      for (const key of ["POS", "NEU", "NEG"]) {
        const piece = el("span");
        piece.style.width = (100 * (aspect.counts[key] ?? 0) / total) + "%";
        piece.style.background = TONE[key][2];
        piece.title = `${TONE[key][0]} ${aspect.counts[key] ?? 0}`;
        split.append(piece);
      }
      box.append(head, split);
      for (const example of aspect.examples) {
        box.append(el("blockquote", "“" + example.text + "”", "quote " + TONE[example.sentiment][1]));
      }
      container.append(box);
    }
    if (!data.aspects.length) container.append(el("p", "No review excerpts are available for this game.", "muted small"));
  } catch {
    container.replaceChildren(el("p", "Could not load review excerpts.", "muted small"));
  }
}

function openDrawer(itemId) {
  const row = state.rows.get(itemId);
  if (!row) return;
  const game = row.game;
  const requestId = state.requestId;
  void logEvent(requestId, "detail", itemId);

  const art = el("div", null, "drawer-cover");
  art.style.backgroundImage = `url("${game.image_url}")`;
  const title = el("h2", game.title);
  title.id = "drawer-title";
  const actions = el("div", null, "drawer-actions");
  const steam = el("a", "Open on Steam ↗", "primary");
  steam.href = game.store_url;
  steam.target = "_blank";
  steam.rel = "noopener noreferrer";
  steam.addEventListener("click", () => { void logEvent(requestId, "click", itemId); });
  const skip = el("button", "Not interested", "secondary");
  skip.type = "button";
  skip.addEventListener("click", () => dismissGame(itemId, requestId));
  actions.append(steam, skip);

  const list = el("ul", null, "reasons");
  reasons(row).forEach((text) => list.append(el("li", text)));
  const reviews = el("div");
  reviews.style.display = "grid";
  reviews.style.gap = "10px";

  $("drawer-content").replaceChildren(
    art,
    section(null, title, el("p", `${price(game)} · rank #${row.rank} · score ${Math.round(row.score * 100)}`, "muted small"),
      el("p", game.description, "muted small"), actions),
    section("Why it’s here", list),
    section("How the score is built", ...scoreParts(row)),
    section("What reviewers say", reviews),
  );
  $("drawer").showModal();
  void loadReviews(row, reviews);
}

/* ---------- Start ---------- */

async function start() {
  const [catalog, session] = await Promise.all([api("/api/catalog"), api("/api/user/session")]);
  state.catalog = catalog;
  for (const game of catalog.games) {
    state.titles.set(game.title.toLowerCase(), game.item_id);
    $("catalog-titles").append(new Option(game.title));
  }
  for (const {id, label} of catalog.aspects) {
    const pill = el("button", label, "pill");
    pill.type = "button";
    pill.dataset.id = id;
    pill.addEventListener("click", () => {
      if (state.aspects.has(id)) state.aspects.delete(id);
      else if (state.aspects.size < 6) state.aspects.add(id);
      renderAspects();
      rerun();
    });
    $("aspects").append(pill);
  }
  renderProfiles();

  $("search").addEventListener("submit", (event) => { event.preventDefault(); void search($("query").value); });
  $("examples").addEventListener("click", (event) => {
    const example = event.target.closest("[data-query]");
    if (!example) return;
    $("query").value = example.dataset.query;
    void search(example.dataset.query);
  });
  $("toggle-liked").addEventListener("click", (event) => togglePanel(event.currentTarget, "liked-panel"));
  $("toggle-aspects").addEventListener("click", (event) => togglePanel(event.currentTarget, "aspect-panel"));
  $("liked-input").addEventListener("change", (event) => {
    const id = state.titles.get(event.target.value.trim().toLowerCase());
    if (id && state.liked.size < 5) {
      state.liked.set(id, state.catalog.games.find((game) => game.item_id === id).title);
      event.target.value = "";
      renderLiked();
      rerun();
    }
  });
  for (const button of document.querySelectorAll("[data-k]")) {
    button.addEventListener("click", () => {
      state.k = Number(button.dataset.k);
      for (const other of document.querySelectorAll("[data-k]")) other.setAttribute("aria-pressed", String(other === button));
      updateReset();
      rerun();
    });
  }
  $("reset").addEventListener("click", () => resetFilters());
  $("switch-user").addEventListener("click", async () => {
    try { await api("/api/user/logout", {}); } catch { /* the session may already be gone */ }
    showSession(null);
  });
  $("drawer-close").addEventListener("click", () => $("drawer").close());
  $("drawer").addEventListener("click", (event) => { if (event.target === $("drawer")) $("drawer").close(); });
  showSession(session.user);
}

start().catch((error) => showError(error.message));
