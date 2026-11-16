const scoringView = (() => {
  const decimal = new Intl.NumberFormat("en-US", {minimumFractionDigits: 4, maximumFractionDigits: 4});
  const percent = new Intl.NumberFormat("en-US", {style: "percent", maximumFractionDigits: 1});
  const labels = {
    popularity: "Steam review popularity",
    cf: "Collaborative filtering",
    content: "Similarity to games you like",
    absa: "ABSA · aspects from your history",
  };
  const format = decimal.format;

  function node(tag, text, className) {
    const element = document.createElement(tag);
    if (text != null) element.textContent = text;
    if (className) element.className = className;
    return element;
  }

  function adjustment(step) {
    const {before, after, quality, coverage} = step;
    const score = format(before);
    let label, formula;
    if (step.kind === "query_aspects") {
      label = "Requested aspect adjustment";
      formula = `clamp₀₋₁(${score} + 0.25 × ${format(coverage)} × (${format(quality)} − 0.5))`;
    } else if (step.kind === "priority_aspects") {
      label = "Aspect preference adjustment";
      formula = `0.80 × ${score} + 0.20 × ${format(quality)} × ${format(coverage)}`;
    } else if (step.kind === "fast_pace") {
      label = step.source === "steam_tag" ? "Fast-paced Steam tag" : "Fast-paced review evidence";
      formula = `${score} + 0.20 × (1 − ${score}) × ${format(step.match)}`;
    } else if (step.kind === "semantic_query") {
      label = "Multilingual E5 query match";
      formula = `${percent.format(1 - step.weight)} × ${score} + ${percent.format(step.weight)} × ${format(step.relevance)}`;
    } else {
      return null;
    }
    const line = node("p", null, "score-step");
    line.append(node("strong", label), node("span", formula + " = " + format(after)));
    if (step.kind === "semantic_query") {
      line.append(node("span", `Relevance = clamp₀₋₁((${format(step.similarity)} − 0.70) / 0.25) = ${format(step.relevance)}`));
    }
    return line;
  }

  function breakdown(scoring) {
    const section = node("div", null, "score-breakdown");
    if (!scoring) {
      section.append(node("p", "No calculation was recorded for this search.", "muted small"));
      return section;
    }
    const table = node("table", null, "score-table");
    const caption = node("caption", "Base score = sum of value × effective weight");
    const header = table.createTHead().insertRow();
    for (const label of ["Signal", "Value", "Weight", "Contribution"]) {
      const cell = node("th", label);
      cell.scope = "col";
      header.append(cell);
    }
    const body = table.createTBody();
    for (const {signal, value, weight, contribution} of scoring.components) {
      const row = body.insertRow();
      const label = node("th", labels[signal] ?? signal);
      label.scope = "row";
      row.append(label, node("td", value == null ? "—" : format(value)),
        node("td", percent.format(weight)), node("td", format(contribution)));
    }
    table.prepend(caption);
    const tableScroll = node("div", null, "table-scroll");
    tableScroll.append(table);
    section.append(tableScroll, node("p", "Base score " + format(scoring.base_score), "score-total"));
    for (const step of scoring.adjustments) {
      const line = adjustment(step);
      if (line) section.append(line);
    }
    section.append(node("p", "Final score " + format(scoring.final_score), "score-total"));
    section.append(node("p", "Values are rounded for display.", "muted small"));
    return section;
  }

  function summary(ranking) {
    return `Top ${ranking.top_k} · ${ranking.eligible_candidates} eligible · ${ranking.returned} shown`;
  }

  return {breakdown, format, summary};
})();
