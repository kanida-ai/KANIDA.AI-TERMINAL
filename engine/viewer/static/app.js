const state = {
  market: "COMBINED",
  direction: "ALL",
  stockQuery: "",
  selectedStock: null,
};

async function fetchJson(path, options) {
  const response = await fetch(path, options);
  if (!response.ok) {
    throw new Error(`Request failed: ${response.status}`);
  }
  return response.json();
}

function pct(value) {
  const n = Number(value || 0);
  return `${(n * 100).toFixed(1)}%`;
}

function score(value) {
  return Number(value || 0).toFixed(3);
}

function marketPath(path) {
  const sep = path.includes("?") ? "&" : "?";
  return `${path}${sep}market=${encodeURIComponent(state.market)}`;
}

function statusBadge(status) {
  const cls = status === "complete" ? "good" : status === "partial" ? "warn" : "pending";
  return `<span class="badge ${cls}">${status}</span>`;
}

function renderSummary(summaryPayload) {
  const cards = document.getElementById("summaryCards");
  const summary = summaryPayload.summary;
  const status = summaryPayload.snapshot_status || {};
  cards.innerHTML = [
    ["Live Setups", summary.outcome_opportunities, "Outcome-first candidates"],
    ["Learned Patterns", summary.outcome_patterns, "Stock-specific behavior rules"],
    ["Snapshot", status.status || "missing", status.finished_at || status.started_at || "No run status"],
    ["Clusters", summary.clusters, "Sector-level agreement"],
  ].map(([label, value, foot]) => `
    <div class="metric-card">
      <div class="metric-label">${label}</div>
      <div class="metric-value">${value ?? 0}</div>
      <div class="metric-foot">${foot}</div>
    </div>
  `).join("");
}

function renderOutcomeOpportunities(rows) {
  const root = document.getElementById("outcomeOpportunitiesList");
  if (!rows.length) {
    root.innerHTML = `<div class="empty">No outcome-first opportunities in this view.</div>`;
    return;
  }
  root.innerHTML = rows.map((row, index) => {
    const directionClass = row.direction === "rally" ? "good" : "warn";
    const target = `${(Number(row.target_move) * 100).toFixed(0)}%`;
    const tierClass = row.tier === "high_conviction" ? "high" : row.tier === "medium" ? "medium" : "";
    const action = row.direction === "rally" ? "Upside setup" : "Downside risk";
    const trusted = row.display_probability || row.trusted_probability || row.target_probability;
    return `
      <div class="opportunity-card ${tierClass}">
        <div class="opportunity-head">
          <div class="rank">${index + 1}</div>
          <div>
            <div class="ticker-title">
              <span class="ticker">${row.ticker}</span>
              <span class="market">${row.market}</span>
              <span class="badge ${directionClass}">${action}</span>
              <span class="badge">${target} in ${row.forward_window}d</span>
            </div>
            <p class="subtle">${cleanPattern(row.behavior_pattern)}</p>
          </div>
          <div class="row">
            <span class="badge ${row.tier === "high_conviction" ? "good" : row.tier === "medium" ? "warn" : "pending"}">${row.tier}</span>
            <span class="badge">${score(row.decision_score)}</span>
          </div>
        </div>
        <div class="analyst-note">${analystNarrative(row)}</div>
        <div class="opportunity-grid">
          <div class="cell"><div class="cell-label">Trust-Adjusted</div><div class="cell-value ${row.direction === "rally" ? "good" : "bad"}">${pct(trusted)}</div></div>
          <div class="cell"><div class="cell-label">Raw Hit Rate</div><div class="cell-value">${pct(row.target_probability)}</div></div>
          <div class="cell"><div class="cell-label">Baseline</div><div class="cell-value">${pct(row.baseline_probability)}</div></div>
          <div class="cell"><div class="cell-label">Lift</div><div class="cell-value">${Number(row.lift || 0).toFixed(2)}x</div></div>
          <div class="cell"><div class="cell-label">Avg Follow-Through</div><div class="cell-value">${pct(row.avg_forward_return)}</div></div>
          <div class="cell"><div class="cell-label">Evidence</div><div class="cell-value">${row.occurrences} matches - ${row.credibility || "exploratory"}</div></div>
          <div class="cell"><div class="cell-label">Confidence Band</div><div class="cell-value">${pct(row.probability_ci_low)}-${pct(row.probability_ci_high)}</div></div>
          <div class="cell"><div class="cell-label">Similarity Today</div><div class="cell-value">${pct(row.similarity)}</div></div>
          <div class="cell"><div class="cell-label">Tape State</div><div class="cell-value">${plainState(row.current_behavior)}</div></div>
        </div>
      </div>
    `;
  }).join("");
}

function cleanPattern(pattern) {
  return String(pattern || "")
    .split(" + ")
    .map(part => part.split(":").pop().replaceAll("_", " "))
    .join(" - ");
}

function plainState(state) {
  const labels = {
    strong_up: "strong uptrend",
    up: "uptrend",
    sideways: "rangebound",
    strong_down: "sharp weakness",
    down: "downtrend",
    up_continuation: "continuation",
    up_pullback: "pullback in uptrend",
    down_continuation: "pressure persists",
    down_reversal_attempt: "reversal attempt",
    range_move: "range move",
    compressed: "compressed volatility",
    expanded: "expanded volatility",
    normal_vol: "normal volatility",
    above_stacked_ma: "above stacked averages",
    above_key_ma: "above key averages",
    below_stacked_ma: "below key averages",
    below_key_ma: "below key averages",
    inside_range: "inside range",
    breakout: "breakout",
    breakdown: "breakdown",
  };
  return String(state || "unknown")
    .split("|")
    .slice(0, 4)
    .map(part => labels[part] || part.replaceAll("_", " "))
    .join(" - ");
}

function analystNarrative(row) {
  const direction = row.direction === "rally" ? "upside" : "downside";
  const move = `${row.direction === "rally" ? "+" : "-"}${(Number(row.target_move) * 100).toFixed(0)}%`;
  const trusted = row.display_probability || row.trusted_probability || row.target_probability;
  const baseline = row.baseline_probability;
  const credibility = row.credibility || "exploratory";
  const sampleText = credibility === "thin_but_interesting"
    ? "The sample is still thin, so this belongs on the active watchlist rather than the automatic-action list."
    : credibility === "solid" || credibility === "strong"
      ? "The evidence base is broad enough to deserve priority monitoring."
      : "This is exploratory and should be treated as discovery evidence.";
  return `${row.ticker} is showing a familiar ${direction} setup. The current tape is ${plainState(row.current_behavior)}, and it resembles ${pct(row.similarity)} of a pattern that has preceded ${move} moves in this stock before. Kanida's trust-adjusted read is ${pct(trusted)} versus a normal baseline of ${pct(baseline)}, with ${Number(row.lift || 0).toFixed(2)}x lift and ${pct(row.avg_forward_return)} average follow-through. ${sampleText}`;
}

function renderVisionAudit(items) {
  const root = document.getElementById("visionAudit");
  root.innerHTML = items.map(item => `
    <div class="audit-item">
      <div class="row">
        <strong>${item.principle}. ${item.title}</strong>
        ${statusBadge(item.status)}
      </div>
      <p class="subtle">${item.detail}</p>
    </div>
  `).join("");
}

function renderOpportunities(rows) {
  const root = document.getElementById("opportunitiesList");
  if (!rows.length) {
    root.innerHTML = `<div class="empty">No opportunities in this view after ranking. Try another market or refresh the prototype.</div>`;
    return;
  }
  root.innerHTML = rows.map(row => `
    <div class="opportunity-card">
      <div class="row">
        <div>
          <strong>${row.market} ${row.ticker}</strong>
          <span class="badge">${row.timeframe} ${row.bias}</span>
        </div>
        <div class="row">
          <span class="badge ${row.conviction_tier === "high_conviction" ? "good" : row.conviction_tier === "medium" ? "warn" : "pending"}">${row.conviction_tier}</span>
          <span class="badge">${row.match_precision}</span>
        </div>
      </div>
      <p class="subtle">${signalContextNarrative(row)}</p>
      <div class="kv">
        <div><div class="kv-label">Win Rate</div><div class="kv-value">${pct(row.win_rate)}</div></div>
        <div><div class="kv-label">Avg 15d Dir Return</div><div class="kv-value">${pct(row.avg_directional_return)}</div></div>
        <div><div class="kv-label">Decision Score</div><div class="kv-value">${score(row.decision_score)}</div></div>
        <div><div class="kv-label">Occurrences</div><div class="kv-value">${row.occurrences}</div></div>
        <div><div class="kv-label">Signals Firing</div><div class="kv-value">${row.current_signals || "recent activity"}</div></div>
        <div><div class="kv-label">Setup Type</div><div class="kv-value">${row.setup_type || "-"}</div></div>
      </div>
    </div>
  `).join("");
}

function signalContextNarrative(row) {
  if (row.setup_summary && !String(row.setup_summary).includes("|")) {
    return row.setup_summary;
  }
  const side = row.bias === "bearish" ? "risk" : "upside";
  const trend = plainState(row.trend_state || "");
  const signals = cleanPattern(row.current_signals || row.pattern || "recent signal activity");
  return `${row.ticker} has a ${side} signal cluster active. Current context reads ${trend || "mixed tape"}, with ${signals}. Historically this playbook has produced a ${pct(row.win_rate)} win rate and ${pct(row.avg_directional_return)} average 15-day directional follow-through across ${row.occurrences || 0} prior matches.`;
}

function renderStocks(rows) {
  const root = document.getElementById("stockGrid");
  if (!rows.length) {
    root.innerHTML = `<div class="empty">No stock playbooks in this view.</div>`;
    return;
  }
  root.innerHTML = rows.map(row => {
    const selected = state.selectedStock && state.selectedStock.market === row.market && state.selectedStock.ticker === row.ticker && state.selectedStock.timeframe === row.timeframe && state.selectedStock.bias === row.bias;
    return `
      <div class="stock-card ${selected ? "selected" : ""}" data-market="${row.market}" data-ticker="${row.ticker}" data-timeframe="${row.timeframe}" data-bias="${row.bias}">
        <div class="row">
          <div>
            <strong>${row.market} ${row.ticker}</strong>
            <span class="badge">${row.timeframe} ${row.bias}</span>
          </div>
          <span class="badge good">${score(row.best_evidence_score)}</span>
        </div>
        <p class="subtle">${row.top_pattern}</p>
        <div class="kv">
          <div><div class="kv-label">Trusted Patterns</div><div class="kv-value">${row.trusted_pattern_count}</div></div>
          <div><div class="kv-label">Top Trend State</div><div class="kv-value">${row.top_trend_state}</div></div>
          <div><div class="kv-label">Avg Win Rate</div><div class="kv-value">${pct(row.avg_win_rate)}</div></div>
          <div><div class="kv-label">Avg Dir Return</div><div class="kv-value">${pct(row.avg_directional_return)}</div></div>
        </div>
      </div>
    `;
  }).join("");

  root.querySelectorAll(".stock-card").forEach(card => {
    card.addEventListener("click", () => {
      state.selectedStock = {
        market: card.dataset.market,
        ticker: card.dataset.ticker,
        timeframe: card.dataset.timeframe,
        bias: card.dataset.bias,
      };
      loadStockDetail();
      renderStocks(rows);
    });
  });
}

function renderStockDetail(detail) {
  const root = document.getElementById("stockDetail");
  if (!detail || !detail.trend_buckets || !detail.trend_buckets.length) {
    root.innerHTML = `<div class="empty">Select a stock playbook to inspect its trusted patterns.</div>`;
    return;
  }
  root.innerHTML = `
    <div>
      <strong>${detail.market} ${detail.ticker}</strong>
      <p class="subtle">${detail.headline || ""}</p>
    </div>
    ${detail.live_matches && detail.live_matches.length ? `
      <div class="opportunity-card">
        <strong>Live Match</strong>
        <p class="subtle">${detail.live_matches[0].setup_summary || `${detail.live_matches[0].pattern} is currently matching this stock playbook.`}</p>
      </div>
    ` : ""}
    ${detail.trend_buckets.map(bucket => `
      <div class="trend-block">
        <div class="row">
          <strong>${bucket.timeframe} ${bucket.bias}</strong>
          <span class="badge">${bucket.trend_state}</span>
        </div>
        <div class="pattern-grid">
          ${bucket.patterns.map(pattern => `
            <div class="pattern-card">
              <div class="row">
                <strong>${pattern.pattern}</strong>
                <span class="badge good">${pattern.roster_state}</span>
              </div>
              <div class="kv">
                <div><div class="kv-label">Occurrences</div><div class="kv-value">${pattern.occurrences}</div></div>
                <div><div class="kv-label">Win Rate</div><div class="kv-value">${pct(pattern.win_rate)}</div></div>
                <div><div class="kv-label">Avg Dir Return</div><div class="kv-value">${pct(pattern.avg_directional_return)}</div></div>
                <div><div class="kv-label">Evidence</div><div class="kv-value">${score(pattern.evidence_score)}</div></div>
              </div>
            </div>
          `).join("")}
        </div>
      </div>
    `).join("")}
  `;
}

async function loadStockDetail() {
  if (!state.selectedStock) {
    renderStockDetail(null);
    return;
  }
  const { market, ticker, timeframe, bias } = state.selectedStock;
  const detail = await fetchJson(`/api/stocks/${market}/${ticker}?timeframe=${encodeURIComponent(timeframe)}&bias=${encodeURIComponent(bias)}`);
  renderStockDetail(detail);
}

async function loadAll() {
  const [summary, audit, outcomeOpportunities, opportunities, stocks, insights] = await Promise.all([
    fetchJson(marketPath("/api/summary")),
    fetchJson("/api/vision-audit"),
    fetchJson(marketPath(`/api/outcome-opportunities?direction=${encodeURIComponent(state.direction)}`)),
    fetchJson(marketPath("/api/opportunities")),
    fetchJson(marketPath(`/api/stocks?q=${encodeURIComponent(state.stockQuery)}`)),
    fetchJson("/api/insights"),
  ]);
  renderSummary(summary);
  renderVisionAudit(audit);
  renderOutcomeOpportunities(outcomeOpportunities);
  renderOpportunities(opportunities);
  renderStocks(stocks);
  const status = summary.snapshot_status || {};
  const statusText = status.status ? `Snapshot: ${status.status} | ${status.finished_at || status.started_at || ""} | ${status.live_opportunities || 0} live opportunities\n\n` : "";
  document.getElementById("insightsPanel").textContent = statusText + (insights.outcome_markdown || insights.insights_markdown || "No insight text available.");
  if (!state.selectedStock && stocks.length) {
    state.selectedStock = {
      market: stocks[0].market,
      ticker: stocks[0].ticker,
      timeframe: stocks[0].timeframe,
      bias: stocks[0].bias,
    };
  }
  await loadStockDetail();
}

async function refreshPrototype() {
  const btn = document.getElementById("refreshBtn");
  btn.disabled = true;
  btn.textContent = "Refreshing...";
  try {
    const result = await fetchJson("/api/refresh", { method: "POST" });
    await loadAll();
    alert(result.ok ? "Prototype refresh complete." : "Refresh finished with errors. Check console/API response.");
    if (!result.ok) {
      console.error(result);
    }
  } catch (err) {
    console.error(err);
    alert("Refresh failed.");
  } finally {
    btn.disabled = false;
    btn.textContent = "Refresh Prototype";
  }
}

function bindEvents() {
  document.getElementById("marketFilter").addEventListener("change", async (event) => {
    state.market = event.target.value;
    state.selectedStock = null;
    await loadAll();
  });
  document.getElementById("directionFilter").addEventListener("change", async (event) => {
    state.direction = event.target.value;
    await loadAll();
  });
  document.getElementById("stockSearch").addEventListener("input", async (event) => {
    state.stockQuery = event.target.value || "";
    await loadAll();
  });
  document.getElementById("refreshBtn").addEventListener("click", refreshPrototype);
}

bindEvents();
loadAll().catch(err => {
  console.error(err);
  document.body.innerHTML = `<div class="app-shell"><div class="empty">Viewer failed to load.</div></div>`;
});
