const state = { overview: null, exoplanets: null, launches: null, apod: null, pipeline: null, habitability: null, lineage: null };
const $ = (selector, root = document) => root.querySelector(selector);
const $$ = (selector, root = document) => [...root.querySelectorAll(selector)];
const fmt = new Intl.NumberFormat("en-US");
const compact = new Intl.NumberFormat("en-US", { notation: "compact", maximumFractionDigits: 1 });

const escapeHTML = (value = "") => String(value).replace(/[&<>'"]/g, c => ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", "'": "&#39;", '"': "&quot;" }[c]));
const shortDate = value => value ? String(value).slice(0, 10) : "—";
const number = (value, digits = 1) => value == null ? "—" : Number(value).toLocaleString("en-US", { maximumFractionDigits: digits });
const debounce = (fn, delay = 300) => { let timer; return (...args) => { clearTimeout(timer); timer = setTimeout(() => fn(...args), delay); }; };

async function api(path, options) {
  const response = await fetch(path, options);
  if (!response.ok) {
    let detail = `Request failed (${response.status})`;
    try { detail = (await response.json()).detail || detail; } catch {}
    throw new Error(detail);
  }
  return response.json();
}

function toast(message, error = false) {
  const node = $("#toast");
  node.textContent = message;
  node.className = `toast show${error ? " error" : ""}`;
  clearTimeout(toast.timer);
  toast.timer = setTimeout(() => node.className = "toast", 3800);
}

function outcomeBadge(outcome) {
  const value = outcome || "UNKNOWN";
  return `<span class="outcome ${value.toLowerCase()}">${escapeHTML(value)}</span>`;
}

function tierBadge(tier) {
  const value = tier || "LOW";
  const cls = value === "HIGH" ? "success" : value === "MODERATE" ? "unknown" : "failure";
  return `<span class="outcome ${cls}">${escapeHTML(value)}</span>`;
}

function metricCard(label, value, sub, icon, color = "var(--cyan)") {
  return `<article class="metric-card" style="--accent:${color};--glow:color-mix(in srgb, ${color} 18%, transparent)">
    <div class="metric-top"><span>${escapeHTML(label)}</span><span class="metric-icon">${icon}</span></div>
    <strong>${escapeHTML(value)}</strong><small>${escapeHTML(sub)}</small>
  </article>`;
}

function lineChart(target, data) {
  if (!data.length) return target.innerHTML = `<div class="empty">No trend data available</div>`;
  const width = 800, height = 260, pad = { l: 42, r: 14, t: 16, b: 30 };
  const values = data.map(d => +d.count), max = Math.max(...values, 1), minYear = +data[0].year, maxYear = +data.at(-1).year;
  const x = year => pad.l + ((year - minYear) / Math.max(maxYear - minYear, 1)) * (width - pad.l - pad.r);
  const y = value => height - pad.b - (value / max) * (height - pad.t - pad.b);
  const points = data.map(d => `${x(+d.year)},${y(+d.count)}`);
  const path = points.map((point, i) => `${i ? "L" : "M"}${point}`).join(" ");
  const area = `${path} L${x(maxYear)},${height - pad.b} L${x(minYear)},${height - pad.b} Z`;
  const grids = [0, .25, .5, .75, 1].map(v => {
    const yy = y(max * v);
    return `<line class="grid-line" x1="${pad.l}" y1="${yy}" x2="${width-pad.r}" y2="${yy}"/><text class="axis-label" x="0" y="${yy+3}">${compact.format(max*v)}</text>`;
  }).join("");
  const tickEvery = Math.max(1, Math.floor(data.length / 7));
  const tickData = data.filter((_, i) => i % tickEvery === 0);
  if (maxYear - Number(tickData.at(-1).year) < 3) tickData[tickData.length - 1] = data.at(-1);
  else tickData.push(data.at(-1));
  const ticks = tickData.map(d => `<text class="axis-label" x="${x(+d.year)}" y="${height-7}" text-anchor="middle">${d.year}</text>`).join("");
  const dots = data.map(d => `<circle class="dot" cx="${x(+d.year)}" cy="${y(+d.count)}" r="2.8"><title>${d.year}: ${fmt.format(d.count)} discoveries</title></circle>`).join("");
  target.innerHTML = `<svg viewBox="0 0 ${width} ${height}" preserveAspectRatio="none"><defs><linearGradient id="area-gradient" x1="0" y1="0" x2="0" y2="1"><stop offset="0" stop-color="#3ee6d0" stop-opacity=".22"/><stop offset="1" stop-color="#3ee6d0" stop-opacity="0"/></linearGradient></defs>${grids}<path class="area" d="${area}"/><path class="line" d="${path}"/>${dots}${ticks}</svg>`;
}

function scatterChart(target, data) {
  if (!data.length) return target.innerHTML = `<div class="empty">No complete mass/radius measurements</div>`;
  const width = 700, height = 270, pad = { l: 44, r: 12, t: 12, b: 32 };
  const valid = data.filter(d => +d.mass > 0 && +d.radius > 0);
  const logsX = valid.map(d => Math.log10(+d.mass)), logsY = valid.map(d => Math.log10(+d.radius));
  const minX = Math.min(...logsX), maxX = Math.max(...logsX), minY = Math.min(...logsY), maxY = Math.max(...logsY);
  const x = v => pad.l + ((Math.log10(v)-minX)/Math.max(maxX-minX,.1))*(width-pad.l-pad.r);
  const y = v => height-pad.b-((Math.log10(v)-minY)/Math.max(maxY-minY,.1))*(height-pad.t-pad.b);
  const grids = [0,.25,.5,.75,1].map(v => `<line class="grid-line" x1="${pad.l}" y1="${pad.t+v*(height-pad.t-pad.b)}" x2="${width-pad.r}" y2="${pad.t+v*(height-pad.t-pad.b)}"/><line class="grid-line" x1="${pad.l+v*(width-pad.l-pad.r)}" y1="${pad.t}" x2="${pad.l+v*(width-pad.l-pad.r)}" y2="${height-pad.b}"/>`).join("");
  const dots = valid.map(d => `<circle class="scatter-dot" cx="${x(+d.mass)}" cy="${y(+d.radius)}" r="2.4"><title>${escapeHTML(d.pl_name)} · ${number(d.mass)} M⊕ · ${number(d.radius)} R⊕</title></circle>`).join("");
  target.innerHTML = `<svg viewBox="0 0 ${width} ${height}" preserveAspectRatio="none">${grids}${dots}<text class="axis-label" x="${width/2}" y="${height-5}" text-anchor="middle">MASS · EARTHS (LOG)</text><text class="axis-label" transform="translate(11 ${height/2}) rotate(-90)" text-anchor="middle">RADIUS · EARTHS (LOG)</text></svg>`;
}

function barChart(target, data, labelKey, valueKey, maxRows = 8) {
  const shown = data.slice(0, maxRows), max = Math.max(...shown.map(d => +d[valueKey]), 1);
  target.innerHTML = shown.map(d => `<div class="bar-row"><span title="${escapeHTML(d[labelKey])}">${escapeHTML(d[labelKey])}</span><div class="bar-track"><i style="width:${(+d[valueKey]/max)*100}%"></i></div><b>${fmt.format(d[valueKey])}</b></div>`).join("") || `<div class="empty">No data available</div>`;
}

async function loadOverview(force = false) {
  if (state.overview && !force) return;
  const data = state.overview = await api("/api/overview");
  $("#overview-metrics").innerHTML = [
    metricCard("Confirmed exoplanets", fmt.format(data.counts.exoplanets), "NASA default parameter sets", "◉", "var(--cyan)"),
    metricCard("SpaceX launches", fmt.format(data.counts.launches), "Historical + recent missions", "↗", "var(--amber)"),
    metricCard("APOD observations", fmt.format(data.counts.apod), "Official NASA daily archive", "✦", "var(--violet)"),
    metricCard("Mission success", `${data.launch_success_rate}%`, "Decided launch outcomes", "✓", "var(--green)"),
  ].join("");
  lineChart($("#discovery-chart"), data.discovery_trend);
  const maxMethod = Math.max(...data.methods.map(m => m.count), 1);
  $("#method-list").innerHTML = data.methods.map(m => `<div class="method-row"><div class="method-label"><span>${escapeHTML(m.method)}</span><b>${fmt.format(m.count)}</b></div><div class="method-bar"><i style="width:${m.count/maxMethod*100}%"></i></div></div>`).join("");
  $("#recent-launches").innerHTML = data.recent_launches.map(item => `<div class="launch-item"><i class="result-dot ${item.outcome === "SUCCESS" ? "" : "unknown"}"></i><div><strong>${escapeHTML(item.mission_name)}</strong><small>${escapeHTML(item.rocket_name || "Vehicle pending")}</small></div><time>${shortDate(item.launch_date)}</time></div>`).join("");
  $("#top-habitability").innerHTML = data.top_habitability.length
    ? data.top_habitability.map(item => `<div class="launch-item"><i class="result-dot ${item.tier === "HIGH" ? "" : "unknown"}"></i><div><strong>${escapeHTML(item.pl_name)}</strong><small>${escapeHTML(item.hostname || "Unknown host")}</small></div><time>${number(item.habitability_score,3)}</time></div>`).join("")
    : `<div class="empty">Run the pipeline to compute habitability scores</div>`;
  const feature = data.featured_apod[0];
  if (feature) {
    const image = feature.hdurl || feature.url;
    $("#featured-apod").style.backgroundImage = `url("${String(image).replace(/["\\]/g, "")}")`;
    $("#featured-apod").innerHTML = `<div class="apod-copy"><span class="eyebrow">TODAY'S WINDOW · ${shortDate(feature.apod_date)}</span><h2>${escapeHTML(feature.title)}</h2><p>${escapeHTML(feature.explanation)}</p><a href="${escapeHTML(image)}" target="_blank" rel="noreferrer">OPEN FULL RESOLUTION ↗</a></div>`;
  }
  const last = data.freshness.map(x => x.run_ts).filter(Boolean).sort().at(-1);
  $("#sidebar-sync").textContent = last ? `Synced ${String(last).slice(0,16)}` : "Cache ready";
}

async function loadExoplanets(force = false) {
  const params = new URLSearchParams({ q: $("#exo-search").value, method: $("#exo-method").value, year_min: $("#exo-year-min").value || 1990, year_max: $("#exo-year-max").value || 2200, limit: 300 });
  const data = state.exoplanets = await api(`/api/exoplanets?${params}`);
  if (!$("#exo-method").dataset.ready) {
    $("#exo-method").insertAdjacentHTML("beforeend", data.methods.map(m => `<option value="${escapeHTML(m.method)}">${escapeHTML(m.method)} · ${fmt.format(m.count)}</option>`).join(""));
    $("#exo-method").dataset.ready = "1";
  }
  $("#exo-result-count").textContent = `${fmt.format(data.total)} matching planets`;
  $("#exo-table").innerHTML = data.items.map(p => `<tr><td>${escapeHTML(p.pl_name)}</td><td>${escapeHTML(p.hostname || "—")}</td><td>${p.disc_year || "—"}</td><td>${escapeHTML(p.discoverymethod || "—")}</td><td>${number(p.pl_radius_earth,2)}</td><td>${number(p.pl_mass_earth,2)}</td><td>${number(p.sy_dist,1)}</td></tr>`).join("") || `<tr><td colspan="7"><div class="empty">No planets match these filters</div></td></tr>`;
  scatterChart($("#scatter-chart"), data.scatter);
  const complete = data.items.filter(p => p.pl_radius_earth != null && p.pl_mass_earth != null).length;
  const nearest = [...data.items].filter(p => p.sy_dist).sort((a,b) => a.sy_dist-b.sy_dist)[0];
  $("#exo-insights").innerHTML = `<span class="eyebrow">CURRENT SELECTION</span><h3>${fmt.format(data.total)} worlds</h3><p style="color:var(--muted);font-size:12px;line-height:1.7">Scientific measurements vary by discovery method. Empty fields reflect genuinely unavailable archive values.</p><div class="insight-grid"><div class="insight"><span>Rows displayed</span><strong>${fmt.format(data.items.length)}</strong></div><div class="insight"><span>Complete samples</span><strong>${fmt.format(complete)}</strong></div><div class="insight"><span>Nearest shown</span><strong>${nearest ? number(nearest.sy_dist,1)+" pc" : "—"}</strong></div><div class="insight"><span>Methods</span><strong>${fmt.format(data.methods.length)}</strong></div></div>`;
}

async function loadLaunches(force = false) {
  const params = new URLSearchParams({ q: $("#launch-search").value, outcome: $("#launch-outcome").value, limit: 400 });
  if ($("#launch-year").value) params.set("year", $("#launch-year").value);
  const data = state.launches = await api(`/api/launches?${params}`);
  const stats = Object.fromEntries(data.stats.map(x => [x.outcome, x.count]));
  const decided = (stats.SUCCESS || 0) + (stats.FAILURE || 0), rate = decided ? (stats.SUCCESS / decided * 100) : 0;
  $("#launch-metrics").innerHTML = [metricCard("Matching missions",fmt.format(data.total),"Current filters","↗","var(--amber)"),metricCard("Successful",fmt.format(stats.SUCCESS||0),"Reached mission outcome","✓","var(--green)"),metricCard("Failures",fmt.format(stats.FAILURE||0),"Recorded launch failures","×","var(--red)"),metricCard("Vehicles",fmt.format(data.rockets.length),"Distinct configurations","◇","var(--blue)")].join("");
  $("#launch-result-count").textContent = `${fmt.format(data.total)} missions`;
  $("#launch-table").innerHTML = data.items.map(x => `<tr><td>${x.flight_number ?? "—"}</td><td title="${escapeHTML(x.details || "")}">${escapeHTML(x.mission_name)}</td><td>${shortDate(x.launch_date)}</td><td>${escapeHTML(x.rocket_name || "—")}</td><td>${escapeHTML(x.launchpad_name || "—")}</td><td>${outcomeBadge(x.outcome)}</td></tr>`).join("") || `<tr><td colspan="6"><div class="empty">No launches match these filters</div></td></tr>`;
  barChart($("#rocket-chart"), data.rockets, "rocket", "count", 9);
  $("#launch-dial").innerHTML = `<div><div class="dial" style="--value:${rate*3.6}deg"><div class="dial-copy"><strong>${rate.toFixed(1)}%</strong><span>SUCCESS RATE</span></div></div><p>${fmt.format(decided)} missions with a decided outcome</p></div>`;
}

async function loadApod(force = false) {
  const params = new URLSearchParams({ q: $("#apod-search").value, media_type: $("#apod-media").value, limit: 90 });
  if ($("#apod-year").value) params.set("year", $("#apod-year").value);
  const data = state.apod = await api(`/api/apod?${params}`);
  $("#apod-result-count").textContent = `${fmt.format(data.total)} observations`;
  $("#apod-grid").innerHTML = data.items.map((item, i) => `<article class="apod-card" data-apod-index="${i}" tabindex="0">${item.media_type === "image" && (item.hdurl || item.url) ? `<img src="${escapeHTML(item.hdurl || item.url)}" alt="${escapeHTML(item.title)}" loading="lazy">` : `<div class="media-placeholder">✦</div>`}<div class="apod-card-copy"><time>${shortDate(item.apod_date)} · ${escapeHTML(item.media_type || "MEDIA")}</time><h3>${escapeHTML(item.title)}</h3></div></article>`).join("") || `<div class="empty">No observations match these filters</div>`;
  $$(".apod-card").forEach(card => {
    const open = () => openApod(data.items[+card.dataset.apodIndex]);
    card.addEventListener("click", open);
    card.addEventListener("keydown", e => { if (e.key === "Enter") open(); });
  });
}

function openApod(item) {
  const dialog = $("#apod-dialog"), image = item.hdurl || item.url;
  $("#dialog-content").innerHTML = `${item.media_type === "image" && image ? `<img class="dialog-image" src="${escapeHTML(image)}" alt="${escapeHTML(item.title)}">` : ""}<div class="dialog-copy"><time>${shortDate(item.apod_date)} · NASA APOD</time><h2>${escapeHTML(item.title)}</h2><p>${escapeHTML(item.explanation || "No description available.")}</p>${item.copyright ? `<small>Credit: ${escapeHTML(item.copyright)}</small>` : ""}</div>`;
  dialog.showModal();
}

const SOURCE_LABELS = { exoplanets: "Exoplanets", launches: "Launches", apod: "APOD" };
const SOURCE_COLORS = { exoplanets: "var(--cyan)", launches: "var(--amber)", apod: "var(--violet)" };
const GOLD_LABELS = { exoplanets: "Gold (habitability-scored)", launches: "Gold (per-year rollup)", apod: "Gold (per-month rollup)" };

function lineageFunnelRow(sourceKey, counts) {
  const label = SOURCE_LABELS[sourceKey];
  const color = SOURCE_COLORS[sourceKey];
  const stageVals = [counts.bronze, counts.silver, counts.gold, counts.sqlite];
  const max = Math.max(...stageVals.filter(v => v != null), 1);
  const stageNames = ["Bronze (raw)", "Silver (cleaned)", GOLD_LABELS[sourceKey], "SQLite (served)"];
  const bars = stageVals.map((v, i) => {
    const pct = v == null ? 0 : Math.max(4, (v / max) * 100);
    const display = v == null ? "—" : fmt.format(v);
    return `<div class="funnel-stage">
      <div class="funnel-track"><i style="width:${pct}%;--c:${color}"></i></div>
      <div class="funnel-label"><span>${stageNames[i]}</span><b>${display}</b></div>
    </div>`;
  }).join("");
  return `<div class="funnel-row">
    <div class="funnel-source"><i style="background:${color}"></i>${label}</div>
    <div class="funnel-stages">${bars}</div>
  </div>`;
}

async function loadLineage(force = false) {
  const data = state.lineage = await api("/api/pipeline/lineage");
  const gold = {
    exoplanets: data.gold.habitability_scored,
    launches: data.gold.launches_per_year_rows,
    apod: data.gold.apod_per_month_rows,
  };
  const rowsHtml = ["exoplanets", "launches", "apod"].map(source => lineageFunnelRow(source, {
    bronze: data.stages[source]?.bronze,
    silver: data.stages[source]?.silver,
    gold: gold[source],
    sqlite: data.sqlite[source],
  })).join("");
  $("#lineage-grid").innerHTML = rowsHtml;
  const latestTs = [
    data.stages.exoplanets?.silver_updated_at, data.stages.launches?.silver_updated_at,
    data.stages.apod?.silver_updated_at, data.gold.habitability_updated_at,
  ].filter(Boolean).sort().at(-1);
  $("#lineage-updated").textContent = latestTs ? `Last measured ${String(latestTs).slice(0,19).replace("T"," ")} UTC` : "No pipeline run yet";
}

async function loadPipeline(force = false) {
  if (state.pipeline && !force) return;
  const data = state.pipeline = await api("/api/pipeline");
  $("#quality-list").innerHTML = data.quality.map(x => `<div class="quality-row"><div><strong>${escapeHTML(x.name)}</strong><small>Observed ${fmt.format(x.value)} · threshold ${fmt.format(x.threshold)}</small></div><span class="check ${x.passed ? "" : "fail"}">${x.passed ? "✓" : "!"}</span></div>`).join("");
  const latest = [...data.metrics].filter((x,i,a) => a.findIndex(y => y.source === x.source) === i);
  $("#pipeline-metrics").innerHTML = latest.map(x => `<div class="run-row"><div><strong>${escapeHTML(x.source.toUpperCase())}</strong><small>${shortDate(x.run_ts)} · ${number(x.duration_seconds,1)} sec</small></div>${outcomeBadge(x.status)}</div>`).join("");
  $("#pipeline-table").innerHTML = data.metrics.map(x => `<tr><td>${escapeHTML(x.source)}</td><td>${escapeHTML(String(x.run_ts || "—").slice(0,19))}</td><td>${fmt.format(x.records_fetched||0)}</td><td>${fmt.format(x.records_written||0)}</td><td>${number(x.duration_seconds,2)} s</td><td>${outcomeBadge(x.status)}</td></tr>`).join("");
  $("#dag-runs-table").innerHTML = data.runs.map(x => `<tr><td>${escapeHTML(x.run_id || "—")}</td><td>${escapeHTML(String(x.execution_date || "—").slice(0,19))}</td><td>${outcomeBadge(x.status)}</td><td>${fmt.format(x.total_records_processed||0)}</td><td>${escapeHTML(x.notes || "—")}</td></tr>`).join("") || `<tr><td colspan="5"><div class="empty">No DAG runs recorded yet — trigger one above</div></td></tr>`;
  await loadLineage(force);
}

async function loadHabitability(force = false) {
  const params = new URLSearchParams({ q: $("#hab-search").value, tier: $("#hab-tier").value, limit: 200 });
  const data = state.habitability = await api(`/api/habitability?${params}`);
  const counts = Object.fromEntries(data.tier_counts.map(x => [x.tier, x.count]));
  $("#habitability-metrics").innerHTML = [
    metricCard("HIGH tier", fmt.format(counts.HIGH||0), "Top ~2% of scored candidates", "⬡", "var(--green)"),
    metricCard("MODERATE tier", fmt.format(counts.MODERATE||0), "Next ~13% of scored candidates", "⬡", "var(--amber)"),
    metricCard("LOW tier", fmt.format(counts.LOW||0), "Remaining scored candidates", "⬡", "var(--muted)"),
    metricCard("Scored total", fmt.format(data.total), "Planets with full measurements", "Σ", "var(--cyan)"),
  ].join("");
  $("#hab-result-count").textContent = `${fmt.format(data.total)} scored candidates`;
  $("#hab-table").innerHTML = data.items.map(p => `<tr><td>${escapeHTML(p.pl_name)}</td><td>${escapeHTML(p.hostname || "—")}</td><td>${number(p.pl_radius_earth,2)}</td><td>${number(p.pl_mass_earth,2)}</td><td>${number(p.pl_orbital_period_days,1)}</td><td>${p.in_habitable_zone_proxy ? "✓" : "—"}</td><td><b>${number(p.habitability_score,3)}</b></td><td>${tierBadge(p.tier)}</td></tr>`).join("") || `<tr><td colspan="8"><div class="empty">No candidates match these filters</div></td></tr>`;
  $("#hab-alerts").innerHTML = data.alerts.length
    ? data.alerts.map(a => `<div class="run-row"><div><strong>${escapeHTML(a.pl_name)}</strong><small>${escapeHTML(a.reason || "")} · ${shortDate(a.created_at)}</small></div>${tierBadge(a.tier)}</div>`).join("")
    : `<div class="empty">No new HIGH-tier alerts yet — run the pipeline to check for new candidates</div>`;
}

const PIPELINE_TASK_LABELS = [
  { id: "start", label: "Start" },
  { id: "check_source_availability", label: "Check source availability" },
  { id: "extract.extract_exoplanet_data", label: "Extract: NASA Exoplanet Archive" },
  { id: "extract.extract_nasa_apod_metadata", label: "Extract: NASA APOD" },
  { id: "extract.extract_spacex_launch_data", label: "Extract: Launch Library 2" },
  { id: "store_raw_data", label: "Write Bronze Parquet" },
  { id: "run_spark_transformations", label: "PySpark: Bronze → Silver → Gold + habitability scoring" },
  { id: "validate_transformed_data", label: "Validate Silver layer" },
  { id: "load_serving_layer", label: "Load Gold → SQLite serving layer" },
  { id: "run_analytics_quality_checks", label: "Run data quality checks" },
  { id: "raise_habitability_alerts", label: "Raise habitability alerts" },
  { id: "record_pipeline_metrics", label: "Record pipeline metrics" },
  { id: "end", label: "End" },
];

const STEP_ICON = { pending: "○", running: "◐", success: "✓", failed: "✕", skipped: "–", upstream_failed: "✕", scheduled: "◐", queued: "◐" };
const STEP_CLASS = { pending: "pending", running: "running", scheduled: "running", queued: "running", success: "success", failed: "failed", upstream_failed: "failed", skipped: "skipped" };
const runState = { runId: null, polling: false, timer: null };

// Graph layout mirroring the real Airflow DAG dependency structure:
// a linear spine, with a 3-way parallel fan-out/fan-in for the extract group.
const DAG_ROW_HEIGHT = 86;
const DAG_COL_WIDTH = 236;
const DAG_GRAPH = {
  width: DAG_COL_WIDTH * 5,
  height: DAG_ROW_HEIGHT * 11 + 40,
  nodes: [
    { id: "start",                               label: "Start",                        short: "start",          col: 2, row: 0 },
    { id: "check_source_availability",           label: "Check source availability",     short: "health check",   col: 2, row: 1 },
    { id: "extract.extract_exoplanet_data",      label: "Extract\nNASA Exoplanet Archive", short: "exoplanets",   col: 0, row: 2 },
    { id: "extract.extract_nasa_apod_metadata",  label: "Extract\nNASA APOD",            short: "apod",           col: 2, row: 2 },
    { id: "extract.extract_spacex_launch_data",  label: "Extract\nLaunch Library 2",     short: "launches",       col: 4, row: 2 },
    { id: "store_raw_data",                      label: "Write Bronze Parquet",          short: "bronze",         col: 2, row: 3 },
    { id: "run_spark_transformations",           label: "PySpark transforms\nSilver → Gold + scoring", short: "spark", col: 2, row: 4 },
    { id: "validate_transformed_data",           label: "Validate Silver layer",         short: "validate",       col: 2, row: 5 },
    { id: "load_serving_layer",                  label: "Load Gold → SQLite",            short: "load",           col: 2, row: 6 },
    { id: "run_analytics_quality_checks",        label: "Run quality checks",            short: "quality",        col: 2, row: 7 },
    { id: "raise_habitability_alerts",           label: "Raise habitability alerts",     short: "alerts",         col: 2, row: 8 },
    { id: "record_pipeline_metrics",             label: "Record pipeline metrics",       short: "metrics",        col: 2, row: 9 },
    { id: "end",                                  label: "End",                           short: "end",            col: 2, row: 10 },
  ],
  edges: [
    ["start", "check_source_availability"],
    ["check_source_availability", "extract.extract_exoplanet_data"],
    ["check_source_availability", "extract.extract_nasa_apod_metadata"],
    ["check_source_availability", "extract.extract_spacex_launch_data"],
    ["extract.extract_exoplanet_data", "store_raw_data"],
    ["extract.extract_nasa_apod_metadata", "store_raw_data"],
    ["extract.extract_spacex_launch_data", "store_raw_data"],
    ["store_raw_data", "run_spark_transformations"],
    ["run_spark_transformations", "validate_transformed_data"],
    ["validate_transformed_data", "load_serving_layer"],
    ["load_serving_layer", "run_analytics_quality_checks"],
    ["run_analytics_quality_checks", "raise_habitability_alerts"],
    ["raise_habitability_alerts", "record_pipeline_metrics"],
    ["record_pipeline_metrics", "end"],
  ],
};

function dagNodeXY(node) {
  return { x: DAG_COL_WIDTH * node.col + DAG_COL_WIDTH / 2, y: DAG_ROW_HEIGHT * node.row + 46 };
}

function renderDagGraph(stateById) {
  const { width, height, nodes, edges } = DAG_GRAPH;
  const byId = Object.fromEntries(nodes.map(n => [n.id, n]));

  const edgePaths = edges.map(([fromId, toId]) => {
    const from = byId[fromId], to = byId[toId];
    const p1 = dagNodeXY(from), p2 = dagNodeXY(to);
    const fromState = stateById[fromId] || "pending";
    const toState = stateById[toId] || "pending";
    const active = fromState === "success" && (toState === "success" || STEP_CLASS[toState] === "running");
    const midY = (p1.y + p2.y) / 2;
    const path = p1.x === p2.x
      ? `M${p1.x},${p1.y + 26} L${p2.x},${p2.y - 26}`
      : `M${p1.x},${p1.y + 26} C${p1.x},${midY} ${p2.x},${midY} ${p2.x},${p2.y - 26}`;
    return `<path class="dag-edge ${active ? "active" : ""}" d="${path}" />`;
  }).join("");

  const nodeShapes = nodes.map(n => {
    const state = stateById[n.id] || "pending";
    const cls = STEP_CLASS[state] || "pending";
    const { x, y } = dagNodeXY(n);
    const lines = n.label.split("\n");
    const text = lines.map((line, i) => `<tspan x="${x}" dy="${i === 0 ? 0 : 13}">${escapeHTML(line)}</tspan>`).join("");
    const icon = STEP_ICON[state] || "○";
    return `<g class="dag-node ${cls}" data-id="${n.id}">
      <rect x="${x - 86}" y="${y - 26}" width="172" height="52" rx="12"></rect>
      <text class="dag-node-icon" x="${x - 70}" y="${y + 4}">${icon}</text>
      <text class="dag-node-label" x="${x + 8}" y="${lines.length > 1 ? y - 2 : y + 4}">${text}</text>
    </g>`;
  }).join("");

  return `<svg viewBox="0 0 ${width} ${height}" preserveAspectRatio="xMidYMin meet">
    <g class="dag-edges">${edgePaths}</g>
    <g class="dag-nodes">${nodeShapes}</g>
  </svg>`;
}

function renderRunSteps(steps) {
  $("#run-steps").innerHTML = steps.map(s => {
    const cls = STEP_CLASS[s.state] || "pending";
    const icon = STEP_ICON[s.state] || "○";
    const timing = s.start_date && s.end_date
      ? `${(((new Date(s.end_date)) - (new Date(s.start_date))) / 1000).toFixed(1)}s`
      : (s.state === "running" || s.state === "scheduled" || s.state === "queued") ? "running…" : "";
    return `<div class="run-step ${cls}">
      <span class="run-step-icon">${icon}</span>
      <span class="run-step-label">${escapeHTML(s.label)}</span>
      <span class="run-step-timing">${escapeHTML(timing)}</span>
    </div>`;
  }).join("");

  const stateById = Object.fromEntries(steps.map(s => [s.id, s.state]));
  $("#dag-graph").innerHTML = renderDagGraph(stateById);
}

function setRunOverallPill(overall) {
  const pill = $("#run-overall-pill");
  const map = { idle: "IDLE", running: "RUNNING", success: "SUCCESS", failed: "FAILED" };
  pill.textContent = "";
  pill.innerHTML = `<i></i> ${map[overall] || overall.toUpperCase()}`;
  pill.className = `status-pill small ${overall === "failed" ? "pill-failed" : overall === "success" ? "pill-success" : overall === "running" ? "pill-running" : ""}`;
}

async function fetchAndShowRunStatus(showFreshResults) {
  if (!runState.runId) return;
  try {
    const data = await api(`/api/pipeline/run-status?run_id=${encodeURIComponent(runState.runId)}`);
    renderRunSteps(data.steps);
    setRunOverallPill(data.overall);
    runState.lastOverall = data.overall;
    $("#run-id-label").textContent = data.run_id;
    $("#run-log").textContent = data.log_tail || "(no output yet)";
    $("#run-log").scrollTop = $("#run-log").scrollHeight;

    const finished = data.overall !== "running" && !data.process_running;
    if (finished) {
      stopRunPolling();
      const button = $("#trigger-run-button");
      button.disabled = false; button.innerHTML = "<span>▶</span> Trigger pipeline run";
      if (data.overall === "success") {
        $("#run-results-panel").style.display = "";
        if (showFreshResults) {
          toast("Pipeline run complete — all tasks succeeded.");
          state.pipeline = null; state.overview = null; state.habitability = null; state.lineage = null;
        }
        try {
          const lineage = await api("/api/pipeline/lineage");
          $("#run-results-metrics").innerHTML = [
            metricCard("Exoplanets scored", fmt.format(lineage.gold.habitability_scored || 0), "Habitability gold layer", "⬡", "var(--cyan)"),
            metricCard("Launches rolled up", fmt.format(lineage.sqlite.launches || 0), "SQLite serving layer", "↗", "var(--amber)"),
            metricCard("APOD entries", fmt.format(lineage.sqlite.apod || 0), "SQLite serving layer", "✦", "var(--violet)"),
          ].join("");
        } catch {}
      } else {
        $("#run-results-panel").style.display = "none";
        if (showFreshResults) toast("Pipeline run failed — check the log tail below.", true);
      }
    } else {
      $("#run-results-panel").style.display = "none";
    }
  } catch (error) {
    stopRunPolling();
    toast(error.message, true);
    const button = $("#trigger-run-button");
    button.disabled = false; button.innerHTML = "<span>▶</span> Trigger pipeline run";
  }
}

function pollRunStatus() {
  return fetchAndShowRunStatus(true);
}

function startRunPolling(skipImmediateFetch) {
  if (runState.polling) return;
  runState.polling = true;
  if (!skipImmediateFetch) pollRunStatus();
  runState.timer = setInterval(pollRunStatus, 2000);
}

function stopRunPolling() {
  runState.polling = false;
  if (runState.timer) clearInterval(runState.timer);
  runState.timer = null;
}

async function loadRun() {
  if (runState.runId) {
    // A run was already triggered (in progress or just finished) — fetch its
    // real current state immediately instead of flashing a fake "pending"
    // list first.
    $("#run-id-label").textContent = runState.runId;
    await fetchAndShowRunStatus(false);
    if (!runState.polling && runState.lastOverall === "running") {
      startRunPolling(true);
    }
    return;
  }
  const steps = PIPELINE_TASK_LABELS.map(t => ({ id: t.id, label: t.label, state: "pending" }));
  renderRunSteps(steps);
  setRunOverallPill("idle");
}

const loaders = { overview: loadOverview, exoplanets: loadExoplanets, launches: loadLaunches, apod: loadApod, habitability: loadHabitability, pipeline: loadPipeline, run: loadRun };
async function switchView(name) {
  $$(".view").forEach(v => v.classList.toggle("active", v.id === `view-${name}`));
  $$(".nav-item").forEach(n => n.classList.toggle("active", n.dataset.view === name));
  $("#breadcrumb").textContent = name.toUpperCase();
  $("#sidebar").classList.remove("open");
  history.replaceState(null, "", `#${name}`);
  try { await loaders[name](); } catch (error) { toast(error.message, true); }
}

function bindUI() {
  $$(".nav-item").forEach(button => button.addEventListener("click", () => switchView(button.dataset.view)));
  $$("[data-jump]").forEach(button => button.addEventListener("click", () => switchView(button.dataset.jump)));
  $("#menu-button").addEventListener("click", () => $("#sidebar").classList.toggle("open"));
  $("#apod-dialog .dialog-close").addEventListener("click", () => $("#apod-dialog").close());

  const exoReload = debounce(() => loadExoplanets(true));
  ["#exo-search", "#exo-method", "#exo-year-min", "#exo-year-max"].forEach(s => $(s).addEventListener($(s).tagName === "INPUT" ? "input" : "change", exoReload));
  const launchReload = debounce(() => loadLaunches(true));
  ["#launch-search", "#launch-outcome", "#launch-year"].forEach(s => $(s).addEventListener($(s).tagName === "INPUT" ? "input" : "change", launchReload));
  const apodReload = debounce(() => loadApod(true));
  ["#apod-search", "#apod-media", "#apod-year"].forEach(s => $(s).addEventListener($(s).tagName === "INPUT" ? "input" : "change", apodReload));
  const habReload = debounce(() => loadHabitability(true));
  ["#hab-search", "#hab-tier"].forEach(s => $(s).addEventListener($(s).tagName === "INPUT" ? "input" : "change", habReload));

  $("#go-to-run-button").addEventListener("click", () => switchView("run"));

  $("#trigger-run-button").addEventListener("click", async event => {
    const button = event.currentTarget;
    button.disabled = true; button.innerHTML = "<span>▶</span> Starting…";
    try {
      const result = await api("/api/pipeline/trigger", { method: "POST" });
      runState.runId = result.run_id;
      toast(`Pipeline triggered: ${result.run_id}`);
      startRunPolling();
    } catch (error) {
      toast(error.message, true);
      button.disabled = false; button.innerHTML = "<span>▶</span> Trigger pipeline run";
    }
  });

  $("#refresh-lineage-button").addEventListener("click", async event => {
    const button = event.currentTarget;
    button.disabled = true;
    try { await loadLineage(true); } catch (error) { toast(error.message, true); }
    finally { button.disabled = false; }
  });

  $("#refresh-button").addEventListener("click", async event => {
    const button = event.currentTarget;
    button.disabled = true; button.innerHTML = "<span>↻</span> Refreshing…";
    toast("Refreshing NASA and launch sources. This can take about a minute.");
    try {
      const result = await api("/api/refresh", { method: "POST" });
      Object.keys(state).forEach(k => state[k] = null);
      await loadOverview(true);
      toast(`Refresh complete · ${Object.values(result).map(x => x.records || 0).reduce((a,b)=>a+b,0).toLocaleString()} records processed`);
    } catch (error) { toast(error.message, true); }
    finally { button.disabled = false; button.innerHTML = "<span>↻</span> Refresh sources"; }
  });
}

function tickClock() { $("#clock").textContent = new Date().toISOString().slice(11,19); }

document.addEventListener("DOMContentLoaded", async () => {
  const year = new Date().getUTCFullYear();
  $("#exo-year-max").value = year; $("#apod-year").max = year; $("#launch-year").max = year;
  bindUI(); tickClock(); setInterval(tickClock, 1000);
  const initial = location.hash.slice(1);
  await switchView(loaders[initial] ? initial : "overview");
});
