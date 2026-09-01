const GH_OWNER = "Clownworldenjoyer76";
const GH_REPO = "bet_tracker";
const GH_BRANCH = "main";
const RAW_BASE = `https://raw.githubusercontent.com/${GH_OWNER}/${GH_REPO}/${GH_BRANCH}`;

/* =================================================== CSV PARSER ==================================================================== */
/* =================================================== CSV PARSER ==================================================================== */
/* =================================================== CSV PARSER ==================================================================== */
/* =================================================== CSV PARSER ==================================================================== */
/* =================================================== CSV PARSER ==================================================================== */
/* =================================================== CSV PARSER ==================================================================== */

function parseCSV(text) {
  const rows = [];
  let row = [];
  let field = "";
  let inQuotes = false;

  const s = text.replace(/\r\n/g, "\n").replace(/\r/g, "\n");
  for (let i = 0; i < s.length; i++) {
    const c = s[i];
    const next = s[i + 1];

    if (c === '"' && inQuotes && next === '"') {
      field += '"';
      i++;
      continue;
    }
    if (c === '"') {
      inQuotes = !inQuotes;
      continue;
    }
    if (!inQuotes && c === ",") {
      row.push(field);
      field = "";
      continue;
    }
    if (!inQuotes && c === "\n") {
      row.push(field);
      rows.push(row);
      row = [];
      field = "";
      continue;
    }
    field += c;
  }

  if (field.length > 0 || row.length > 0) {
    row.push(field);
    rows.push(row);
  }

  if (rows.length === 0) return [];

  const headers = rows[0].map(h =>
    (h ?? "").trim().replace(/^\uFEFF/, "")
  );

  const objects = [];
  for (let r = 1; r < rows.length; r++) {
    const values = rows[r];
    if (values.length === 1 && values[0].trim() === "") continue;
    const obj = {};
    for (let c = 0; c < headers.length; c++) {
      obj[headers[c]] = (values[c] ?? "").trim();
    }
    objects.push(obj);
  }
  return objects;
}

function setStatus(msg) {
  const el = document.getElementById("status");
  if (el) el.textContent = msg || "";
}

function format2(n) {
  const x = Number(n);
  return Number.isFinite(x) ? x.toFixed(2) : "";
}

function formatPct(n) {
  const x = Number(n);
  return Number.isFinite(x) ? `${(x * 100).toFixed(2)}%` : "";
}

function escapeHtml(str) {
  return (str ?? "").toString()
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;")
    .replaceAll('"', "&quot;")
    .replaceAll("'", "&#039;");
}

async function fetchText(url) {
  const res = await fetch(url);
  if (!res.ok) throw new Error(`HTTP ${res.status}`);
  return res.text();
}

function mlClassFromProb(p) {
  const x = Number(p);
  if (!Number.isFinite(x)) return "";
  if (x > 0.65) return "ml-green";
  if (x > 0.50) return "ml-orange";
  return "ml-pink";
}

/* ===== TIME SORTING (AM/PM AWARE) ===== */

function timeToMinutes(t) {
  if (!t) return 0;
  const m = t.match(/(\d+):(\d+)\s*(AM|PM)/i);
  if (!m) return 0;

  let h = Number(m[1]);
  const min = Number(m[2]);
  const ap = m[3].toUpperCase();

  if (ap === "PM" && h !== 12) h += 12;
  if (ap === "AM" && h === 12) h = 0;

  return h * 60 + min;
}

/* =================================================== SOCCER ==================================================================== */
/* =================================================== SOCCER ==================================================================== */
/* =================================================== SOCCER ==================================================================== */
/* =================================================== SOCCER ==================================================================== */
/* =================================================== SOCCER ==================================================================== */
/* =================================================== SOCCER ==================================================================== */

async function loadSoccerDaily(selectedDate) {
  setStatus("");
  document.getElementById("games").innerHTML = "";

  const [yyyy, mm, dd] = selectedDate.split("-");
  const d = `${yyyy}_${mm}_${dd}`;

  const finalUrl = `${RAW_BASE}/docs/win/final/final_soc_${d}.csv`;
  const totalsUrl = `${RAW_BASE}/docs/win/soc/edge_soc_totals_${d}.csv`;

  let rows = [];
  let totals = [];

  try {
    rows = parseCSV(await fetchText(finalUrl));
    totals = parseCSV(await fetchText(totalsUrl));
  } catch {
    setStatus("Failed to load soccer file.");
    return;
  }

  if (!rows.length) {
    setStatus("No soccer games found for this date.");
    return;
  }

  const totalsByGame = new Map();
  for (const t of totals) {
    totalsByGame.set(t.game_id, t);
  }

  const games = new Map();
  const order = [];

  for (const r of rows) {
    if (!games.has(r.game_id)) {
      games.set(r.game_id, []);
      order.push(r.game_id);
    }
    games.get(r.game_id).push(r);
  }

  order.sort((a, b) =>
    timeToMinutes(games.get(a)?.[0]?.time) -
    timeToMinutes(games.get(b)?.[0]?.time)
  );

  renderSoccerGames(order, games, totalsByGame);
}

function renderSoccerGames(order, games, totalsByGame) {
  const container = document.getElementById("games");

  for (const gid of order) {
    const rows = games.get(gid);
    const totals = totalsByGame.get(gid) || {};

    const winRows = rows.filter(r => r.bet_type === "win");
    const drawRow = rows.find(r => r.bet_type === "draw");

    if (winRows.length !== 2) continue;

    const a = winRows[0];
    const b = winRows[1];
    const time = a.time || "";

    const ouClass =
      totals.side && totals.side !== "NO PLAY"
        ? mlClassFromProb(totals.model_probability)
        : "no-play";

    const box = document.createElement("div");
    box.className = "game-box";

    box.innerHTML = `
      <div class="game-header">
        ${escapeHtml(a.team)} vs ${escapeHtml(b.team)}${time ? ` — ${escapeHtml(time)}` : ""}
      </div>
      <table class="game-grid">
        <thead>
          <tr>
            <th></th>
            <th>Win Probability</th>
            <th>Projected Goals</th>
            <th>Take O/U at</th>
            <th>Take ML at</th>
          </tr>
        </thead>
        <tbody>
          <tr>
            <td><strong>${escapeHtml(a.team)}</strong></td>
            <td>${formatPct(a.win_probability)}</td>
            <td>${format2(a.goals)}</td>
            <td>${escapeHtml(totals.market_total)}</td>
            <td class="${mlClassFromProb(a.win_probability)}">
              ${escapeHtml(a.personally_acceptable_american_odds)}
            </td>
          </tr>
          <tr>
            <td><strong>${escapeHtml(b.team)}</strong></td>
            <td>${formatPct(b.win_probability)}</td>
            <td>${format2(b.goals)}</td>
            <td class="${totals.side === 'NO PLAY' ? 'no-play' : ''}">
              ${escapeHtml(totals.side)}
            </td>
            <td class="${mlClassFromProb(b.win_probability)}">
              ${escapeHtml(b.personally_acceptable_american_odds)}
            </td>
          </tr>
          ${drawRow ? `
          <tr class="draw-row">
            <td><strong>DRAW</strong></td>
            <td>${formatPct(drawRow.draw_probability)}</td>
            <td></td>
            <td class="${ouClass}">
              ${escapeHtml(totals.acceptable_american_odds)}
            </td>
            <td>${escapeHtml(drawRow.personally_acceptable_american_odds)}</td>
          </tr>` : ""}
        </tbody>
      </table>
    `;

    container.appendChild(box);
  }
}

/* ================================================================= NHL =================================================================== */
/* ================================================================= NHL =================================================================== */
/* ================================================================= NHL =================================================================== */
/* ================================================================= NHL =================================================================== */
/* ================================================================= NHL =================================================================== */

async function loadNHLDaily(selectedDate) {
  setStatus("");
  document.getElementById("games").innerHTML = "";

  const [yyyy, mm, dd] = selectedDate.split("-");
  const d = `${yyyy}_${mm}_${dd}`;

  const spreadsUrl = `${RAW_BASE}/docs/win/nhl/spreads/nhl_spreads_${d}.csv`;
  const edgeUrl = `${RAW_BASE}/docs/win/edge/edge_nhl_${yyyy}_${mm}_${dd}.csv`;
  const totalsUrl = `${RAW_BASE}/docs/win/nhl/edge_nhl_totals_${d}.csv`;

  let spreads = [];
  let edgeRows = [];
  let totalsRows = [];

  try {
    spreads = parseCSV(await fetchText(spreadsUrl));
    edgeRows = parseCSV(await fetchText(edgeUrl));
    totalsRows = parseCSV(await fetchText(totalsUrl));
  } catch {
    setStatus("Failed to load NHL files.");
    return;
  }

  if (!spreads.length) {
    setStatus("No NHL games found for this date.");
    return;
  }

  // team -> acceptable ML
  const mlByTeam = new Map();
  for (const r of edgeRows) {
    if (r.team && r.acceptable_american_odds && !mlByTeam.has(r.team)) {
      mlByTeam.set(r.team, r.acceptable_american_odds);
    }
  }

  // game_id -> acceptable O/U odds
  const ouByGame = new Map();
  for (const r of totalsRows) {
    if (r.game_id && r.personally_acceptable_american_odds) {
      ouByGame.set(r.game_id, r.personally_acceptable_american_odds);
    }
  }

  spreads.sort((a, b) => timeToMinutes(a.time) - timeToMinutes(b.time));

  renderNHLGames(spreads, mlByTeam, ouByGame);
}

function renderNHLGames(spreads, mlByTeam, ouByGame) {
  const container = document.getElementById("games");

  for (const r of spreads) {
    const awayTeam = r.away_team;
    const homeTeam = r.home_team;

    const awayWin = Number(r.away_win_prob);
    const homeWin = Number(r.home_win_prob);

    const awayUnderdog =
      Number.isFinite(awayWin) &&
      Number.isFinite(homeWin) &&
      awayWin < homeWin;

    const homeUnderdog =
      Number.isFinite(awayWin) &&
      Number.isFinite(homeWin) &&
      homeWin < awayWin;

    const box = document.createElement("div");
    box.className = "game-box";

    box.innerHTML = `
      <div class="game-header">
        ${escapeHtml(awayTeam)} vs ${escapeHtml(homeTeam)} — ${escapeHtml(r.time)}
      </div>

      <table class="game-grid">
        <thead>
          <tr>
            <th></th>
            <th>Win Probability</th>
            <th>Projected Goals</th>
            <th>Take O/U at</th>
            <th>Take ML at</th>
            <th>Take +1.5</th>
          </tr>
        </thead>
        <tbody>
          <tr>
            <td><strong>${escapeHtml(awayTeam)}</strong></td>
            <td>${formatPct(awayWin)}</td>
            <td>${format2(r.away_goals)}</td>
            <td>${escapeHtml(r.market_total)}</td>
            <td class="${mlClassFromProb(awayWin)}">
              ${escapeHtml(mlByTeam.get(awayTeam) || "")}
            </td>
            <td>${awayUnderdog ? escapeHtml(r.puck_line_acceptable_amer) : ""}</td>
          </tr>

          <tr>
            <td><strong>${escapeHtml(homeTeam)}</strong></td>
            <td>${formatPct(homeWin)}</td>
            <td>${format2(r.home_goals)}</td>
            <td class="${r.side === "NO PLAY" ? "no-play" : ""}">
              ${escapeHtml(r.side)}
            </td>
            <td class="${mlClassFromProb(homeWin)}">
              ${escapeHtml(mlByTeam.get(homeTeam) || "")}
            </td>
            <td>${homeUnderdog ? escapeHtml(r.puck_line_acceptable_amer) : ""}</td>
          </tr>

          <tr class="draw-row">
            <td><strong>Over/Under</strong></td>
            <td></td>
            <td></td>
            <td>${escapeHtml(ouByGame.get(r.game_id) || "")}</td>
            <td></td>
            <td></td>
          </tr>
        </tbody>
      </table>
    `;

    container.appendChild(box);
  }
}


/* ======================================================== NBA ======================================================================= */
/* ======================================================== NBA ======================================================================= */
/* ======================================================== NBA ======================================================================= */
/* ======================================================== NBA ======================================================================= */
/* ======================================================== NBA ======================================================================= */
/* ======================================================== NBA (MODERNIZED) ======================================================== */

async function loadNBADaily(selectedDate) {
  setStatus("");
  document.getElementById("games").innerHTML = "";

  const [yyyy, mm, dd] = selectedDate.split("-");
  const d = `${yyyy}_${mm}_${dd}`;

  const mlUrl =
    `${RAW_BASE}/docs/win/final/step_2/nba/ml/juice_nba_ml_${d}.csv`;

  const spreadsUrl =
    `${RAW_BASE}/docs/win/final/step_2/nba/spreads/juice_nba_spreads_${d}.csv`;

  const totalsUrl =
    `${RAW_BASE}/docs/win/final/step_2/nba/totals/juice_nba_totals_${d}.csv`;

  const dkUrl =
    `${RAW_BASE}/docs/win/manual/normalized/dk_nba_moneyline_${d}.csv`;

  let mlRows = [];
  let spreads = [];
  let totals = [];
  let dkRows = [];

  try {
    mlRows = parseCSV(await fetchText(mlUrl));
  } catch {
    setStatus("No NBA ML file found for this date.");
    return;
  }

  try { spreads = parseCSV(await fetchText(spreadsUrl)); } catch {}
  try { totals = parseCSV(await fetchText(totalsUrl)); } catch {}
  try { dkRows = parseCSV(await fetchText(dkUrl)); } catch {}

  if (!mlRows.length) {
    setStatus("No NBA games found for this date.");
    return;
  }

  const spreadsByGame = new Map();
  for (const s of spreads) spreadsByGame.set(s.game_id, s);

  const totalsByGame = new Map();
  for (const t of totals) totalsByGame.set(t.game_id, t);

  const timeByGame = new Map();
  for (const r of dkRows) {
    if (r.game_id && r.time) {
      timeByGame.set(r.game_id, r.time);
    }
  }

  mlRows.sort((a, b) =>
    timeToMinutes(timeByGame.get(a.game_id)) -
    timeToMinutes(timeByGame.get(b.game_id))
  );

  renderNBAGamesModern(mlRows, spreadsByGame, totalsByGame, timeByGame);
}

function renderNBAGamesModern(
  mlRows,
  spreadsByGame,
  totalsByGame,
  timeByGame
) {
  const container = document.getElementById("games");

  for (const g of mlRows) {
    const spreads = spreadsByGame.get(g.game_id) || {};
    const totals = totalsByGame.get(g.game_id) || {};
    const gameTime = timeByGame.get(g.game_id);

    const box = document.createElement("div");
    box.className = "game-box";

    box.innerHTML = `
      <div class="game-header">
        ${escapeHtml(g.away_team)} @ ${escapeHtml(g.home_team)}
        ${gameTime ? ` — ${escapeHtml(gameTime)}` : ""}
      </div>

      <div class="game-subheader">
        Projected Total: ${format2(g.game_projected_points)}
      </div>

      <table class="game-grid large-grid">
        <thead>
          <tr>
            <th></th>
            <th>Win %</th>
            <th>Proj Pts</th>
            <th>Fair ML</th>
            <th>Accept ML</th>
            <th>DK ML</th>
            <th>Spread</th>
            <th>Spread Accept</th>
            <th>Handle %</th>
            <th>Bets %</th>
          </tr>
        </thead>
        <tbody>

          <tr>
            <td><strong>${escapeHtml(g.away_team)}</strong></td>
            <td>${formatPct(g.away_team_moneyline_win_prob)}</td>
            <td>${format2(g.away_team_projected_points)}</td>
            <td>${escapeHtml(g.away_ml_fair_american_odds)}</td>
            <td>${escapeHtml(g.away_ml_acceptable_american_odds)}</td>
            <td>${escapeHtml(g.dk_away_odds)}</td>
            <td>${escapeHtml(spreads.away_spread || "")}</td>
            <td>${escapeHtml(spreads.away_spread_acceptable_american_odds || "")}</td>
            <td>${format2(g.away_handle_pct)}</td>
            <td>${format2(g.away_bets_pct)}</td>
          </tr>

          <tr>
            <td><strong>${escapeHtml(g.home_team)}</strong></td>
            <td>${formatPct(g.home_team_moneyline_win_prob)}</td>
            <td>${format2(g.home_team_projected_points)}</td>
            <td>${escapeHtml(g.home_ml_fair_american_odds)}</td>
            <td>${escapeHtml(g.home_ml_acceptable_american_odds)}</td>
            <td>${escapeHtml(g.dk_home_odds)}</td>
            <td>${escapeHtml(spreads.home_spread || "")}</td>
            <td>${escapeHtml(spreads.home_spread_acceptable_american_odds || "")}</td>
            <td>${format2(g.home_handle_pct)}</td>
            <td>${format2(g.home_bets_pct)}</td>
          </tr>

        </tbody>
      </table>

      <div class="totals-section">
        <table class="game-grid large-grid">
          <thead>
            <tr>
              <th>Total</th>
              <th>Over %</th>
              <th>Under %</th>
              <th>Over Accept</th>
              <th>Under Accept</th>
              <th>DK Over</th>
              <th>DK Under</th>
            </tr>
          </thead>
          <tbody>
            <tr>
              <td>${escapeHtml(totals.total || "")}</td>
              <td>${formatPct(totals.over_probability)}</td>
              <td>${formatPct(totals.under_probability)}</td>
              <td>${escapeHtml(totals.over_acceptable_american_odds || "")}</td>
              <td>${escapeHtml(totals.under_acceptable_american_odds || "")}</td>
              <td>${escapeHtml(totals.dk_over_odds || "")}</td>
              <td>${escapeHtml(totals.dk_under_odds || "")}</td>
            </tr>
          </tbody>
        </table>
      </div>
    `;

    container.appendChild(box);
  }
}

/* ======================================================== TOP PICKS ======================================================== */
/* ======================================================== TOP PICKS ======================================================== */
/* ======================================================== TOP PICKS ======================================================== */
/* ======================================================== TOP PICKS ======================================================== */
/* ======================================================== TOP PICKS ======================================================== */
/* ======================================================== TOP PICKS ======================================================== */
/* ======================================================== TOP PICKS ======================================================== */
/* ======================================================== TOP PICKS ======================================================== */
/* ======================================================== TOP PICKS ======================================================== */
/* ======================================================== TOP PICKS ======================================================== */
/* ======================================================== TOP PICKS ======================================================== */

async function loadTopPicks(selectedDate) {
  setStatus("");
  document.getElementById("games").innerHTML = "";

  const [yyyy, mm, dd] = selectedDate.split("-");
  const d = `${yyyy}_${mm}_${dd}`;

  const url =
    `${RAW_BASE}/docs/win/winners/step_02/top_picks_${d}.csv`;

  let rows = [];

  try {
    rows = parseCSV(await fetchText(url));
  } catch {
    setStatus("No Top Picks file found for this date.");
    return;
  }

  if (!rows.length) {
    setStatus("No Top Picks found.");
    return;
  }

  renderTopPicks(rows);
}

function renderTopPicks(rows) {
  const container = document.getElementById("games");

  for (const r of rows) {
    const box = document.createElement("div");
    box.className = "game-box";

    box.innerHTML = `
      <div class="game-header">
        ${escapeHtml(r.league)} — ${escapeHtml(r.market)}
      </div>

      <div>
        ${escapeHtml(r.away_team)} @ ${escapeHtml(r.home_team)}
      </div>
    `;

    container.appendChild(box);
  }
}

async function loadNCAABAltSpreads() {

  const params = new URLSearchParams(window.location.search);
  const date = params.get("date");
  const gameId = params.get("game_id");

  const container = document.getElementById("alt-spreads");

  if (!date || !gameId) {
    container.innerText = "Missing game data.";
    return;
  }

  const url =
    `${RAW_BASE}/docs/win/juice/spreads_alt/ncaab_altspreads_${date}.csv`;

  let rows;

  try {
    rows = parseCSV(await fetchText(url));
  } catch {
    container.innerText = "Alt spreads file not found.";
    return;
  }

  const filtered = rows.filter(r => r.game_id === gameId);

  if (!filtered.length) {
    container.innerText = "No alt spreads available.";
    return;
  }

  const table = document.createElement("table");
  table.className = "game-grid large-grid";

  table.innerHTML = `
    <thead>
      <tr>
        <th>Team</th>
        <th>Spread</th>
        <th>Fair</th>
        <th>Accept</th>
        <th>DK</th>
        <th>Edge</th>
      </tr>
    </thead>
    <tbody>
      ${filtered.map(r => `
        <tr>
          <td>${escapeHtml(r.team)}</td>
          <td>${escapeHtml(r.spread)}</td>
          <td>${escapeHtml(r.fair_american_odds)}</td>
          <td>${escapeHtml(r.acceptable_american_odds)}</td>
          <td>${escapeHtml(r.dk_odds)}</td>
          <td>${format2(r.edge)}</td>
        </tr>
      `).join("")}
    </tbody>
  `;

  container.appendChild(table);
}
