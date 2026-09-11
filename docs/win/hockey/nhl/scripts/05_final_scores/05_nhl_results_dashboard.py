#!/usr/bin/env python3
# docs/win/hockey/nhl/scripts/05_final_scores/05_nhl_results_dashboard.py
#
# Builds the NHL league dashboard and Hockey master dashboard from Stage 05
# reporting, calibration, and CLV outputs.
#
# Optional historical season snapshots:
#   docs/win/hockey/nhl/05_final_scores/seasons/<season>/nhl_market_tally.csv
#   docs/win/hockey/nhl/05_final_scores/seasons/<season>/reports/...
#   docs/win/hockey/nhl/05_final_scores/seasons/<season>/clv/NHL_clv.csv
#
# Outputs:
#   frontend/nhl_dashboard.html
#   frontend/hockey_dashboard.html

from __future__ import annotations

from datetime import datetime, UTC
from pathlib import Path
import html
import json
import sys

import pandas as pd


BASE = Path("docs/win/hockey/nhl/05_final_scores")
CURRENT_CLV_ROOT = Path("docs/win/hockey/nhl/clv")
HISTORICAL_ROOT = BASE / "seasons"

OUTPUT = Path("frontend/nhl_dashboard.html")
MASTER_OUTPUT = Path("frontend/hockey_dashboard.html")

MARKETS = [
    {
        "key": "moneyline",
        "display": "Moneyline",
        "folder": "moneyline",
        "prefix": "nhl_moneyline",
        "dimensions": ["ev", "kelly", "odds", "win_prob"],
    },
    {
        "key": "puck_line",
        "display": "Puck Line",
        "folder": "puckline",
        "prefix": "nhl_puck_line",
        "dimensions": ["ev", "kelly", "odds", "side", "win_prob"],
    },
    {
        "key": "total",
        "display": "Total",
        "folder": "total",
        "prefix": "nhl_total",
        "dimensions": ["ev", "kelly", "odds", "side", "total_range", "win_prob"],
    },
]

CSS = r'''
:root {
  --bg:#0e1117;
  --panel:#161b22;
  --panel2:#1c232c;
  --text:#e6edf3;
  --muted:#8b949e;
  --accent:#58a6ff;
  --good:#3fb950;
  --bad:#f85149;
  --border:#30363d;
  --table-border:#46515e;
}
*{box-sizing:border-box}
html,body{
  margin:0;
  padding:0;
  background:var(--bg);
  color:var(--text);
  font-family:-apple-system,BlinkMacSystemFont,"Segoe UI",Roboto,sans-serif;
}
header{
  padding:18px 24px;
  border-bottom:1px solid var(--border);
  display:flex;
  justify-content:space-between;
  align-items:baseline;
  gap:12px;
  flex-wrap:wrap;
}
header h1{margin:0;font-size:20px}
header .ts{font-size:12px;color:var(--muted)}
.selector-bar{
  padding:10px 24px;
  border-bottom:1px solid var(--border);
  background:var(--panel);
  display:flex;
  gap:6px;
  flex-wrap:wrap;
  align-items:center;
  position:sticky;
  top:0;
  z-index:10;
}
.selector-bar .lbl{
  color:var(--muted);
  font-size:12px;
  text-transform:uppercase;
  letter-spacing:.06em;
  margin-right:6px;
}
.season-btn{
  background:var(--panel2);
  border:1px solid var(--border);
  color:var(--text);
  padding:6px 14px;
  border-radius:4px;
  cursor:pointer;
  font-size:13px;
}
.season-btn.active{
  background:var(--accent);
  color:#0e1117;
  border-color:var(--accent);
  font-weight:600;
}
main{max-width:1500px;margin:0 auto;padding:18px 24px}
h2{
  font-size:16px;
  margin:24px 0 8px;
  border-bottom:1px solid var(--border);
  padding-bottom:4px;
}
.kpis{
  display:grid;
  grid-template-columns:repeat(auto-fit,minmax(135px,1fr));
  gap:10px;
  margin:12px 0;
}
.kpi{
  background:var(--panel);
  border:1px solid var(--border);
  border-radius:6px;
  padding:12px;
}
.kpi .label{
  font-size:11px;
  color:var(--muted);
  text-transform:uppercase;
  letter-spacing:.05em;
}
.kpi .value{font-size:22px;font-weight:600;margin-top:4px}
.good{color:var(--good)}
.bad{color:var(--bad)}
.muted{color:var(--muted)}
.tabs{display:flex;gap:4px;flex-wrap:wrap;margin:14px 0 0}
.tab{
  background:var(--panel2);
  border:1px solid var(--border);
  color:var(--muted);
  padding:7px 12px;
  border-radius:6px 6px 0 0;
  cursor:pointer;
  font-size:13px;
}
.tab.active{color:var(--text);border-bottom-color:var(--panel2)}
.tab-body{
  background:var(--panel2);
  border:1px solid var(--border);
  border-top:none;
  padding:14px;
  border-radius:0 6px 6px 6px;
}
.controls{
  display:flex;
  align-items:center;
  gap:10px;
  flex-wrap:wrap;
  margin-bottom:10px;
}
.controls label{font-size:12px;color:var(--muted)}
.controls select{
  background:var(--panel);
  border:1px solid var(--border);
  color:var(--text);
  padding:5px 8px;
  border-radius:4px;
}
.scroll{
  overflow:auto;
  max-height:62vh;
  border:1px solid var(--table-border);
  border-radius:6px;
}
table{width:100%;border-collapse:collapse;font-size:13px}
th,td{
  padding:7px 9px;
  text-align:center;
  border:1px solid var(--table-border);
  white-space:nowrap;
  vertical-align:middle;
}
th{
  position:sticky;
  top:0;
  background:var(--panel2);
  color:var(--muted);
  font-size:11px;
  text-transform:uppercase;
  letter-spacing:.04em;
  cursor:pointer;
  user-select:none;
}
td.num{text-align:center;font-variant-numeric:tabular-nums}
.note{
  padding:10px 12px;
  border:1px solid var(--border);
  border-radius:6px;
  background:var(--panel);
  color:var(--muted);
  font-size:12px;
}
td.win-pct-strong{background:rgba(63,185,80,.24);color:#7ee787;font-weight:700}
td.win-pct-green{background:rgba(63,185,80,.16);color:#56d364;font-weight:600}
td.win-pct-light{background:rgba(63,185,80,.09);color:#8ddb8c;font-weight:600}
td.win-pct-neutral{background:rgba(139,148,158,.09);color:#c9d1d9}
td.win-pct-red{background:rgba(248,81,73,.14);color:#ff7b72;font-weight:600}
'''

JS = r'''
let ACTIVE_SEASON = null;

function fmtPct(v){
  if(v==null||isNaN(Number(v)))return "";
  return (Number(v)*100).toFixed(2)+"%";
}
function fmtNum(v,d=3){
  if(v==null||isNaN(Number(v)))return "";
  return Number(v).toFixed(d);
}
function fmtInt(v){
  if(v==null||isNaN(Number(v)))return "";
  return Number(v).toLocaleString();
}
function signedClass(v){
  if(v==null||isNaN(Number(v)))return "";
  return Number(v)>0?"good":(Number(v)<0?"bad":"");
}
function winPctClass(v){
  if(v==null||isNaN(Number(v)))return "";
  const pct=Number(v);
  if(pct>=0.80)return "win-pct-strong";
  if(pct>=0.70)return "win-pct-green";
  if(pct>=0.60)return "win-pct-light";
  if(pct>=0.50)return "win-pct-neutral";
  return "win-pct-red";
}
function showTab(host,key){
  host.querySelectorAll(":scope > .tabs .tab").forEach(x=>{
    x.classList.toggle("active",x.dataset.key===key);
  });
  host.querySelectorAll(":scope > .tab-body > .tab-panel").forEach(x=>{
    x.style.display=x.dataset.key===key?"":"none";
  });
}
function renderTable(data,columns,container){
  if(!data||!data.length){
    container.innerHTML='<div class="muted">No rows available.</div>';
    return;
  }

  const wrap=document.createElement("div");
  wrap.className="scroll";
  const table=document.createElement("table");
  const thead=document.createElement("thead");
  const tbody=document.createElement("tbody");
  const hr=document.createElement("tr");

  let sortKey=null;
  let sortDir="desc";

  columns.forEach(c=>{
    const th=document.createElement("th");
    th.textContent=c.label;
    th.addEventListener("click",()=>{
      if(sortKey===c.key)sortDir=sortDir==="asc"?"desc":"asc";
      else{
        sortKey=c.key;
        sortDir="desc";
      }
      draw();
    });
    hr.appendChild(th);
  });

  thead.appendChild(hr);

  function draw(){
    const rows=data.slice();

    if(sortKey){
      rows.sort((a,b)=>{
        const av=a[sortKey],bv=b[sortKey];
        if(av==null)return 1;
        if(bv==null)return -1;

        const an=Number(av),bn=Number(bv);
        if(!Number.isNaN(an)&&!Number.isNaN(bn)){
          return sortDir==="asc"?an-bn:bn-an;
        }

        return sortDir==="asc"
          ?String(av).localeCompare(String(bv))
          :String(bv).localeCompare(String(av));
      });
    }

    tbody.innerHTML="";

    rows.forEach(r=>{
      const tr=document.createElement("tr");

      columns.forEach(c=>{
        const td=document.createElement("td");
        const v=r[c.key];

        if(c.fmt==="int"){
          td.classList.add("num");
          td.textContent=fmtInt(v);
        }else if(c.fmt==="pct"){
          td.classList.add("num");
          td.textContent=fmtPct(v);

          if(String(c.key).toLowerCase()==="win_pct"){
            const cls=winPctClass(v);
            if(cls)td.classList.add(cls);
          }
        }else if(c.fmt==="num"){
          td.classList.add("num");
          td.textContent=fmtNum(v,c.decimals==null?3:c.decimals);

          if(c.signed){
            const cls=signedClass(v);
            if(cls)td.classList.add(cls);
          }
        }else{
          td.textContent=v==null?"":String(v);
        }

        tr.appendChild(td);
      });

      tbody.appendChild(tr);
    });
  }

  draw();
  table.appendChild(thead);
  table.appendChild(tbody);
  wrap.appendChild(table);
  container.innerHTML="";
  container.appendChild(wrap);
}

const RESULT_COLUMNS=[
  {key:"variable",label:"Bucket"},
  {key:"side_group",label:"Side"},
  {key:"Win",label:"W",fmt:"int"},
  {key:"Loss",label:"L",fmt:"int"},
  {key:"Push",label:"P",fmt:"int"},
  {key:"Total",label:"Total",fmt:"int"},
  {key:"Win_Pct",label:"Win %",fmt:"pct"},
  {key:"units",label:"Units",fmt:"num",decimals:3,signed:true},
  {key:"roi",label:"ROI",fmt:"pct"},
  {key:"avg_odds",label:"Avg Odds",fmt:"num",decimals:1},
  {key:"avg_ev",label:"Avg EV",fmt:"pct"},
  {key:"avg_kelly",label:"Avg Kelly",fmt:"pct"},
  {key:"avg_win_prob",label:"Avg Win Prob",fmt:"pct"}
];

const TALLY_COLUMNS=[
  {key:"market_type",label:"Market"},
  {key:"Win",label:"W",fmt:"int"},
  {key:"Loss",label:"L",fmt:"int"},
  {key:"Push",label:"P",fmt:"int"},
  {key:"Total",label:"Total",fmt:"int"},
  {key:"Win_Pct",label:"Win %",fmt:"pct"},
  {key:"units",label:"Units",fmt:"num",decimals:3,signed:true},
  {key:"roi",label:"ROI",fmt:"pct"},
  {key:"avg_odds",label:"Avg Odds",fmt:"num",decimals:1},
  {key:"avg_ev",label:"Avg EV",fmt:"pct"},
  {key:"avg_kelly",label:"Avg Kelly",fmt:"pct"},
  {key:"avg_win_prob",label:"Avg Win Prob",fmt:"pct"}
];

const CAL_METRICS=[
  {key:"market_type",label:"Market"},
  {key:"bets",label:"Bets",fmt:"int"},
  {key:"expected_win_rate",label:"Expected",fmt:"pct"},
  {key:"realized_win_rate",label:"Realized",fmt:"pct"},
  {key:"calibration_gap",label:"Gap",fmt:"num",decimals:4,signed:true},
  {key:"brier_score",label:"Brier",fmt:"num",decimals:4},
  {key:"log_loss",label:"Log Loss",fmt:"num",decimals:4},
  {key:"expected_wins",label:"Expected Wins",fmt:"num",decimals:2},
  {key:"realized_wins",label:"Realized Wins",fmt:"num",decimals:0}
];

const CAL_PROB=[
  {key:"market_type",label:"Market"},
  {key:"probability_bucket",label:"Bucket"},
  {key:"bets",label:"Bets",fmt:"int"},
  {key:"avg_model_prob",label:"Avg Model",fmt:"pct"},
  {key:"realized_win_rate",label:"Realized",fmt:"pct"},
  {key:"calibration_gap",label:"Gap",fmt:"num",decimals:4,signed:true},
  {key:"brier_score",label:"Brier",fmt:"num",decimals:4},
  {key:"log_loss",label:"Log Loss",fmt:"num",decimals:4}
];

const CAL_EXPECTED=[
  {key:"market_type",label:"Market"},
  {key:"bets",label:"Bets",fmt:"int"},
  {key:"expected_probability",label:"Expected",fmt:"pct"},
  {key:"realized_probability",label:"Realized",fmt:"pct"},
  {key:"difference",label:"Difference",fmt:"num",decimals:4,signed:true},
  {key:"expected_wins",label:"Expected Wins",fmt:"num",decimals:2},
  {key:"realized_wins",label:"Realized Wins",fmt:"num",decimals:0}
];

const WALK=[
  {key:"market_type",label:"Market"},
  {key:"through_game_date",label:"Through Date"},
  {key:"Win",label:"W",fmt:"int"},
  {key:"Loss",label:"L",fmt:"int"},
  {key:"Push",label:"P",fmt:"int"},
  {key:"bets_including_pushes",label:"Bets",fmt:"int"},
  {key:"win_pct",label:"Win %",fmt:"pct"},
  {key:"units",label:"Units",fmt:"num",decimals:3,signed:true},
  {key:"roi",label:"ROI",fmt:"pct"},
  {key:"calibration_gap",label:"Cal Gap",fmt:"num",decimals:4,signed:true},
  {key:"brier_score",label:"Brier",fmt:"num",decimals:4},
  {key:"log_loss",label:"Log Loss",fmt:"num",decimals:4}
];

const CLV_COLUMNS=[
  {key:"game_date",label:"Date"},
  {key:"away_team",label:"Away"},
  {key:"home_team",label:"Home"},
  {key:"market_type",label:"Market"},
  {key:"bet_side",label:"Side"},
  {key:"selected_provider_name",label:"Selected Book"},
  {key:"dk_odds_american",label:"Selected Odds"},
  {key:"closing_provider_name",label:"Closing Book"},
  {key:"closing_odds_american",label:"Closing Odds"},
  {key:"clv_implied_probability",label:"Implied CLV",fmt:"num",decimals:4,signed:true},
  {key:"clv_decimal_ratio",label:"Decimal CLV",fmt:"num",decimals:4,signed:true},
  {key:"line_clv",label:"Line CLV",fmt:"num",decimals:3,signed:true},
  {key:"clv_status",label:"Status"}
];

function kpi(label,value,fmt){
  let text="N/A";
  let cls="";

  if(value!=null&&!Number.isNaN(Number(value))){
    if(fmt==="pct"){
      text=fmtPct(value);
    }else if(fmt==="int"){
      text=fmtInt(value);
    }else if(fmt==="num"){
      text=fmtNum(value,3);
      cls=signedClass(value);
    }else if(fmt==="odds"){
      text=fmtNum(value,1);
    }
  }

  return '<div class="kpi"><div class="label">'+label+
    '</div><div class="value '+cls+'">'+text+'</div></div>';
}

function buildMarketPanel(market,panel){
  const dims=Object.keys(market.by||{}).filter(d=>
    ((market.by[d]||[]).length||(market.by_side[d]||[]).length)
  );

  if(!dims.length){
    panel.innerHTML='<div class="muted">No report rows available.</div>';
    return;
  }

  panel.innerHTML=
    '<div class="controls">'+
      '<label>Dimension: <select class="dim">'+
        dims.map(d=>
          '<option value="'+d+'">'+d.replaceAll("_"," ")+'</option>'
        ).join("")+
      '</select></label>'+
      '<label>View: <select class="view">'+
        '<option value="overall">Overall</option>'+
        '<option value="side">Split home/away</option>'+
      '</select></label>'+
    '</div>'+
    '<div class="tablehost"></div>';

  const dim=panel.querySelector(".dim");
  const view=panel.querySelector(".view");
  const host=panel.querySelector(".tablehost");

  function refresh(){
    const d=dim.value;
    const rows=view.value==="side"
      ?((market.by_side||{})[d]||[])
      :((market.by||{})[d]||[]);

    renderTable(rows,RESULT_COLUMNS,host);
  }

  dim.addEventListener("change",refresh);
  view.addEventListener("change",refresh);
  refresh();
}

function buildDashboard(data){
  const h=data.headline||{};

  document.getElementById("headline").innerHTML=[
    kpi("Bets",h.bets,"int"),
    kpi("Wins",h.wins,"int"),
    kpi("Losses",h.losses,"int"),
    kpi("Pushes",h.pushes,"int"),
    kpi("Win %",h.win_pct,"pct"),
    kpi("Units",h.units,"num"),
    kpi("ROI",h.roi,"pct"),
    kpi("Avg Odds",h.avg_odds,"odds"),
    kpi("Avg EV",h.avg_ev,"pct"),
    kpi("Avg Kelly",h.avg_kelly,"pct"),
    kpi("Avg Win Prob",h.avg_win_prob,"pct")
  ].join("");

  renderTable(
    data.tally||[],
    TALLY_COLUMNS,
    document.getElementById("by-market")
  );

  const cal=document.getElementById("calibration");
  const calCols={
    metrics:CAL_METRICS,
    probability:CAL_PROB,
    expected:CAL_EXPECTED,
    walk_forward:WALK
  };

  Object.entries(data.calibration||{}).forEach(([key,rows])=>{
    const panel=cal.querySelector('.tab-panel[data-key="'+key+'"]');
    if(panel)renderTable(rows||[],calCols[key]||[],panel);
  });

  const ch=(data.clv||{}).headline||{};

  document.getElementById("clv-headline").innerHTML=[
    kpi("Selections",ch.selections,"int"),
    kpi("Comparable",ch.comparable,"int"),
    kpi("Positive CLV",ch.positive,"int"),
    kpi("Positive CLV %",ch.positive_pct,"pct"),
    kpi("Avg Implied CLV",ch.avg_implied_clv,"pct"),
    kpi("Avg Decimal CLV",ch.avg_decimal_ratio,"pct")
  ].join("");

  renderTable(
    (data.clv||{}).rows||[],
    CLV_COLUMNS,
    document.getElementById("clv-table")
  );

  const markets=document.getElementById("markets");

  Object.entries(data.markets||{}).forEach(([key,market])=>{
    const panel=markets.querySelector(".market-panel-"+key);
    if(panel)buildMarketPanel(market,panel);
  });
}

function selectSeason(season){
  if(!ALL_DATA[season])return;

  ACTIVE_SEASON=season;

  document.querySelectorAll(".season-btn").forEach(btn=>{
    btn.classList.toggle("active",btn.dataset.season===season);
  });

  buildDashboard(ALL_DATA[season].data);

  try{
    localStorage.setItem("nhl_dash_season",season);
  }catch(e){}
}

document.addEventListener("DOMContentLoaded",()=>{
  const cal=document.getElementById("calibration");
  cal.querySelectorAll(":scope > .tabs .tab").forEach(tab=>{
    tab.addEventListener("click",()=>showTab(cal,tab.dataset.key));
  });

  const markets=document.getElementById("markets");
  markets.querySelectorAll(":scope > .tabs .tab").forEach(tab=>{
    tab.addEventListener("click",()=>showTab(markets,tab.dataset.key));
  });

  let initial=DEFAULT_SEASON;

  try{
    const stored=localStorage.getItem("nhl_dash_season");
    if(stored&&ALL_DATA[stored])initial=stored;
  }catch(e){}

  selectSeason(initial);
});
'''

HTML_TEMPLATE = r'''
<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>NHL Dashboard</title>
<link rel="stylesheet" href="assets/css/matstheme.css">
<style>__CSS_HTML__</style>
</head>
<body>
<div id="nav-placeholder"></div>

<header>
  <h1>NHL Dashboard</h1>
  <span class="ts">Built __GENERATED__ UTC</span>
</header>

<div class="selector-bar season-bar"__SEASON_BAR_STYLE__>
  <span class="lbl">Season:</span>
  __SEASON_BUTTONS__
</div>

<main>
<h2>NHL Analytics</h2>
<div class="kpis" id="headline"></div>

<h2>By Market</h2>
<div id="by-market"></div>

<h2>Model Calibration</h2>
<div id="calibration">
  <div class="tabs">
    <button class="tab active" data-key="metrics">Metrics</button>
    <button class="tab" data-key="probability">Probability Buckets</button>
    <button class="tab" data-key="expected">Expected vs Realized</button>
    <button class="tab" data-key="walk_forward">Walk Forward</button>
  </div>
  <div class="tab-body">
    <div class="tab-panel" data-key="metrics"></div>
    <div class="tab-panel" data-key="probability" style="display:none"></div>
    <div class="tab-panel" data-key="expected" style="display:none"></div>
    <div class="tab-panel" data-key="walk_forward" style="display:none"></div>
  </div>
</div>

<h2>Closing Line Value</h2>
<div class="note">
  Positive implied-probability CLV and positive decimal-ratio CLV mean the selected price beat the close, matching the NHL pipeline's CLV definitions.
</div>
<div class="kpis" id="clv-headline"></div>
<div id="clv-table"></div>

<h2>Per Market Drilldown</h2>
<div id="markets">
  <div class="tabs">__MARKET_TABS__</div>
  <div class="tab-body">__MARKET_PANELS__</div>
</div>
</main>

<script src="assets/js/shared/nav.js"></script>
<script>
__JS_HTML__

const ALL_DATA=__DATA__;
const DEFAULT_SEASON="__DEFAULT_SEASON__";
</script>
</body>
</html>
'''

def read_csv(path: Path, required: bool = False) -> pd.DataFrame:
    if not path.exists():
        print(
            f"{'ERROR' if required else 'WARNING'}: missing {path}",
            file=sys.stderr,
        )
        return pd.DataFrame()

    try:
        return pd.read_csv(path)
    except Exception as exc:
        print(
            f"WARNING: unable to read {path}: {type(exc).__name__}: {exc}",
            file=sys.stderr,
        )
        return pd.DataFrame()


def clean_value(value):
    if value is None:
        return None

    try:
        if pd.isna(value):
            return None
    except Exception:
        pass

    if hasattr(value, "item"):
        try:
            return value.item()
        except Exception:
            pass

    return value


def records(df: pd.DataFrame) -> list[dict]:
    if df.empty:
        return []

    return [
        {k: clean_value(v) for k, v in row.items()}
        for row in df.to_dict(orient="records")
    ]


def num(series: pd.Series) -> pd.Series:
    return pd.to_numeric(series, errors="coerce")


def weighted_mean(
    df: pd.DataFrame,
    value_col: str,
    weight_col: str = "Total",
):
    if (
        df.empty
        or value_col not in df.columns
        or weight_col not in df.columns
    ):
        return None

    values = num(df[value_col])
    weights = num(df[weight_col])

    mask = (
        values.notna()
        & weights.notna()
        & (weights > 0)
    )

    if not mask.any():
        return None

    return clean_value(
        (values[mask] * weights[mask]).sum()
        / weights[mask].sum()
    )


def build_headline(tally: pd.DataFrame) -> dict:
    if tally.empty:
        return {}

    tally = tally.copy()

    for col in ["Win", "Loss", "Push", "Total", "units"]:
        if col in tally.columns:
            tally[col] = num(tally[col])

    wins = int(tally["Win"].sum()) if "Win" in tally.columns else 0
    losses = int(tally["Loss"].sum()) if "Loss" in tally.columns else 0
    pushes = int(tally["Push"].sum()) if "Push" in tally.columns else 0

    total = (
        int(tally["Total"].sum())
        if "Total" in tally.columns
        else wins + losses + pushes
    )

    units = (
        clean_value(tally["units"].sum(min_count=1))
        if "units" in tally.columns
        else None
    )

    return {
        "bets": total,
        "wins": wins,
        "losses": losses,
        "pushes": pushes,
        "win_pct": wins / (wins + losses) if wins + losses else None,
        "units": units,
        "roi": float(units) / total if units is not None and total else None,
        "avg_odds": weighted_mean(tally, "avg_odds"),
        "avg_ev": weighted_mean(tally, "avg_ev"),
        "avg_kelly": weighted_mean(tally, "avg_kelly"),
        "avg_win_prob": weighted_mean(tally, "avg_win_prob"),
    }


def truthy(value) -> bool:
    return str(value).strip().lower() in {"1", "true", "yes", "y"}


def season_display_name(name: str) -> str:
    return name.replace("_", "-").strip()


def discover_seasons() -> list[tuple[str, str, Path, Path]]:
    seasons = [
        (
            "current",
            "Current",
            BASE,
            CURRENT_CLV_ROOT,
        )
    ]

    if HISTORICAL_ROOT.exists():
        historical = sorted(
            [
                path
                for path in HISTORICAL_ROOT.iterdir()
                if path.is_dir()
            ],
            key=lambda path: path.name,
            reverse=True,
        )

        for path in historical:
            seasons.append(
                (
                    path.name,
                    season_display_name(path.name),
                    path,
                    path / "clv",
                )
            )

    return seasons


def load_markets(report_root: Path) -> dict:
    out = {}

    for spec in MARKETS:
        base = report_root / spec["folder"]

        market = {
            "display": spec["display"],
            "by": {},
            "by_side": {},
        }

        for dim in spec["dimensions"]:
            root = f"{spec['prefix']}_by_{dim}"

            market["by"][dim] = records(
                read_csv(base / f"{root}.csv")
            )

            market["by_side"][dim] = records(
                read_csv(base / f"{root}_home_away_summary.csv")
            )

        out[spec["key"]] = market

    return out


def build_clv(clv_root: Path) -> dict:
    df = read_csv(clv_root / "NHL_clv.csv")

    if df.empty:
        return {
            "headline": {},
            "rows": [],
        }

    comparable = (
        df["price_comparable"].map(truthy)
        if "price_comparable" in df.columns
        else pd.Series(False, index=df.index)
    )

    implied = (
        num(df["clv_implied_probability"])
        if "clv_implied_probability" in df.columns
        else pd.Series(float("nan"), index=df.index)
    )

    decimal = (
        num(df["clv_decimal_ratio"])
        if "clv_decimal_ratio" in df.columns
        else pd.Series(float("nan"), index=df.index)
    )

    valid = comparable & implied.notna()
    positive = valid & (implied > 0)
    decimal_valid = comparable & decimal.notna()

    return {
        "headline": {
            "selections": len(df),
            "comparable": int(valid.sum()),
            "positive": int(positive.sum()),
            "positive_pct": (
                float(positive.sum()) / float(valid.sum())
                if valid.any()
                else None
            ),
            "avg_implied_clv": (
                clean_value(implied[valid].mean())
                if valid.any()
                else None
            ),
            "avg_decimal_ratio": (
                clean_value(decimal[decimal_valid].mean())
                if decimal_valid.any()
                else None
            ),
        },
        "rows": records(df),
    }


def build_payload(
    base_root: Path,
    clv_root: Path,
    *,
    required: bool = False,
) -> dict:
    report_root = base_root / "reports"
    calibration_root = report_root / "calibration"

    calibration_files = {
        "metrics": calibration_root / "nhl_calibration_metrics.csv",
        "probability": calibration_root / "nhl_probability_calibration.csv",
        "expected": calibration_root / "nhl_expected_vs_realized.csv",
        "walk_forward": calibration_root / "nhl_walk_forward_performance.csv",
    }

    tally = read_csv(
        base_root / "nhl_market_tally.csv",
        required=required,
    )

    calibration = {
        key: records(read_csv(path))
        for key, path in calibration_files.items()
    }

    return {
        "headline": build_headline(tally),
        "tally": records(tally),
        "markets": load_markets(report_root),
        "calibration": calibration,
        "clv": build_clv(clv_root),
    }


def build_html() -> str:
    season_defs = discover_seasons()

    all_data = {
        season_key: {
            "label": season_label,
            "data": build_payload(
                season_root,
                clv_root,
                required=(season_key == "current"),
            ),
        }
        for (
            season_key,
            season_label,
            season_root,
            clv_root,
        ) in season_defs
    }

    season_buttons = "\n".join(
        (
            f'<button class="season-btn{" active" if i == 0 else ""}" '
            f'data-season="{html.escape(season_key)}" '
            f'onclick="selectSeason(\'{html.escape(season_key)}\')">'
            f'{html.escape(season_label)}</button>'
        )
        for i, (
            season_key,
            season_label,
            _,
            _,
        ) in enumerate(season_defs)
    )

    season_bar_style = (
        ' style="display:none"'
        if len(season_defs) <= 1
        else ""
    )

    tabs = "\n".join(
        '<button class="tab{}" data-key="{}">{}</button>'.format(
            " active" if i == 0 else "",
            market["key"],
            html.escape(market["display"]),
        )
        for i, market in enumerate(MARKETS)
    )

    panels = "\n".join(
        '<div class="tab-panel market-panel-{}" data-key="{}"{}></div>'.format(
            market["key"],
            market["key"],
            ' style="display:none"' if i else "",
        )
        for i, market in enumerate(MARKETS)
    )

    data = (
        json.dumps(
            all_data,
            ensure_ascii=False,
            allow_nan=False,
        )
        .replace("</", "<\\/")
    )

    return (
        HTML_TEMPLATE
        .replace("__CSS_HTML__", CSS)
        .replace("__JS_HTML__", JS)
        .replace(
            "__GENERATED__",
            html.escape(
                datetime.now(UTC).isoformat(timespec="seconds")
            ),
        )
        .replace("__SEASON_BUTTONS__", season_buttons)
        .replace("__SEASON_BAR_STYLE__", season_bar_style)
        .replace("__MARKET_TABS__", tabs)
        .replace("__MARKET_PANELS__", panels)
        .replace("__DATA__", data)
        .replace("__DEFAULT_SEASON__", season_defs[0][0])
    )


def main() -> None:
    # ANALYTICS_MULTI_OUTPUT
    OUTPUT.parent.mkdir(parents=True, exist_ok=True)

    league_page = build_html()

    master_page = (
        league_page
        .replace(
            "<title>NHL Dashboard</title>",
            "<title>Hockey Dashboard</title>",
            1,
        )
        .replace(
            "<h1>NHL Dashboard</h1>",
            "<h1>Hockey Dashboard</h1>",
            1,
        )
    )

    OUTPUT.write_text(
        league_page,
        encoding="utf-8",
    )

    MASTER_OUTPUT.write_text(
        master_page,
        encoding="utf-8",
    )

    print(f"NHL dashboard generated: {OUTPUT}")
    print(f"Hockey master dashboard generated: {MASTER_OUTPUT}")


if __name__ == "__main__":
    main()
