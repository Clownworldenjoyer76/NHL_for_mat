#!/usr/bin/env python3
"""Item 11: staged NHL secondary-model search (research only)."""
from __future__ import annotations

import argparse
import itertools
import json
import math
import sys
from dataclasses import dataclass, asdict
from pathlib import Path
from typing import Any, Sequence

import numpy as np
import pandas as pd
import sklearn
from sklearn.ensemble import (
    GradientBoostingClassifier, GradientBoostingRegressor,
    HistGradientBoostingClassifier, HistGradientBoostingRegressor,
    RandomForestClassifier, RandomForestRegressor,
)
from sklearn.linear_model import ElasticNet, HuberRegressor, LinearRegression, LogisticRegression, Ridge
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import SplineTransformer, StandardScaler

VERSION = "ITEM11-2026-09-12-v2"
SEED = 20260912
EPS = 1e-6
REPO_ROOT = Path(__file__).resolve().parents[6]
NHL_ROOT = REPO_ROOT / "docs" / "win" / "hockey" / "nhl"
HISTORY_ROOT = NHL_ROOT / "research" / "sdv_challenger"
OUTPUT_ROOT = NHL_ROOT / "research" / "model_search" / "item11_secondary_models"

PREGAME = [
    "drat_home_win_prob", "sdv_home_win_prob", "prob_disagreement",
    "drat_exp_margin", "sdv_exp_margin", "margin_disagreement",
    "drat_exp_total", "sdv_exp_total", "total_disagreement", "neutral_site_num",
]
TARGET = {"moneyline": "actual_home_win", "margin": "actual_margin", "total": "actual_total"}
PRIMARY = {"moneyline": "log_loss", "margin": "rmse", "total": "rmse"}
SECONDARY = {"moneyline": "brier", "margin": "mae", "total": "mae"}
FEATURES = {
    "moneyline": {
        "drat_only": ["drat_home_win_prob"],
        "sdv_only": ["sdv_home_win_prob"],
        "drat_sdv": ["drat_home_win_prob", "sdv_home_win_prob"],
        "drat_sdv_disagreement": ["drat_home_win_prob", "sdv_home_win_prob", "prob_disagreement"],
        "verified_pregame_full": PREGAME,
    },
    "margin": {
        "drat_only": ["drat_exp_margin"],
        "sdv_only": ["sdv_exp_margin"],
        "drat_sdv": ["drat_exp_margin", "sdv_exp_margin"],
        "drat_sdv_disagreement": ["drat_exp_margin", "sdv_exp_margin", "margin_disagreement"],
        "verified_pregame_full": PREGAME,
    },
    "total": {
        "drat_only": ["drat_exp_total"],
        "sdv_only": ["sdv_exp_total"],
        "drat_sdv": ["drat_exp_total", "sdv_exp_total"],
        "drat_sdv_disagreement": ["drat_exp_total", "sdv_exp_total", "total_disagreement"],
        "verified_pregame_full": PREGAME,
    },
}
REQUIRED = ["game_id", "game_date", *[x for x in PREGAME if x != "neutral_site_num"], *TARGET.values()]

@dataclass(frozen=True)
class Spec:
    component: str
    family: str
    feature_set: str
    params: dict[str, Any]
    stage: str

    @property
    def candidate_id(self) -> str:
        return f"{self.component}|{self.family}|{self.feature_set}|{json.dumps(self.params, sort_keys=True, separators=(',', ':'))}"


def args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Item 11 staged NHL secondary-model search")
    p.add_argument("--season", type=int, action="append", default=None)
    p.add_argument("--min-train-rows", type=int, default=100)
    p.add_argument("--validation-folds", type=int, default=6)
    p.add_argument("--retain-broad", type=int, default=16)
    p.add_argument("--retain-final", type=int, default=5)
    p.add_argument("--n-jobs", type=int, default=1)
    p.add_argument("--output-dir", type=Path, default=OUTPUT_ROOT)
    return p.parse_args()


def b2f(v: Any) -> float:
    if pd.isna(v): return 0.0
    if isinstance(v, (bool, np.bool_)): return float(v)
    s = str(v).strip().lower()
    if s in {"true", "1", "yes", "y"}: return 1.0
    if s in {"false", "0", "no", "n", ""}: return 0.0
    raise ValueError(f"Bad neutral_site={v!r}")


def load_history(seasons: Sequence[int] | None) -> tuple[pd.DataFrame, list[Path]]:
    files = sorted(HISTORY_ROOT.glob("season_*/standalone_comparison.csv"))
    if seasons:
        wanted = {f"season_{int(x)}" for x in seasons}
        files = [f for f in files if any(part in wanted for part in f.parts)]
    if not files:
        raise SystemExit(f"No standalone_comparison.csv files found under {HISTORY_ROOT}")
    frames = []
    for f in files:
        d = pd.read_csv(f, dtype={"game_id": "string"})
        miss = sorted(set(REQUIRED) - set(d.columns))
        if miss: raise RuntimeError(f"{f} missing columns: {miss}")
        d["_history_file"] = str(f)
        frames.append(d)
    d = pd.concat(frames, ignore_index=True)
    d["_date"] = pd.to_datetime(d["game_date"], errors="coerce").dt.normalize()
    if d["_date"].isna().any(): raise RuntimeError("Unparseable game_date found")
    if "sdv_as_of_rule" in d.columns:
        bad = d["sdv_as_of_rule"].notna() & (d["sdv_as_of_rule"].astype(str) != "source_game_date < target_game_date")
        if bad.any(): raise RuntimeError("Unexpected SDV chronology rule")
    if "game_date_drat" in d.columns:
        dd = pd.to_datetime(d["game_date_drat"], errors="coerce").dt.normalize()
        bad = dd.notna() & (dd != d["_date"])
        if bad.any(): raise RuntimeError("D-Ratings target date mismatch")
    d["neutral_site_num"] = d["neutral_site"].map(b2f) if "neutral_site" in d.columns else 0.0
    for c in PREGAME + list(TARGET.values()):
        d[c] = pd.to_numeric(d[c], errors="coerce")
    d = d.dropna(subset=["game_id", "_date", *PREGAME, *TARGET.values()]).copy()
    if d.duplicated("game_id").any(): raise RuntimeError("Duplicate game_id in Item 11 history")
    if (~d["actual_home_win"].isin([0, 1])).any(): raise RuntimeError("actual_home_win must be 0/1")
    for c in ("drat_home_win_prob", "sdv_home_win_prob"):
        if (~d[c].between(0, 1)).any(): raise RuntimeError(f"{c} outside [0,1]")
    return d.sort_values(["_date", "game_id"]).reset_index(drop=True), files


def make_folds(d: pd.DataFrame, min_rows: int, n_folds: int):
    dates = sorted(pd.Timestamp(x) for x in d["_date"].unique())
    start = next((i for i, day in enumerate(dates) if int((d["_date"] < day).sum()) >= min_rows), None)
    if start is None: raise RuntimeError("No date has enough prior training rows")
    eligible = dates[start:]
    if len(eligible) < n_folds: raise RuntimeError("Not enough eligible dates for requested folds")
    folds, meta = [], []
    for n, block in enumerate(np.array_split(np.array(eligible, dtype="datetime64[ns]"), n_folds), 1):
        block = [pd.Timestamp(x) for x in block]
        first, last = min(block), max(block)
        tr = np.flatnonzero((d["_date"] < first).to_numpy())
        va = np.flatnonzero(d["_date"].isin(block).to_numpy())
        if len(tr) < min_rows or not len(va): raise RuntimeError(f"Bad fold {n}")
        if not (d.iloc[tr]["_date"].max() < d.iloc[va]["_date"].min()): raise AssertionError("Fold leakage")
        folds.append((tr, va))
        meta.append({
            "fold": n, "train_rows": len(tr), "validation_rows": len(va),
            "train_start": str(d.iloc[tr]["_date"].min().date()),
            "train_end": str(d.iloc[tr]["_date"].max().date()),
            "validation_start": str(first.date()), "validation_end": str(last.date()),
        })
    return folds, meta


def inner_split(d: pd.DataFrame, frac: float = .75):
    dates = sorted(pd.Timestamp(x) for x in d["_date"].unique())
    cut = max(1, min(len(dates)-1, int(len(dates)*frac)))
    for c in range(cut, len(dates)):
        early, late = dates[:c], dates[c:]
        a = np.flatnonzero(d["_date"].isin(early).to_numpy())
        b = np.flatnonzero(d["_date"].isin(late).to_numpy())
        if len(a) >= 50 and len(b) >= 10:
            if not d.iloc[a]["_date"].max() < d.iloc[b]["_date"].min(): raise AssertionError("Inner leakage")
            return a, b
    raise RuntimeError("Cannot create chronological inner split")


def clip(p): return np.clip(np.asarray(p, float), EPS, 1-EPS)
def ll(y,p):
    y=np.asarray(y,float); p=clip(p); return float(-np.mean(y*np.log(p)+(1-y)*np.log(1-p)))
def brier(y,p): return float(np.mean((clip(p)-np.asarray(y,float))**2))
def rmse(y,p): return float(np.sqrt(np.mean((np.asarray(p,float)-np.asarray(y,float))**2)))
def mae(y,p): return float(np.mean(np.abs(np.asarray(p,float)-np.asarray(y,float))))


def sklearn_at_least(major: int, minor: int) -> bool:
    parts = []
    for token in sklearn.__version__.split(".")[:2]:
        digits = "".join(ch for ch in token if ch.isdigit())
        parts.append(int(digits or 0))
    while len(parts) < 2:
        parts.append(0)
    return tuple(parts[:2]) >= (major, minor)


def logistic_regression(kind: str, c: float = 1.0) -> LogisticRegression:
    """Construct logistic regression without deprecated sklearn arguments.

    sklearn <1.8 uses the legacy penalty API. sklearn >=1.8 deprecates
    penalty= and maps regularization through l1_ratio/C instead.
    """
    common = {
        "max_iter": 3000,
        "random_state": SEED,
    }
    if sklearn_at_least(1, 8):
        if kind == "none":
            return LogisticRegression(C=1e12, l1_ratio=0.0, solver="lbfgs", **common)
        if kind == "l2":
            return LogisticRegression(C=float(c), l1_ratio=0.0, solver="lbfgs", **common)
        if kind == "l1":
            return LogisticRegression(C=float(c), l1_ratio=1.0, solver="liblinear", **common)
        raise KeyError(kind)

    if kind == "none":
        return LogisticRegression(penalty=None, solver="lbfgs", **common)
    if kind == "l2":
        return LogisticRegression(penalty="l2", C=float(c), solver="lbfgs", **common)
    if kind == "l1":
        return LogisticRegression(penalty="l1", C=float(c), solver="liblinear", **common)
    raise KeyError(kind)


def classifier(fam: str, p: dict[str, Any], n_jobs: int):
    if fam == "logistic":
        return Pipeline([("s",StandardScaler()),("m",logistic_regression("none"))])
    if fam == "regularized_logistic":
        penalty=str(p.get("penalty","l2")).lower()
        if penalty not in {"l1", "l2"}:
            raise ValueError(f"Unsupported regularized_logistic penalty={penalty!r}")
        return Pipeline([("s",StandardScaler()),("m",logistic_regression(penalty,float(p.get("C",1))))])
    if fam == "gam_logistic":
        return Pipeline([("sp",SplineTransformer(n_knots=int(p.get("n_knots",4)),degree=int(p.get("degree",2)),include_bias=False)),("s",StandardScaler()),("m",logistic_regression("l2",float(p.get("C",1))))])
    if fam == "gradient_boosting":
        return GradientBoostingClassifier(n_estimators=int(p.get("n_estimators",150)),learning_rate=float(p.get("learning_rate",.05)),max_depth=int(p.get("max_depth",2)),min_samples_leaf=int(p.get("min_samples_leaf",10)),random_state=SEED)
    if fam == "random_forest":
        return RandomForestClassifier(n_estimators=int(p.get("n_estimators",300)),max_depth=p.get("max_depth",6),min_samples_leaf=int(p.get("min_samples_leaf",8)),max_features=p.get("max_features","sqrt"),n_jobs=n_jobs,random_state=SEED)
    if fam == "hist_gradient_boosting":
        return HistGradientBoostingClassifier(learning_rate=float(p.get("learning_rate",.05)),max_iter=int(p.get("max_iter",175)),max_leaf_nodes=int(p.get("max_leaf_nodes",15)),min_samples_leaf=int(p.get("min_samples_leaf",12)),l2_regularization=float(p.get("l2_regularization",.1)),random_state=SEED)
    raise KeyError(fam)


def regressor(fam: str, p: dict[str, Any], n_jobs: int):
    if fam == "linear": return LinearRegression()
    if fam == "ridge": return Pipeline([("s",StandardScaler()),("m",Ridge(alpha=float(p.get("alpha",1))))])
    if fam == "elastic_net": return Pipeline([("s",StandardScaler()),("m",ElasticNet(alpha=float(p.get("alpha",.01)),l1_ratio=float(p.get("l1_ratio",.5)),max_iter=20000,random_state=SEED))])
    if fam == "random_forest": return RandomForestRegressor(n_estimators=int(p.get("n_estimators",300)),max_depth=p.get("max_depth",7),min_samples_leaf=int(p.get("min_samples_leaf",6)),max_features=p.get("max_features",1.0),n_jobs=n_jobs,random_state=SEED)
    if fam == "gradient_boosting": return GradientBoostingRegressor(n_estimators=int(p.get("n_estimators",175)),learning_rate=float(p.get("learning_rate",.05)),max_depth=int(p.get("max_depth",2)),min_samples_leaf=int(p.get("min_samples_leaf",8)),loss=p.get("loss","squared_error"),random_state=SEED)
    if fam == "hist_gradient_boosting": return HistGradientBoostingRegressor(learning_rate=float(p.get("learning_rate",.05)),max_iter=int(p.get("max_iter",175)),max_leaf_nodes=int(p.get("max_leaf_nodes",15)),min_samples_leaf=int(p.get("min_samples_leaf",12)),l2_regularization=float(p.get("l2_regularization",.1)),loss=p.get("loss","squared_error"),random_state=SEED)
    if fam == "robust_huber": return Pipeline([("s",StandardScaler()),("m",HuberRegressor(epsilon=float(p.get("epsilon",1.35)),alpha=float(p.get("alpha",.0001)),max_iter=2000))])
    raise KeyError(fam)


def weighted_prob(tr, va, p):
    step=float(p.get("weight_step",.05)); grid=np.arange(0,1+step/2,step); y=tr.actual_home_win.to_numpy(float)
    a=tr.drat_home_win_prob.to_numpy(float); b=tr.sdv_home_win_prob.to_numpy(float)
    w=float(grid[int(np.argmin([ll(y,x*a+(1-x)*b) for x in grid]))])
    return w*va.drat_home_win_prob.to_numpy(float)+(1-w)*va.sdv_home_win_prob.to_numpy(float)


def weighted_num(component,tr,va,p):
    if component=="margin": ycol,a,b="actual_margin","drat_exp_margin","sdv_exp_margin"
    else: ycol,a,b="actual_total","drat_exp_total","sdv_exp_total"
    step=float(p.get("weight_step",.05)); grid=np.arange(0,1+step/2,step); y=tr[ycol].to_numpy(float)
    av=tr[a].to_numpy(float); bv=tr[b].to_numpy(float)
    w=float(grid[int(np.argmin([rmse(y,x*av+(1-x)*bv) for x in grid]))])
    return w*va[a].to_numpy(float)+(1-w)*va[b].to_numpy(float)


def calibrated_tree(spec,tr,va,cols,n_jobs):
    ai,bi=inner_split(tr,float(spec.params.get("early_fraction",.75))); a,b=tr.iloc[ai],tr.iloc[bi]
    if a.actual_home_win.nunique()<2 or b.actual_home_win.nunique()<2: raise RuntimeError("One-class calibration split")
    basefam="random_forest" if spec.family=="calibrated_random_forest" else "hist_gradient_boosting"
    base=classifier(basefam,spec.params,n_jobs); base.fit(a[cols].to_numpy(float),a.actual_home_win.to_numpy(int))
    pb=clip(base.predict_proba(b[cols].to_numpy(float))[:,1])
    cal=logistic_regression("l2",float(spec.params.get("calibration_C",1)))
    cal.fit(pb.reshape(-1,1),b.actual_home_win.to_numpy(int))
    pv=clip(base.predict_proba(va[cols].to_numpy(float))[:,1])
    return cal.predict_proba(pv.reshape(-1,1))[:,1]


def stacking(spec,tr,va,cols,n_jobs):
    ai,bi=inner_split(tr,float(spec.params.get("early_fraction",.70))); a,b=tr.iloc[ai],tr.iloc[bi]
    if a.actual_home_win.nunique()<2 or b.actual_home_win.nunique()<2: raise RuntimeError("One-class stacking split")
    l=classifier("regularized_logistic",{"penalty":"l2","C":float(spec.params.get("logit_C",1))},n_jobs)
    h=classifier("hist_gradient_boosting",{"max_iter":150,"max_leaf_nodes":int(spec.params.get("hist_leaf",15)),"learning_rate":.05,"l2_regularization":.1},n_jobs)
    x=a[cols].to_numpy(float); y=a.actual_home_win.to_numpy(int); l.fit(x,y); h.fit(x,y)
    xb=b[cols].to_numpy(float); meta=np.c_[l.predict_proba(xb)[:,1],h.predict_proba(xb)[:,1]]
    m=logistic_regression("l2",float(spec.params.get("meta_C",1))); m.fit(meta,b.actual_home_win.to_numpy(int))
    xv=va[cols].to_numpy(float); return m.predict_proba(np.c_[l.predict_proba(xv)[:,1],h.predict_proba(xv)[:,1]])[:,1]


def bootstrap_ridge(spec,tr,va,cols):
    x=tr[cols].to_numpy(float); y=tr[TARGET[spec.component]].to_numpy(float); xv=va[cols].to_numpy(float)
    rng=np.random.default_rng(SEED+len(tr)+len(cols)*997); out=[]
    for _ in range(int(spec.params.get("n_bootstrap",25))):
        ix=rng.integers(0,len(tr),len(tr)); m=Pipeline([("s",StandardScaler()),("m",Ridge(alpha=float(spec.params.get("alpha",1))))]); m.fit(x[ix],y[ix]); out.append(m.predict(xv))
    return np.mean(np.vstack(out),axis=0)


def predict(spec,tr,va,n_jobs):
    cols=FEATURES[spec.component][spec.feature_set]
    if set(cols)-set(PREGAME): raise AssertionError("Non-whitelisted feature")
    if spec.component=="moneyline":
        if tr.actual_home_win.nunique()<2: raise RuntimeError("One-class training fold")
        if spec.family=="weighted_probability_ensemble": return weighted_prob(tr,va,spec.params)
        if spec.family in {"calibrated_random_forest","calibrated_hist_gradient"}: return calibrated_tree(spec,tr,va,cols,n_jobs)
        if spec.family=="simple_stacking": return stacking(spec,tr,va,cols,n_jobs)
        m=classifier(spec.family,spec.params,n_jobs); m.fit(tr[cols].to_numpy(float),tr.actual_home_win.to_numpy(int)); return m.predict_proba(va[cols].to_numpy(float))[:,1]
    if spec.family=="weighted_numeric_ensemble": return weighted_num(spec.component,tr,va,spec.params)
    if spec.family=="bootstrap_ridge_simulation": return bootstrap_ridge(spec,tr,va,cols)
    m=regressor(spec.family,spec.params,n_jobs); m.fit(tr[cols].to_numpy(float),tr[TARGET[spec.component]].to_numpy(float)); return m.predict(va[cols].to_numpy(float))


def evaluate(spec,d,folds,n_jobs):
    ys=[]; ps=[]
    try:
        for trix,vaix in folds:
            tr,va=d.iloc[trix],d.iloc[vaix]
            if not tr._date.max() < va._date.min(): raise AssertionError("Leakage")
            p=np.asarray(predict(spec,tr,va,n_jobs),float)
            if p.shape!=(len(va),) or not np.isfinite(p).all(): raise RuntimeError("Invalid predictions")
            if spec.component=="moneyline": p=clip(p)
            ys.append(va[TARGET[spec.component]].to_numpy(float)); ps.append(p)
        y=np.concatenate(ys); p=np.concatenate(ps)
        r={"candidate_id":spec.candidate_id,"stage":spec.stage,"component":spec.component,"family":spec.family,"feature_set":spec.feature_set,"params":json.dumps(spec.params,sort_keys=True),"features":json.dumps(FEATURES[spec.component][spec.feature_set]),"rows":len(y),"folds":len(folds),"fit_failures":0,"error":""}
        if spec.component=="moneyline": r.update(log_loss=ll(y,p),brier=brier(y,p),rmse=np.nan,mae=np.nan)
        else: r.update(log_loss=np.nan,brier=np.nan,rmse=rmse(y,p),mae=mae(y,p))
        return r
    except Exception as e:
        return {"candidate_id":spec.candidate_id,"stage":spec.stage,"component":spec.component,"family":spec.family,"feature_set":spec.feature_set,"params":json.dumps(spec.params,sort_keys=True),"features":json.dumps(FEATURES[spec.component][spec.feature_set]),"rows":0,"folds":len(folds),"fit_failures":1,"error":f"{type(e).__name__}: {e}","log_loss":np.nan,"brier":np.nan,"rmse":np.nan,"mae":np.nan}


def broad_specs(component):
    fs=list(FEATURES[component]); out=[]
    if component=="moneyline":
        fams=[
            ("logistic",{}),("regularized_logistic",{"penalty":"l2","C":1}),
            ("gam_logistic",{"n_knots":4,"degree":2,"C":1}),
            ("gradient_boosting",{"n_estimators":150,"learning_rate":.05,"max_depth":2,"min_samples_leaf":10}),
            ("random_forest",{"n_estimators":300,"max_depth":6,"min_samples_leaf":8,"max_features":"sqrt"}),
            ("hist_gradient_boosting",{"learning_rate":.05,"max_iter":175,"max_leaf_nodes":15,"min_samples_leaf":12,"l2_regularization":.1}),
            ("calibrated_random_forest",{"n_estimators":300,"max_depth":6,"min_samples_leaf":8,"max_features":"sqrt","early_fraction":.75,"calibration_C":1}),
            ("calibrated_hist_gradient",{"learning_rate":.05,"max_iter":175,"max_leaf_nodes":15,"min_samples_leaf":12,"l2_regularization":.1,"early_fraction":.75,"calibration_C":1}),
            ("simple_stacking",{"early_fraction":.70,"logit_C":1,"hist_leaf":15,"meta_C":1}),
        ]
        for fam,p in fams:
            for f in fs: out.append(Spec(component,fam,f,p,"broad"))
        out.append(Spec(component,"weighted_probability_ensemble","drat_sdv",{"weight_step":.05},"broad"))
    else:
        fams=[
            ("linear",{}),("ridge",{"alpha":1}),("elastic_net",{"alpha":.01,"l1_ratio":.5}),
            ("random_forest",{"n_estimators":300,"max_depth":7,"min_samples_leaf":6,"max_features":1.0}),
            ("gradient_boosting",{"n_estimators":175,"learning_rate":.05,"max_depth":2,"min_samples_leaf":8,"loss":"squared_error"}),
            ("hist_gradient_boosting",{"learning_rate":.05,"max_iter":175,"max_leaf_nodes":15,"min_samples_leaf":12,"l2_regularization":.1,"loss":"squared_error"}),
            ("robust_huber",{"epsilon":1.35,"alpha":.0001}),
            ("bootstrap_ridge_simulation",{"alpha":1,"n_bootstrap":25}),
        ]
        for fam,p in fams:
            for f in fs: out.append(Spec(component,fam,f,p,"broad"))
        out.append(Spec(component,"weighted_numeric_ensemble","drat_sdv",{"weight_step":.05},"broad"))
    return out


def variants(spec):
    f=spec.family
    if f in {"logistic","linear"}: return [spec.params]
    if f=="regularized_logistic": return [{"penalty":q,"C":c} for q,c in [("l2",.1),("l2",.5),("l2",2),("l2",10),("l1",.5),("l1",2)]]
    if f=="gam_logistic": return [{"n_knots":k,"degree":deg,"C":c} for k,deg,c in [(3,2,1),(5,2,1),(5,3,.5),(5,3,2)]]
    if f=="gradient_boosting":
        loss={"loss":spec.params.get("loss","squared_error")} if spec.component!="moneyline" else {}
        return [{"n_estimators":n,"learning_rate":lr,"max_depth":md,"min_samples_leaf":leaf,**loss} for n,lr,md,leaf in [(100,.03,1,12),(200,.03,2,10),(200,.07,2,8),(250,.04,3,10)]]
    if f=="random_forest":
        mf="sqrt" if spec.component=="moneyline" else 1.0
        return [{"n_estimators":n,"max_depth":md,"min_samples_leaf":leaf,"max_features":mf} for n,md,leaf in [(400,4,10),(400,8,5),(500,None,10)]]
    if f=="hist_gradient_boosting":
        loss={"loss":spec.params.get("loss","squared_error")} if spec.component!="moneyline" else {}
        return [{"learning_rate":lr,"max_iter":it,"max_leaf_nodes":leaf,"min_samples_leaf":ms,"l2_regularization":l2,**loss} for lr,it,leaf,ms,l2 in [(.03,225,7,15,.5),(.05,225,15,10,0),(.04,250,31,15,1)]]
    if f=="calibrated_random_forest": return [{"n_estimators":400,"max_depth":md,"min_samples_leaf":leaf,"max_features":"sqrt","early_fraction":ef,"calibration_C":c} for md,leaf,ef,c in [(4,10,.70,.5),(8,6,.75,1),(None,10,.80,2)]]
    if f=="calibrated_hist_gradient": return [{"learning_rate":.04,"max_iter":225,"max_leaf_nodes":leaf,"min_samples_leaf":12,"l2_regularization":l2,"early_fraction":ef,"calibration_C":c} for leaf,l2,ef,c in [(7,.5,.70,.5),(15,0,.75,1),(31,1,.80,2)]]
    if f=="simple_stacking": return [{"early_fraction":ef,"logit_C":c,"hist_leaf":leaf,"meta_C":c} for ef,c,leaf in [(.65,.5,15),(.75,1,15),(.80,2,7)]]
    if f in {"weighted_probability_ensemble","weighted_numeric_ensemble"}: return [{"weight_step":.02},{"weight_step":.01}]
    if f=="ridge": return [{"alpha":a} for a in [.01,.1,10,100]]
    if f=="elastic_net": return [{"alpha":a,"l1_ratio":r} for a,r in [(.001,.2),(.01,.2),(.01,.8),(.1,.5)]]
    if f=="robust_huber": return [{"epsilon":e,"alpha":a} for e,a in [(1.1,.0001),(1.35,.001),(1.75,.0001),(2,.001)]]
    if f=="bootstrap_ridge_simulation": return [{"alpha":a,"n_bootstrap":n} for a,n in [(.1,40),(1,50),(10,50)]]
    raise KeyError(f)


def sorted_ok(r,component):
    p,s=PRIMARY[component],SECONDARY[component]
    q=r[(r.fit_failures==0)&r[p].notna()&r[s].notna()].copy()
    return q.sort_values([p,s,"candidate_id"]).reset_index(drop=True)


def retain(r,component,limit):
    q=sorted_ok(r,component)
    if q.empty: return q
    picked=[]
    for _,g in q.groupby("family",sort=False): picked.append(g.index[0])
    for _,g in q.groupby("feature_set",sort=False):
        if g.index[0] not in picked: picked.append(g.index[0])
    for i in q.index:
        if i not in picked: picked.append(i)
        if len(picked)>=limit: break
    z=q.loc[sorted(set(picked))].sort_values([PRIMARY[component],SECONDARY[component],"candidate_id"])
    return z.head(limit).reset_index(drop=True)


def eval_specs(specs,d,folds,n_jobs,label):
    rows=[]
    for i,s in enumerate(specs,1):
        r=evaluate(s,d,folds,n_jobs); rows.append(r); print(f"{label}: {i}/{len(specs)} {s.family} {s.feature_set} {'ok' if not r['fit_failures'] else 'FAIL'}",flush=True)
    return pd.DataFrame(rows)


def refine_specs(retained):
    out=[]; seen=set()
    for row in retained.itertuples(index=False):
        base=Spec(row.component,row.family,row.feature_set,json.loads(row.params),"refine")
        for p in [base.params,*variants(base)]:
            s=Spec(base.component,base.family,base.feature_set,p,"refine")
            if s.candidate_id not in seen: seen.add(s.candidate_id); out.append(s)
    return out


def baselines(d,folds):
    ix=np.concatenate([v for _,v in folds]); v=d.iloc[ix]; out=[]; y=v.actual_home_win.to_numpy(float)
    for n,c in [("drat_raw","drat_home_win_prob"),("sdv_raw","sdv_home_win_prob")]: out.append({"component":"moneyline","baseline":n,"rows":len(v),"log_loss":ll(y,v[c]),"brier":brier(y,v[c]),"rmse":np.nan,"mae":np.nan})
    for comp,yc,a,b in [("margin","actual_margin","drat_exp_margin","sdv_exp_margin"),("total","actual_total","drat_exp_total","sdv_exp_total")]:
        y=v[yc].to_numpy(float)
        for n,c in [("drat_raw",a),("sdv_raw",b)]: out.append({"component":comp,"baseline":n,"rows":len(v),"log_loss":np.nan,"brier":np.nan,"rmse":rmse(y,v[c]),"mae":mae(y,v[c])})
    return pd.DataFrame(out)


def bundles(final):
    groups={}
    for c in ("moneyline","margin","total"):
        g=sorted_ok(final[final.component==c],c).copy(); g["rank"]=np.arange(1,len(g)+1); best=float(g.iloc[0][PRIMARY[c]]); g["gap"]=g[PRIMARY[c]]/best-1; groups[c]=g
    out=[]
    for a,b,c in itertools.product(groups["moneyline"].to_dict("records"),groups["margin"].to_dict("records"),groups["total"].to_dict("records")):
        out.append({"moneyline_candidate":a["candidate_id"],"margin_candidate":b["candidate_id"],"total_candidate":c["candidate_id"],"moneyline_log_loss":a["log_loss"],"moneyline_brier":a["brier"],"margin_rmse":b["rmse"],"margin_mae":b["mae"],"total_rmse":c["rmse"],"total_mae":c["mae"],"moneyline_rank":a["rank"],"margin_rank":b["rank"],"total_rank":c["rank"],"rank_sum":a["rank"]+b["rank"]+c["rank"],"relative_primary_gap_sum":a["gap"]+b["gap"]+c["gap"]})
    return pd.DataFrame(out).sort_values(["relative_primary_gap_sum","rank_sum"]).reset_index(drop=True)


def main():
    a=args(); d,files=load_history(a.season); folds,foldmeta=make_folds(d,a.min_train_rows,a.validation_folds); out=a.output_dir.resolve(); out.mkdir(parents=True,exist_ok=True)
    pd.DataFrame(foldmeta).to_csv(out/"item11_validation_folds.csv",index=False); baselines(d,folds).to_csv(out/"item11_baselines.csv",index=False)
    broad_all=[]; kept_all=[]; refine_all=[]; final_all=[]
    for comp in ("moneyline","margin","total"):
        broad=eval_specs(broad_specs(comp),d,folds,a.n_jobs,f"broad/{comp}"); broad_all.append(broad)
        required_families = {x.family for x in broad_specs(comp)}
        successful_families = set(broad.loc[broad.fit_failures == 0, "family"])
        missing_families = sorted(required_families - successful_families)
        if missing_families:
            raise RuntimeError(f"Entire required {comp} model families failed: {missing_families}")
        kept=retain(broad,comp,a.retain_broad)
        if kept.empty: raise RuntimeError(f"All broad {comp} candidates failed")
        kept_all.append(kept.assign(retained_stage="broad"))
        refined=eval_specs(refine_specs(kept),d,folds,a.n_jobs,f"refine/{comp}"); refine_all.append(refined)
        pool=pd.concat([kept,refined],ignore_index=True).drop_duplicates("candidate_id",keep="first")
        final=retain(pool,comp,a.retain_final)
        if final.empty: raise RuntimeError(f"No final {comp} candidates")
        final_all.append(final.assign(retained_stage="final"))
    broad=pd.concat(broad_all,ignore_index=True); kept=pd.concat(kept_all,ignore_index=True); refined=pd.concat(refine_all,ignore_index=True); final=pd.concat(final_all,ignore_index=True)
    failures=pd.concat([broad[broad.fit_failures>0],refined[refined.fit_failures>0]],ignore_index=True); combo=bundles(final)
    broad.to_csv(out/"item11_broad_results.csv",index=False); kept.to_csv(out/"item11_broad_retained.csv",index=False); refined.to_csv(out/"item11_refinement_results.csv",index=False); final.to_csv(out/"item11_final_retained_components.csv",index=False); failures.to_csv(out/"item11_fit_failures.csv",index=False); combo.to_csv(out/"item11_retained_component_combinations.csv",index=False)
    report={"script_version":VERSION,"status":"COMPLETE","purpose":"Item 11 staged research search; no automatic production promotion.","input_files":[str(x.resolve()) for x in files],"history_rows":len(d),"history_first_date":str(d._date.min().date()),"history_last_date":str(d._date.max().date()),"walk_forward_rule":"training_date < first_validation_date for every fold","same_day_outcomes_allowed_in_training":False,"future_outcomes_allowed_in_training":False,"validation_folds":foldmeta,"feature_whitelist":PREGAME,"feature_sets":FEATURES,"broad_candidates":len(broad),"broad_fit_failures":int(broad.fit_failures.sum()),"refinement_candidates":len(refined),"refinement_fit_failures":int(refined.fit_failures.sum()),"final_retained_counts":{c:int((final.component==c).sum()) for c in TARGET},"retained_component_combinations":len(combo),"selection_metrics":{"moneyline":["log_loss","brier"],"margin":["rmse","mae"],"total":["rmse","mae"]},"promotion_decision":None}
    (out/"item11_report.json").write_text(json.dumps(report,indent=2,sort_keys=True),encoding="utf-8")
    print(f"Item 11 staged model search complete: history_rows={len(d)} broad_candidates={len(broad)} refinement_candidates={len(refined)} fit_failures={len(failures)} retained_bundles={len(combo)}")
    print(f"Final components: {out/'item11_final_retained_components.csv'}"); print(f"Bundles: {out/'item11_retained_component_combinations.csv'}"); print(f"Report: {out/'item11_report.json'}")

if __name__=="__main__": main()
