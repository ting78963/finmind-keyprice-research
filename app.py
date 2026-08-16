from __future__ import annotations

import json
import os
import shutil
import threading
import time
import uuid
from datetime import datetime
from pathlib import Path

import pandas as pd
import requests
from flask import Flask, jsonify, render_template, request, send_file

app = Flask(__name__)

API = "https://api.finmindtrade.com/api/v4/data"
DATA_ROOT = Path(os.environ.get("DATA_ROOT", "/tmp/finmind_web"))
DATA_ROOT.mkdir(parents=True, exist_ok=True)

# FinMind Sponsor 6000 calls/hour ≈ 1.67 req/sec.
# 0.65 秒/次稍微保守，避免撞限額。
API_SLEEP = float(os.environ.get("FINMIND_API_SLEEP", "0.65"))

jobs = {}
jobs_lock = threading.Lock()


class FinMindClient:
    def __init__(self, token: str):
        if not token:
            raise RuntimeError("Render 尚未設定 FINMIND_TOKEN 環境變數")
        self.token = token
        self.session = requests.Session()

    def get(self, dataset: str, **params) -> pd.DataFrame:
        q = {"dataset": dataset, "token": self.token, **params}
        for attempt in range(7):
            r = self.session.get(API, params=q, timeout=90)
            if r.status_code == 429:
                wait = min(120, 3 * (2 ** attempt))
                time.sleep(wait)
                continue
            r.raise_for_status()
            payload = r.json()
            if payload.get("status") != 200:
                raise RuntimeError(
                    f"FinMind {dataset}: {payload.get('msg', payload)}"
                )
            time.sleep(API_SLEEP)
            return pd.DataFrame(payload.get("data", []))
        raise RuntimeError("FinMind API 持續回傳 429，請稍後再試")


def update_job(job_id, **kwargs):
    with jobs_lock:
        jobs.setdefault(job_id, {}).update(kwargs)


def get_job(job_id):
    with jobs_lock:
        return dict(jobs.get(job_id, {}))


def pct(x, total):
    if total <= 0:
        return 0
    return int(min(100, max(0, x / total * 100)))


def get_stock_universe(client: FinMindClient) -> set[str]:
    df = client.get("TaiwanStockInfo")
    if df.empty:
        raise RuntimeError("TaiwanStockInfo 無資料")

    df["stock_id"] = df["stock_id"].astype(str)
    if "date" in df.columns:
        df["date"] = pd.to_datetime(df["date"], errors="coerce")
        df = (
            df.sort_values("date")
              .groupby("stock_id", as_index=False)
              .tail(1)
        )

    # 第一版保守處理：只留 4 位純數字。
    df = df[df["stock_id"].str.fullmatch(r"\d{4}", na=False)].copy()

    # 若有市場類型，盡量只留上市櫃。
    if "type" in df.columns:
        allowed = {"twse", "tpex"}
        mask = df["type"].astype(str).str.lower().isin(allowed)
        if mask.any():
            df = df[mask]

    # 排除名稱/產業明顯為 ETF 等非普通股標的。
    text_cols = [c for c in ["stock_name", "industry_category"] if c in df.columns]
    if text_cols:
        text = df[text_cols].fillna("").astype(str).agg(" ".join, axis=1)
        bad = text.str.contains("ETF|ETN|權證|受益證券|存託憑證", case=False, regex=True)
        df = df[~bad]

    return set(df["stock_id"].astype(str))


def fetch_daily_all(client: FinMindClient, start: str, end: str, out_dir: Path, job_id: str):
    daily_dir = out_dir / "daily"
    daily_dir.mkdir(parents=True, exist_ok=True)

    days = pd.date_range(start, end, freq="D")
    pieces = []

    for i, d in enumerate(days, 1):
        day = d.strftime("%Y-%m-%d")
        update_job(job_id, message=f"抓日線 {day}", progress=5 + pct(i, len(days)) * 0.15)

        # 明確限制只抓這一天，避免 start_date 單獨使用時抓到之後所有日期。
        df = client.get(
            "TaiwanStockPrice",
            start_date=day,
            end_date=day,
        )

        if df.empty:
            continue

        df.to_csv(daily_dir / f"{day}.csv", index=False, encoding="utf-8-sig")
        pieces.append(df)

    if not pieces:
        raise RuntimeError("指定期間沒有抓到 TaiwanStockPrice")

    daily = pd.concat(pieces, ignore_index=True)
    daily["stock_id"] = daily["stock_id"].astype(str)
    daily["date"] = pd.to_datetime(daily["date"]).dt.normalize()
    return daily


def build_candidates(daily: pd.DataFrame, universe: set[str], start: str, end: str, screen_pct: float):
    d = daily[daily["stock_id"].isin(universe)].copy()

    for col in ["max", "close"]:
        if col not in d.columns:
            raise RuntimeError(f"TaiwanStockPrice 缺少欄位 {col}")

    d = d.sort_values(["stock_id", "date"])
    d["prev_close"] = d.groupby("stock_id")["close"].shift(1)
    d["day_high_pct"] = (d["max"] / d["prev_close"] - 1) * 100

    s = pd.Timestamp(start)
    e = pd.Timestamp(end)
    c = d[
        d["prev_close"].notna()
        & (d["date"] >= s)
        & (d["date"] <= e)
        & (d["day_high_pct"] >= screen_pct)
    ].copy()

    keep = [c for c in [
        "date", "stock_id", "prev_close", "open", "max", "min", "close",
        "Trading_Volume", "Trading_money", "day_high_pct"
    ] if c in c.columns]

    return c[keep].sort_values(["date", "stock_id"])


def fetch_kbar(client: FinMindClient, stock_id: str, day: str) -> pd.DataFrame:
    return client.get(
        "TaiwanStockKBar",
        data_id=stock_id,
        start_date=day,
    )


def first5(df: pd.DataFrame):
    if df.empty or "minute" not in df.columns:
        return None
    first_minutes = {"09:00:00", "09:01:00", "09:02:00", "09:03:00", "09:04:00"}
    x = df[df["minute"].astype(str).isin(first_minutes)].copy()
    if x.empty:
        return None
    return {
        "high": float(pd.to_numeric(x["high"], errors="coerce").max()),
        "volume": float(pd.to_numeric(x["volume"], errors="coerce").sum()),
    }


def previous_days_for_stock(daily: pd.DataFrame, sid: str, day: pd.Timestamp, n: int):
    ds = (
        daily[(daily["stock_id"] == sid) & (daily["date"] < day)]["date"]
        .drop_duplicates()
        .sort_values()
        .tail(n)
    )
    return [pd.Timestamp(x) for x in ds]


def run_job(job_id: str, cfg: dict):
    try:
        token = os.environ.get("FINMIND_TOKEN", "").strip()
        client = FinMindClient(token)

        start = cfg["start"]
        end = cfg["end"]
        screen_pct = float(cfg["screen_pct"])
        warmup_days = int(cfg["warmup_days"])

        job_dir = DATA_ROOT / job_id
        if job_dir.exists():
            shutil.rmtree(job_dir)
        job_dir.mkdir(parents=True)

        update_job(job_id, status="running", progress=1, message="讀取台股標的清單…")
        universe = get_stock_universe(client)

        # 日線往前多抓 45 天，讓第一天也能有 prev_close + RVOL warmup。
        daily_start = (pd.Timestamp(start) - pd.Timedelta(days=45)).strftime("%Y-%m-%d")
        daily = fetch_daily_all(client, daily_start, end, job_dir, job_id)

        update_job(job_id, progress=22, message="建立候選股票清單…")
        candidates = build_candidates(daily, universe, start, end, screen_pct)
        candidates.to_csv(job_dir / "candidates.csv", index=False, encoding="utf-8-sig")

        if candidates.empty:
            raise RuntimeError("沒有符合篩選條件的 stock-day")

        # 所有 KBar 任務：候選日 + 該股票之前 warmup_days 天。
        tasks = set()
        for r in candidates.itertuples(index=False):
            sid = str(r.stock_id)
            day = pd.Timestamp(r.date).normalize()
            tasks.add((sid, day.strftime("%Y-%m-%d"), True))
            for hd in previous_days_for_stock(daily, sid, day, warmup_days):
                tasks.add((sid, hd.strftime("%Y-%m-%d"), False))

        # 同一 sid/day 若同時候選與 warmup，只保留 candidate=True
        merged = {}
        for sid, day, is_candidate in tasks:
            merged[(sid, day)] = merged.get((sid, day), False) or is_candidate
        tasks2 = [(sid, day, cand) for (sid, day), cand in merged.items()]
        tasks2.sort(key=lambda x: (x[1], x[0]))

        kbar_dir = job_dir / "kbar"
        kbar_dir.mkdir(exist_ok=True)

        first5_cache = {}
        total = len(tasks2)

        for i, (sid, day, is_candidate) in enumerate(tasks2, 1):
            update_job(
                job_id,
                progress=25 + int(55 * i / max(total, 1)),
                message=f"抓 1分K {i}/{total}｜{sid} {day}"
            )

            df = fetch_kbar(client, sid, day)
            if df.empty:
                continue

            first5_cache[(sid, day)] = first5(df)

            if is_candidate:
                p = kbar_dir / day
                p.mkdir(exist_ok=True)
                df.to_csv(p / f"{sid}.csv", index=False, encoding="utf-8-sig")

        # 建立 research_base
        update_job(job_id, progress=82, message="計算開盤5分鐘漲幅與 RVOL5…")
        rows = []

        for r in candidates.itertuples(index=False):
            sid = str(r.stock_id)
            day = pd.Timestamp(r.date).normalize()
            day_s = day.strftime("%Y-%m-%d")
            cur = first5_cache.get((sid, day_s))
            if not cur:
                continue

            prev_close = float(r.prev_close)
            f5_high_pct = (cur["high"] / prev_close - 1) * 100

            hist = []
            for hd in previous_days_for_stock(daily, sid, day, warmup_days):
                h = first5_cache.get((sid, hd.strftime("%Y-%m-%d")))
                if h and h["volume"] > 0:
                    hist.append(h["volume"])

            hist_mean = float(pd.Series(hist).mean()) if hist else None
            rvol5 = cur["volume"] / hist_mean if hist_mean and hist_mean > 0 else None

            rows.append({
                "date": day_s,
                "stock_id": sid,
                "prev_close": prev_close,
                "day_high_pct": float(r.day_high_pct),
                "first5m_high_pct": f5_high_pct,
                "first5m_volume": cur["volume"],
                "rvol5_hist_n": len(hist),
                "rvol5_hist_mean": hist_mean,
                "rvol5": rvol5,
                "early_3pct": f5_high_pct >= 3.0,
                "early_3_5pct": f5_high_pct >= 3.5,
                "early_4pct": f5_high_pct >= 4.0,
                "early_4_5pct": f5_high_pct >= 4.5,
                "early_5pct": f5_high_pct >= 5.0,
            })

        research = pd.DataFrame(rows)
        research.to_csv(job_dir / "research_base.csv", index=False, encoding="utf-8-sig")

        meta = {
            "created_at": datetime.now().isoformat(timespec="seconds"),
            "start": start,
            "end": end,
            "screen_pct": screen_pct,
            "warmup_days": warmup_days,
            "candidate_events": len(candidates),
            "research_rows": len(research),
            "kbar_tasks": total,
        }
        (job_dir / "metadata.json").write_text(
            json.dumps(meta, ensure_ascii=False, indent=2),
            encoding="utf-8"
        )

        zip_path = DATA_ROOT / f"{job_id}.zip"
        if zip_path.exists():
            zip_path.unlink()
        shutil.make_archive(str(zip_path.with_suffix("")), "zip", job_dir)

        update_job(
            job_id,
            status="done",
            progress=100,
            message="完成，可以下載 ZIP",
            download=f"/download/{job_id}",
            meta=meta,
        )

    except Exception as e:
        update_job(
            job_id,
            status="error",
            progress=100,
            message=str(e),
        )


@app.get("/")
def index():
    return render_template("index.html")


@app.post("/start")
def start_job():
    payload = request.get_json(force=True)
    start = payload.get("start", "2026-05-01")
    end = payload.get("end", "2026-08-14")
    screen_pct = float(payload.get("screen_pct", 3.0))
    warmup_days = int(payload.get("warmup_days", 20))

    if pd.Timestamp(end) < pd.Timestamp(start):
        return jsonify({"error": "結束日期不能早於開始日期"}), 400

    job_id = uuid.uuid4().hex[:12]
    update_job(
        job_id,
        status="queued",
        progress=0,
        message="排隊中…",
        cfg={
            "start": start,
            "end": end,
            "screen_pct": screen_pct,
            "warmup_days": warmup_days,
        }
    )

    t = threading.Thread(
        target=run_job,
        args=(job_id, {
            "start": start,
            "end": end,
            "screen_pct": screen_pct,
            "warmup_days": warmup_days,
        }),
        daemon=True,
    )
    t.start()

    return jsonify({"job_id": job_id})


@app.get("/status/<job_id>")
def status(job_id):
    job = get_job(job_id)
    if not job:
        return jsonify({"error": "找不到工作"}), 404
    return jsonify(job)


@app.get("/download/<job_id>")
def download(job_id):
    zip_path = DATA_ROOT / f"{job_id}.zip"
    if not zip_path.exists():
        return "檔案尚未完成或已被 Render 重啟清除", 404
    return send_file(
        zip_path,
        as_attachment=True,
        download_name=f"finmind_research_{job_id}.zip",
    )


@app.get("/health")
def health():
    return {"ok": True}


if __name__ == "__main__":
    port = int(os.environ.get("PORT", "10000"))
    app.run(host="0.0.0.0", port=port, debug=False)
