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
DATA_ROOT = Path(os.environ.get("DATA_ROOT", "/tmp/finmind_keyprice_v33"))
DATA_ROOT.mkdir(parents=True, exist_ok=True)

# 預設 0.2 秒；若 Render Environment 設有 FINMIND_API_SLEEP，會以環境變數為準。
API_SLEEP = float(os.environ.get("FINMIND_API_SLEEP", "0.2"))

jobs = {}
jobs_lock = threading.Lock()


class JobCancelled(Exception):
    pass


class FinMindClient:
    def __init__(self, token: str, job_id: str):
        if not token:
            raise RuntimeError("Render 尚未設定 FINMIND_TOKEN")
        self.token = token
        self.job_id = job_id
        self.session = requests.Session()

    def get(self, dataset: str, **params) -> pd.DataFrame:
        check_cancel(self.job_id)
        q = {"dataset": dataset, "token": self.token, **params}

        for attempt in range(7):
            check_cancel(self.job_id)
            r = self.session.get(API, params=q, timeout=90)

            if r.status_code == 429:
                wait = min(120, 3 * (2 ** attempt))
                end = time.time() + wait
                while time.time() < end:
                    check_cancel(self.job_id)
                    time.sleep(0.5)
                continue

            r.raise_for_status()
            payload = r.json()

            if payload.get("status") != 200:
                raise RuntimeError(
                    f"FinMind {dataset}: {payload.get('msg', payload)}"
                )

            if API_SLEEP > 0:
                time.sleep(API_SLEEP)

            return pd.DataFrame(payload.get("data", []))

        raise RuntimeError("FinMind API 持續回傳 429，請稍後再試")


def update_job(job_id, **kwargs):
    with jobs_lock:
        jobs.setdefault(job_id, {}).update(kwargs)


def get_job(job_id):
    with jobs_lock:
        return dict(jobs.get(job_id, {}))


def active_job():
    with jobs_lock:
        for jid, job in jobs.items():
            if job.get("status") in {"queued", "running"}:
                return jid
    return None


def check_cancel(job_id):
    job = get_job(job_id)

    if not job:
        raise JobCancelled("工作不存在")

    if job.get("cancel_requested"):
        raise JobCancelled("工作已停止")


def get_universe(client: FinMindClient) -> set[str]:
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

    df = df[df["stock_id"].str.fullmatch(r"\d{4}", na=False)].copy()

    if "type" in df.columns:
        market_mask = df["type"].astype(str).str.lower().isin({"twse", "tpex"})
        if market_mask.any():
            df = df[market_mask]

    text_cols = [c for c in ["stock_name", "industry_category"] if c in df.columns]

    if text_cols:
        text = df[text_cols].fillna("").astype(str).agg(" ".join, axis=1)
        bad = text.str.contains(
            "ETF|ETN|權證|受益證券|存託憑證",
            case=False,
            regex=True
        )
        df = df[~bad]

    return set(df["stock_id"])


def get_volume_column(df: pd.DataFrame) -> str:
    for c in ["Trading_Volume", "Trading_volume", "volume"]:
        if c in df.columns:
            return c
    raise RuntimeError("TaiwanStockPrice 找不到成交量欄位")


def fetch_daily_range_with_prev(
    client: FinMindClient,
    start: str,
    end: str,
    job_id: str,
) -> pd.DataFrame:
    """
    只多抓開始日前 7 個曆日，用來取得前一交易日收盤。
    不抓任何歷史 1 分 K。
    """
    fetch_start = (pd.Timestamp(start) - pd.Timedelta(days=7)).strftime("%Y-%m-%d")
    days = pd.date_range(fetch_start, end, freq="D")
    parts = []

    for i, d in enumerate(days, 1):
        check_cancel(job_id)

        day = d.strftime("%Y-%m-%d")

        update_job(
            job_id,
            progress=5 + int(20 * i / max(len(days), 1)),
            message=f"抓日線 {day}"
        )

        df = client.get(
            "TaiwanStockPrice",
            start_date=day,
            end_date=day,
        )

        if not df.empty:
            df["stock_id"] = df["stock_id"].astype(str)
            df["date"] = pd.to_datetime(df["date"]).dt.normalize()
            parts.append(df)

    if not parts:
        raise RuntimeError("指定期間沒有日線資料")

    return pd.concat(parts, ignore_index=True)


def build_candidates(
    daily: pd.DataFrame,
    universe: set[str],
    start: str,
    end: str,
    screen_pct: float,
    min_lots: float,
) -> pd.DataFrame:
    d = daily[daily["stock_id"].isin(universe)].copy()

    if "max" not in d.columns or "close" not in d.columns:
        raise RuntimeError("TaiwanStockPrice 缺少 max / close 欄位")

    vol_col = get_volume_column(d)

    d["day_lots"] = (
        pd.to_numeric(d[vol_col], errors="coerce").fillna(0) / 1000.0
    )

    d = d.sort_values(["stock_id", "date"]).copy()

    d["prev_close"] = d.groupby("stock_id")["close"].shift(1)

    d["day_high_pct"] = (
        pd.to_numeric(d["max"], errors="coerce")
        / pd.to_numeric(d["prev_close"], errors="coerce")
        - 1
    ) * 100

    start_ts = pd.Timestamp(start)
    end_ts = pd.Timestamp(end)

    c = d[
        (d["date"] >= start_ts)
        & (d["date"] <= end_ts)
        & d["prev_close"].notna()
        & (d["day_lots"] >= min_lots)
        & (d["day_high_pct"] >= screen_pct)
    ].copy()

    keep = [
        "date",
        "stock_id",
        "prev_close",
        "day_high_pct",
        "day_lots",
        "open",
        "max",
        "min",
        "close",
    ]
    keep = [x for x in keep if x in c.columns]

    c = c[keep].sort_values(["date", "stock_id"])
    c["date"] = pd.to_datetime(c["date"]).dt.strftime("%Y-%m-%d")

    return c


def run_job(job_id: str, cfg: dict):
    job_dir = DATA_ROOT / job_id

    try:
        client = FinMindClient(
            os.environ.get("FINMIND_TOKEN", "").strip(),
            job_id
        )

        job_dir.mkdir(parents=True, exist_ok=True)

        update_job(
            job_id,
            status="running",
            progress=2,
            message="讀取台股標的清單…"
        )

        universe = get_universe(client)

        daily = fetch_daily_range_with_prev(
            client=client,
            start=cfg["start"],
            end=cfg["end"],
            job_id=job_id,
        )

        update_job(
            job_id,
            progress=28,
            message="依漲幅＋成交量建立候選清單…"
        )

        candidates = build_candidates(
            daily=daily,
            universe=universe,
            start=cfg["start"],
            end=cfg["end"],
            screen_pct=cfg["screen_pct"],
            min_lots=cfg["min_lots"],
        )

        if candidates.empty:
            raise RuntimeError("沒有股票同時符合漲幅與成交量門檻")

        candidates.to_csv(
            job_dir / "candidates.csv",
            index=False,
            encoding="utf-8-sig"
        )

        kbar_root = job_dir / "kbar"
        kbar_root.mkdir(exist_ok=True)

        total = len(candidates)

        for i, row in enumerate(candidates.itertuples(index=False), 1):
            check_cancel(job_id)

            sid = str(row.stock_id)
            day = str(row.date)

            update_job(
                job_id,
                progress=30 + int(65 * i / max(total, 1)),
                message=f"抓當日 1分K {i}/{total}｜{sid} {day}"
            )

            kbar = client.get(
                "TaiwanStockKBar",
                data_id=sid,
                start_date=day,
            )

            if not kbar.empty:
                day_dir = kbar_root / day
                day_dir.mkdir(exist_ok=True)

                kbar.to_csv(
                    day_dir / f"{sid}.csv",
                    index=False,
                    encoding="utf-8-sig"
                )

        metadata = {
            "created_at": datetime.now().isoformat(timespec="seconds"),
            **cfg,
            "candidate_events": total,
            "api_sleep_seconds": API_SLEEP,
        }

        (job_dir / "metadata.json").write_text(
            json.dumps(metadata, ensure_ascii=False, indent=2),
            encoding="utf-8"
        )

        zip_path = DATA_ROOT / f"{job_id}.zip"

        if zip_path.exists():
            zip_path.unlink()

        shutil.make_archive(
            str(zip_path.with_suffix("")),
            "zip",
            job_dir
        )

        update_job(
            job_id,
            status="done",
            progress=100,
            message=f"完成：{total} 筆候選",
            download=f"/download/{job_id}",
            meta=metadata,
        )

    except JobCancelled as e:
        shutil.rmtree(job_dir, ignore_errors=True)

        update_job(
            job_id,
            status="cancelled",
            progress=0,
            message=str(e),
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
def start():
    running = active_job()

    if running:
        return jsonify({
            "error": "目前已有工作正在執行，請先停止或等待完成。"
        }), 409

    payload = request.get_json(force=True)

    cfg = {
        "start": payload.get("start"),
        "end": payload.get("end"),
        "screen_pct": float(payload.get("screen_pct", 4)),
        "min_lots": float(payload.get("min_lots", 4000)),
    }

    if not cfg["start"] or not cfg["end"]:
        return jsonify({"error": "請選擇日期"}), 400

    if pd.Timestamp(cfg["end"]) < pd.Timestamp(cfg["start"]):
        return jsonify({"error": "結束日期不能早於開始日期"}), 400

    job_id = uuid.uuid4().hex[:12]

    update_job(
        job_id,
        status="queued",
        progress=0,
        message="排隊中…",
        cancel_requested=False,
        cfg=cfg,
    )

    threading.Thread(
        target=run_job,
        args=(job_id, cfg),
        daemon=True
    ).start()

    return jsonify({"job_id": job_id})


@app.post("/cancel/<job_id>")
def cancel(job_id):
    if not get_job(job_id):
        return jsonify({"error": "找不到工作"}), 404

    update_job(
        job_id,
        cancel_requested=True,
        message="正在停止…"
    )

    return jsonify({"ok": True})


@app.get("/status/<job_id>")
def status(job_id):
    job = get_job(job_id)

    if not job:
        return jsonify({"error": "找不到工作"}), 404

    return jsonify(job)


@app.get("/download/<job_id>")
def download(job_id):
    path = DATA_ROOT / f"{job_id}.zip"

    if not path.exists():
        return "檔案不存在", 404

    return send_file(
        path,
        as_attachment=True,
        download_name=f"finmind_keyprice_{job_id}.zip"
    )


@app.get("/health")
def health():
    return {"ok": True}


if __name__ == "__main__":
    port = int(os.environ.get("PORT", "10000"))
    app.run(host="0.0.0.0", port=port, debug=False)
