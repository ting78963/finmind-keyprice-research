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
DATA_ROOT = Path(os.environ.get("DATA_ROOT", "/tmp/finmind_keyprice_v2"))
DATA_ROOT.mkdir(parents=True, exist_ok=True)

API_SLEEP = float(os.environ.get("FINMIND_API_SLEEP", "0.65"))
HEARTBEAT_TIMEOUT = float(os.environ.get("JOB_HEARTBEAT_TIMEOUT", "20"))

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
                for _ in range(int(wait * 2)):
                    check_cancel(self.job_id)
                    time.sleep(0.5)
                continue

            r.raise_for_status()
            payload = r.json()

            if payload.get("status") != 200:
                raise RuntimeError(
                    f"FinMind {dataset}: {payload.get('msg', payload)}"
                )

            # 分段 sleep，讓取消可以快速生效
            slept = 0.0
            while slept < API_SLEEP:
                check_cancel(self.job_id)
                step = min(0.25, API_SLEEP - slept)
                time.sleep(step)
                slept += step

            return pd.DataFrame(payload.get("data", []))

        raise RuntimeError("FinMind API 持續回傳 429，請稍後再試")


def now_ts():
    return time.time()


def update_job(job_id, **kwargs):
    with jobs_lock:
        jobs.setdefault(job_id, {}).update(kwargs)


def get_job(job_id):
    with jobs_lock:
        return dict(jobs.get(job_id, {}))


def any_active_job():
    with jobs_lock:
        for jid, j in jobs.items():
            if j.get("status") in {"queued", "running"}:
                return jid
    return None


def check_cancel(job_id):
    job = get_job(job_id)
    if not job:
        raise JobCancelled("工作不存在")

    if job.get("cancel_requested"):
        raise JobCancelled("使用者已停止工作")

    # 網頁若關閉/失聯，心跳逾時自動停止。
    last_heartbeat = job.get("last_heartbeat")
    if last_heartbeat and (now_ts() - last_heartbeat > HEARTBEAT_TIMEOUT):
        update_job(job_id, cancel_requested=True)
        raise JobCancelled("網頁已關閉或失聯，已自動停止工作")


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

    # 保守：只留 4 位純數字普通股候選
    df = df[df["stock_id"].str.fullmatch(r"\d{4}", na=False)].copy()

    if "type" in df.columns:
        allowed = {"twse", "tpex"}
        mask = df["type"].astype(str).str.lower().isin(allowed)
        if mask.any():
            df = df[mask]

    text_cols = [c for c in ["stock_name", "industry_category"] if c in df.columns]
    if text_cols:
        text = df[text_cols].fillna("").astype(str).agg(" ".join, axis=1)
        bad = text.str.contains(
            "ETF|ETN|權證|受益證券|存託憑證",
            case=False,
            regex=True
        )
        df = df[~bad]

    return set(df["stock_id"].astype(str))


def fetch_daily_for_selected_dates(
    client: FinMindClient,
    start: str,
    end: str,
    out_dir: Path,
    job_id: str,
) -> pd.DataFrame:
    daily_dir = out_dir / "daily"
    daily_dir.mkdir(parents=True, exist_ok=True)

    days = pd.date_range(start, end, freq="D")
    pieces = []

    for i, d in enumerate(days, 1):
        check_cancel(job_id)

        day = d.strftime("%Y-%m-%d")
        progress = 5 + int(15 * i / max(len(days), 1))
        update_job(
            job_id,
            message=f"抓日線 {day}",
            progress=progress,
        )

        # 只抓選定日期，不往前抓歷史資料
        df = client.get(
            "TaiwanStockPrice",
            start_date=day,
            end_date=day,
        )

        if df.empty:
            continue

        df.to_csv(
            daily_dir / f"{day}.csv",
            index=False,
            encoding="utf-8-sig"
        )
        pieces.append(df)

    if not pieces:
        raise RuntimeError("指定期間沒有抓到 TaiwanStockPrice")

    daily = pd.concat(pieces, ignore_index=True)
    daily["stock_id"] = daily["stock_id"].astype(str)
    daily["date"] = pd.to_datetime(daily["date"]).dt.normalize()

    return daily


def fetch_previous_close_for_stock_day(
    client: FinMindClient,
    stock_id: str,
    day: pd.Timestamp,
) -> float | None:
    """
    為了計算當日漲幅，只需要前一個交易日收盤。
    不抓 5/20 天歷史分鐘資料。
    往前最多找 7 個曆日，直到有資料。
    """
    for delta in range(1, 8):
        prev_day = (day - pd.Timedelta(days=delta)).strftime("%Y-%m-%d")
        df = client.get(
            "TaiwanStockPrice",
            data_id=stock_id,
            start_date=prev_day,
            end_date=prev_day,
        )
        if not df.empty and "close" in df.columns:
            try:
                return float(df.iloc[-1]["close"])
            except Exception:
                pass
    return None


def build_candidates(
    client: FinMindClient,
    daily: pd.DataFrame,
    universe: set[str],
    screen_pct: float,
    job_id: str,
) -> pd.DataFrame:
    """
    使用當日最高價 / 前一交易日收盤價做候選篩選。
    為避免抓所有股票前收，先用日線內可用資料。
    若資料沒有 reference/昨日收盤欄位，才逐檔補前收。
    """
    d = daily[daily["stock_id"].isin(universe)].copy()

    if "max" not in d.columns:
        raise RuntimeError("TaiwanStockPrice 缺少 max 欄位")

    prev_col = None
    for c in ["reference_price", "yesterday_close", "previous_close"]:
        if c in d.columns:
            prev_col = c
            break

    rows = []
    total = len(d)

    for i, r in enumerate(d.itertuples(index=False), 1):
        check_cancel(job_id)

        sid = str(getattr(r, "stock_id"))
        day = pd.Timestamp(getattr(r, "date")).normalize()
        high = float(getattr(r, "max"))

        if prev_col:
            prev_close = getattr(r, prev_col, None)
            try:
                prev_close = float(prev_close)
            except Exception:
                prev_close = None
        else:
            prev_close = None

        # 若當日日線本身沒有前收欄位，逐檔補前收。
        # 只在需要時呼叫 API。
        if not prev_close or prev_close <= 0:
            if i % 25 == 0 or i == 1:
                update_job(
                    job_id,
                    progress=20 + int(20 * i / max(total, 1)),
                    message=f"確認前收 {i}/{total}｜{sid}"
                )
            prev_close = fetch_previous_close_for_stock_day(
                client, sid, day
            )

        if not prev_close or prev_close <= 0:
            continue

        day_high_pct = (high / prev_close - 1.0) * 100

        if day_high_pct >= screen_pct:
            row = {
                "date": day.strftime("%Y-%m-%d"),
                "stock_id": sid,
                "prev_close": prev_close,
                "day_high_pct": day_high_pct,
            }

            for col in [
                "open", "max", "min", "close",
                "Trading_Volume", "Trading_money"
            ]:
                if hasattr(r, col):
                    row[col] = getattr(r, col)

            rows.append(row)

    return pd.DataFrame(rows)


def fetch_kbar(
    client: FinMindClient,
    stock_id: str,
    day: str
) -> pd.DataFrame:
    return client.get(
        "TaiwanStockKBar",
        data_id=stock_id,
        start_date=day,
    )


def first5_stats(df: pd.DataFrame, prev_close: float):
    first_minutes = {
        "09:00:00", "09:01:00", "09:02:00",
        "09:03:00", "09:04:00"
    }

    x = df[df["minute"].astype(str).isin(first_minutes)].copy()

    if x.empty:
        return None

    high = float(pd.to_numeric(x["high"], errors="coerce").max())
    volume = float(pd.to_numeric(x["volume"], errors="coerce").sum())
    high_pct = (high / prev_close - 1.0) * 100

    return {
        "first5m_high": high,
        "first5m_high_pct": high_pct,
        "first5m_volume": volume,
    }


def run_job(job_id: str, cfg: dict):
    job_dir = DATA_ROOT / job_id

    try:
        token = os.environ.get("FINMIND_TOKEN", "").strip()
        client = FinMindClient(token, job_id)

        start = cfg["start"]
        end = cfg["end"]
        screen_pct = float(cfg["screen_pct"])

        if job_dir.exists():
            shutil.rmtree(job_dir)
        job_dir.mkdir(parents=True)

        update_job(
            job_id,
            status="running",
            progress=1,
            message="讀取台股標的清單…"
        )

        universe = get_stock_universe(client)

        check_cancel(job_id)

        daily = fetch_daily_for_selected_dates(
            client, start, end, job_dir, job_id
        )

        update_job(
            job_id,
            progress=20,
            message="建立強勢股候選清單…"
        )

        candidates = build_candidates(
            client=client,
            daily=daily,
            universe=universe,
            screen_pct=screen_pct,
            job_id=job_id,
        )

        if candidates.empty:
            raise RuntimeError("沒有符合預篩條件的 stock-day")

        candidates.to_csv(
            job_dir / "candidates.csv",
            index=False,
            encoding="utf-8-sig",
        )

        kbar_dir = job_dir / "kbar"
        kbar_dir.mkdir(exist_ok=True)

        research_rows = []
        total = len(candidates)

        for i, r in enumerate(candidates.itertuples(index=False), 1):
            check_cancel(job_id)

            sid = str(r.stock_id)
            day = str(r.date)

            update_job(
                job_id,
                progress=42 + int(50 * i / max(total, 1)),
                message=f"抓當日 1分K {i}/{total}｜{sid} {day}"
            )

            df = fetch_kbar(client, sid, day)

            if df.empty:
                continue

            p = kbar_dir / day
            p.mkdir(exist_ok=True)
            df.to_csv(
                p / f"{sid}.csv",
                index=False,
                encoding="utf-8-sig",
            )

            stats = first5_stats(df, float(r.prev_close))
            if stats:
                research_rows.append({
                    "date": day,
                    "stock_id": sid,
                    "prev_close": float(r.prev_close),
                    "day_high_pct": float(r.day_high_pct),
                    **stats,
                    "early_3pct": stats["first5m_high_pct"] >= 3.0,
                    "early_3_5pct": stats["first5m_high_pct"] >= 3.5,
                    "early_4pct": stats["first5m_high_pct"] >= 4.0,
                    "early_4_5pct": stats["first5m_high_pct"] >= 4.5,
                    "early_5pct": stats["first5m_high_pct"] >= 5.0,
                })

        research = pd.DataFrame(research_rows)
        research.to_csv(
            job_dir / "research_base.csv",
            index=False,
            encoding="utf-8-sig",
        )

        meta = {
            "created_at": datetime.now().isoformat(timespec="seconds"),
            "start": start,
            "end": end,
            "screen_pct": screen_pct,
            "candidate_events": len(candidates),
            "research_rows": len(research),
        }

        (job_dir / "metadata.json").write_text(
            json.dumps(meta, ensure_ascii=False, indent=2),
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
            message="完成，可以下載 ZIP",
            download=f"/download/{job_id}",
            meta=meta,
        )

    except JobCancelled as e:
        try:
            if job_dir.exists():
                shutil.rmtree(job_dir)
        except Exception:
            pass

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
def start_job():
    # 防止同時開多個工作，避免浪費 API / RAM
    active = any_active_job()
    if active:
        return jsonify({
            "error": "目前已有工作正在執行，請先停止或等待完成。",
            "active_job_id": active,
        }), 409

    payload = request.get_json(force=True)

    start = payload.get("start")
    end = payload.get("end")
    screen_pct = float(payload.get("screen_pct", 3.0))

    if not start or not end:
        return jsonify({"error": "請選擇開始與結束日期"}), 400

    if pd.Timestamp(end) < pd.Timestamp(start):
        return jsonify({"error": "結束日期不能早於開始日期"}), 400

    job_id = uuid.uuid4().hex[:12]

    update_job(
        job_id,
        status="queued",
        progress=0,
        message="排隊中…",
        cancel_requested=False,
        last_heartbeat=now_ts(),
        cfg={
            "start": start,
            "end": end,
            "screen_pct": screen_pct,
        },
    )

    t = threading.Thread(
        target=run_job,
        args=(job_id, {
            "start": start,
            "end": end,
            "screen_pct": screen_pct,
        }),
        daemon=True,
    )
    t.start()

    return jsonify({"job_id": job_id})


@app.post("/heartbeat/<job_id>")
def heartbeat(job_id):
    job = get_job(job_id)
    if not job:
        return jsonify({"error": "找不到工作"}), 404

    if job.get("status") in {"queued", "running"}:
        update_job(job_id, last_heartbeat=now_ts())

    return jsonify({"ok": True})


@app.post("/cancel/<job_id>")
def cancel(job_id):
    job = get_job(job_id)
    if not job:
        return jsonify({"error": "找不到工作"}), 404

    if job.get("status") in {"queued", "running"}:
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
    zip_path = DATA_ROOT / f"{job_id}.zip"

    if not zip_path.exists():
        return "檔案尚未完成或已被清除", 404

    return send_file(
        zip_path,
        as_attachment=True,
        download_name=f"finmind_keyprice_{job_id}.zip",
    )


@app.get("/health")
def health():
    return {"ok": True}


if __name__ == "__main__":
    port = int(os.environ.get("PORT", "10000"))
    app.run(host="0.0.0.0", port=port, debug=False)
