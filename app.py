from __future__ import annotations
import json, os, shutil, threading, time, uuid
from datetime import datetime
from pathlib import Path
import pandas as pd
import requests
from flask import Flask, jsonify, render_template, request, send_file

app = Flask(__name__)
API = "https://api.finmindtrade.com/api/v4/data"
DATA_ROOT = Path(os.environ.get("DATA_ROOT", "/tmp/finmind_keyprice_v3"))
DATA_ROOT.mkdir(parents=True, exist_ok=True)
API_SLEEP = float(os.environ.get("FINMIND_API_SLEEP", "0.65"))
HEARTBEAT_TIMEOUT = float(os.environ.get("JOB_HEARTBEAT_TIMEOUT", "20"))
jobs, jobs_lock = {}, threading.Lock()

class JobCancelled(Exception): pass

class FinMindClient:
    def __init__(self, token, job_id):
        if not token: raise RuntimeError("Render 尚未設定 FINMIND_TOKEN")
        self.token, self.job_id = token, job_id
        self.session = requests.Session()
    def get(self, dataset, **params):
        check_cancel(self.job_id)
        q = {"dataset": dataset, "token": self.token, **params}
        for attempt in range(7):
            check_cancel(self.job_id)
            r = self.session.get(API, params=q, timeout=90)
            if r.status_code == 429:
                for _ in range(int(min(120, 3*(2**attempt))*2)):
                    check_cancel(self.job_id); time.sleep(.5)
                continue
            r.raise_for_status()
            p = r.json()
            if p.get("status") != 200:
                raise RuntimeError(f"FinMind {dataset}: {p.get('msg', p)}")
            time.sleep(API_SLEEP)
            return pd.DataFrame(p.get("data", []))
        raise RuntimeError("FinMind API 持續回傳 429，請稍後再試")

def update_job(jid, **kw):
    with jobs_lock: jobs.setdefault(jid, {}).update(kw)
def get_job(jid):
    with jobs_lock: return dict(jobs.get(jid, {}))
def active_job():
    with jobs_lock:
        return next((jid for jid,j in jobs.items() if j.get("status") in {"queued","running"}), None)
def check_cancel(jid):
    j=get_job(jid)
    if not j or j.get("cancel_requested"): raise JobCancelled("工作已停止")
    hb=j.get("last_heartbeat")
    if hb and time.time()-hb > HEARTBEAT_TIMEOUT:
        update_job(jid,cancel_requested=True)
        raise JobCancelled("網頁已關閉或失聯，已自動停止工作")

def universe(client):
    d=client.get("TaiwanStockInfo")
    if d.empty: raise RuntimeError("TaiwanStockInfo 無資料")
    d["stock_id"]=d["stock_id"].astype(str)
    if "date" in d:
        d["date"]=pd.to_datetime(d["date"],errors="coerce")
        d=d.sort_values("date").groupby("stock_id",as_index=False).tail(1)
    d=d[d["stock_id"].str.fullmatch(r"\d{4}",na=False)]
    if "type" in d:
        m=d["type"].astype(str).str.lower().isin({"twse","tpex"})
        if m.any(): d=d[m]
    cols=[c for c in ["stock_name","industry_category"] if c in d]
    if cols:
        txt=d[cols].fillna("").astype(str).agg(" ".join,axis=1)
        d=d[~txt.str.contains("ETF|ETN|權證|受益證券|存託憑證",case=False,regex=True)]
    return set(d["stock_id"])

def run_job(jid,cfg):
    jobdir=DATA_ROOT/jid
    try:
        client=FinMindClient(os.environ.get("FINMIND_TOKEN","").strip(),jid)
        jobdir.mkdir(parents=True,exist_ok=True)
        update_job(jid,status="running",progress=2,message="讀取台股標的清單…")
        uni=universe(client)
        days=pd.date_range(cfg["start"],cfg["end"],freq="D")
        daily_parts=[]
        for i,d in enumerate(days,1):
            check_cancel(jid)
            day=d.strftime("%Y-%m-%d")
            update_job(jid,progress=5+int(20*i/max(len(days),1)),message=f"抓日線 {day}")
            x=client.get("TaiwanStockPrice",start_date=day,end_date=day)
            if not x.empty:
                x["stock_id"]=x["stock_id"].astype(str)
                x["date"]=pd.to_datetime(x["date"]).dt.normalize()
                daily_parts.append(x)
        if not daily_parts: raise RuntimeError("指定期間沒有日線資料")
        daily=pd.concat(daily_parts,ignore_index=True)
        daily=daily[daily["stock_id"].isin(uni)].copy()

        # 先用當日總量做超便宜的流動性預篩，避免後面大量 KBar/API。
        volcol=next((c for c in ["Trading_Volume","Trading_volume","volume"] if c in daily.columns),None)
        if not volcol: raise RuntimeError("TaiwanStockPrice 找不到成交量欄位")
        daily["day_shares"]=pd.to_numeric(daily[volcol],errors="coerce").fillna(0)
        daily["day_lots"]=daily["day_shares"]/1000.0
        daily=daily[daily["day_lots"] >= cfg["min_lots"]].copy()
        if daily.empty: raise RuntimeError("沒有股票通過最低成交量門檻")

        # 為算漲幅，只對已通過 4000 張門檻的股票補前一交易日收盤。
        candidates=[]
        rows=list(daily.itertuples(index=False))
        for i,r in enumerate(rows,1):
            check_cancel(jid)
            sid=str(r.stock_id); day=pd.Timestamp(r.date)
            prev=None
            for delta in range(1,8):
                pd_str=(day-pd.Timedelta(days=delta)).strftime("%Y-%m-%d")
                p=client.get("TaiwanStockPrice",data_id=sid,start_date=pd_str,end_date=pd_str)
                if not p.empty and "close" in p:
                    prev=float(p.iloc[-1]["close"]); break
            if not prev: continue
            high=float(getattr(r,"max"))
            hp=(high/prev-1)*100
            update_job(jid,progress=25+int(30*i/max(len(rows),1)),
                       message=f"預篩 {i}/{len(rows)}｜{sid}")
            if hp >= cfg["screen_pct"]:
                candidates.append({
                    "date":day.strftime("%Y-%m-%d"),"stock_id":sid,
                    "prev_close":prev,"day_high_pct":hp,
                    "day_volume_lots":float(getattr(r,"day_lots"))
                })
        c=pd.DataFrame(candidates)
        if c.empty: raise RuntimeError("沒有股票同時符合漲幅與成交量門檻")
        c.to_csv(jobdir/"candidates.csv",index=False,encoding="utf-8-sig")

        kbdir=jobdir/"kbar"; kbdir.mkdir(exist_ok=True)
        for i,r in enumerate(c.itertuples(index=False),1):
            check_cancel(jid)
            update_job(jid,progress=55+int(40*i/max(len(c),1)),
                       message=f"抓當日1分K {i}/{len(c)}｜{r.stock_id} {r.date}")
            k=client.get("TaiwanStockKBar",data_id=str(r.stock_id),start_date=str(r.date))
            if not k.empty:
                p=kbdir/str(r.date); p.mkdir(exist_ok=True)
                k.to_csv(p/f"{r.stock_id}.csv",index=False,encoding="utf-8-sig")

        meta={"created_at":datetime.now().isoformat(timespec="seconds"),
              **cfg,"candidate_events":len(c)}
        (jobdir/"metadata.json").write_text(json.dumps(meta,ensure_ascii=False,indent=2),encoding="utf-8")
        zp=DATA_ROOT/f"{jid}.zip"
        shutil.make_archive(str(zp.with_suffix("")),"zip",jobdir)
        update_job(jid,status="done",progress=100,message=f"完成：{len(c)} 筆候選",
                   download=f"/download/{jid}",meta=meta)
    except JobCancelled as e:
        shutil.rmtree(jobdir,ignore_errors=True)
        update_job(jid,status="cancelled",progress=0,message=str(e))
    except Exception as e:
        update_job(jid,status="error",progress=100,message=str(e))

@app.get("/")
def index(): return render_template("index.html")

@app.post("/start")
def start():
    a=active_job()
    if a: return jsonify({"error":"目前已有工作正在執行，請先停止或等待完成。"}),409
    p=request.get_json(force=True)
    cfg={"start":p.get("start"),"end":p.get("end"),
         "screen_pct":float(p.get("screen_pct",4)),
         "min_lots":float(p.get("min_lots",4000))}
    if not cfg["start"] or not cfg["end"]: return jsonify({"error":"請選日期"}),400
    jid=uuid.uuid4().hex[:12]
    update_job(jid,status="queued",progress=0,message="排隊中…",
               cancel_requested=False,last_heartbeat=time.time(),cfg=cfg)
    threading.Thread(target=run_job,args=(jid,cfg),daemon=True).start()
    return jsonify({"job_id":jid})

@app.post("/heartbeat/<jid>")
def heartbeat(jid):
    if not get_job(jid): return jsonify({"error":"找不到工作"}),404
    update_job(jid,last_heartbeat=time.time()); return jsonify({"ok":True})

@app.post("/cancel/<jid>")
def cancel(jid):
    if not get_job(jid): return jsonify({"error":"找不到工作"}),404
    update_job(jid,cancel_requested=True,message="正在停止…"); return jsonify({"ok":True})

@app.get("/status/<jid>")
def status(jid):
    j=get_job(jid)
    return jsonify(j) if j else (jsonify({"error":"找不到工作"}),404)

@app.get("/download/<jid>")
def download(jid):
    p=DATA_ROOT/f"{jid}.zip"
    if not p.exists(): return "檔案不存在",404
    return send_file(p,as_attachment=True,download_name=f"finmind_keyprice_{jid}.zip")

@app.get("/health")
def health(): return {"ok":True}
