# FinMind Key Price Research — Render 網頁版

## 這個版本做什麼

你把 repo 放到 GitHub，再部署到 Render 後，就會得到一個網頁。

在網頁上輸入：

- 開始日期
- 結束日期
- 日內最高漲幅預篩（建議 3%）
- RVOL 歷史交易日（建議 20）

按「開始抓資料」後，Render 伺服器會：

1. 抓 `TaiwanStockInfo`
2. 抓期間日線 `TaiwanStockPrice`
3. 用前收計算「當日最高曾達多少 %」
4. 保留至少碰到 +3% 的 stock-day
5. 抓候選日 `TaiwanStockKBar`
6. 補候選股票之前 20 個交易日的 KBar，用來算開盤前 5 分鐘 RVOL
7. 產生 `research_base.csv`
8. 將結果包成 ZIP，網頁出現下載按鈕

下一階段再用 ZIP 做：

- Key Price
- Test 1 / 2 / 3 / 4
- C3 / C4 壓縮率
- 第一次進入 35% / 30% / 25% 竭盡區
- 卡位後 1 / 3 / 5 / 10 分鐘爆發率
- Key 2 成功 / 失敗
- Key 2 失敗的保護獲利

---

## GitHub

建立一個新 repo，例如：

`finmind-keyprice-research`

把以下檔案全部放進 repo：

```text
app.py
requirements.txt
render.yaml
.gitignore
templates/
  index.html
```

**不要把 FinMind Token 寫進 GitHub。**

---

## Render

### 方法 A：Blueprint

Render → New → Blueprint → 選你的 GitHub repo。

因為 repo 裡有 `render.yaml`，Render 會自動讀取部署設定。

### 方法 B：Web Service

如果不用 Blueprint：

- Build Command:
  `pip install -r requirements.txt`

- Start Command:
  `gunicorn --workers 1 --threads 8 --timeout 0 app:app`

---

## 最重要：設定 FinMind Token

Render Dashboard → 你的 Service → Environment：

新增：

```text
FINMIND_TOKEN = 你的 Sponsor Token
```

然後 Save / Deploy。

**Token 只存在 Render Server，不會出現在 HTML，也不要 commit 到 GitHub。**

---

## 第一輪建議

網頁設定：

- 開始：2026-05-01
- 結束：2026-08-14
- 預篩：3%
- RVOL：20 日

先跑這個範圍。

---

## 關於 Render

這個簡化版把工作放在 Web Service 背景 thread 中執行。

優點：
- 不會因單一 HTTP request 太久而讓瀏覽器一直卡住。
- 網頁可以輪詢進度。
- 最簡單，現在就可以用。

限制：
- Render 如果在工作途中重新部署/重啟，工作會中止。
- `/tmp` 檔案屬暫存，重啟後可能消失。
- 因此跑完請立刻下載 ZIP。

如果後續我們要跑半年/一年全市場大量研究，我會再升級成：
- Render Background Worker
- PostgreSQL / persistent disk
- 工作佇列
- 每日自動更新

目前先不要複雜化。
