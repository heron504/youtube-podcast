#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
YouTube 订阅每日抓取（方案A：YouTube Data API v3）
------------------------------------------------
功能：
- OAuth 登录（desktop flow），token.json 缓存
- 列出“我的订阅”（subscriptions.list?mine=true，自动翻页）
- 对每个频道找到 uploads 播放列表 → 抓取最近 N 页（默认5页≈250条）
- 批量调用 videos.list 补充 statistics 与时长
- 去重与增量：state.json 维护 seen_ids，多次运行不重复写
- 生成 Tokyo 日历命名的增量 CSV（updates_YYYY-MM-DD.csv）与全量 CSV（alltime_videos.csv）

准备：
1) 在同目录放置 Google Cloud Console 下载的 OAuth 客户端文件：client_secret.json
2) pip install -r requirements.txt
3) 首次运行会弹浏览器授权；之后使用 token.json 自动续期

用法：
    python youtube_subs_daily.py               # 抓取每频道最近5页（每页50条）
    python youtube_subs_daily.py --pages 2     # 改为每频道最近2页
适合配合 cron（Asia/Shanghai 每天 09:10 举例）：
    10 9 * * * /usr/bin/python3 /abs/path/youtube_subs_daily.py >> /abs/path/youtube_subs_daily.log 2>&1
"""

from __future__ import annotations
import os, sys, csv, json, logging, argparse
from pathlib import Path
from datetime import datetime, timezone
from typing import Dict, List

# Google API
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials

# ---------- 配置 ----------
SCOPES = ["https://www.googleapis.com/auth/youtube.readonly"]
LOCAL_TZ = "Asia/Shanghai"
BATCH_SIZE_VIDEOS = 50      # videos.list 一次最多 50
MAX_RESULTS = 50            # subscriptions / playlistItems 每页最大 50

BASE_DIR = Path(__file__).resolve().parent
OUT_DIR = BASE_DIR / "outputs"
OUT_DIR.mkdir(exist_ok=True)

STATE_FILE = BASE_DIR / "state.json"
ALLTIME_CSV = OUT_DIR / "alltime_videos.csv"
CLIENT_SECRET = BASE_DIR / "client_secret.json"
TOKEN_FILE = BASE_DIR / "token.json"
LOG_FILE = BASE_DIR / "run.log"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler(sys.stdout), logging.FileHandler(LOG_FILE, encoding="utf-8")]
)

# ---------- 工具函数 ----------
def iso_local_today() -> str:
    """Return today's date in Asia/Shanghai as YYYY-MM-DD.

    On Windows or environments without IANA tz database, ZoneInfo may not
    find "Asia/Shanghai". Fall back to a fixed UTC+8 offset (China; no DST)
    to avoid crashes like ZoneInfoNotFoundError.
    """
    try:
        from zoneinfo import ZoneInfo  # Python 3.9+
        tz = ZoneInfo(LOCAL_TZ)
    except Exception:
        # Fallback: fixed UTC+8 (China has no DST)
        from datetime import timedelta
        tz = timezone(timedelta(hours=8))
    return datetime.now(tz).strftime("%Y-%m-%d")

def now_utc_iso() -> str:
    return datetime.now(timezone.utc).isoformat()

def read_state() -> Dict:
    if STATE_FILE.exists():
        with open(STATE_FILE, "r", encoding="utf-8") as f:
            return json.load(f)
    return {"seen_ids": [], "last_run_utc": None}

def write_state(state: Dict) -> None:
    with open(STATE_FILE, "w", encoding="utf-8") as f:
        json.dump(state, f, ensure_ascii=False, indent=2)

def ensure_creds() -> Credentials:
    if not CLIENT_SECRET.exists():
        logging.error("缺少 client_secret.json（请从 Google Cloud Console 下载并放到脚本目录）")
        sys.exit(1)
    creds = None
    if TOKEN_FILE.exists():
        creds = Credentials.from_authorized_user_file(str(TOKEN_FILE), SCOPES)
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            logging.info("刷新 OAuth 凭据…")
            creds.refresh(Request())
        else:
            logging.info("启动 OAuth 本地授权流程…")
            flow = InstalledAppFlow.from_client_secrets_file(str(CLIENT_SECRET), SCOPES)
            creds = flow.run_local_server(port=0)
        with open(TOKEN_FILE, "w", encoding="utf-8") as token:
            token.write(creds.to_json())
    return creds

def yt_service(creds: Credentials):
    # 关闭本地缓存以避免某些环境报错
    return build("youtube", "v3", credentials=creds, cache_discovery=False)

def list_all_subscriptions(youtube) -> List[str]:
    """返回所有订阅频道的 channelId 列表"""
    channel_ids: List[str] = []
    page_token = None
    while True:
        resp = youtube.subscriptions().list(
            part="snippet",
            mine=True,
            maxResults=MAX_RESULTS,
            pageToken=page_token
        ).execute()
        for item in resp.get("items", []):
            rid = item["snippet"]["resourceId"]
            if rid.get("kind") == "youtube#channel":
                channel_ids.append(rid["channelId"])
        page_token = resp.get("nextPageToken")
        if not page_token:
            break
    logging.info(f"订阅频道数：{len(channel_ids)}")
    return channel_ids

def get_uploads_playlist_id(youtube, channel_id: str) -> str | None:
    resp = youtube.channels().list(part="contentDetails", id=channel_id, maxResults=1).execute()
    items = resp.get("items", [])
    if not items:
        return None
    return items[0]["contentDetails"]["relatedPlaylists"]["uploads"]

def fetch_playlist_items_pages(youtube, uploads_id: str, pages: int) -> List[dict]:
    """抓取 uploads 播放列表最近 N 页（每页最多50）"""
    items: List[dict] = []
    page_token = None
    fetched = 0
    while True:
        resp = youtube.playlistItems().list(
            part="snippet,contentDetails",
            playlistId=uploads_id,
            maxResults=MAX_RESULTS,
            pageToken=page_token
        ).execute()
        items.extend(resp.get("items", []))
        page_token = resp.get("nextPageToken")
        fetched += 1
        if not page_token or fetched >= pages:
            break
    return items

def chunked(seq, n):
    for i in range(0, len(seq), n):
        yield seq[i:i+n]

def enrich_videos(youtube, video_ids: List[str]) -> Dict[str, dict]:
    """
    批量 videos.list（snippet,statistics,contentDetails）
    返回 { videoId: {snippet, statistics, contentDetails, id} }
    """
    out: Dict[str, dict] = {}
    for chunk in chunked(video_ids, BATCH_SIZE_VIDEOS):
        resp = youtube.videos().list(
            part="snippet,statistics,contentDetails",
            id=",".join(chunk),
            maxResults=len(chunk)
        ).execute()
        for it in resp.get("items", []):
            out[it["id"]] = it
    return out

def parse_iso_dt(s: str):
    try:
        return datetime.fromisoformat(s.replace("Z", "+00:00"))
    except Exception:
        return datetime.min.replace(tzinfo=timezone.utc)

def write_csv(rows: List[dict]) -> tuple[str | None, int]:
    if not rows:
        return None, 0
    rows.sort(key=lambda r: parse_iso_dt(r["published"]))  # 按发布时间升序

    day_file = OUT_DIR / f"updates_{iso_local_today()}.csv"
    write_header_day = not day_file.exists()
    write_header_all = not ALLTIME_CSV.exists()

    fields = [
        "published", "channel_title", "channel_id",
        "title", "url", "video_id",
        "description", "viewCount", "likeCount", "commentCount", "duration"
    ]

    with open(day_file, "a", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=fields)
        if write_header_day: w.writeheader()
        for r in rows: w.writerow(r)

    with open(ALLTIME_CSV, "a", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=fields)
        if write_header_all: w.writeheader()
        for r in rows: w.writerow(r)

    return str(day_file), len(rows)

# ---------- 主流程 ----------
def run(pages: int = 5):
    state = read_state()
    seen_ids = set(state.get("seen_ids", []))

    creds = ensure_creds()
    youtube = yt_service(creds)

    # 1) 我的订阅
    try:
        channel_ids = list_all_subscriptions(youtube)
    except HttpError as e:
        logging.exception(f"拉取订阅失败：{e}")
        sys.exit(1)

    new_rows: List[dict] = []

    # 2) 逐频道抓取最近 N 页 uploads
    for ch_id in channel_ids:
        try:
            uploads_id = get_uploads_playlist_id(youtube, ch_id)
            if not uploads_id:
                continue

            items = fetch_playlist_items_pages(youtube, uploads_id, pages)
            # 先收集“未见过”的 videoId
            fresh_ids: List[str] = []
            for it in items:
                vid = it["contentDetails"]["videoId"]
                if vid not in seen_ids:
                    fresh_ids.append(vid)

            if not fresh_ids:
                continue

            # 3) 批量补充详情
            details = enrich_videos(youtube, fresh_ids)

            for it in items:
                vid = it["contentDetails"]["videoId"]
                if vid not in details or vid in seen_ids:
                    continue

                sni = it["snippet"]
                ch_title = sni.get("channelTitle", "")
                title = (sni.get("title", "") or "").replace("\n", " ").strip()
                desc = sni.get("description", "") or ""
                if len(desc) > 1000:
                    desc = desc[:1000] + " …"

                publish_iso = it["contentDetails"].get("videoPublishedAt", sni.get("publishedAt", ""))

                dv = details[vid]
                v_stats = dv.get("statistics", {})
                v_snip = dv.get("snippet", {})
                v_detl = dv.get("contentDetails", {})
                duration = v_detl.get("duration", "")  # ISO8601 PT#M#S

                row = {
                    "published": publish_iso,
                    "channel_title": ch_title or v_snip.get("channelTitle", ""),
                    "channel_id": ch_id,
                    "title": title,
                    "url": f"https://www.youtube.com/watch?v={vid}",
                    "video_id": vid,
                    "description": desc,
                    "viewCount": v_stats.get("viewCount", ""),
                    "likeCount": v_stats.get("likeCount", ""),
                    "commentCount": v_stats.get("commentCount", ""),
                    "duration": duration,
                }
                new_rows.append(row)
                seen_ids.add(vid)

        except HttpError as e:
            logging.warning(f"[跳过频道] {ch_id}: {e}")
            continue

    # 4) 写 CSV + 状态
    day_file, n = write_csv(new_rows)
    if n:
        logging.info(f"新增视频 {n} 条 → {day_file}")
    else:
        logging.info("今天没有新增视频。")

    state["seen_ids"] = sorted(seen_ids)
    state["last_run_utc"] = now_utc_iso()
    write_state(state)

def main():
    ap = argparse.ArgumentParser(description="YouTube 订阅每日抓取（Data API 方案A）")
    ap.add_argument("--pages", type=int, default=5, help="每频道抓取最近的页数（每页最多50），默认5页≈250条")
    args = ap.parse_args()

    if sys.version_info < (3, 10):
        print("[ERR] 请使用 Python 3.10+")
        sys.exit(1)

    run(pages=max(1, args.pages))

if __name__ == "__main__":
    main()
