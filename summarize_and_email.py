#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os, csv, json, smtplib, ssl, time
from pathlib import Path
from datetime import datetime
from dateutil import tz
from typing import List, Dict, Any
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
import re, json

import requests
from jinja2 import Environment, FileSystemLoader, select_autoescape

BASE_DIR = Path(__file__).resolve().parent
OUT_DIR = BASE_DIR / "outputs"
TEMPLATE_DIR = BASE_DIR / "templates"
TEMPLATE_FILE = "daily_report.html.j2"

# ==== 环境变量/Secrets ====
OPENROUTER_API_KEY = os.environ.get("OPENROUTER_API_KEY", "")
SMTP_SERVER = os.environ.get("SMTP_SERVER", "")
SMTP_PORT = int(os.environ.get("SMTP_PORT", "465"))
SMTP_USERNAME = os.environ.get("SMTP_USERNAME", "")
SMTP_PASSWORD = os.environ.get("SMTP_PASSWORD", "")
MAIL_FROM = os.environ.get("MAIL_FROM", "")
MAIL_TO = os.environ.get("MAIL_TO", "heron259@qq.com")  # 收件人

MODEL = "google/gemini-2.5-pro"
OPENROUTER_BASE = "https://openrouter.ai/api/v1"

# ==== 工具 ====
def today_date_str_tz(tz_str="Asia/Shanghai") -> str:
    tzinfo = tz.gettz(tz_str)
    return datetime.now(tzinfo).strftime("%Y-%m-%d")

def load_today_updates() -> List[Dict[str, str]]:
    f = OUT_DIR / f"updates_{today_date_str_tz('Asia/Shanghai')}.csv"
    if not f.exists():
        return []
    with open(f, "r", encoding="utf-8") as fh:
        return list(csv.DictReader(fh))

def post_with_retries(url: str, headers: Dict[str, str], payload: Dict[str, Any], tries: int = 5, timeout: int = 120):
    backoff = 2
    for i in range(tries):
        resp = requests.post(url, headers=headers, data=json.dumps(payload), timeout=timeout)
        # 对速率/网关错误做退避
        if resp.status_code in (429, 500, 502, 503, 504):
            if i == tries - 1:
                resp.raise_for_status()
            time.sleep(backoff)
            backoff = min(backoff * 2, 60)
            continue
        resp.raise_for_status()
        return resp

def call_openrouter(messages: List[Dict[str, str]], temperature: float = 0.2, max_tokens: int = 10000) -> str:
    headers = {
        "Authorization": f"Bearer {OPENROUTER_API_KEY}",
        "Content-Type": "application/json",
        "HTTP-Referer": "https://github.com/",
        "X-Title": "youtube-daily-summarizer"
    }
    payload = {
        "model": MODEL,
        "messages": messages,
        "temperature": temperature,
        "max_tokens": max_tokens,
        # 可按需启用函数/工具；这里直接请求模型“原生理解”链接内容
    }
    resp = post_with_retries(f"{OPENROUTER_BASE}/chat/completions", headers, payload)
    data = resp.json()
    return data["choices"][0]["message"]["content"].strip()

def build_messages(title: str, url: str, channel: str, description: str) -> List[Dict[str, str]]:
    system = (
        "你是面向投资研究的长视频/播客摘要助手。请直接阅读提供的 YouTube 链接内容；"
        "若无法直接访问，则基于可得信息（标题/频道/简介/常识）做稳健概括。"
        "目标：用中文、结构化要点，尽可能完整覆盖关键信息——议题、核心观点、证据/数据、涉及主体/公司、人名、时间线与进展、结论/影响、风险/不确定性、后续行动。"
        "避免空话和水分，严禁编造未出现的具体数据或结论。"
        "严格输出：不允许出现任何解释文字、Markdown代码块或前后缀，不允许 ``` 符号。"
        "只返回一行紧凑 JSON："
        "{\"one_line\":\"(可留空，<=20字)\",\"points\":[\"要点1：主结论+依据/数字/时间/主体(<=120字)\",...最多12条]}"
    )
    user = (
        f"视频标题：{title}\n"
        f"频道：{channel}\n"
        f"URL：{url}\n"
        f"简介（可为空）：{(description or '')[:800]}\n\n"
        "请只返回上一段格式的一行 JSON。"
    )
    return [{"role": "system", "content": system}, {"role": "user", "content": user}]


def parse_json_or_fallback(text: str) -> Dict[str, Any]:
    s = text.strip()

    # 1) 去掉 Markdown 代码块外壳（```json ... ``` 或 ``` ... ```）
    if s.startswith("```"):
        # 抓取三引号包裹内容
        m = re.search(r"```(?:json|JSON)?\s*(.*?)\s*```", s, flags=re.S)
        if m:
            s = m.group(1).strip()

    # 2) 提取最外层 {...} 片段（有些模型前后仍会加提示语）
    if "{" in s and "}" in s:
        start = s.find("{")
        end = s.rfind("}")
        s = s[start:end+1].strip()

    # 3) 去掉可能的尾逗号（JSON 容易因行尾逗号报错）
    s = re.sub(r",\s*([\]}])", r"\1", s)

    # 4) 正常 JSON 解析
    try:
        obj = json.loads(s)
        one = (obj.get("one_line") or "").strip()[:60]
        pts_raw = obj.get("points", [])
        if isinstance(pts_raw, str):
            # 若误返回成字符串，按换行/分号/顿号拆分
            pts = [p.strip() for p in re.split(r"[\n；;、]", pts_raw) if p.strip()]
        else:
            pts = [str(p).strip() for p in pts_raw if str(p).strip()]
        # 过滤掉像 "one_line": 这种误混入的键行
        pts = [p for p in pts if not p.startswith('"one_line"') and not p.startswith("{") and not p.startswith("}")]
        return {"one_line": one, "points": pts[:12]}
    except Exception:
        # 5) 兜底：把文本按行拆成“首行=摘要、余下=要点”
        lines = [ln.strip(" \t-•·") for ln in s.splitlines() if ln.strip()]
        one = lines[0][:60] if lines else "（解析失败）"
        pts = [ln[:120] for ln in lines[1:13]]
        return {"one_line": one, "points": pts}


def render_html(items: List[Dict[str, Any]], date_str: str) -> str:
    env = Environment(loader=FileSystemLoader(str(TEMPLATE_DIR)), autoescape=select_autoescape(["html", "xml"]))
    tpl = env.get_template(TEMPLATE_FILE)
    return tpl.render(date=date_str, items=items)

def send_email_html(subject: str, html: str):
    assert SMTP_SERVER and SMTP_USERNAME and SMTP_PASSWORD, "SMTP 环境变量未配置完整"
    from_addr = MAIL_FROM or SMTP_USERNAME
    to_addrs = [addr.strip() for addr in (MAIL_TO or SMTP_USERNAME).split(",") if addr.strip()]

    # 构造 MIME 邮件
    msg = MIMEMultipart("alternative")
    msg["From"] = from_addr
    msg["To"] = ", ".join(to_addrs)
    msg["Subject"] = subject
    msg.attach(MIMEText(html, "html", "utf-8"))

    server = None
    try:
        if int(SMTP_PORT) == 465:
            # SSL 直连
            context = ssl.create_default_context()
            server = smtplib.SMTP_SSL(SMTP_SERVER, int(SMTP_PORT), timeout=60, context=context)
            server.ehlo()
        else:
            # 587等端口：明文握手后 STARTTLS
            server = smtplib.SMTP(SMTP_SERVER, int(SMTP_PORT), timeout=60)
            server.ehlo()
            context = ssl.create_default_context()
            server.starttls(context=context)
            server.ehlo()

        server.login(SMTP_USERNAME, SMTP_PASSWORD)
        server.sendmail(from_addr, to_addrs, msg.as_string())

        # 有些服务商在 QUIT 时直接掐连接；忽略退出异常避免 -1,b'\x00\x00\x00'
        try:
            server.quit()
        except Exception:
            pass
    finally:
        try:
            if server:
                server.close()
        except Exception:
            pass


def main():
    assert OPENROUTER_API_KEY, "OPENROUTER_API_KEY 未配置"
    rows = load_today_updates()
    date_str = today_date_str_tz("Asia/Shanghai")

    # 今日无更新：仍然发空日报，方便你在邮箱里有心智节拍
    if not rows:
        html = render_html([], date_str)
        OUT_DIR.mkdir(exist_ok=True)
        (OUT_DIR / f"daily_report_{date_str}.html").write_text(html, encoding="utf-8")
        send_email_html(f"YouTube 播客日报 · {date_str}（无更新）", html)
        print("No updates today. Sent empty report.")
        return

    tzinfo = tz.gettz("Asia/Shanghai")
    items: List[Dict[str, Any]] = []

    for r in rows:
        vid = r.get("video_id","")
        url = r.get("url","")
        title = r.get("title","")
        ch = r.get("channel_title","")
        desc = r.get("description","") or ""
        published_iso = r.get("published","")
        try:
            dt = datetime.fromisoformat(published_iso.replace("Z","+00:00")).astimezone(tzinfo)
            published_local = dt.strftime("%Y-%m-%d %H:%M")
        except Exception:
            published_local = published_iso

        try:
            raw = call_openrouter(build_messages(title, url, ch, desc))
            parsed = parse_json_or_fallback(raw)
        except Exception as e:
            parsed = {"one_line": "（模型调用失败，保留占位）", "points": []}

        items.append({
            "video_id": vid,
            "url": url,
            "title": title,
            "channel_title": ch,
            "published_local": published_local,
            "one_line_summary": parsed["one_line"],
            "key_points": parsed["points"],
        })

    html = render_html(items, date_str)
    OUT_DIR.mkdir(exist_ok=True)
    (OUT_DIR / f"daily_report_{date_str}.html").write_text(html, encoding="utf-8")
    send_email_html(f"YouTube 播客日报 · {date_str}", html)
    print(f"Report generated for {date_str}, items: {len(items)}")

if __name__ == "__main__":
    main()
