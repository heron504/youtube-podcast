#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os, csv, smtplib, ssl, time, re, html
from pathlib import Path
from datetime import datetime
from dateutil import tz
from typing import List, Dict, Any
import requests
from jinja2 import Environment, FileSystemLoader, select_autoescape
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart

BASE_DIR = Path(__file__).resolve().parent
OUT_DIR = BASE_DIR / "outputs"
TEMPLATE_DIR = BASE_DIR / "templates"
TEMPLATE_FILE = "daily_report_email.html.j2"   # 邮件友好模板（无目录）

# ==== Secrets / 环境变量 ====
OPENROUTER_API_KEY = os.environ.get("OPENROUTER_API_KEY", "")
SMTP_SERVER = os.environ.get("SMTP_SERVER", "")
SMTP_PORT = int(os.environ.get("SMTP_PORT", "465"))
SMTP_USERNAME = os.environ.get("SMTP_USERNAME", "")
SMTP_PASSWORD = os.environ.get("SMTP_PASSWORD", "")
MAIL_FROM = os.environ.get("MAIL_FROM", "")
MAIL_TO = os.environ.get("MAIL_TO", "heron259@qq.com")   # 收件人

MODEL = "google/gemini-2.5-pro"
OPENROUTER_BASE = "https://openrouter.ai/api/v1"

# ==== 小工具 ====
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
        resp = requests.post(url, headers=headers, json=payload, timeout=timeout)
        if resp.status_code in (429, 500, 502, 503, 504):
            if i == tries - 1:
                resp.raise_for_status()
            time.sleep(backoff)
            backoff = min(backoff * 2, 60)
            continue
        resp.raise_for_status()
        return resp

def call_openrouter(messages: List[Dict[str, str]], temperature: float = 0.15, max_tokens: int = 10000) -> str:
    assert OPENROUTER_API_KEY, "OPENROUTER_API_KEY 未配置"
    headers = {
        "Authorization": f"Bearer {OPENROUTER_API_KEY}",
        "Content-Type": "application/json",
        "HTTP-Referer": "https://github.com/",
        "X-Title": "youtube-daily-summarizer"
    }
    payload = {"model": MODEL, "messages": messages, "temperature": temperature, "max_tokens": max_tokens}
    resp = post_with_retries(f"{OPENROUTER_BASE}/chat/completions", headers, payload)
    data = resp.json()
    return data["choices"][0]["message"]["content"].strip()

# ====== 你的极简提示词：URL + 指令（不强制结构）======
def build_messages(url: str) -> List[Dict[str, str]]:
    # 系统提示只约束中文与不要代码围栏；不要求 JSON，不限格式
    system = (
        "只用中文回答。不要使用 Markdown 代码围栏(```)，直接输出正文。"
        "任务：总结该 YouTube 视频的内容要点，详细一点，结构化，不要删减重要信息。"
        "允许使用小标题与项目符号列表，优先保留关键信息（议题、观点、证据/数据、主体/公司、人名、时间线、影响、风险、行动项）。"
        "若无法直接读取链接内容，则基于标题/简介/常识稳健概括，禁止编造具体数字。"
    )
    user = f"{url}\n请你总结内容要点，详细一点，结构化，不要删减重要信息，用中文。"
    return [{"role": "system", "content": system}, {"role": "user", "content": user}]

# ====== 输出清洗：去掉 ``` 包裹，保留完整文本 ======
def strip_code_fences(text: str) -> str:
    s = text.strip()
    # 去除 ```xxx\n ... \n``` 外壳
    m = re.fullmatch(r"```(?:[\w-]+)?\s*(.*?)\s*```", s, flags=re.S)
    if m:
        return m.group(1).strip()
    # 有些模型会前后多段 fenced，尽量去外层
    m2 = re.search(r"```(?:[\w-]+)?\s*(.*?)\s*```", s, flags=re.S)
    if m2 and len(m2.group(1)) > len(s) * 0.4:
        return m2.group(1).strip()
    return s

# ====== 生成“摘要区”的一行预览（不改变正文）======
def make_snippet(full_text: str, limit: int = 40) -> str:
    # 取第一条非空行，去掉开头的列表符号
    for ln in full_text.splitlines():
        t = ln.strip()
        if not t:
            continue
        t = re.sub(r"^[-*•·\u2022]+\s*", "", t)
        return (t[:limit]).strip()
    return ""

# ====== 将自由文本转成简洁 HTML（保留全部内容）======
def text_to_html(text: str) -> str:
    # 先整体转义，防止注入
    esc = html.escape(text)
    lines = esc.splitlines()

    html_chunks: List[str] = []
    in_ul = False

    def flush_ul():
        nonlocal in_ul
        if in_ul:
            html_chunks.append("</ul>")
            in_ul = False

    for raw in lines:
        ln = raw.strip()
        if not ln:
            flush_ul()
            html_chunks.append("<p style=\"margin:8px 0;\"></p>")
            continue

        # 列表项（- * • · 开头）
        if re.match(r"^(&#45;|&#42;|•|·)\s+", ln):
            if not in_ul:
                html_chunks.append("<ul style=\"padding-left:20px; margin:6px 0;\">")
                in_ul = True
            # 去掉转义后的符号（- -> &#45;  * -> &#42;）
            li = re.sub(r"^(&#45;|&#42;|•|·)\s+", "", ln)
            html_chunks.append(f"<li style=\"margin:4px 0;\">{li}</li>")
        else:
            flush_ul()
            # 简单把“结尾是全角冒号/冒号”的行当作小标题加粗
            if ln.endswith("：") or ln.endswith(":"):
                html_chunks.append(f"<p style=\"margin:8px 0; font-weight:600;\">{ln}</p>")
            else:
                html_chunks.append(f"<p style=\"margin:8px 0;\">{ln}</p>")

    flush_ul()
    return "\n".join(html_chunks)

# ====== 渲染邮件 HTML ======
def render_html(items: List[Dict[str, Any]], date_str: str) -> str:
    env = Environment(loader=FileSystemLoader(str(TEMPLATE_DIR)), autoescape=select_autoescape(["html", "xml"]))
    tpl = env.get_template(TEMPLATE_FILE)
    return tpl.render(date=date_str, items=items)

# ====== 纯文本备份（降级）======
def html_to_text_fallback(html_str: str) -> str:
    txt = re.sub(r"<br\s*/?>", "\n", html_str, flags=re.I)
    txt = re.sub(r"</p\s*>", "\n\n", txt, flags=re.I)
    txt = re.sub(r"<li\s*>", "• ", txt, flags=re.I)
    txt = re.sub(r"<[^>]+>", "", txt)
    txt = re.sub(r"\n{3,}", "\n\n", txt)
    return txt.strip()

# ====== 可靠发信（465 SSL / 587 STARTTLS；QUIT 异常忽略）======
def send_email_html(subject: str, html_body: str):
    assert SMTP_SERVER and SMTP_USERNAME and SMTP_PASSWORD, "SMTP 环境变量未配置完整"
    from_addr = MAIL_FROM or SMTP_USERNAME
    to_addrs = [addr.strip() for addr in (MAIL_TO or SMTP_USERNAME).split(",") if addr.strip()]

    text_fallback = html_to_text_fallback(html_body)
    msg = MIMEMultipart("alternative")
    msg["From"] = from_addr
    msg["To"] = ", ".join(to_addrs)
    msg["Subject"] = subject
    msg.attach(MIMEText(text_fallback, "plain", "utf-8"))
    msg.attach(MIMEText(html_body, "html", "utf-8"))

    server = None
    try:
        if int(SMTP_PORT) == 465:
            context = ssl.create_default_context()
            server = smtplib.SMTP_SSL(SMTP_SERVER, int(SMTP_PORT), timeout=60, context=context)
            server.ehlo()
        else:
            server = smtplib.SMTP(SMTP_SERVER, int(SMTP_PORT), timeout=60)
            server.ehlo()
            context = ssl.create_default_context()
            server.starttls(context=context)
            server.ehlo()

        server.login(SMTP_USERNAME, SMTP_PASSWORD)
        server.sendmail(from_addr, to_addrs, msg.as_string())
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
    date_str = today_date_str_tz("Asia/Shanghai")
    rows = load_today_updates()

    # 今日无更新也发空日报（保留节拍）
    if not rows:
        html_body = render_html([], date_str)
        OUT_DIR.mkdir(exist_ok=True)
        (OUT_DIR / f"daily_report_{date_str}.html").write_text(html_body, encoding="utf-8")
        send_email_html(f"YouTube 播客日报 · {date_str}（无更新）", html_body)
        print("No updates today. Sent empty report.")
        return

    tzinfo = tz.gettz("Asia/Shanghai")
    items: List[Dict[str, Any]] = []

    for r in rows:
        url = r.get("url","")
        title = r.get("title","")
        ch = r.get("channel_title","")
        published_iso = r.get("published","")
        try:
            dt = datetime.fromisoformat(published_iso.replace("Z","+00:00")).astimezone(tzinfo)
            published_local = dt.strftime("%Y-%m-%d %H:%M")
        except Exception:
            published_local = published_iso

        # 调 LLM：只给 URL + 极简中文指令
        try:
            raw = call_openrouter(build_messages(url))
        except Exception as e:
            raw = f"（模型调用失败：{e}）"

        # 去掉 ``` 包裹，保留完整文本
        cleaned = strip_code_fences(raw)
        snippet = make_snippet(cleaned, limit=40)
        summary_html = text_to_html(cleaned)

        items.append({
            "video_id": r.get("video_id",""),
            "url": url,
            "title": title,
            "channel_title": ch,
            "published_local": published_local,
            "snippet": snippet,               # 今日摘要里显示
            "summary_html": summary_html,     # 正文完整内容
        })

    html_body = render_html(items, date_str)
    OUT_DIR.mkdir(exist_ok=True)
    (OUT_DIR / f"daily_report_{date_str}.html").write_text(html_body, encoding="utf-8")
    send_email_html(f"YouTube 播客日报 · {date_str}", html_body)
    print(f"Report generated for {date_str}, items: {len(items)}")

if __name__ == "__main__":
    main()
