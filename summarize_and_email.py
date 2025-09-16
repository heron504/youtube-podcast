#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os, csv, smtplib, ssl, time, re, io, json
from pathlib import Path
from datetime import datetime
from dateutil import tz
from typing import List, Dict, Any
import requests
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.mime.application import MIMEApplication

# -------- LLM ----------
OPENROUTER_API_KEY = os.environ.get("OPENROUTER_API_KEY", "")
MODEL = "google/gemini-2.5-pro"
OPENROUTER_BASE = "https://openrouter.ai/api/v1"

# -------- 邮件 ----------
SMTP_SERVER = os.environ.get("SMTP_SERVER", "")
SMTP_PORT = int(os.environ.get("SMTP_PORT", "465"))
SMTP_USERNAME = os.environ.get("SMTP_USERNAME", "")
SMTP_PASSWORD = os.environ.get("SMTP_PASSWORD", "")
MAIL_FROM = os.environ.get("MAIL_FROM", "")
MAIL_TO = os.environ.get("MAIL_TO", "heron259@qq.com")

# -------- 路径 ----------
BASE_DIR = Path(__file__).resolve().parent
OUT_DIR = BASE_DIR / "outputs"

# -------- ReportLab（PDF）--------
from reportlab.lib.pagesizes import A4
from reportlab.lib.units import mm
from reportlab.lib.styles import ParagraphStyle, getSampleStyleSheet
from reportlab.lib import colors
from reportlab.platypus import SimpleDocTemplate, Paragraph, Spacer, ListFlowable, ListItem, PageBreak
from reportlab.pdfbase import pdfmetrics
from reportlab.pdfbase.cidfonts import UnicodeCIDFont

# 注册内置 CJK 字体，避免中文乱码（无需外部字体文件）
try:
    pdfmetrics.registerFont(UnicodeCIDFont("STSong-Light"))
    BASE_FONT = "STSong-Light"
except Exception:
    # 兜底：若环境异常，仍然用 STSong-Light 名称（大多 runner 可用）
    BASE_FONT = "STSong-Light"

# ========= 工具 =========
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

def call_openrouter(messages: List[Dict[str, str]], temperature: float = 0.15, max_tokens: int = 3072) -> str:
    assert OPENROUTER_API_KEY, "OPENROUTER_API_KEY 未配置"
    headers = {
        "Authorization": f"Bearer {OPENROUTER_API_KEY}",
        "Content-Type": "application/json",
        "HTTP-Referer": "https://github.com/",
        "X-Title": "youtube-daily-summarizer"
    }
    payload = {"model": MODEL, "messages": messages, "temperature": temperature, "max_tokens": max_tokens}
    data = post_with_retries(f"{OPENROUTER_BASE}/chat/completions", headers, payload).json()
    return data["choices"][0]["message"]["content"].strip()

# ====== 你的极简提示词（仅加“必须仅基于该链接内容”）======
def build_messages(url: str) -> List[Dict[str, str]]:
    system = (
        "必须仅基于该链接视频的实际内容进行总结。"
        "只用中文回答。不要使用 Markdown 代码围栏(```)，不要写“好的/以下是/这是…”之类开场白。"
        "要求：详细、结构化，不删减重要信息。"
        "若无法读取链接，则仅基于标题/简介稳健概括，并在文中明确标注“（基于标题+简介）”。"
    )
    user = f"{url}\n请你总结内容要点，详细一点，结构化，不要删减重要信息，用中文。"
    return [{"role": "system", "content": system}, {"role": "user", "content": user}]

# ====== 输出清洗 ======
def strip_code_fences(text: str) -> str:
    s = text.strip()
    m = re.fullmatch(r"```(?:[\w-]+)?\s*(.*?)\s*```", s, flags=re.S)
    if m: return m.group(1).strip()
    m2 = re.search(r"```(?:[\w-]+)?\s*(.*?)\s*```", s, flags=re.S)
    if m2 and len(m2.group(1)) > len(s) * 0.4: return m2.group(1).strip()
    return s

def clean_leading_prefixes(text: str) -> str:
    lines = [ln.rstrip() for ln in text.splitlines()]
    drop = [
        r"^(好的|以下是|这是|这里是|本视频|总结|内容要点|概述)\s*[:：]",
        r"^这是对.+?(视频|YouTube).+?(总结|要点)"
    ]
    out = []
    skipped = 0
    for i, ln in enumerate(lines):
        if skipped < 2 and any(re.match(p, ln.strip(), flags=re.I) for p in drop):
            skipped += 1
            continue
        out = lines[i:]
        break
    return "\n".join(out) if out else text

def make_snippet(full_text: str, limit: int = 40) -> str:
    for ln in full_text.splitlines():
        t = ln.strip()
        if not t: continue
        t = re.sub(r"^[-*•·\u2022]+\s*", "", t)
        return (t[:limit]).strip()
    return ""

# ====== PDF 排版 ======
def _styles():
    ss = getSampleStyleSheet()
    base = ParagraphStyle(
        "Base",
        parent=ss["Normal"],
        fontName=BASE_FONT, fontSize=10.5, leading=16,
        textColor=colors.black, spaceBefore=0, spaceAfter=0
    )
    h1 = ParagraphStyle("H1", parent=base, fontSize=18, leading=24, spaceBefore=4, spaceAfter=6)
    h2 = ParagraphStyle("H2", parent=base, fontSize=14, leading=20, spaceBefore=8, spaceAfter=4)
    meta = ParagraphStyle("Meta", parent=base, fontSize=8.5, textColor=colors.HexColor("#555555"))
    item = ParagraphStyle("Item", parent=base, fontSize=11, leading=17)
    return {"base": base, "h1": h1, "h2": h2, "meta": meta, "item": item}

def _md_to_flowables(text: str, styles) -> List:
    """把自由文本的‘伪 Markdown’转成段落和列表（支持 ###/## 标题、-/*/• 列表、普通段落）。"""
    lines = text.splitlines()
    flows, buf_list = [], []

    def flush_list():
        if buf_list:
            flows.append(ListFlowable(
                [ListItem(Paragraph(re.sub(r"^\s*[-*•·]\s+", "", s).strip(), styles["item"]), leftIndent=10, value=None)
                 for s in buf_list],
                bulletType="bullet", bulletFontName=BASE_FONT, bulletFontSize=10.5, bulletOffsetY=0, leftPadding=10
            ))
            buf_list.clear()

    for raw in lines:
        ln = raw.rstrip()
        if not ln:
            flush_list(); flows.append(Spacer(1, 3*mm)); continue

        if re.match(r"^\s*[#]{3}\s+", ln):
            flush_list(); flows.append(Paragraph(re.sub(r"^\s*###\s+", "", ln), styles["h2"])); continue
        if re.match(r"^\s*[#]{2}\s+", ln):
            flush_list(); flows.append(Paragraph(re.sub(r"^\s*##\s+", "", ln), styles["h2"])); continue

        if re.match(r"^\s*[-*•·]\s+", ln):
            buf_list.append(ln); continue

        flush_list()
        flows.append(Paragraph(ln, styles["base"]))

    flush_list()
    return flows

def build_pdf(date_str: str, items: List[Dict[str, Any]], pdf_path: Path):
    styles = _styles()
    story = []

    # 封面标题
    story.append(Paragraph(f"{date_str} · YouTube 播客日报", styles["h1"]))
    story.append(Spacer(1, 2*mm))

    # 今日摘要
    story.append(Paragraph("今日摘要", styles["h2"]))
    if not items:
        story.append(Paragraph("（无更新）", styles["base"]))
    else:
        for idx, it in enumerate(items, 1):
            title = it["title"]
            ch = it["channel_title"]
            url = it["url"]
            one = it["snippet"]
            line = f"{idx}. <b>{ch}</b> · {title}：{one}（{url}）"
            story.append(Paragraph(line, styles["base"]))
            story.append(Spacer(1, 1.2*mm))

    story.append(Spacer(1, 3*mm))

    # 逐条正文
    for it in items:
        story.append(Paragraph(it["title"], styles["h2"]))
        meta = f"{it['channel_title']} · {it['published_local']} · {it['url']}"
        story.append(Paragraph(meta, styles["meta"]))
        story.append(Spacer(1, 1.5*mm))
        # 正文（完整保留模型输出，做基础标题/列表解析）
        story.extend(_md_to_flowables(it["summary_md"], styles))
        story.append(Spacer(1, 4*mm))

    pdf_path.parent.mkdir(exist_ok=True)
    doc = SimpleDocTemplate(
        str(pdf_path),
        pagesize=A4,
        leftMargin=15*mm, rightMargin=15*mm, topMargin=15*mm, bottomMargin=15*mm
    )
    doc.build(story)

# ====== 发邮件（正文很短，附上 PDF；若 550 再尝试 zip 附件） ======
def send_email_with_pdf(subject: str, pdf_path: Path):
    assert SMTP_SERVER and SMTP_USERNAME and SMTP_PASSWORD, "SMTP 环境变量未配置完整"
    from_addr = MAIL_FROM or SMTP_USERNAME
    to_addrs = [addr.strip() for addr in (MAIL_TO or SMTP_USERNAME).split(",") if addr.strip()]

    def _connect():
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
        return server

    def _send(msg):
        server = None
        try:
            server = _connect()
            server.sendmail(from_addr, to_addrs, msg.as_string())
            try: server.quit()
            except Exception: pass
        finally:
            try:
                if server: server.close()
            except Exception: pass

    # 尝试 1：直接附 PDF
    msg1 = MIMEMultipart()
    msg1["From"] = from_addr
    msg1["To"] = ", ".join(to_addrs)
    msg1["Subject"] = subject
    msg1.attach(MIMEText("请查收附件 PDF（移动端友好排版）。", "plain", "utf-8"))
    with open(pdf_path, "rb") as f:
        part = MIMEApplication(f.read(), Name=pdf_path.name)
    part.add_header("Content-Disposition", "attachment", filename=pdf_path.name)
    msg1.attach(part)
    try:
        _send(msg1)
        return
    except smtplib.SMTPDataError as e:
        if e.smtp_code != 550: raise  # 不是风控，抛出
        # 550 再试 zip
    except Exception:
        raise

    # 尝试 2：把 PDF 打包 zip 再发（进一步绕过风控）
    import zipfile
    zip_bytes = io.BytesIO()
    with zipfile.ZipFile(zip_bytes, "w", zipfile.ZIP_DEFLATED) as zf:
        zf.write(pdf_path, arcname=pdf_path.name)
    zip_bytes.seek(0)
    msg2 = MIMEMultipart()
    msg2["From"] = from_addr
    msg2["To"] = ", ".join(to_addrs)
    msg2["Subject"] = subject + "（压缩附件）"
    msg2.attach(MIMEText("PDF 作为压缩包发送。", "plain", "utf-8"))
    zip_part = MIMEApplication(zip_bytes.read(), Name=pdf_path.stem + ".zip")
    zip_part.add_header("Content-Disposition", "attachment", filename=pdf_path.stem + ".zip")
    msg2.attach(zip_part)
    _send(msg2)

# ========= 主流程 =========
def main():
    date_str = today_date_str_tz("Asia/Shanghai")
    rows = load_today_updates()

    tzinfo = tz.gettz("Asia/Shanghai")
    items: List[Dict[str, Any]] = []

    if not rows:
        # 仍生成空 PDF，方便你每天有节拍
        pdf_path = OUT_DIR / f"daily_report_{date_str}.pdf"
        build_pdf(date_str, [], pdf_path)
        send_email_with_pdf(f"YouTube 播客日报 · {date_str}（无更新）", pdf_path)
        print("No updates today. Sent empty PDF.")
        return

    for r in rows:
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
            raw = call_openrouter(build_messages(url))
        except Exception as e:
            raw = f"（模型调用失败：{e}）（基于标题+简介）\n标题：{title}\n频道：{ch}\n简介：{desc[:400]}"

        cleaned = strip_code_fences(raw)
        cleaned = clean_leading_prefixes(cleaned)
        snippet = make_snippet(cleaned, limit=40)

        items.append({
            "video_id": r.get("video_id",""),
            "url": url,
            "title": title,
            "channel_title": ch,
            "published_local": published_local,
            "snippet": snippet,        # 摘要预览
            "summary_md": cleaned,     # 正文（Markdown/纯文本），进入 PDF 解析
        })

    pdf_path = OUT_DIR / f"daily_report_{date_str}.pdf"
    build_pdf(date_str, items, pdf_path)
    send_email_with_pdf(f"YouTube 播客日报 · {date_str}", pdf_path)
    print(f"PDF report generated and sent: {pdf_path}")

if __name__ == "__main__":
    main()
