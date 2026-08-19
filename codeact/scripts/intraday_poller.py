#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
盘中交易时段同花顺API采集调度器 v4.0（SQLite 数仓版）

功能：
  - 交易日（周一至周五）的交易时段内，每30分钟调用 ths_api_poller.py 采集数据
  - 采集成功后自动：同步market_data.json → 写入板块资金数仓 → 生成前端JSON → 注入指数+行情+信号 → 部署到GitHub Pages
  - 收盘后（~15:00）首轮额外触发 generate_signals.py --latest 生成信号（回写数仓）
  - 非交易时段 / 非交易日 / 采集正常 → 静默退出
  - 采集失败 / 信号生成失败 → 通知主人

v4.0 变更：
  - 板块资金数据主存储从 JSON 迁移到 SQLite (sector_fund.db)
  - fund_collector_ths.py 直接写入数仓，不再需要 JSON 文件拷贝步骤
  - generate_frontend_json.py 从数仓读取生成前端 JSON
  - 信号生成同步回写数仓

参数：
  result_mode: auto | display_only | notify | no_reply（默认 auto）
    auto → 正常静默(no_reply)，异常通知(notify)
  --force: 跳过交易时段 / 交易日检查，强制执行采集（调试用）

依赖状态库：./codeact/output/intraday_poller_state.db
  - run_state 表：记录每日收盘触发状态，防重复触发
"""

import asyncio
import json
import os
import re
import shutil
import sqlite3
import subprocess
import sys
import urllib.parse
import urllib.request
import tempfile
from datetime import datetime, time
from pathlib import Path

from codeact_sdk import CodeActSDK

# 将 skill scripts 目录加入 path，以便导入 sector_fund_db
SKILL_SCRIPTS_DIR = os.path.join(
    os.path.dirname(os.path.abspath(__file__)), "..", "..",
    ".skills", "skill_baluosi-pro", "scripts"
)
if os.path.isdir(SKILL_SCRIPTS_DIR) and SKILL_SCRIPTS_DIR not in sys.path:
    sys.path.insert(0, SKILL_SCRIPTS_DIR)

try:
    import sector_fund_db as sdb
    HAS_SQLITE_DB = True
except ImportError:
    HAS_SQLITE_DB = False

# ============================================================
# 路径配置
# ============================================================
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
WORKSPACE_ROOT = os.path.normpath(
    os.path.join(SCRIPT_DIR, "..", "..")
)

# 采集脚本路径
POLLER_SCRIPT = os.path.join(
    WORKSPACE_ROOT, ".skills", "skill_baluosi-pro", "scripts", "ths_api_poller.py"
)
SIGNALS_SCRIPT = os.path.join(
    WORKSPACE_ROOT, ".skills", "skill_baluosi-pro", "scripts", "generate_signals.py"
)

# 中盘信号文件（midday_analysis.py 产出）
MIDDAY_SIGNALS_JSON = os.path.join(
    WORKSPACE_ROOT, "投资", "分析报告", "板块资金", "signals_midday.json"
)

# 板块资金 SQLite 数仓
FUND_DB_PATH = os.path.join(
    WORKSPACE_ROOT, "投资", "分析报告", "板块资金", "sector_fund.db"
)

# 板块资金采集脚本（skill 目录版，写入数仓）
FUND_COLLECTOR_SKILL = os.path.join(
    WORKSPACE_ROOT, ".skills", "skill_baluosi-pro", "scripts", "fund_collector_ths.py"
)

# market_data.json 路径
MARKET_DATA_FILE = os.path.join(
    os.path.dirname(POLLER_SCRIPT), "market_data.json"
)
MARKET_DATA_INVEST = os.path.join(
    WORKSPACE_ROOT, "投资", "分析报告", "板块资金", "market_data.json"
)

# 板块资金历史JSON路径（fund_collector_ths.py 产出）
HISTORY_JSON_SKILL = os.path.join(
    WORKSPACE_ROOT, ".skills", "skill_baluosi-pro", "scripts", "板块资金历史_同花顺90.json"
)
HISTORY_JSON_INVEST = os.path.join(
    WORKSPACE_ROOT, "投资", "分析报告", "板块资金", "板块资金历史_同花顺90.json"
)

# 前端JSON生成器与输出
GENERATOR_SCRIPT = os.path.join(
    WORKSPACE_ROOT, "投资", "产品", "dashboard_v2.3", "generate_json.py"
)
OUTPUT_JSON = os.path.join(
    WORKSPACE_ROOT, "投资", "产品", "fund_data_frontend.json"
)

# GitHub Pages 部署配置
DASHBOARD_DIR = os.path.join(WORKSPACE_ROOT, "投资", "产品", "dashboard_v2.3")
GH_TOKEN_FILE = os.path.join(DASHBOARD_DIR, ".gh_token")
GH_REPO_DIR = os.path.join(tempfile.gettempdir(), "cheshuai_dashboard_deploy")
GH_USER = "cheshuai77"
GH_REPO = "baluosi-pro"
GH_BRANCH = "main"

# 状态库
STATE_DB = os.path.join(
    os.path.dirname(SCRIPT_DIR), "output", "intraday_poller_state.db"
)

# 新浪指数API（降级用）
SINA_INDEX_API = "http://hq.sinajs.cn/list=sh000001,sz399001,sz399006,sh000688,bj899050"

# 交易时段（含缓冲，避免边界漏触发）
# 注意：MORNING_START 原设为 9:25，但 9:30 的日程执行常因系统延迟在 9:25 左右启动，
# 此时被判断为"非交易时段"直接退出，导致 9:30 采集丢失。提前到 9:20 给更多缓冲时间。
MORNING_START = time(9, 20)
MORNING_END = time(11, 45)
AFTERNOON_START = time(12, 0)
AFTERNOON_END = time(15, 35)
CLOSE_TRIGGER_START = time(15, 0)

# 精确采集时间表（仅在以下时间点执行采集，其余触发静默退出）
# 上午: 9:30, 10:00, 10:30, 11:00, 11:30
# 午休: 不采集
# 下午: 13:30, 14:00, 14:30, 15:00, 15:30
# 注意: 13:00 午后刚开盘，数据无意义，跳过
ALLOWED_COLLECTION_TIMES = [
    time(10, 0),
    time(11, 0),
    time(12, 0),
    time(14, 0),
    time(15, 0),
]

# subprocess 超时
POLLER_TIMEOUT = 180
SIGNALS_TIMEOUT = 180
FUND_COLLECTOR_TIMEOUT = 300
GENERATOR_TIMEOUT = 120


def log(msg: str):
    t = datetime.now().strftime("%Y-%m-%dT%H:%M:%S")
    print(f"[{t}] {msg}")


def is_weekday() -> bool:
    """周一至周五返回 True"""
    return datetime.now().weekday() < 5


def is_trading_window() -> bool:
    """当前是否在盘中交易时段（含缓冲）"""
    now = datetime.now().time()
    return (MORNING_START <= now <= MORNING_END) or (AFTERNOON_START <= now <= AFTERNOON_END)


def is_scheduled_collection_time() -> bool:
    """当前时间是否在精确采集时间表内（±15分钟容差）
    
    放宽到20分钟是因为日程执行存在10-17分钟的系统延迟，
    15分钟容差仍会导致10:00采集偶尔丢失。
    """
    now = datetime.now().time()
    for t in ALLOWED_COLLECTION_TIMES:
        # 计算时间差（处理跨午夜边界）
        diff = abs((now.hour * 60 + now.minute) - (t.hour * 60 + t.minute))
        if diff <= 20:
            return True
    return False


def is_after_close_trigger() -> bool:
    """当前是否已过收盘触发时间（>=15:00）"""
    return datetime.now().time() >= CLOSE_TRIGGER_START


# ============================================================
# 数据完整性检查
# ============================================================

# 收盘快照完整度阈值（目标90个板块，允许少量缺失）
CLOSE_DATA_COMPLETE_THRESHOLD = 80
# 收盘快照最大重试次数
CLOSE_DATA_MAX_RETRIES = 3
# 每次重试等待秒数
CLOSE_DATA_RETRY_WAIT_SEC = 30


def count_today_close_sectors() -> int:
    """统计今日 is_close=1 的板块记录数（用于判断收盘快照完整性）。

    数据来源：sector_fund.db 的 sector_fund 表，date=今天 AND is_close=1。
    注意：is_close=1 的记录由 signal 生成/回写时标记，
    代表完整的收盘快照数据行（含全部资金字段）。
    """
    db_path = os.path.normpath(FUND_DB_PATH)
    if not os.path.exists(db_path):
        return 0
    today = datetime.now().strftime("%Y-%m-%d")
    try:
        conn = sqlite3.connect(db_path)
        cursor = conn.execute(
            "SELECT COUNT(DISTINCT sector_name) FROM sector_fund "
            "WHERE date = ? AND is_close = 1",
            (today,),
        )
        count = cursor.fetchone()[0]
        conn.close()
        return count or 0
    except Exception as e:
        log(f"[收盘完整性] 统计失败: {e}")
        return 0


# ============================================================
# 状态库 — run_state（收盘触发去重）
# ============================================================
def init_state_db() -> sqlite3.Connection:
    os.makedirs(os.path.dirname(STATE_DB), exist_ok=True)
    conn = sqlite3.connect(STATE_DB)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS run_state (
            date TEXT PRIMARY KEY,
            close_trigger_done INTEGER DEFAULT 0,
            last_poller_at TEXT
        )
    """)
    conn.commit()
    return conn


def is_close_trigger_done(conn: sqlite3.Connection) -> bool:
    today = datetime.now().strftime("%Y-%m-%d")
    row = conn.execute(
        "SELECT close_trigger_done FROM run_state WHERE date = ?", (today,)
    ).fetchone()
    return row is not None and row[0] == 1


def mark_close_trigger_done(conn: sqlite3.Connection):
    today = datetime.now().strftime("%Y-%m-%d")
    now = datetime.now().strftime("%Y-%m-%dT%H:%M:%S")
    conn.execute(
        "INSERT OR REPLACE INTO run_state (date, close_trigger_done, last_poller_at) VALUES (?, 1, ?)",
        (today, now)
    )
    conn.commit()


def mark_poller_run(conn: sqlite3.Connection):
    today = datetime.now().strftime("%Y-%m-%d")
    now = datetime.now().strftime("%Y-%m-%dT%H:%M:%S")
    conn.execute(
        """
        INSERT INTO run_state (date, close_trigger_done, last_poller_at)
        VALUES (?, 0, ?)
        ON CONFLICT(date) DO UPDATE SET last_poller_at = excluded.last_poller_at
        """,
        (today, now)
    )
    conn.commit()


# ============================================================
# 子进程调用
# ============================================================
def run_script(script_path: str, args: list[str] | None = None,
               timeout: int = 120, label: str = "") -> tuple[bool, str]:
    """
    通用子进程调用。返回 (success, message)。
    """
    cmd = ["python3", script_path] + (args or [])
    cwd = os.path.dirname(os.path.abspath(script_path))
    log(f"[{label}] 执行: {' '.join(cmd)}")
    try:
        result = subprocess.run(
            cmd, capture_output=True, text=True, timeout=timeout, cwd=cwd,
        )
        stdout_tail = (result.stdout or "").strip()
        stderr_tail = (result.stderr or "").strip()
        combined = (stdout_tail + "\n" + stderr_tail).strip()[:500]
        if result.returncode != 0:
            log(f"[{label}] ❌ 退出码={result.returncode}")
            return False, f"退出码={result.returncode} | {combined[:200]}"
        log(f"[{label}] ✅ 完成")
        return True, combined[:200] or "OK"
    except subprocess.TimeoutExpired:
        log(f"[{label}] ❌ 超时 ({timeout}s)")
        return False, f"超时 ({timeout}s)"
    except Exception as e:
        log(f"[{label}] ❌ 异常: {e}")
        return False, str(e)


def run_poller() -> tuple[bool, str]:
    """运行 ths_api_poller.py，返回 (success, message)"""
    poller_path = os.path.normpath(POLLER_SCRIPT)
    if not os.path.exists(poller_path):
        return False, f"采集脚本不存在: {poller_path}"

    ok, msg = run_script(poller_path, timeout=POLLER_TIMEOUT, label="采集")

    if ok:
        # 验证 market_data.json 是否产出且包含有效数据
        md_path = os.path.normpath(MARKET_DATA_FILE)
        if not os.path.exists(md_path):
            return False, "采集完成但 market_data.json 未生成"
        try:
            mtime = os.path.getmtime(md_path)
            age_sec = datetime.now().timestamp() - mtime
            if age_sec > 300:  # 超过5分钟，可能是旧文件
                return False, f"market_data.json 未更新 (文件年龄 {age_sec/60:.0f}min)"
            with open(md_path, "r", encoding="utf-8") as f:
                data = json.load(f)
            # 检查是否有实质数据（非空骨架）
            has_data = any(
                k not in ("meta", "_failures") and v
                for k, v in (data.items() if isinstance(data, dict) else [])
            )
            if not has_data:
                return False, "market_data.json 内容为空（可能非交易日/无数据）"
        except (json.JSONDecodeError, OSError) as e:
            return False, f"market_data.json 读取失败: {e}"

    return ok, msg


def run_signals() -> tuple[bool, str]:
    """运行 generate_signals.py --latest"""
    signals_path = os.path.normpath(SIGNALS_SCRIPT)
    if not os.path.exists(signals_path):
        return False, f"信号脚本不存在: {signals_path}"

    return run_script(signals_path, args=["--latest"], timeout=SIGNALS_TIMEOUT, label="信号")


def run_fund_collector() -> tuple[bool, str]:
    """运行 fund_collector_ths.py（skill版）更新历史库 — 收盘触发用"""
    collector_path = os.path.normpath(FUND_COLLECTOR_SKILL)
    if not os.path.exists(collector_path):
        return False, f"历史库采集脚本不存在: {collector_path}"

    # 记录文件修改时间，用于验证是否更新
    history_path = os.path.normpath(HISTORY_JSON_SKILL)
    old_mtime = None
    if os.path.exists(history_path):
        old_mtime = os.path.getmtime(history_path)

    ok, msg = run_script(
        collector_path, timeout=FUND_COLLECTOR_TIMEOUT, label="历史库采集",
    )

    if ok:
        # 验证历史库是否更新
        if not os.path.exists(history_path):
            return False, "采集完成但 板块资金历史_同花顺90.json 未生成"
        new_mtime = os.path.getmtime(history_path)
        if old_mtime is not None and new_mtime <= old_mtime:
            return False, "板块资金历史_同花顺90.json 未更新（文件修改时间未变）"
        # 验证 JSON 有效性
        try:
            with open(history_path, "r", encoding="utf-8") as f:
                data = json.load(f)
            if not data or "data" not in data:
                return False, "板块资金历史_同花顺90.json 内容为空/无效"
        except (json.JSONDecodeError, OSError) as e:
            return False, f"板块资金历史_同花顺90.json 读取失败: {e}"

        # 同步历史库到投资目录（供 generate_frontend_json 和其他报告使用）
        try:
            shutil.copy2(history_path, HISTORY_JSON_INVEST)
        except Exception as sync_err:
            print(f"[历史库] 同步到投资目录失败（不阻塞）: {sync_err}")

    return ok, msg


# ============================================================
# JSON → SQLite 同步（弥合 fund_collector 写 JSON / generate_json 读 SQLite 的 gap）
# ============================================================
def _sync_json_to_sqlite():
    """读取 fund_collector 产出的 JSON，将当日最新时间点的数据写入 SQLite 数仓。"""
    json_path = os.path.normpath(HISTORY_JSON_SKILL)
    if not os.path.exists(json_path):
        log(f"[JSON→SQLite] JSON 不存在: {json_path}")
        return

    with open(json_path, "r", encoding="utf-8") as f:
        data = json.load(f)

    today = datetime.now().strftime("%Y-%m-%d")
    data_dict = data.get("data", {})
    if not data_dict:
        log("[JSON→SQLite] JSON data 为空")
        return

    # 收集今天所有板块的快照，按时间点分组
    time_groups: dict[str, list[dict]] = {}
    for sector_name, sector_dates in data_dict.items():
        if today not in sector_dates:
            continue
        for time_key, record in sector_dates[today].items():
            if not isinstance(record, dict):
                continue
            if time_key not in time_groups:
                time_groups[time_key] = []
            time_groups[time_key].append({
                "code": str(record.get("code", "")),
                "name": sector_name,
                "change_pct": record.get("change_pct"),
                "inflow": record.get("inflow"),
                "outflow": record.get("outflow"),
                "main_net_in": record.get("main_net_in"),
                "stock_count": record.get("stock_count"),
                "rise_count": record.get("rise_count"),
                "fall_count": record.get("fall_count"),
                "leader_stock": record.get("leader_stock"),
                "leader_change_pct": record.get("leader_change_pct"),
            })

    if not time_groups:
        log(f"[JSON→SQLite] 未找到今日({today})数据")
        return

    db_path = os.path.normpath(FUND_DB_PATH)
    total_rows = 0
    # 只同步最新的 2 个时间点（避免重复写入历史）
    sorted_times = sorted(time_groups.keys())[-2:]
    for t in sorted_times:
        sectors = time_groups[t]
        # 统一时间格式: "1043" → "1043", "0930" → "0930"
        time_str = t.replace(":", "")
        rows = sdb.insert_sector_snapshots(db_path, today, time_str, sectors)
        total_rows += rows
        log(f"[JSON→SQLite] {today} {time_str}: 写入 {rows} 条")

    log(f"[JSON→SQLite] 共写入 {total_rows} 条 ({today})")


# ============================================================
# 指数注入（从 market_data.json 读取，降级新浪API）
# ============================================================
def fetch_index_data() -> list | None:
    """从新浪财经获取实时指数（降级用）。
    标准格式: 名称,今开,昨收,当前价,最高,最低,...
    """
    try:
        req = urllib.request.Request(SINA_INDEX_API)
        req.add_header("Referer", "https://finance.sina.com.cn")
        with urllib.request.urlopen(req, timeout=10) as resp:
            raw = resp.read().decode("gbk")
        indices = []
        for line in raw.strip().split("\n"):
            if not line.strip():
                continue
            m = re.search(r'var hq_str_\w+="(.+)"', line)
            if not m:
                continue
            parts = m.group(1).split(",")
            if len(parts) < 31:
                continue
            # 日期字段校验：parts[30] 为日期（YYYY-MM-DD），非今日数据视为过期，不注入
            data_date = parts[30].strip()
            today_str = datetime.now().strftime("%Y-%m-%d")
            if data_date != today_str:
                log(f"[指数注入] ❌ 新浪API数据过期：返回日期={data_date}，今日={today_str}，跳过注入")
                continue
            name = parts[0]
            # 标准格式: parts[1]=今开, parts[2]=昨收, parts[3]=当前价, parts[4]=最高, parts[5]=最低
            open_val = float(parts[1]) if parts[1] else 0.0
            prev_close = float(parts[2]) if parts[2] else 0.0
            value = float(parts[3]) if parts[3] else 0.0
            high = float(parts[4]) if parts[4] else 0.0
            low = float(parts[5]) if parts[5] else 0.0
            change = round((value - prev_close) / prev_close * 100, 2) if prev_close else 0.0
            change_amount = round(value - prev_close, 2)
            indices.append({
                "name": name, "value": value, "change": change,
                "change_amount": change_amount,
                "open": open_val, "high": high, "low": low,
            })
        return indices if indices else None
    except Exception as e:
        log(f"实时指数获取失败: {e}")
        return None


def generate_and_inject_signals(json_path: str):
    """读取前端JSON → 基于轻量规则生成四区信号 → 注入 signals 字段。"""
    try:
        with open(json_path, "r", encoding="utf-8") as f:
            data = json.load(f)

        dates = sorted(data.get("sectorsByDate", {}).keys(), reverse=True)
        if not dates:
            log("⚠ sectorsByDate 为空，跳过信号生成")
            return
        today = dates[0]
        sectors = data.get("sectorsByDate", {}).get(today, [])
        if not sectors:
            log(f"⚠ {today} 板块数据为空，跳过信号生成")
            return

        # --- mainLine: 主线信号 ---
        mainline_pool = [
            s for s in sectors
            if s.get("inflow", 0) > 10 and s.get("change", 0) > 0
        ]
        mainline_pool.sort(key=lambda x: x.get("inflow", 0), reverse=True)
        mainline_top = mainline_pool[:5]
        mainline_names = {s.get("name") for s in mainline_top}

        def _calc_stars(change_pct):
            if change_pct >= 3.0:
                return 5
            elif change_pct >= 1.5:
                return 3
            else:
                return 1

        def _build_signal(s, include_stars=False):
            sig = {
                "name": s.get("name"),
                "change": s.get("change"),
                "inflow": s.get("inflow"),
            }
            if include_stars:
                sig["stars"] = _calc_stars(s.get("change", 0))
            if s.get("leader"):
                sig["leader"] = s.get("leader")
                sig["leaderChange"] = s.get("leaderChange")
            return sig

        mainline_signals = [_build_signal(s, include_stars=True) for s in mainline_top]

        # --- latent: 潜伏信号 ---
        latent_pool = [
            s for s in sectors
            if s.get("accum3d", 0) > 0 and s.get("name") not in mainline_names
        ]
        latent_pool.sort(key=lambda x: x.get("inflow", 0), reverse=True)
        latent_signals = [_build_signal(s) for s in latent_pool[:5]]

        # --- disaster: 砸盘信号 ---
        disaster_pool = [s for s in sectors if s.get("inflow", 0) < -15]
        disaster_pool.sort(key=lambda x: x.get("inflow", 0))
        disaster_signals = [_build_signal(s) for s in disaster_pool[:5]]

        # --- risk: 风险信号（价涨资金出） ---
        risk_pool = [s for s in sectors if s.get("change", 0) > 0 and s.get("inflow", 0) < 0]
        risk_pool.sort(key=lambda x: x.get("inflow", 0))
        risk_signals = [_build_signal(s) for s in risk_pool[:5]]

        # --- 合并写入 signals 字段 ---
        signals = data.get("signals", {})
        signals[today] = {
            "mainLine": mainline_signals,
            "latent": latent_signals,
            "disaster": disaster_signals,
            "risk": risk_signals,
        }
        data["signals"] = signals

        # ---- 回填 riseCount/fallCount/risePct 从 sectorsByDate → signals ----
        sector_stats = {}
        for s in sectors:
            name = s.get("name", "")
            if name:
                rc = s.get("riseCount", 0) or 0
                fc = s.get("fallCount", 0) or 0
                total = rc + fc
                sector_stats[name] = {
                    "riseCount": rc,
                    "fallCount": fc,
                    "risePct": s.get("risePct", 0) or 0,
                    "fallPct": round(fc / total * 100, 1) if total > 0 else 0,
                }

        for zone in ["mainLine", "latent"]:
            for sig in signals[today].get(zone, []):
                name = sig.get("name", "")
                if name in sector_stats:
                    sig["riseCount"] = sector_stats[name]["riseCount"]
                    sig["risePct"] = sector_stats[name]["risePct"]

        for sig in signals[today].get("disaster", []):
            name = sig.get("name", "")
            if name in sector_stats:
                sig["fallCount"] = sector_stats[name]["fallCount"]
                sig["fallPct"] = sector_stats[name]["fallPct"]

        log(f"[信号] 📊 回填涨跌家数: {sum(1 for s in sector_stats.values() if s['riseCount'] > 0 or s['fallCount'] > 0)} 个板块有数据")

        with open(json_path, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=2)

        log(f"[信号] ✅ 四区信号注入: mainLine={len(mainline_signals)} latent={len(latent_signals)} "
            f"disaster={len(disaster_signals)} risk={len(risk_signals)} (date={today})")
    except Exception as e:
        log(f"[信号] ⚠ 信号生成注入失败（不阻塞）: {e}")


def inject_midday_signals_if_available(json_path: str):
    """中盘时段：读取 midday_analysis.py 产出的 signals_midday.json，
    将 LLM 生成的更准确四区信号注入前端JSON，覆盖规则生成的信号。
    仅在中盘窗口（11:30-12:30）且文件存在+当日数据时执行。
    """
    # 仅在中盘窗口执行
    now = datetime.now().time()
    if not (time(11, 30) <= now <= time(12, 30)):
        log("[中盘信号] 非中盘窗口，跳过")
        return

    signals_path = os.path.normpath(MIDDAY_SIGNALS_JSON)
    if not os.path.exists(signals_path):
        log("[中盘信号] signals_midday.json 不存在，跳过")
        return

    try:
        # 验证时效性（文件修改时间不超过60分钟）
        mtime = os.path.getmtime(signals_path)
        age_min = (datetime.now().timestamp() - mtime) / 60
        if age_min > 120:
            log(f"[中盘信号] signals_midday.json 过旧 ({age_min:.0f}min)，跳过")
            return

        with open(signals_path, "r", encoding="utf-8") as f:
            mid_signals = json.load(f)

        mid_date = mid_signals.get("date", "")
        today = datetime.now().strftime("%Y-%m-%d")
        if mid_date != today:
            log(f"[中盘信号] 日期不匹配: {mid_date} ≠ {today}，跳过")
            return

        # 读取前端JSON
        with open(json_path, "r", encoding="utf-8") as f:
            data = json.load(f)

        # 转换中盘信号格式为前端格式
        def _to_frontend(signal_list: list, zone: str) -> list:
            result = []
            for s in (signal_list or []):
                entry = {
                    "name": s.get("name", ""),
                    "change": s.get("change_pct", s.get("change", 0)),
                    "inflow": s.get("main_net_in", s.get("inflow", 0)),
                }
                # mainLine 特有字段
                if zone == "mainLine":
                    entry["stars"] = s.get("stars", 1)
                    entry["verdict"] = s.get("verdict", "")
                    if s.get("rise_count") is not None:
                        entry["riseCount"] = s.get("rise_count")
                        entry["fallCount"] = s.get("fall_count")
                        entry["risePct"] = s.get("rise_pct", 0)
                # latent 特有字段
                if zone == "latent":
                    if s.get("reasons"):
                        entry["reasons"] = s["reasons"]
                    if s.get("accum_3d_net") is not None:
                        entry["accum3d"] = s["accum_3d_net"]
                # disaster 特有字段
                if zone == "disaster":
                    entry["verdict"] = s.get("verdict", "")
                    if s.get("fall_count") is not None:
                        entry["fallCount"] = s.get("fall_count")
                        entry["fallPct"] = s.get("fall_pct", 0)
                # risk 特有字段
                if zone == "risk":
                    entry["type"] = s.get("type", "")
                    entry["signal"] = s.get("signal", "")
                result.append(entry)
            return result

        # 注入四区信号，覆盖规则生成的版本
        signals = data.get("signals", {})
        signals[today] = {
            "mainLine": _to_frontend(mid_signals.get("mainLine", []), "mainLine"),
            "latent": _to_frontend(mid_signals.get("latent", []), "latent"),
            "disaster": _to_frontend(mid_signals.get("disaster", []), "disaster"),
            "risk": _to_frontend(mid_signals.get("risk", []), "risk"),
            "_source": "midday_llm",  # 标记来源
        }
        data["signals"] = signals

        # ---- 回填 riseCount/fallCount/risePct 从 sectorsByDate → signals ----
        sectors = data.get("sectorsByDate", {}).get(today, [])
        sector_stats = {}
        for s in sectors:
            name = s.get("name", "")
            if name:
                rc = s.get("riseCount", 0) or 0
                fc = s.get("fallCount", 0) or 0
                total = rc + fc
                sector_stats[name] = {
                    "riseCount": rc,
                    "fallCount": fc,
                    "risePct": s.get("risePct", 0) or 0,
                    "fallPct": round(fc / total * 100, 1) if total > 0 else 0,
                }

        for zone in ["mainLine", "latent"]:
            for sig in signals[today].get(zone, []):
                name = sig.get("name", "")
                if name in sector_stats:
                    sig["riseCount"] = sector_stats[name]["riseCount"]
                    sig["risePct"] = sector_stats[name]["risePct"]

        for sig in signals[today].get("disaster", []):
            name = sig.get("name", "")
            if name in sector_stats:
                sig["fallCount"] = sector_stats[name]["fallCount"]
                sig["fallPct"] = sector_stats[name]["fallPct"]

        log(f"[中盘信号] 📊 回填涨跌家数: {sum(1 for s in sector_stats.values() if s['riseCount'] > 0 or s['fallCount'] > 0)} 个板块有数据")

        with open(json_path, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=2)

        log(f"[中盘信号] ✅ LLM信号已覆盖: mainLine={len(mid_signals.get('mainLine',[]))} "
            f"latent={len(mid_signals.get('latent',[]))} "
            f"disaster={len(mid_signals.get('disaster',[]))} "
            f"risk={len(mid_signals.get('risk',[]))} (date={today})")
    except Exception as e:
        log(f"[中盘信号] ⚠ 注入失败（不阻塞）: {e}")


def inject_indices(json_path: str):
    """注入大盘指数数据到前端JSON。
    优先从 market_data.json 读取全字段，降级用新浪s_前缀API（仅3字段）。
    """
    try:
        with open(json_path, "r", encoding="utf-8") as f:
            data = json.load(f)

        mo = data.get("marketOverview")
        if not mo:
            log("[指数注入] ⚠ marketOverview 不存在，跳过")
            return

        dates = sorted(mo.keys(), reverse=True)
        if not dates:
            log("[指数注入] ⚠ marketOverview 无日期条目，跳过")
            return

        # 优先 market_data.json（全字段）
        indices = None
        md_path = MARKET_DATA_INVEST
        try:
            if os.path.exists(md_path):
                with open(md_path, "r", encoding="utf-8") as f:
                    md = json.load(f)
                md_indices = md.get("indices", [])
                if len(md_indices) >= 4:
                    indices = []
                    for idx in md_indices:
                        indices.append({
                            "name": idx["name"],
                            "value": idx["value"],
                            "change": idx["change"],
                            "change_amount": idx.get("change_amount", 0),
                            "open": idx.get("open", 0),
                            "high": idx.get("high", 0),
                            "low": idx.get("low", 0),
                        })
                    log(f"[指数注入] ✅ 从 market_data.json 读取 {len(indices)} 个指数（全字段）")
        except Exception as e:
            log(f"[指数注入] market_data.json 读取失败: {e}")

        if not indices:
            indices = fetch_index_data()
            if not indices:
                log("[指数注入] ⚠ 指数数据为空，跳过注入")
                return
            log(f"[指数注入] ⚠ 降级使用新浪实时API（仅{len(indices)}个指数，无open/high/low）")

        latest_date = dates[0]
        mo[latest_date]["indices"] = indices
        log(f"[指数注入] 成功（最新日期 {latest_date}）: {[(i['name'], i['change']) for i in indices]}")

        with open(json_path, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
    except Exception as e:
        log(f"[指数注入] ⚠ 注入失败（不阻塞）: {e}")


def inject_market_extra(json_path: str):
    """计算行情概览补充字段 → 注入 marketOverview[latest_date]。
    注入字段: totalStocks, totalRise, totalFall, risePct, sectorUp, sectorDown,
    topSector, topSectorChange, totalFlowIn, totalFlowOut, netFlow, avgChange, updateTime
    同时回填板块级 riseCount/fallCount（从 market_data.json blocks 成分股统计）。
    """
    try:
        with open(json_path, "r", encoding="utf-8") as f:
            data = json.load(f)

        mo = data.get("marketOverview")
        if not mo:
            log("[行情概览] ⚠ marketOverview 不存在，跳过")
            return

        dates = sorted(mo.keys(), reverse=True)
        if not dates:
            log("[行情概览] ⚠ marketOverview 无日期条目，跳过")
            return

        latest_date = dates[0]
        sectors = data.get("sectorsByDate", {}).get(latest_date, [])

        # ── 从 market_data.json 读取精确涨跌家数 ──
        mkt_data = {}
        blocks_data = {}
        for mp in [MARKET_DATA_FILE, MARKET_DATA_INVEST]:
            if os.path.exists(mp):
                try:
                    with open(mp, "r", encoding="utf-8") as mf:
                        md = json.load(mf)
                    mkt_data = md.get("market", {})
                    blocks_data = md.get("blocks", {})
                    log(f"[行情概览] 读取 market_data.json: up={mkt_data.get('up_count')}, down={mkt_data.get('down_count')}")
                    break
                except Exception:
                    pass

        # ── 从 blocks 成分股计算板块涨跌家数 ──
        block_rise_fall = {}  # {板块code: (rise, fall)}
        for code, bdata in blocks_data.items():
            members = bdata.get("members", [])
            if not members:
                continue
            rise = sum(1 for m in members if (m.get("change_pct") or 0) > 0)
            fall = sum(1 for m in members if (m.get("change_pct") or 0) < 0)
            block_rise_fall[code] = (rise, fall)

        # ── 回填板块 riseCount/fallCount ──
        sectors_with_rf = 0
        for sec in sectors:
            code = sec.get("code", "")
            if code in block_rise_fall:
                rc, fc = block_rise_fall[code]
                sec["riseCount"] = rc
                sec["fallCount"] = fc
                total_rf = rc + fc
                sec["risePct"] = round(rc / total_rf * 100, 1) if total_rf > 0 else 0
                sectors_with_rf += 1
            # 如果原来就有非零值则保留（来自 fund_collector_ths.py 采集）
            elif sec.get("riseCount", 0) > 0 or sec.get("fallCount", 0) > 0:
                sectors_with_rf += 1

        if sectors_with_rf > 0:
            log(f"[行情概览] 板块涨跌家数: {sectors_with_rf}/{len(sectors)} 个板块有数据")

        # ── 汇总大盘数据 ──
        total_stocks = 0
        sector_up = 0
        sector_down = 0
        sector_flat = 0
        total_flow_in = 0.0
        total_flow_out = 0.0
        net_flow = 0.0
        sum_change = 0.0
        top_sector = ""
        top_sector_change = -999.0

        for sec in sectors:
            sc = sec.get("stockCount", 0) or 0
            chg = sec.get("change", 0) or 0
            inflow = sec.get("inflow", 0) or 0

            total_stocks += sc
            if chg > 0:
                sector_up += 1
            elif chg < 0:
                sector_down += 1
            else:
                sector_flat += 1

            if inflow > 0:
                total_flow_in += inflow
            elif inflow < 0:
                total_flow_out += abs(inflow)

            net_flow += inflow
            sum_change += chg

            if chg > top_sector_change:
                top_sector_change = chg
                top_sector = sec.get("name", "")

        # 涨跌家数：优先用 indexflash API 精确值，降级用板块 stockCount 估算
        api_rise = mkt_data.get("up_count", 0)
        api_down = mkt_data.get("down_count", 0)
        if api_rise > 0 or api_down > 0:
            total_rise = api_rise
            total_fall = api_down
        else:
            # 降级：按板块涨跌方向 × stockCount 估算
            total_rise = sum(
                (sec.get("stockCount", 0) or 0)
                for sec in sectors if (sec.get("change", 0) or 0) > 0
            )
            total_fall = sum(
                (sec.get("stockCount", 0) or 0)
                for sec in sectors if (sec.get("change", 0) or 0) < 0
            )

        total_rf = total_rise + total_fall
        rise_pct = round(total_rise / total_rf * 100, 1) if total_rf > 0 else 0.0
        avg_change = round(sum_change / len(sectors), 2) if sectors else 0.0
        total_flow_out = round(total_flow_out, 1)
        total_flow_in = round(total_flow_in, 1)
        net_flow = round(net_flow, 1)

        entry = mo[latest_date]
        entry["totalStocks"] = total_stocks
        entry["totalRise"] = total_rise
        entry["totalFall"] = total_fall
        entry["risePct"] = rise_pct
        entry["sectorUp"] = sector_up
        entry["sectorDown"] = sector_down
        entry["sectorFlat"] = sector_flat
        entry["topSector"] = top_sector
        entry["topSectorChange"] = top_sector_change if top_sector else 0.0
        entry["totalFlowIn"] = total_flow_in
        entry["totalFlowOut"] = total_flow_out
        entry["netFlow"] = net_flow
        entry["avgChange"] = avg_change
        entry["updateTime"] = datetime.now().strftime("%H:%M")

        with open(json_path, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=2)

        log(f"[行情概览] ✅ stocks={total_stocks} rise={total_rise} fall={total_fall} "
            f"rise_pct={rise_pct}% up={sector_up} down={sector_down} flat={sector_flat} net={net_flow} avg={avg_change}%")
    except Exception as e:
        log(f"[行情概览] ⚠ 注入失败（不阻塞）: {e}")


# ============================================================
# 前端JSON生成 + GitHub Pages 部署流程
# ============================================================
def _deploy_to_github_pages(json_file: str) -> tuple[bool, str]:
    """
    将前端JSON组装HTML并推送到GitHub Pages。
    Returns: (success, message)
    """
    try:
        # 读取 GitHub Token
        if not os.path.exists(GH_TOKEN_FILE):
            return False, f"Token文件不存在: {GH_TOKEN_FILE}"
        with open(GH_TOKEN_FILE, "r") as f:
            token = f.read().strip()
        if not token or token.startswith("COZE_CRED_DUMMY"):
            return False, "GitHub Token无效或为占位符"

        # 组装 HTML
        template_head = os.path.join(DASHBOARD_DIR, "template_head.html")
        template_tail = os.path.join(DASHBOARD_DIR, "template_tail.html")
        if not os.path.exists(template_head) or not os.path.exists(template_tail):
            return False, "HTML模板文件缺失"

        assembled = os.path.join(tempfile.gettempdir(), f"dashboard_v23_{os.getpid()}.html")
        with open(assembled, "w", encoding="utf-8") as f:
            with open(template_head, "r", encoding="utf-8") as th:
                f.write(th.read())
            with open(json_file, "r", encoding="utf-8") as jf:
                f.write(jf.read())
            f.write(";\n")
            with open(template_tail, "r", encoding="utf-8") as tt:
                f.write(tt.read())

        repo_url = f"https://{token}@github.com/{GH_USER}/{GH_REPO}.git"

        # 克隆或更新 repo
        if os.path.isdir(os.path.join(GH_REPO_DIR, ".git")):
            subprocess.run(["git", "remote", "set-url", "origin", repo_url], cwd=GH_REPO_DIR, capture_output=True, timeout=30)
            subprocess.run(["git", "fetch", "origin"], cwd=GH_REPO_DIR, capture_output=True, timeout=30)
            subprocess.run(["git", "reset", "--hard", f"origin/{GH_BRANCH}"], cwd=GH_REPO_DIR, capture_output=True, timeout=30)
        else:
            if os.path.exists(GH_REPO_DIR):
                shutil.rmtree(GH_REPO_DIR)
            r = subprocess.run(["git", "clone", repo_url, GH_REPO_DIR], capture_output=True, text=True, timeout=60)
            if r.returncode != 0:
                return False, f"git clone失败: {r.stderr}"
            subprocess.run(["git", "config", "user.email", f"{GH_USER}@users.noreply.github.com"], cwd=GH_REPO_DIR, capture_output=True, timeout=10)
            subprocess.run(["git", "config", "user.name", GH_USER], cwd=GH_REPO_DIR, capture_output=True, timeout=10)

        # 复制文件
        data_dir = os.path.join(GH_REPO_DIR, "data")
        os.makedirs(data_dir, exist_ok=True)
        shutil.copy2(assembled, os.path.join(GH_REPO_DIR, "index.html"))
        shutil.copy2(json_file, os.path.join(data_dir, "fund_data_frontend.json"))

        # 同步 daily_analysis.json（Tab2 数据）
        daily_analysis = os.path.join(WORKSPACE_ROOT, "投资", "产品", "daily_analysis.json")
        if os.path.exists(daily_analysis):
            shutil.copy2(daily_analysis, os.path.join(data_dir, "daily_analysis.json"))

        # 清理临时 HTML
        try:
            os.remove(assembled)
        except:
            pass

        # 提交推送
        subprocess.run(["git", "add", "-A"], cwd=GH_REPO_DIR, capture_output=True, timeout=30)
        diff_r = subprocess.run(["git", "diff", "--cached", "--quiet"], cwd=GH_REPO_DIR, capture_output=True, timeout=10)
        if diff_r.returncode == 0:
            return True, "数据无变化，跳过推送"

        date_tag = datetime.now().strftime("%Y-%m-%d %H:%M")
        subprocess.run(["git", "commit", "-m", f"auto: 看板数据更新 {date_tag}"], cwd=GH_REPO_DIR, capture_output=True, timeout=30)
        push_r = subprocess.run(["git", "push", "origin", GH_BRANCH], cwd=GH_REPO_DIR, capture_output=True, text=True, timeout=60)
        if push_r.returncode != 0:
            return False, f"git push失败: {push_r.stderr}"

        return True, f"已部署到 https://{GH_USER}.github.io/{GH_REPO}/"

    except Exception as e:
        return False, f"部署异常: {e}"


async def update_frontend_json() -> tuple[str, str]:
    """
    前端JSON生成 + GitHub Pages 部署流程。
    每一步失败则提前返回 ("failed", msg)，不继续后续步骤。
    注入步骤失败只记录日志，不中断流程。

    Returns:
        ("updated", msg)  — 前端JSON更新+部署成功
        ("failed", msg)   — 某步骤失败
    """
    try:
        # ---- Step 1: 同步 market_data.json ----
        md_src = os.path.normpath(MARKET_DATA_FILE)
        md_dst = os.path.normpath(MARKET_DATA_INVEST)
        try:
            os.makedirs(os.path.dirname(md_dst), exist_ok=True)
            shutil.copy2(md_src, md_dst)
            log("[前端] 1/6 ✅ market_data.json 同步完成")
        except Exception as e:
            log(f"[前端] 1/6 ❌ market_data.json 同步失败: {e}")
            return "failed", f"同步market_data.json失败: {e}"

        # ---- Step 2: 运行 fund_collector_ths.py（skill版 v4.2） ----
        collector_path = os.path.normpath(FUND_COLLECTOR_SKILL)
        if not os.path.exists(collector_path):
            log(f"[前端] 2/6 ❌ 采集脚本不存在: {collector_path}")
            return "failed", f"板块采集脚本不存在: {collector_path}"

        ok, msg = run_script(
            collector_path,
            args=[],
            timeout=FUND_COLLECTOR_TIMEOUT,
            label="板块历史库(盘中)",
        )
        if not ok:
            log(f"[前端] 2/6 ⚠️ 板块历史库更新失败（继续执行后续步骤）: {msg}")
            # 不 return，继续执行后续步骤（历史库不影响前端展示）
        else:
            log("[前端] 2/6 ✅ 板块历史库更新完成")
            # 同步 skill 目录的历史库到投资目录（备份）
            try:
                skill_history = os.path.normpath(HISTORY_JSON_SKILL)
                if os.path.exists(skill_history):
                    shutil.copy2(skill_history, HISTORY_JSON_INVEST)
                    log("[前端] 2/6 历史库已同步到投资目录")
            except Exception as e:
                log(f"[前端] 2/6 历史库同步失败（不阻塞）: {e}")

        # ---- Step 2.5: JSON → SQLite 同步 ----
        if HAS_SQLITE_DB:
            try:
                _sync_json_to_sqlite()
                log("[前端] 2.5/6 ✅ JSON→SQLite 同步完成")
            except Exception as e:
                log(f"[前端] 2.5/6 ⚠️ JSON→SQLite 同步失败（不阻塞）: {e}")
        else:
            log("[前端] 2.5/6 ⚠️ sector_fund_db 模块不可用，跳过 JSON→SQLite 同步")

        # ---- Step 3: 运行 generate_frontend_json.py ----
        gen_path = os.path.normpath(GENERATOR_SCRIPT)
        if not os.path.exists(gen_path):
            log(f"[前端] 3/6 ❌ 生成器不存在: {gen_path}（数据采集已完成）")
            return "data_only", "前端JSON生成器不存在，数据采集完成"

        ok, msg = run_script(
            gen_path,
            args=["no_reply", FUND_DB_PATH, OUTPUT_JSON],
            timeout=GENERATOR_TIMEOUT,
            label="前端JSON生成",
        )
        if not ok:
            log(f"[前端] 3/6 ❌ 前端JSON生成失败: {msg}")
            return "failed", f"前端JSON生成失败: {msg}"
        if not os.path.exists(OUTPUT_JSON):
            log("[前端] 3/6 ❌ 前端JSON未生成")
            return "failed", "前端JSON文件未产出"
        log("[前端] 3/6 ✅ 前端JSON生成完成")

        # ---- Step 3.5: 注入四区信号（主线确认/潜伏/砸盘/风险） ----
        log("[前端] 3.5/6 注入四区信号...")
        generate_and_inject_signals(OUTPUT_JSON)

        # ---- Step 3.6: 中盘LLM信号覆盖（午盘时段优先级更高） ----
        log("[前端] 3.6/6 中盘信号覆盖...")
        inject_midday_signals_if_available(OUTPUT_JSON)

        # ---- Step 4: 注入指数数据 ----
        log("[前端] 4/6 注入指数数据...")
        inject_indices(OUTPUT_JSON)

        # ---- Step 5: 注入行情概览 ----
        log("[前端] 5/6 注入行情概览...")
        inject_market_extra(OUTPUT_JSON)

        # ---- Step 6: 部署到 GitHub Pages ----
        log("[前端] 6/6 部署到 GitHub Pages...")
        deploy_ok, deploy_msg = _deploy_to_github_pages(OUTPUT_JSON)
        if deploy_ok:
            log(f"[前端] 6/6 ✅ 部署成功: {deploy_msg}")
        else:
            log(f"[前端] 6/6 ⚠️ 部署失败（不阻塞）: {deploy_msg}")

        log("[前端] ✅ 前端JSON更新+部署完成")
        return "updated", f"前端JSON已更新，部署{'成功' if deploy_ok else '失败: ' + deploy_msg}"

    except Exception as e:
        log(f"[前端] 未预期异常: {e}")
        return "failed", str(e)


# ============================================================
# 主流程
# ============================================================
async def main():
    args = sys.argv[1:]
    force_mode = "--force" in args
    # 过滤掉 --force，剩下的第一个非 flag 参数是 result_mode
    clean_args = [a for a in args if a != "--force"]
    result_mode = clean_args[0] if clean_args else "auto"
    log(f"intraday_poller v3.0 启动 | result_mode={result_mode} | force={force_mode}")

    sdk = CodeActSDK()

    try:
        # ---- 第1步：日期检查 ----
        if not force_mode and not is_weekday():
            log("非交易日（周末），静默退出")
            actual = "no_reply" if result_mode == "auto" else result_mode
            await sdk.submit_result(
                result_mode=actual, status="success",
                message="非交易日，静默退出",
            )
            return

        # ---- 第2步：交易时段检查 ----
        now = datetime.now()
        if not force_mode and not is_trading_window():
            log(f"非交易时段 ({now.strftime('%H:%M')})，静默退出")
            actual = "no_reply" if result_mode == "auto" else result_mode
            await sdk.submit_result(
                result_mode=actual, status="success",
                message="非交易时段，静默退出",
            )
            return

        # ---- 第2.5步：精确采集时间表检查 ----
        if not force_mode and not is_scheduled_collection_time():
            log(f"非采集时间点 ({now.strftime('%H:%M')})，静默退出 | 允许时间: {[str(t) for t in ALLOWED_COLLECTION_TIMES]}")
            actual = "no_reply" if result_mode == "auto" else result_mode
            await sdk.submit_result(
                result_mode=actual, status="success",
                message=f"非采集时间点({now.strftime('%H:%M')})，静默退出",
            )
            return

        # ---- 第3步：初始化状态库 ----
        conn = init_state_db()

        # ---- 第4步：执行采集 ----
        log(f"===== 盘中采集开始 {now.strftime('%Y-%m-%d %H:%M:%S')} =====")
        poller_ok, poller_msg = run_poller()
        mark_poller_run(conn)

        # ---- 第5步：采集成功后 → 前端JSON生成 ----
        frontend_status = "skipped"
        frontend_msg = ""
        if poller_ok:
            frontend_status, frontend_msg = await update_frontend_json()
            log(f"[前端] 结果: {frontend_status} | {frontend_msg}")

        # ---- 第6步：收盘后触发信号生成 + 历史库更新 ----
        signals_ok = True
        signals_msg = ""
        collector_ok = True
        collector_msg = ""
        if poller_ok and is_after_close_trigger():
            if not is_close_trigger_done(conn):
                log("触发收盘信号生成 ...")
                signals_ok, signals_msg = run_signals()

                log("触发收盘历史库更新 ...")
                collector_ok, collector_msg = run_fund_collector()

                # ===== 收盘快照完整性检查 + 重试 =====
                # 15:06 收盘快照经常只采到12-17个板块（目标90个），需要重采机制
                close_count = count_today_close_sectors()
                log(f"[收盘完整性] is_close=1 板块数: {close_count}/{CLOSE_DATA_COMPLETE_THRESHOLD}")
                if close_count < CLOSE_DATA_COMPLETE_THRESHOLD:
                    log(f"[收盘完整性] 数据不完整（{close_count} < {CLOSE_DATA_COMPLETE_THRESHOLD}），开始重试 ...")
                    retry_count = 0
                    while (close_count < CLOSE_DATA_COMPLETE_THRESHOLD
                           and retry_count < CLOSE_DATA_MAX_RETRIES
                           and collector_ok):
                        retry_count += 1
                        log(f"[收盘完整性] 第 {retry_count}/{CLOSE_DATA_MAX_RETRIES} 次重试 "
                            f"(等待 {CLOSE_DATA_RETRY_WAIT_SEC}s 后重采)")
                        # 等待后重新采集 + 信号生成（重新标记 is_close）
                        await asyncio.sleep(CLOSE_DATA_RETRY_WAIT_SEC)

                        # 重新采集历史库
                        retry_coll_ok, retry_coll_msg = run_fund_collector()
                        if retry_coll_ok:
                            collector_msg = f"{collector_msg}; 重试{retry_count}次成功"
                        else:
                            collector_ok = False
                            collector_msg = f"重试{retry_count}次失败: {retry_coll_msg}"
                            break

                        # 重新生成信号（重新标记 is_close=1）
                        retry_sig_ok, retry_sig_msg = run_signals()
                        if not retry_sig_ok:
                            signals_ok = False
                            signals_msg = f"重试{retry_count}次信号失败: {retry_sig_msg}"

                        # 重新检查数量
                        close_count = count_today_close_sectors()
                        log(f"[收盘完整性] 重试后 is_close=1 板块数: {close_count}/{CLOSE_DATA_COMPLETE_THRESHOLD}")

                    if close_count >= CLOSE_DATA_COMPLETE_THRESHOLD:
                        log(f"[收盘完整性] ✅ 数据完整（{close_count} 个板块），经过 {retry_count} 次重试")
                    else:
                        log(f"[收盘完整性] ⚠️ 重试 {CLOSE_DATA_MAX_RETRIES} 次后仍不足 "
                            f"({close_count}/{CLOSE_DATA_COMPLETE_THRESHOLD})，放弃")
                        # 不修改 collector_ok/signals_ok 状态，保留已有数据
                # ===== 收盘完整性检查结束 =====

                if signals_ok:
                    mark_close_trigger_done(conn)
            else:
                log("收盘信号已触发过，跳过")
                signals_msg = "已触发过，跳过"

        conn.close()

        # ---- 第7步：提交结果 ----
        poller_failed = not poller_ok
        close_failed = not signals_ok or not collector_ok

        if poller_failed:
            # 采集失败 → 通知
            await sdk.submit_result(
                result_mode="notify", status="error",
                message=f"[主人](at://owner) 盘中采集异常: {poller_msg}",
                data={"poller_ok": False, "time": now.strftime("%H:%M:%S")},
            )
        elif close_failed:
            # 收盘触发步骤失败 → 通知
            errors = []
            if not signals_ok:
                errors.append(f"信号: {signals_msg}")
            if not collector_ok:
                errors.append(f"历史库: {collector_msg}")
            await sdk.submit_result(
                result_mode="notify", status="error",
                message=f"[主人](at://owner) 收盘触发异常: {'; '.join(errors)}",
                data={
                    "poller_ok": True,
                    "signals_ok": signals_ok,
                    "collector_ok": collector_ok,
                    "frontend_status": frontend_status,
                },
            )
        else:
            # 正常完成 → 静默
            actual = "no_reply" if result_mode == "auto" else "display_only"
            parts = []
            if signals_msg and signals_msg != "已触发过，跳过":
                parts.append("收盘信号")
            if collector_ok and collector_msg and collector_msg != "N/A":
                parts.append("历史库")
            extra = f" + {'+'.join(parts)}" if parts else ""
            await sdk.submit_result(
                result_mode=actual, status="success",
                message=f"盘中采集完成{extra} (前端{frontend_status})",
                data={
                    "poller": poller_msg,
                    "frontend_status": frontend_status,
                    "signals": signals_msg or "N/A",
                    "collector": collector_msg or "N/A",
                    "time": now.strftime("%H:%M:%S"),
                },
            )

    except Exception as e:
        log(f"未预期异常: {e}")
        await sdk.submit_result(
            result_mode="notify", status="error",
            message=f"intraday_poller 执行异常: {e}",
            data={"error_type": type(e).__name__},
        )


if __name__ == "__main__":
    asyncio.run(main())
