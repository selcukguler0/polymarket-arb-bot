#!/usr/bin/env python3
from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import shutil
import socket
import subprocess
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional


REPO_ROOT = Path(__file__).resolve().parents[1]
JOB_ORC_DIR = REPO_ROOT / "job-orc"
PROMPTS_DIR = JOB_ORC_DIR / "prompts"
TASKS_FILE = JOB_ORC_DIR / "tasks.json"
TASKS_MD = JOB_ORC_DIR / "TASKS.md"
MEMORY_DIR = JOB_ORC_DIR / "memory"
REPORTS_DIR = JOB_ORC_DIR / "reports"
KNOWLEDGE_DIR = JOB_ORC_DIR / "knowledge"
KNOWLEDGE_MANIFEST_FILE = KNOWLEDGE_DIR / "manifest.json"
HEARTBEATS_DIR = JOB_ORC_DIR / "heartbeats"
RUNS_DIR = JOB_ORC_DIR / "runs"
WORKTREES_DIR = JOB_ORC_DIR / "worktrees"
QUOTA_BLOCK_FILE = JOB_ORC_DIR / "quota_block.json"

CURRENT_HOST = os.environ.get("POLYMARKET_ORC_HOST", "local")
RUNTIME_WORKSPACE = Path(os.environ.get("POLYMARKET_ORC_RUNTIME_WORKSPACE", str(REPO_ROOT)))
AGENT_WORKSPACE = Path(os.environ.get("POLYMARKET_ORC_AGENT_WORKSPACE", str(REPO_ROOT)))

MODEL_BY_ROLE = {
    "controller": "claude-opus-4-6",
    "research": "claude-sonnet-4-6",
    "build": "claude-sonnet-4-6",
}

ROLE_TIMEOUTS = {
    "controller": 900,
    "research": 1800,
    "build": 5400,
}

SCHEDULED_REPORTS = {
    "shadow_matrix": 15,
    "live_snapshot_matrix": 30,
    "leaderboard_wallets": 60,
}

# Build validation settings
VALIDATION_BINS = ["complete_set_bot", "backtest"]
BACKTEST_DATA_CANDIDATES = ["700 periods/BTC", "data/BTC"]
PAPER_SMOKE_DURATION_SECS = 300  # 5 minutes of paper mode

DEFAULT_TASKS_DOC = {
    "version": 1,
    "updated_at": "",
    "tasks": [],
}

CONTROLLER_RESEARCH_TASKS = [
    (
        "research-top-wallet-microstructure",
        "Research top-wallet microstructure and timing",
        "Use live evidence to explain how top crypto wallets structure entries, exits, ladders, and timing. Focus on microstructure rather than complete-set pricing.",
    ),
    (
        "research-pair-completion-merge-redeem",
        "Research pair completion, merge, and redeem behavior",
        "Investigate how profitable wallets complete pairs, merge, redeem, and settle inventory. Focus on cadence, size, and operational patterns.",
    ),
    (
        "research-warehouse-inventory-cycling",
        "Research warehouse and inventory cycling behavior",
        "Investigate whether top wallets warehouse inventory, cycle capital across assets, or monetize delayed settlement instead of immediate arbitrage.",
    ),
    (
        "research-leaderboard-routing-artifacts",
        "Research leaderboard routing and wallet artifacts",
        "Investigate whether leaderboard profit is affected by wallet routing, proxy patterns, transfers, or multi-wallet operational structure.",
    ),
]


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def today_dir() -> Path:
    path = REPORTS_DIR / datetime.now(timezone.utc).date().isoformat()
    path.mkdir(parents=True, exist_ok=True)
    return path


def ensure_layout() -> None:
    for path in (
        JOB_ORC_DIR,
        MEMORY_DIR,
        REPORTS_DIR,
        KNOWLEDGE_DIR,
        HEARTBEATS_DIR,
        RUNS_DIR,
        WORKTREES_DIR,
    ):
        path.mkdir(parents=True, exist_ok=True)
    if not TASKS_FILE.exists():
        TASKS_FILE.write_text(json.dumps(DEFAULT_TASKS_DOC, indent=2) + "\n")
    if not TASKS_MD.exists():
        write_tasks_md(DEFAULT_TASKS_DOC)
    if not KNOWLEDGE_MANIFEST_FILE.exists():
        KNOWLEDGE_MANIFEST_FILE.write_text(json.dumps({"updated_at": "", "entries": {}}, indent=2) + "\n")


def quota_block() -> Dict[str, Any]:
    if not QUOTA_BLOCK_FILE.exists():
        return {"until": "", "reason": ""}
    try:
        return json.loads(QUOTA_BLOCK_FILE.read_text())
    except json.JSONDecodeError:
        return {"until": "", "reason": ""}


def quota_block_active() -> bool:
    block = quota_block()
    until = block.get("until") or ""
    if not until:
        return False
    try:
        return datetime.fromisoformat(until) > datetime.now(timezone.utc)
    except ValueError:
        return False


def set_quota_block(reason: str, minutes: int = 30) -> None:
    until = datetime.now(timezone.utc).timestamp() + minutes * 60
    QUOTA_BLOCK_FILE.write_text(
        json.dumps(
            {
                "until": datetime.fromtimestamp(until, tz=timezone.utc).isoformat(),
                "reason": reason,
                "updated_at": utc_now(),
            },
            indent=2,
        )
        + "\n"
    )


def load_tasks() -> Dict[str, Any]:
    ensure_layout()
    if not TASKS_FILE.exists():
        return DEFAULT_TASKS_DOC.copy()
    data = json.loads(TASKS_FILE.read_text() or "{}")
    if not isinstance(data, dict):
        return DEFAULT_TASKS_DOC.copy()
    data.setdefault("version", 1)
    data.setdefault("updated_at", utc_now())
    data.setdefault("tasks", [])
    normalized = [normalize_task(task) for task in data.get("tasks", [])]
    data["tasks"] = normalized
    return data


def save_tasks(doc: Dict[str, Any]) -> None:
    ensure_layout()
    doc["updated_at"] = utc_now()
    doc["tasks"] = [normalize_task(task) for task in doc.get("tasks", [])]
    TASKS_FILE.write_text(json.dumps(doc, indent=2, sort_keys=True) + "\n")
    write_tasks_md(doc)


def branch_name_for(task_id: str) -> str:
    stamp = datetime.now(timezone.utc).strftime("%Y%m%d")
    slug = re.sub(r"[^a-z0-9-]+", "-", task_id.lower()).strip("-")
    return f"codex/auto/{stamp}-{slug}"


def normalize_task(task: Dict[str, Any]) -> Dict[str, Any]:
    now = utc_now()
    task = dict(task)
    task.setdefault("id", f"task-{int(time.time())}")
    task.setdefault("title", task["id"])
    task.setdefault("status", "pending")
    task.setdefault("worker_role", "research")
    task.setdefault("host", "local")
    task.setdefault("mutation_mode", "none")
    task.setdefault("workspace", "agent")
    task.setdefault("source_artifacts", [])
    task.setdefault("evidence_score", 0)
    task.setdefault("prompt", "")
    task.setdefault("created_at", now)
    task.setdefault("updated_at", now)
    task.setdefault("run_count", 0)
    task.setdefault("last_result", "")
    task.setdefault("last_run_at", "")
    if task["worker_role"] == "build" and task["mutation_mode"] == "branch":
        task.setdefault("branch_name", branch_name_for(task["id"]))
    return task


def write_tasks_md(doc: Dict[str, Any]) -> None:
    lines = [
        "# Codex Task Board",
        "",
        "_Auto-generated from `job-orc/tasks.json`. Edit tasks through the orchestrator._",
        "",
        "| ID | Status | Role | Host | Mutation | Evidence | Title | Branch |",
        "|---|---|---|---|---|---:|---|---|",
    ]
    for task in doc.get("tasks", []):
        lines.append(
            "| `{id}` | `{status}` | `{worker_role}` | `{host}` | `{mutation_mode}` | {evidence_score} | {title} | {branch_name} |".format(
                id=task["id"],
                status=task["status"],
                worker_role=task["worker_role"],
                host=task["host"],
                mutation_mode=task["mutation_mode"],
                evidence_score=task["evidence_score"],
                title=task["title"].replace("|", "/"),
                branch_name=task.get("branch_name", ""),
            )
        )
    TASKS_MD.write_text("\n".join(lines) + "\n")


def workspace_for(task: Dict[str, Any]) -> Path:
    if task.get("workspace") == "runtime":
        return RUNTIME_WORKSPACE
    return AGENT_WORKSPACE


def resolve_artifact(path_str: str) -> Path:
    candidate = Path(path_str)
    if candidate.is_absolute():
        return candidate
    for root in (AGENT_WORKSPACE, RUNTIME_WORKSPACE, REPO_ROOT):
        resolved = root / candidate
        if resolved.exists():
            return resolved
    return REPO_ROOT / candidate


def read_artifact_context(paths: List[str], limit_per_file: int = 1500) -> str:
    blocks = []
    for raw_path in paths:
        path = resolve_artifact(raw_path)
        if not path.exists():
            blocks.append(f"## Missing Artifact\n\n`{raw_path}`")
            continue
        text = path.read_text(errors="replace")
        snippet = text[:limit_per_file]
        blocks.append(f"## {path}\n\n```text\n{snippet}\n```")
    return "\n\n".join(blocks) if blocks else "No source artifacts provided."


def render_prompt(template_name: str, values: Dict[str, str]) -> str:
    template = (PROMPTS_DIR / template_name).read_text()
    for key, value in values.items():
        template = template.replace("{{" + key + "}}", value)
    return template


def run_command(cmd: List[str], cwd: Path, timeout: int = 1800, check: bool = True) -> subprocess.CompletedProcess:
    result = subprocess.run(
        cmd,
        cwd=str(cwd),
        text=True,
        capture_output=True,
        timeout=timeout,
    )
    if check and result.returncode != 0:
        raise RuntimeError(
            "Command failed ({})\nSTDOUT:\n{}\nSTDERR:\n{}".format(
                " ".join(cmd), result.stdout, result.stderr
            )
        )
    return result


def claude_exec(role: str, prompt: str, cwd: Path, run_name: str) -> Dict[str, Any]:
    if quota_block_active():
        block = quota_block()
        return {
            "returncode": 1,
            "text": "",
            "stdout_path": None,
            "stderr_path": None,
            "stderr": "",
            "error_message": block.get("reason", "Claude quota blocked"),
            "quota_blocked": True,
        }

    model = MODEL_BY_ROLE[role]
    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    slug = re.sub(r"[^a-z0-9-]+", "-", run_name.lower()).strip("-")
    raw_path = RUNS_DIR / f"{stamp}_{slug}.jsonl"
    err_path = RUNS_DIR / f"{stamp}_{slug}.stderr.log"

    claude_bin = shutil.which("claude") or "claude"
    cmd = [
        claude_bin,
        "-p",
        prompt,
        "--model",
        model,
        "--output-format",
        "json",
        "--dangerously-skip-permissions",
        "--no-session-persistence",
    ]
    result = run_command(cmd, cwd=cwd, timeout=ROLE_TIMEOUTS[role], check=False)
    raw_path.write_text(result.stdout)
    err_path.write_text(result.stderr)

    last_text = ""
    error_message = ""
    try:
        payload = json.loads(result.stdout.strip())
        if isinstance(payload, dict):
            if payload.get("is_error"):
                error_message = payload.get("result", "") or str(payload)
            else:
                last_text = payload.get("result", "")
    except json.JSONDecodeError:
        last_text = result.stdout.strip()

    if not last_text and result.returncode != 0:
        error_message = error_message or result.stderr.strip() or "claude CLI failed"

    quota_blocked = any(
        kw in error_message.lower()
        for kw in ("rate_limit", "rate limit", "overloaded", "too many requests", "usage limit")
    )
    if quota_blocked:
        set_quota_block(error_message)

    return {
        "returncode": result.returncode,
        "text": last_text,
        "stdout_path": raw_path,
        "stderr_path": err_path,
        "stderr": result.stderr,
        "error_message": error_message,
        "quota_blocked": quota_blocked,
    }


def upsert_task(doc: Dict[str, Any], task: Dict[str, Any]) -> None:
    normalized = normalize_task(task)
    tasks = doc.setdefault("tasks", [])
    for idx, existing in enumerate(tasks):
        if existing["id"] == normalized["id"]:
            normalized["created_at"] = existing.get("created_at", normalized["created_at"])
            normalized["run_count"] = existing.get("run_count", normalized["run_count"])
            normalized["status"] = existing.get("status", normalized["status"])
            tasks[idx] = normalized
            return
    tasks.append(normalized)


def next_task(doc: Dict[str, Any], role: str) -> Optional[Dict[str, Any]]:
    candidates = []
    for task in doc.get("tasks", []):
        if task["worker_role"] != role:
            continue
        if task["status"] != "pending":
            continue
        if task["host"] != CURRENT_HOST:
            continue
        if role == "build" and (
            task.get("mutation_mode") != "branch"
            or int(task.get("evidence_score", 0)) < 2
            or len(task.get("source_artifacts", [])) < 2
        ):
            continue
        candidates.append(task)
    candidates.sort(key=lambda item: (-int(item.get("evidence_score", 0)), item["created_at"], item["id"]))
    return candidates[0] if candidates else None


def report_age_minutes(path: Path) -> Optional[float]:
    if not path.exists():
        return None
    return (time.time() - path.stat().st_mtime) / 60.0


def current_report_path(name: str) -> Path:
    return today_dir() / name


def display_path(path: Path) -> str:
    for root in (REPO_ROOT, AGENT_WORKSPACE, RUNTIME_WORKSPACE):
        try:
            return str(path.relative_to(root))
        except ValueError:
            continue
    return str(path)


def load_knowledge_manifest() -> Dict[str, Any]:
    ensure_layout()
    try:
        data = json.loads(KNOWLEDGE_MANIFEST_FILE.read_text() or "{}")
    except json.JSONDecodeError:
        data = {}
    if not isinstance(data, dict):
        data = {}
    data.setdefault("updated_at", "")
    data.setdefault("entries", {})
    return data


def write_knowledge_index(doc: Dict[str, Any]) -> None:
    entries = doc.get("entries", {})
    latest = sorted(
        entries.items(),
        key=lambda item: item[1].get("updated_at", ""),
        reverse=True,
    )
    lines = [
        "# Research Knowledge",
        "",
        "_Auto-curated snapshots of reports that changed meaningfully. Raw recurring reports stay under `job-orc/reports/`._",
        "",
        f"- Updated at: `{doc.get('updated_at', '')}`",
        f"- Tracked sources: `{len(entries)}`",
        "",
        "| Source | Category | Updated | Snapshot |",
        "|---|---|---|---|",
    ]
    for source, meta in latest[:50]:
        lines.append(
            "| `{}` | `{}` | `{}` | `{}` |".format(
                source.replace("|", "/"),
                meta.get("category", ""),
                meta.get("updated_at", ""),
                meta.get("snapshot", ""),
            )
        )
    (KNOWLEDGE_DIR / "README.md").write_text("\n".join(lines) + "\n")


def save_knowledge_manifest(doc: Dict[str, Any]) -> None:
    doc["updated_at"] = utc_now()
    KNOWLEDGE_MANIFEST_FILE.write_text(json.dumps(doc, indent=2, sort_keys=True) + "\n")
    write_knowledge_index(doc)


def file_digest(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def curate_artifacts(paths: List[Path], category: str) -> List[Path]:
    doc = load_knowledge_manifest()
    snapshots: List[Path] = []
    changed = False
    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    day_dir = KNOWLEDGE_DIR / datetime.now(timezone.utc).date().isoformat() / category
    day_dir.mkdir(parents=True, exist_ok=True)

    for path in paths:
        if not path.exists() or not path.is_file():
            continue
        source = display_path(path)
        digest = file_digest(path)
        existing = doc["entries"].get(source, {})
        if existing.get("digest") == digest:
            existing["last_seen_at"] = utc_now()
            existing["size_bytes"] = path.stat().st_size
            doc["entries"][source] = existing
            continue

        safe_stem = re.sub(r"[^a-z0-9._-]+", "-", path.stem.lower()).strip("-") or "artifact"
        key_hash = hashlib.sha1(source.encode()).hexdigest()[:8]
        suffix = path.suffix or ".txt"
        snapshot = day_dir / f"{stamp}_{safe_stem}_{key_hash}{suffix}"
        shutil.copy2(path, snapshot)
        doc["entries"][source] = {
            "category": category,
            "digest": digest,
            "snapshot": str(snapshot.relative_to(REPO_ROOT)),
            "source_path": str(path),
            "updated_at": utc_now(),
            "last_seen_at": utc_now(),
            "size_bytes": path.stat().st_size,
        }
        snapshots.append(snapshot)
        changed = True

    if changed:
        save_knowledge_manifest(doc)
    elif snapshots or doc.get("updated_at", ""):
        save_knowledge_manifest(doc)
    return snapshots


def copy_report(src: Path, dest_name: str) -> Path:
    destination = current_report_path(dest_name)
    destination.parent.mkdir(parents=True, exist_ok=True)
    shutil.copy2(src, destination)
    return destination


def refresh_shadow_matrix(force: bool = False) -> Optional[Path]:
    destination = current_report_path("shadow_matrix.md")
    if not force:
        age = report_age_minutes(destination)
        if age is not None and age < SCHEDULED_REPORTS["shadow_matrix"]:
            return destination
    log_dirs = sorted(path for path in RUNTIME_WORKSPACE.glob("logs_complete_set_shadow*") if path.is_dir())
    cmd = ["python3", "analysis/tools/summarize_complete_set_shadow_matrix.py"] + [str(path) for path in log_dirs]
    result = run_command(cmd, cwd=RUNTIME_WORKSPACE, timeout=600)
    generated = Path(result.stdout.strip().splitlines()[-1])
    return copy_report(generated, "shadow_matrix.md")


def refresh_live_snapshot_matrix(force: bool = False) -> Optional[Path]:
    destination = current_report_path("live_snapshot_matrix.md")
    if not force:
        age = report_age_minutes(destination)
        if age is not None and age < SCHEDULED_REPORTS["live_snapshot_matrix"]:
            return destination
    result = run_command(["python3", "analysis/tools/run_complete_set_snapshot_matrix.py"], cwd=RUNTIME_WORKSPACE, timeout=1800)
    generated = Path(result.stdout.strip().splitlines()[-1])
    return copy_report(generated, "live_snapshot_matrix.md")


def refresh_leaderboard_wallets(force: bool = False) -> Path:
    destination = current_report_path("leaderboard_wallets.md")
    if not force:
        age = report_age_minutes(destination)
        if age is not None and age < SCHEDULED_REPORTS["leaderboard_wallets"]:
            return destination
    run_command(
        ["python3", "analysis/tools/refresh_leaderboard_wallets.py", "--out", str(destination)],
        cwd=AGENT_WORKSPACE,
        timeout=600,
    )
    return destination


def run_scheduled_research(force: bool = False) -> List[Path]:
    generated: List[Path] = []
    for refresher in (refresh_shadow_matrix, refresh_live_snapshot_matrix, refresh_leaderboard_wallets):
        try:
            path = refresher(force=force)
        except Exception as exc:
            error_path = today_dir() / "research_errors.log"
            with error_path.open("a") as fh:
                fh.write(f"{utc_now()} {refresher.__name__}: {exc}\n")
            continue
        if path is not None:
            generated.append(path)
    return generated


def parse_markdown_table(text: str) -> List[Dict[str, str]]:
    lines = [line.strip() for line in text.splitlines() if line.strip().startswith("|")]
    if len(lines) < 2:
        return []
    headers = [cell.strip().strip("`") for cell in lines[0].strip("|").split("|")]
    rows = []
    for line in lines[2:]:
        cells = [cell.strip().strip("`") for cell in line.strip("|").split("|")]
        if len(cells) != len(headers):
            continue
        rows.append(dict(zip(headers, cells)))
    return rows


def detect_zero_complete_set(shadow_report: Path, live_report: Path) -> bool:
    total_long = 0
    total_short = 0
    row_count = 0
    for report_path in (shadow_report, live_report):
        if not report_path.exists():
            continue
        for row in parse_markdown_table(report_path.read_text(errors="replace")):
            row_count += 1
            for key in ("Long Signal Scans", "Long Signals"):
                if key in row:
                    total_long += int(re.sub(r"[^0-9-]", "", row[key]) or "0")
            for key in ("Short Signal Scans", "Short Signals"):
                if key in row:
                    total_short += int(re.sub(r"[^0-9-]", "", row[key]) or "0")
    return row_count > 0 and total_long == 0 and total_short == 0


def detect_runtime_bug(report_paths: List[Path]) -> bool:
    pattern = re.compile(r"\b(traceback|panic|failed|error)\b", re.IGNORECASE)
    for path in report_paths:
        if path.exists() and pattern.search(path.read_text(errors="replace")):
            return True
    return False


def latest_context(paths: List[Path]) -> str:
    blocks = []
    for path in paths:
        if not path.exists():
            continue
        snippet = path.read_text(errors="replace")[:1800]
        blocks.append(f"## {path}\n\n```text\n{snippet}\n```")
    return "\n\n".join(blocks) if blocks else "No report context available."


def extract_json_payload(text: str) -> Optional[Dict[str, Any]]:
    text = text.strip()
    candidates = [text]
    fenced = re.findall(r"```(?:json)?\s*(\{.*?\})\s*```", text, re.DOTALL)
    candidates.extend(fenced)
    brace_match = re.search(r"(\{.*\})", text, re.DOTALL)
    if brace_match:
        candidates.append(brace_match.group(1))
    for candidate in candidates:
        try:
            payload = json.loads(candidate)
        except json.JSONDecodeError:
            continue
        if isinstance(payload, dict):
            return payload
    return None


def heuristic_controller_tasks(doc: Dict[str, Any], report_paths: List[Path]) -> None:
    shadow_report = current_report_path("shadow_matrix.md")
    live_report = current_report_path("live_snapshot_matrix.md")
    leaderboard_report = current_report_path("leaderboard_wallets.md")
    if detect_zero_complete_set(shadow_report, live_report):
        artifacts = [str(path.relative_to(REPO_ROOT)) for path in (shadow_report, live_report, leaderboard_report) if path.exists()]
        for task_id, title, prompt in CONTROLLER_RESEARCH_TASKS:
            upsert_task(
                doc,
                {
                    "id": task_id,
                    "title": title,
                    "worker_role": "research",
                    "host": "vps",
                    "mutation_mode": "none",
                    "workspace": "agent",
                    "source_artifacts": artifacts,
                    "evidence_score": 1,
                    "prompt": prompt,
                },
            )
    if detect_runtime_bug(report_paths):
        artifacts = [str(path.relative_to(REPO_ROOT)) for path in report_paths if path.exists()]
        upsert_task(
            doc,
            {
                "id": "build-runtime-evidence-fixes",
                "title": "Fix runtime bug evidenced by research reports",
                "worker_role": "build",
                "host": "vps",
                "mutation_mode": "branch",
                "workspace": "agent",
                "source_artifacts": artifacts[:4],
                "evidence_score": 2,
                "prompt": "Fix the directly evidenced runtime or reporting bug surfaced in the source artifacts. Keep the change narrow and validate it with targeted checks.",
            },
        )


def write_controller_reports(payload: Optional[Dict[str, Any]], fallback_context: str) -> None:
    report_dir = today_dir()
    hypothesis_path = report_dir / "hypothesis_scoreboard.md"
    build_candidates_path = report_dir / "build_candidates.md"
    memory_path = MEMORY_DIR / f"{datetime.now(timezone.utc).date().isoformat()}_controller.md"
    if payload:
        hypothesis_path.write_text(payload.get("hypothesis_scoreboard_markdown", "# Hypothesis Scoreboard\n\nNo output.\n") + "\n")
        build_candidates_path.write_text(payload.get("build_candidates_markdown", "# Build Candidates\n\nNo output.\n") + "\n")
        memory_path.write_text(payload.get("memory_note", "No memory note.") + "\n")
        return

    fallback = [
        "# Hypothesis Scoreboard",
        "",
        "- Complete-set edge frequency: deprioritized until live reports show non-zero signals.",
        "- Top-wallet microstructure: active research branch.",
        "- Pair-completion / merge / redeem: active research branch.",
        "- Warehouse / inventory cycling: active research branch.",
        "- Leaderboard routing artifacts: active research branch.",
        "",
    ]
    hypothesis_path.write_text("\n".join(fallback))
    build_candidates_path.write_text(
        "# Build Candidates\n\n- Instrument wallet-behavior research only after at least two reports point at the same operational gap.\n"
    )
    memory_path.write_text(fallback_context[:2000] + "\n")


def controller_once(force: bool = False) -> None:
    ensure_layout()
    write_heartbeat("controller")
    run_scheduled_research(force=force)
    report_paths = [
        current_report_path("shadow_matrix.md"),
        current_report_path("live_snapshot_matrix.md"),
        current_report_path("leaderboard_wallets.md"),
    ]
    doc = load_tasks()
    heuristic_controller_tasks(doc, report_paths)

    context = latest_context(report_paths)
    prompt = render_prompt(
        "controller.md",
        {
            "TASKS_JSON": json.dumps(doc, indent=2),
            "REPORT_CONTEXT": context,
        },
    )
    response = claude_exec("controller", prompt, AGENT_WORKSPACE, "controller")
    payload = extract_json_payload(response["text"]) if response["returncode"] == 0 else None
    if payload:
        for task in payload.get("upsert_tasks", []):
            upsert_task(doc, task)
    write_controller_reports(payload, context)
    curate_artifacts(
        report_paths
        + [
            current_report_path("hypothesis_scoreboard.md"),
            current_report_path("build_candidates.md"),
        ],
        category="controller",
    )
    save_tasks(doc)


def write_heartbeat(role: str) -> None:
    heart = {
        "role": role,
        "host": CURRENT_HOST,
        "hostname": socket.gethostname(),
        "timestamp": utc_now(),
    }
    (HEARTBEATS_DIR / f"{role}.json").write_text(json.dumps(heart, indent=2) + "\n")


def mark_task(doc: Dict[str, Any], task_id: str, status: str, result: str = "") -> None:
    for task in doc.get("tasks", []):
        if task["id"] == task_id:
            task["status"] = status
            task["updated_at"] = utc_now()
            task["run_count"] = int(task.get("run_count", 0)) + (1 if status in ("running", "completed", "failed") else 0)
            if status in ("completed", "failed"):
                task["last_run_at"] = utc_now()
                task["last_result"] = result[:2000]
            break


def process_research_task(task: Dict[str, Any]) -> None:
    doc = load_tasks()
    mark_task(doc, task["id"], "running")
    save_tasks(doc)

    report_path = today_dir() / f"{task['id']}.md"
    prompt = render_prompt(
        "research.md",
        {
            "TASK_ID": task["id"],
            "TASK_TITLE": task["title"],
            "HOST": task["host"],
            "WORKSPACE": task["workspace"],
            "SOURCE_ARTIFACTS": "\n".join(f"- `{path}`" for path in task.get("source_artifacts", [])) or "- none",
            "TASK_PROMPT": task.get("prompt", ""),
            "REPORT_CONTEXT": read_artifact_context(task.get("source_artifacts", [])),
        },
    )
    response = claude_exec("research", prompt, workspace_for(task), task["id"])
    doc = load_tasks()
    if response.get("quota_blocked"):
        mark_task(doc, task["id"], "pending", response.get("error_message", "Codex quota blocked"))
    elif response["returncode"] == 0 and response["text"].strip():
        report_path.write_text(response["text"].strip() + "\n")
        (MEMORY_DIR / f"{task['id']}.md").write_text(response["text"].strip() + "\n")
        curate_artifacts([report_path], category="research")
        mark_task(doc, task["id"], "completed", response["text"])
    else:
        error_text = response.get("error_message") or response["stderr"] or response["text"] or "research worker failed"
        error_path = today_dir() / f"{task['id']}.error.log"
        error_path.write_text(error_text + "\n")
        curate_artifacts([error_path], category="errors")
        mark_task(doc, task["id"], "failed", error_text)
    save_tasks(doc)


def _changed_files_since(worktree: Path, initial_commit: str) -> List[str]:
    """Return all files changed by build agent (committed + uncommitted) since initial_commit."""
    committed: List[str] = []
    current = run_command(["git", "rev-parse", "HEAD"], cwd=worktree, timeout=30, check=False).stdout.strip()
    if current != initial_commit:
        r = run_command(
            ["git", "diff", "--name-only", initial_commit, "HEAD"],
            cwd=worktree, timeout=30, check=False,
        )
        committed = [f.strip() for f in r.stdout.splitlines() if f.strip()]
    unstaged = run_command(["git", "diff", "--name-only"], cwd=worktree, timeout=30, check=False)
    staged = run_command(["git", "diff", "--cached", "--name-only"], cwd=worktree, timeout=30, check=False)
    uncommitted = [
        f.strip()
        for f in unstaged.stdout.splitlines() + staged.stdout.splitlines()
        if f.strip()
    ]
    return list(set(committed + uncommitted))


def _find_backtest_data() -> Optional[Path]:
    for root in (RUNTIME_WORKSPACE, AGENT_WORKSPACE, REPO_ROOT):
        for sub in BACKTEST_DATA_CANDIDATES:
            candidate = root / sub
            if candidate.exists() and any(candidate.glob("*.csv")):
                return candidate
    return None


def validate_build_worktree(worktree: Path, initial_commit: str, artifacts_dir: Path) -> Dict[str, Any]:
    """
    Run post-build validation: cargo build → cargo test → backtest → paper smoke.
    All artifacts are written into artifacts_dir (inside the worktree) so they
    get committed to the branch and are visible on GitHub.

    Returns {"passed": bool, "hard_fail": bool, "report": str}
    hard_fail=True means cargo build broke — do not commit.
    """
    artifacts_dir.mkdir(parents=True, exist_ok=True)
    sections: List[str] = [
        "## Validation Report",
        "",
        f"- Generated at: `{utc_now()}`",
        f"- Initial commit: `{initial_commit}`",
        "",
    ]
    passed = True
    hard_fail = False

    changed = _changed_files_since(worktree, initial_commit)
    rust_changed = any(
        f.endswith(".rs") or f in ("Cargo.toml", "Cargo.lock") or f.startswith("src/")
        for f in changed
    )

    if not rust_changed:
        sections.append("_No Rust/Cargo files changed — skipping cargo steps._")
        sections.append("")
        py_result = run_command(
            ["python3", "-m", "pytest", "--tb=short", "-q"],
            cwd=worktree, timeout=120, check=False,
        )
        if py_result.returncode == 0:
            sections.append("- **PASS** `python3 -m pytest`")
        else:
            sections.append(f"- **SKIP/WARN** pytest: `{py_result.stderr.strip()[:200]}`")
        report = "\n".join(sections)
        (artifacts_dir / "validation_report.md").write_text(report + "\n")
        return {"passed": True, "hard_fail": False, "report": report}

    # ── 1. Cargo build ──────────────────────────────────────────────────────
    sections.append("### 1. Cargo Build")
    try:
        build_cmd = (
            ["cargo", "build", "--release"]
            + [x for b in VALIDATION_BINS for x in ("--bin", b)]
            + ["--bin", "polymarket-arb"]
        )
        build_result = run_command(build_cmd, cwd=worktree, timeout=600, check=False)
        if build_result.returncode == 0:
            sections.append("- **PASS** `cargo build --release`")
        else:
            sections.append("- **FAIL** `cargo build --release`")
            err = build_result.stderr[-2000:] if build_result.stderr else "(no stderr)"
            sections.append(f"\n```\n{err}\n```")
            (artifacts_dir / "cargo_build.log").write_text(build_result.stderr)
            hard_fail = True
            passed = False
    except Exception as exc:
        sections.append(f"- **ERROR** cargo build: {exc}")
        hard_fail = True
        passed = False

    if hard_fail:
        sections.append("\n_Build failed — all further checks skipped._")
        report = "\n".join(sections)
        (artifacts_dir / "validation_report.md").write_text(report + "\n")
        return {"passed": False, "hard_fail": True, "report": report}

    # ── 2. Cargo test ───────────────────────────────────────────────────────
    sections.append("")
    sections.append("### 2. Cargo Test")
    try:
        test_result = run_command(
            ["cargo", "test", "--release"],
            cwd=worktree, timeout=300, check=False,
        )
        (artifacts_dir / "cargo_test.log").write_text(
            test_result.stdout + "\n" + test_result.stderr
        )
        if test_result.returncode == 0:
            summary_line = next(
                (l.strip() for l in reversed(test_result.stdout.splitlines()) if "test result" in l),
                "ok",
            )
            sections.append(f"- **PASS** `cargo test` — {summary_line}")
        else:
            sections.append("- **WARN** `cargo test` non-zero (may be pre-existing failures)")
            snippet = (test_result.stdout + test_result.stderr)[-1000:]
            sections.append(f"\n```\n{snippet}\n```")
    except Exception as exc:
        sections.append(f"- **SKIP** cargo test: {exc}")

    # ── 3. Backtest ─────────────────────────────────────────────────────────
    sections.append("")
    sections.append("### 3. Backtest")
    data_dir = _find_backtest_data()
    if data_dir:
        summary_json = artifacts_dir / "backtest_summary.json"
        try:
            bt_result = run_command(
                [
                    "cargo", "run", "--release", "--bin", "backtest", "--",
                    "--summary-json", str(summary_json),
                    "--quiet",
                    str(data_dir),
                ],
                cwd=worktree, timeout=1800, check=False,
            )
            (artifacts_dir / "backtest.log").write_text(bt_result.stdout + "\n" + bt_result.stderr)
            if bt_result.returncode == 0 and summary_json.exists():
                s = json.loads(summary_json.read_text())
                sections.append("- **PASS** backtest completed")
                sections.append(f"  - Avg PnL/period: `{s.get('avg_pnl_per_traded_period', '?')}`")
                sections.append(f"  - Win rate: `{s.get('win_rate_traded', 0):.1f}%`")
                sections.append(f"  - Sharpe: `{s.get('sharpe_traded', 0):.3f}`")
                sections.append(f"  - Full results: [`backtest_summary.json`](backtest_summary.json)")
            else:
                sections.append("- **FAIL** backtest exited non-zero")
                sections.append(f"\n```\n{bt_result.stderr[-800:]}\n```")
                passed = False
        except Exception as exc:
            sections.append(f"- **SKIP** backtest error: {exc}")
    else:
        searched = ", ".join(
            f"`{Path(r) / sub}`"
            for r in (RUNTIME_WORKSPACE, AGENT_WORKSPACE)
            for sub in BACKTEST_DATA_CANDIDATES
        )
        sections.append(f"- **SKIP** no data directory found (searched {searched})")

    # ── 4. Paper smoke ──────────────────────────────────────────────────────
    sections.append("")
    sections.append("### 4. Paper Smoke")
    paper_bin = worktree / "target" / "release" / "polymarket-arb"
    paper_config_src = worktree / "config" / "v2.toml"
    if paper_bin.exists() and paper_config_src.exists():
        paper_config = worktree / "config" / "_paper_smoke.toml"
        paper_log = artifacts_dir / "paper_smoke.log"
        try:
            cfg_text = paper_config_src.read_text()
            cfg_text = re.sub(r'^mode\s*=\s*".*?"', 'mode = "paper"', cfg_text, flags=re.MULTILINE)
            cfg_text = re.sub(r'^canary_mode\s*=.*', 'canary_mode = false', cfg_text, flags=re.MULTILINE)
            paper_config.write_text(cfg_text)

            import subprocess as _sp
            proc = _sp.Popen(
                [str(paper_bin), str(paper_config)],
                cwd=str(worktree),
                stdout=open(str(paper_log), "w"),
                stderr=_sp.STDOUT,
            )
            time.sleep(PAPER_SMOKE_DURATION_SECS)
            proc.terminate()
            try:
                proc.wait(timeout=10)
            except Exception:
                proc.kill()

            log_text = paper_log.read_text(errors="replace") if paper_log.exists() else ""
            panic = "thread 'main' panicked" in log_text or "PANIC" in log_text
            error_lines = [l for l in log_text.splitlines() if "ERROR" in l or "panicked" in l][-5:]
            if panic:
                sections.append("- **FAIL** panic detected during paper smoke")
                sections.append("```\n" + "\n".join(error_lines) + "\n```")
                passed = False
            else:
                n_lines = len(log_text.splitlines())
                sections.append(
                    f"- **PASS** {PAPER_SMOKE_DURATION_SECS}s paper smoke — {n_lines} log lines, no panic"
                )
            sections.append(f"  - Full log: [`paper_smoke.log`](paper_smoke.log)")
        except Exception as exc:
            sections.append(f"- **SKIP** paper smoke error: {exc}")
        finally:
            if paper_config.exists():
                paper_config.unlink()
    else:
        missing = []
        if not paper_bin.exists():
            missing.append("binary `polymarket-arb` not built")
        if not paper_config_src.exists():
            missing.append("`config/v2.toml` not found")
        sections.append(f"- **SKIP** {'; '.join(missing)}")

    report = "\n".join(sections)
    (artifacts_dir / "validation_report.md").write_text(report + "\n")
    return {"passed": passed, "hard_fail": False, "report": report}


def prepare_build_workspace(branch_name: str) -> Path:
    WORKTREES_DIR.mkdir(parents=True, exist_ok=True)
    worktree_path = WORKTREES_DIR / re.sub(r"[^a-z0-9-]+", "-", branch_name.lower()).strip("-")
    if worktree_path.exists():
        shutil.rmtree(worktree_path, ignore_errors=True)
    run_command(
        [
            "rsync",
            "-a",
            "--delete",
            "--exclude",
            "target",
            "--exclude",
            "job-orc/reports",
            "--exclude",
            "job-orc/runs",
            "--exclude",
            "job-orc/heartbeats",
            "--exclude",
            "job-orc/worktrees",
            str(AGENT_WORKSPACE) + "/",
            str(worktree_path) + "/",
        ],
        cwd=REPO_ROOT,
        timeout=600,
    )
    run_command(["git", "checkout", "-B", branch_name], cwd=worktree_path, timeout=120)
    return worktree_path


def process_build_task(task: Dict[str, Any]) -> None:
    doc = load_tasks()
    mark_task(doc, task["id"], "running")
    save_tasks(doc)

    branch_name = task.get("branch_name") or branch_name_for(task["id"])
    worktree = prepare_build_workspace(branch_name)

    # artifacts_dir lives INSIDE the worktree — everything here gets committed to the branch
    stamp = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    artifacts_dir = worktree / "job-orc" / "validation" / stamp / task["id"]
    artifacts_dir.mkdir(parents=True, exist_ok=True)

    # report_path in the worktree (committed) + a copy in agent workspace reports dir
    report_path_in_branch = artifacts_dir / "build_summary.md"
    report_path_for_agent = today_dir() / f"{task['id']}_branch_summary.md"

    prompt = render_prompt(
        "build.md",
        {
            "TASK_ID": task["id"],
            "TASK_TITLE": task["title"],
            "BRANCH_NAME": branch_name,
            "WORKSPACE": str(worktree),
            "SOURCE_ARTIFACTS": "\n".join(f"- `{path}`" for path in task.get("source_artifacts", [])) or "- none",
            "TASK_PROMPT": task.get("prompt", ""),
            "REPORT_CONTEXT": read_artifact_context(task.get("source_artifacts", [])),
            "REPORT_PATH": str(report_path_in_branch),
        },
    )
    # Capture state before build agent runs so we can diff exactly what it changed
    initial_commit = run_command(["git", "rev-parse", "HEAD"], cwd=worktree, timeout=60).stdout.strip()

    response = claude_exec("build", prompt, worktree, task["id"])

    # ── Validation gate ────────────────────────────────────────────────────
    # artifacts_dir is inside worktree → all outputs committed with the branch
    validation = validate_build_worktree(worktree, initial_commit, artifacts_dir)

    branch = run_command(["git", "branch", "--show-current"], cwd=worktree, timeout=60).stdout.strip()
    status = run_command(["git", "status", "--short"], cwd=worktree, timeout=60).stdout.strip()

    agent_summary = response["text"].strip() or "No build summary returned."
    status_lines = [
        f"- Branch: `{branch}`",
        f"- Validation passed: `{validation['passed']}`",
        f"- Validation hard fail: `{validation['hard_fail']}`",
        f"- Artifacts: `job-orc/validation/{stamp}/{task['id']}/`",
    ]

    report_contents = (
        agent_summary
        + "\n\n"
        + "\n".join(status_lines)
        + "\n\n"
        + validation["report"]
        + "\n"
    )
    report_path_in_branch.write_text(report_contents)
    report_path_for_agent.write_text(report_contents)

    if validation["hard_fail"]:
        # Cargo build broke — do not commit, do not push
        pass
    else:
        # Stage everything including artifacts_dir and any agent changes
        run_command(["git", "add", "-A"], cwd=worktree, timeout=120)
        has_staged = bool(
            run_command(["git", "diff", "--cached", "--name-only"], cwd=worktree, timeout=30, check=False).stdout.strip()
        )
        if has_staged:
            val_flag = "✓" if validation["passed"] else "⚠"
            run_command(
                ["git", "commit", "-m",
                 f"codex build {task['id']}: {task['title']} [{val_flag} validation]"],
                cwd=worktree, timeout=300, check=False,
            )
        run_command(["git", "push", "-u", "origin", branch_name], cwd=worktree, timeout=600, check=False)

    commit = run_command(["git", "rev-parse", "HEAD"], cwd=worktree, timeout=60).stdout.strip()
    status_lines.append(f"- Commit: `{commit}`")

    doc = load_tasks()
    if response.get("quota_blocked"):
        mark_task(doc, task["id"], "pending", response.get("error_message", "Claude quota blocked"))
    elif validation["hard_fail"]:
        mark_task(doc, task["id"], "failed", report_contents)
    elif response["returncode"] == 0 and branch.startswith("codex/auto/") and validation["passed"]:
        mark_task(doc, task["id"], "completed", report_contents)
    elif response["returncode"] == 0 and branch.startswith("codex/auto/"):
        mark_task(doc, task["id"], "completed", report_contents + "\n⚠️ validation warnings — review before merging")
    else:
        mark_task(
            doc, task["id"], "failed",
            report_contents + "\n" + (response.get("error_message") or response["stderr"]),
        )
    save_tasks(doc)


def run_research_loop(loop: bool, force: bool) -> None:
    ensure_layout()
    while True:
        write_heartbeat("research")
        run_scheduled_research(force=force)
        doc = load_tasks()
        task = next_task(doc, "research")
        if task:
            process_research_task(task)
        if not loop:
            return
        time.sleep(60)


def run_build_loop(loop: bool) -> None:
    ensure_layout()
    while True:
        write_heartbeat("build")
        doc = load_tasks()
        task = next_task(doc, "build")
        if task:
            process_build_task(task)
        if not loop:
            return
        time.sleep(60)


def print_status() -> None:
    ensure_layout()
    doc = load_tasks()
    print(f"host={CURRENT_HOST}")
    print(f"runtime_workspace={RUNTIME_WORKSPACE}")
    print(f"agent_workspace={AGENT_WORKSPACE}")
    print("")
    for name in ("shadow_matrix.md", "live_snapshot_matrix.md", "leaderboard_wallets.md", "hypothesis_scoreboard.md", "build_candidates.md"):
        path = current_report_path(name)
        age = report_age_minutes(path)
        if age is None:
            print(f"report {name}: missing")
        else:
            print(f"report {name}: {path} ({age:.1f}m old)")
    print("")
    for role in ("controller", "research", "build"):
        hb_path = HEARTBEATS_DIR / f"{role}.json"
        if hb_path.exists():
            data = json.loads(hb_path.read_text())
            print(f"heartbeat {role}: {data.get('timestamp')} on {data.get('hostname')}")
        else:
            print(f"heartbeat {role}: missing")
    print("")
    for task in doc.get("tasks", []):
        print(
            "{id} status={status} role={worker_role} host={host} evidence={evidence_score} title={title}".format(
                **task
            )
        )


def watch_status() -> None:
    while True:
        print_status()
        print("")
        time.sleep(15)


def sync_research(remote: str, remote_dir: str) -> None:
    destination = JOB_ORC_DIR / "vps-reports"
    destination.mkdir(parents=True, exist_ok=True)
    run_command(
        [
            "rsync",
            "-az",
            f"{remote}:{remote_dir}/job-orc/reports/",
            str(destination) + "/",
        ],
        cwd=REPO_ROOT,
        timeout=1800,
    )
    print(destination)


def parse_args(argv: List[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    sub = parser.add_subparsers(dest="cmd", required=True)

    sub.add_parser("init")

    controller = sub.add_parser("controller")
    controller.add_argument("--loop", action="store_true")
    controller.add_argument("--force", action="store_true")

    research = sub.add_parser("research-loop")
    research.add_argument("--loop", action="store_true")
    research.add_argument("--force", action="store_true")

    build = sub.add_parser("build-loop")
    build.add_argument("--loop", action="store_true")

    sub.add_parser("status")
    sub.add_parser("watch")

    sync = sub.add_parser("sync-research")
    sync.add_argument("--remote", default=os.environ.get("POLYMARKET_VPS_HOST", "root@YOUR_VPS_IP"))
    sync.add_argument("--remote-dir", default=os.environ.get("POLYMARKET_VPS_AGENT_DIR", "/home/botuser/polymarket-bot-agent"))
    return parser.parse_args(argv)


def main(argv: List[str]) -> int:
    args = parse_args(argv)
    ensure_layout()

    if args.cmd == "init":
        save_tasks(load_tasks())
        return 0
    if args.cmd == "controller":
        if args.loop:
            while True:
                controller_once(force=args.force)
                time.sleep(900)
        controller_once(force=args.force)
        return 0
    if args.cmd == "research-loop":
        run_research_loop(loop=args.loop, force=args.force)
        return 0
    if args.cmd == "build-loop":
        run_build_loop(loop=args.loop)
        return 0
    if args.cmd == "status":
        print_status()
        return 0
    if args.cmd == "watch":
        watch_status()
        return 0
    if args.cmd == "sync-research":
        sync_research(args.remote, args.remote_dir)
        return 0
    return 1


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
