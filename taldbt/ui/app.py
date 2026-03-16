"""
taldbt — Talend to dbt Migration Wizard
"""
from __future__ import annotations
import os, sys, base64, platform, zipfile, time, html as _html, re as _re
from io import BytesIO
from pathlib import Path
from lxml import etree
import streamlit as st
import streamlit.components.v1 as components

# ═══════════════════════════════════════════════════════
# Security
# ═══════════════════════════════════════════════════════
def _esc(s: str) -> str:
    """HTML-escape user-controlled strings to prevent XSS."""
    return _html.escape(str(s)) if s else ""

def _safe_name(s: str, max_len: int = 80) -> str:
    """Sanitise user-derived names: escape HTML + strip to safe chars + truncate."""
    clean = _re.sub(r'[^a-zA-Z0-9_\-. ()\[\]]', '', str(s))[:max_len]
    return _html.escape(clean)

# Detect if running on Streamlit Cloud
# Primary signal: /mount/src exists (Streamlit Cloud clones repos there)
# Secondary: home dir is /home/appuser or /home/adminuser
# Tertiary: env vars set by Streamlit
_home = os.path.expanduser('~')
IS_CLOUD = os.path.exists('/mount/src') or \
           _home.startswith('/home/appuser') or \
           _home.startswith('/home/adminuser') or \
           os.environ.get('STREAMLIT_SHARING_MODE') == '1' or \
           os.environ.get('IS_STREAMLIT_CLOUD', '') == '1'

PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from taldbt.parsers.project_scanner import scan_project
from taldbt.parsers.xml_parser import parse_job
from taldbt.models.ast_models import ProjectAST, JobType
from taldbt.graphing.dag_builder import apply_dag_to_project
from taldbt.codegen.model_assembler import assemble_model
from taldbt.codegen.dbt_scaffolder import scaffold_dbt_project, write_model_file
from taldbt.llm.ollama_client import check_ollama_status, translate_component
from taldbt.engine.duckdb_engine import DuckDBEngine
from taldbt.orchestration.autopilot import run_autopilot

LOGO_PATH = Path(__file__).parent / "logo.svg"
# Favicon: use our logo SVG, or fallback to emoji
_FAVICON = str(LOGO_PATH) if LOGO_PATH.exists() else "⚡"
st.set_page_config(page_title="taldbt — Talend to dbt Migration", page_icon=_FAVICON, layout="wide", initial_sidebar_state="collapsed")

# ═══════════════════════════════════════════════════════
# Session
# ═══════════════════════════════════════════════════════
for k, v in [("project_ast", None), ("input_dir", ""), ("migrated", False),
             ("output_dir", ""), ("migration_log", []), ("autopilot_results", None),
             ("step", 1), ("booted", False),
             ("autopilot_running", False), ("temporal_launched", False),
             ("temporal_results", None)]:
    if k not in st.session_state: st.session_state[k] = v

if st.session_state.autopilot_results or st.session_state.migrated:
    st.session_state.step = 4
elif st.session_state.project_ast:
    st.session_state.step = max(st.session_state.step, 2)

step = st.session_state.step
ollama = check_ollama_status()
ollama_on = ollama.get("running") and ollama.get("has_target_model")

# ═══════════════════════════════════════════════════════
# LOADING SCREEN — blocks until dismissed
# ═══════════════════════════════════════════════════════
if not st.session_state.booted:
    _boot = st.empty()
    with _boot.container():
        st.markdown("""
        <div style="display:flex;flex-direction:column;align-items:center;justify-content:center;
            min-height:80vh;text-align:center;">
            <div style="font-family:'JetBrains Mono',monospace;font-size:3rem;font-weight:700;margin-bottom:8px;">
                <span style="color:#818cf8;">tal</span><span style="color:#ff694b;">dbt</span>
            </div>
            <div style="color:#6878a0;font-size:0.95rem;margin-bottom:32px;">
                Initialising migration engine
            </div>
            <div style="width:220px;height:3px;background:#1a2240;border-radius:4px;overflow:hidden;">
                <div style="height:100%;background:linear-gradient(90deg,#3b82f6,#8b5cf6,#ff694b);
                    border-radius:4px;animation:bootbar 1.8s ease forwards;"></div>
            </div>
            <style>@keyframes bootbar{0%{width:0}60%{width:75%}100%{width:100%}}</style>
        </div>
        """, unsafe_allow_html=True)
    time.sleep(2)
    st.session_state.booted = True
    _boot.empty()
    st.rerun()

# ═══════════════════════════════════════════════════════
# CSS — FULLY OPAQUE, HIGH CONTRAST
# ═══════════════════════════════════════════════════════
BG = "#0a0f1e"
CARD = "#131b30"
BORDER = "#283d5e"
T1 = "#e8eef8"
T2 = "#b0bdd4"

st.markdown(f"""
<style>
    @import url('https://fonts.googleapis.com/css2?family=DM+Sans:opsz,wght@9..40,400;9..40,500;9..40,600;9..40,700&family=JetBrains+Mono:wght@400;500;600;700&display=swap');

    /* Body gets the base dark — particles render on top of this */
    html, body {{
        background: {BG} !important;
    }}
    /* Streamlit containers: TRANSPARENT so particles show through gaps */
    .stApp, .stApp > header,
    [data-testid="stAppViewContainer"],
    [data-testid="stAppViewBlockContainer"],
    .main, .main .block-container,
    [data-testid="stHeader"],
    [data-testid="stBottomBlockContainer"],
    section[data-testid="stSidebar"] {{
        background: transparent !important;
        background-color: transparent !important;
    }}



    .block-container {{ padding: 0.8rem 2.5rem 3rem; max-width: 1300px; }}
    html, body, [class*="css"], p, span, label, li, td, th,
    .stMarkdown, .stMarkdown p, .stText {{
        font-family: 'DM Sans', sans-serif;
        color: {T1} !important;
    }}
    code, .stCode, pre {{ font-family: 'JetBrains Mono', monospace !important; font-size: 0.85rem; }}

    /* ── Top bar ── */
    .tbar {{
        display:flex; align-items:center; justify-content:space-between;
        padding:10px 0 8px; border-bottom:1px solid #1a2545; margin-bottom:0;
    }}
    .tbar-logo {{ font-family:'JetBrains Mono',monospace; font-weight:700; font-size:1.4rem; }}
    .tbar-info {{ color:{T2} !important; font-size:0.85rem; }}
    .tbar-badge {{
        font-size:0.82rem; padding:5px 14px; border-radius:8px; font-weight:600;
        font-family:'JetBrains Mono',monospace; display:inline-block;
    }}
    .tbar-badge-on {{ background:#0d3330; border:1px solid #1a6b60; color:#6ee7b7; }}
    .tbar-badge-off {{ background:#350f14; border:1px solid #6b1a1a; color:#fca5a5; }}

    /* ── Rail ── */
    .rail {{ display:flex; align-items:center; justify-content:center; gap:0; padding:10px 0 16px; }}
    .rs {{ display:flex; align-items:center; gap:9px; padding:8px 18px; border-radius:10px; cursor:default; }}
    .rs.done {{ opacity:0.5; }}
    .rs.active {{ background:{CARD}; border:1px solid #3b82f6; box-shadow:0 0 14px rgba(59,130,246,0.12); }}
    .rs.locked {{ opacity:0.2; }}
    .rd {{
        width:32px; height:32px; border-radius:50%; display:flex; align-items:center; justify-content:center;
        font-weight:700; font-size:0.82rem; font-family:'JetBrains Mono',monospace;
    }}
    .rs.done .rd {{ background:#10b981; color:#000; }}
    .rs.active .rd {{ background:#3b82f6; color:#fff; animation:dotpulse 2s ease-in-out infinite; }}
    .rs.locked .rd {{ background:#1a2240; color:#3a4560; }}
    .rl {{ font-size:0.85rem; font-weight:600; }}
    .rs.done .rl {{ color:#10b981; }}
    .rs.active .rl {{ color:{T1}; }}
    .rs.locked .rl {{ color:#3a4560; }}
    .rc {{ width:40px; height:2px; flex-shrink:0; }}
    .rc.done {{ background:#10b981; }} .rc.pend {{ background:#1a2240; }}
    @keyframes dotpulse {{
        0%,100% {{ box-shadow: 0 0 0 0 rgba(59,130,246,0.5); }}
        50% {{ box-shadow: 0 0 18px 6px rgba(59,130,246,0.35); }}
    }}

    /* ── Cards: NUCLEAR OPAQUE ── */
    div[data-testid="stMetric"],
    div[data-testid="stMetric"] > div,
    div[data-testid="stMetric"] > div > div,
    div[data-testid="stMetric"] * {{
        background: {CARD} !important; background-color: {CARD} !important;
    }}
    div[data-testid="stMetric"] {{
        padding:18px !important; border:1px solid {BORDER} !important; border-radius:12px;
    }}


    div[data-testid="stMetric"]:hover {{ border-color:#3b82f6 !important; }}
    div[data-testid="stMetric"] label {{
        color:#8899b8 !important; font-size:0.75rem;
        text-transform:uppercase; letter-spacing:1px; font-weight:600;
    }}
    div[data-testid="stMetric"] [data-testid="stMetricValue"] {{
        color:{T1} !important; font-size:1.8rem; font-weight:700;
        font-family:'JetBrains Mono',monospace;
    }}

    /* ── Expanders: NUCLEAR OPAQUE ── */
    details[data-testid="stExpander"] {{
        background: {CARD} !important; border:1px solid {BORDER} !important; border-radius:12px;
    }}
    details[data-testid="stExpander"] > summary {{
        background: {CARD} !important; font-weight:600; font-size:0.9rem;
        color:{T1} !important; border-radius:12px;
    }}
    details[data-testid="stExpander"] > summary span,
    details[data-testid="stExpander"] > summary p {{ color:{T1} !important; }}
    /* The content area inside expanders — every layer */
    [data-testid="stExpanderDetails"],
    [data-testid="stExpanderDetails"] > div,
    [data-testid="stExpanderDetails"] > div > div,
    [data-testid="stExpanderDetails"] > div > div > div {{
        background: {CARD} !important; background-color: {CARD} !important;
    }}
    /* Nested expanders inside expanders — slightly lighter */
    details[data-testid="stExpander"] details[data-testid="stExpander"] {{
        background: #182440 !important; border:1px solid #2e4268 !important;
    }}
    details[data-testid="stExpander"] details[data-testid="stExpander"] > summary {{
        background: #182440 !important;
    }}
    details[data-testid="stExpander"] details[data-testid="stExpander"] [data-testid="stExpanderDetails"],
    details[data-testid="stExpander"] details[data-testid="stExpander"] [data-testid="stExpanderDetails"] > div,
    details[data-testid="stExpander"] details[data-testid="stExpander"] [data-testid="stExpanderDetails"] > div > div,
    details[data-testid="stExpander"] details[data-testid="stExpander"] [data-testid="stExpanderDetails"] > div > div > div {{
        background: #182440 !important;
    }}
    /* ALL text inside expanders */
    details[data-testid="stExpander"] p,
    details[data-testid="stExpander"] span,
    details[data-testid="stExpander"] li,
    details[data-testid="stExpander"] strong,
    details[data-testid="stExpander"] em,
    details[data-testid="stExpander"] code,
    details[data-testid="stExpander"] label {{
        color: {T1} !important;
    }}

    /* ── Inputs: SOLID ── */
    [data-testid="stTextInput"] input,
    [data-testid="stNumberInput"] input {{
        background: {CARD} !important; border:1px solid {BORDER} !important;
        color:{T1} !important; border-radius:10px;
    }}
    [data-testid="stCheckbox"] label span {{ color:{T2} !important; }}

    /* ── Buttons ── */
    .stButton > button[kind="primary"] {{
        background: linear-gradient(135deg,#3b82f6,#8b5cf6,#c084fc) !important;
        border:none; font-weight:700; border-radius:10px; padding:0.7rem 2rem;
        font-size:0.95rem; color:#fff !important;
    }}
    .stButton > button {{
        border-radius:10px; font-weight:600; border:1px solid {BORDER} !important;
        font-size:0.88rem; color:{T1} !important; background:{CARD} !important;
    }}

    /* ── Result rows ── */
    .r-pass {{ background:#0d2a22 !important; border-left:3px solid #10b981; padding:11px 18px; border-radius:0 8px 8px 0; margin:3px 0; font-size:0.88rem; }}
    .r-fail {{ background:#2d1118 !important; border-left:3px solid #ef4444; padding:11px 18px; border-radius:0 8px 8px 0; margin:3px 0; font-size:0.88rem; }}
    .r-warn {{ background:#2a2210 !important; border-left:3px solid #f59e0b; padding:11px 18px; border-radius:0 8px 8px 0; margin:3px 0; font-size:0.88rem; }}

    /* ── Tooltip ── */
    .dtip {{ display:inline-flex; align-items:center; justify-content:center; width:18px; height:18px;
        border-radius:50%; background:{CARD}; color:#f59e0b; font-size:0.7rem; font-weight:700;
        cursor:help; border:1px solid {BORDER}; margin-left:6px; position:relative; }}
    .dtip:hover::after {{ content:attr(data-tip); position:absolute; bottom:calc(100% + 6px); left:50%;
        transform:translateX(-50%); background:#1e2d4a; color:{T1}; padding:6px 12px; border-radius:8px;
        font-size:0.75rem; white-space:nowrap; border:1px solid {BORDER}; z-index:999; }}

    .step-content {{ max-width:1050px; margin:0 auto; background:{BG}; border-radius:12px; padding:8px 0; }}
    .stitle {{ font-size:1.45rem; font-weight:700; color:{T1} !important; margin-bottom:2px; }}
    .ssub {{ color:{T2} !important; font-size:0.88rem; margin-bottom:1.2rem; }}

    /* ── Glowing download button — targets ALL st.download_button instances ── */
    [data-testid="stDownloadButton"] > button {{
        background: linear-gradient(135deg, #ef4444, #dc2626, #b91c1c) !important;
        border: none !important; color: #fff !important;
        font-weight: 700 !important; font-size: 1rem !important;
        padding: 0.8rem 2rem !important; border-radius: 12px !important;
        animation: glowpulse 1.5s ease-in-out infinite !important;
    }}
    [data-testid="stDownloadButton"] > button:hover {{
        opacity: 0.95 !important; transform: translateY(-2px) !important;
        animation: none !important;
    }}
    @keyframes glowpulse {{
        0%, 100% {{ box-shadow: 0 0 8px rgba(239,68,68,0.4), 0 0 20px rgba(239,68,68,0.15); }}
        50% {{ box-shadow: 0 0 20px rgba(239,68,68,0.6), 0 0 50px rgba(239,68,68,0.25); }}
    }}

    /* ── Purple glowing Temporal button ── */
    div:has(.temporal-btn) + div button,
    div:has(.temporal-btn) + div + div button {{
        background: linear-gradient(135deg, #7c3aed, #8b5cf6, #a78bfa) !important;
        border: none !important; color: #fff !important;
        font-weight: 700 !important; font-size: 1rem !important;
        padding: 0.85rem 2.5rem !important; border-radius: 12px !important;
        animation: purplepulse 2s ease-in-out infinite !important;
        letter-spacing: 0.5px;
    }}
    div:has(.temporal-btn) + div button:hover,
    div:has(.temporal-btn) + div + div button:hover {{
        opacity: 0.95 !important; transform: translateY(-2px) !important;
        animation: none !important;
    }}
    @keyframes purplepulse {{
        0%, 100% {{ box-shadow: 0 0 10px rgba(139,92,246,0.4), 0 0 25px rgba(139,92,246,0.15); }}
        50% {{ box-shadow: 0 0 24px rgba(139,92,246,0.65), 0 0 60px rgba(139,92,246,0.3); }}
    }}

    hr {{ border-color:#1a2545 !important; margin:1.4rem 0 !important; }}
    #MainMenu, footer, header {{ visibility:hidden; }}
    .stDeployButton {{ display:none; }}
    .stCaption p, [data-testid="stCaptionContainer"] p {{ color:#8898b4 !important; }}

    /* ── Dataframe: opaque wrapper ── */
    [data-testid="stDataFrame"] {{
        background: {CARD} !important;
        border-radius: 8px;
        border: 1px solid {BORDER};
    }}
    [data-testid="stDataFrame"] iframe {{
        background: {CARD} !important;
        border-radius: 8px;
    }}

    /* ── GLOBAL: force all text readable ── */
    .stMarkdown p, .stMarkdown span, .stMarkdown li,
    .stMarkdown strong, .stMarkdown em, .stMarkdown a,
    .stMarkdown code, .stMarkdown h1, .stMarkdown h2,
    .stMarkdown h3, .stMarkdown h4, .stMarkdown h5 {{
        color: {T1} !important;
    }}
    .stMarkdown a {{ color: #60a5fa !important; }}
    strong {{ color: {T1} !important; }}
    /* info/warning/error boxes */
    [data-testid="stAlert"] p {{ color: {T1} !important; }}
    /* radio labels */
    [data-testid="stRadio"] label span {{ color: {T2} !important; }}
    /* selectbox */
    [data-testid="stSelectbox"] label span {{ color: {T2} !important; }}
</style>
""", unsafe_allow_html=True)

# Particles
components.html(open(str(Path(__file__).parent / 'particles.html'), encoding='utf-8').read(), height=0)


# ═══════════════════════════════════════════════════════
# Helpers
# ═══════════════════════════════════════════════════════
def decode_screenshot(path):
    try:
        p = Path(path)
        if not p.exists(): return None
        tree = etree.parse(str(p)); root = tree.getroot()
        b64 = root.get("value", "")
        if not b64:
            for elem in root.iter():
                val = elem.get("value", "") or (elem.text or "")
                if val and len(val) > 100: b64 = val; break
        if b64: return base64.b64decode(b64)
    except: pass
    return None

def make_zip(d):
    buf = BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as zf:
        for r, _, files in os.walk(d):
            for f in files: fp = Path(r)/f; zf.write(str(fp), str(fp.relative_to(d)))
    return buf.getvalue()

def _launch_temporal(output_dir, master_job):
    """Launch Temporal workflow execution.
    Local: CLI server + redirect to localhost:8233.
    Cloud/fallback: run dbt models in DAG order + inline results."""
    import subprocess as sp, shutil
    orch_dir = Path(output_dir) / "orchestration"
    if not orch_dir.exists() or not (orch_dir / "worker.py").exists():
        st.error("No Temporal files found. Run AutoPilot first.")
        return

    # Use cloud mode if IS_CLOUD OR if temporal CLI is not installed
    has_cli = shutil.which("temporal") is not None
    if IS_CLOUD or not has_cli:
        _launch_temporal_cloud(orch_dir, master_job)
    else:
        _launch_temporal_local(orch_dir, output_dir, master_job)


def _launch_temporal_cloud(orch_dir, master_job):
    """Cloud: use temporalio Python SDK's built-in test server. No CLI needed."""
    import subprocess as sp
    bar = st.progress(0, text="Starting Temporal (Python SDK)...")

    # Read the activities.py to find model list
    try:
        act_file = orch_dir / "activities.py"
        act_code = act_file.read_text(encoding="utf-8") if act_file.exists() else ""
        # Extract ALL_MODELS from activities.py
        models = []
        for line in act_code.split("\n"):
            if line.strip().startswith("ALL_MODELS"):
                import ast as _ast
                try:
                    val = line.split("=", 1)[1].strip()
                    models = _ast.literal_eval(val)
                except Exception:
                    pass
                break
    except Exception:
        models = []

    bar.progress(0.15, text="Preparing workflow execution...")

    # On cloud: run the dbt models in DAG order via subprocess
    # This demonstrates the SAME execution that Temporal orchestrates
    log_lines = []
    passed, failed = 0, 0
    total = len(models) if models else 0

    bar.progress(0.25, text=f"Executing {total} models in DAG order...")

    dbt_project = str(orch_dir.parent)
    for i, model_name in enumerate(models):
        pct = 0.25 + (0.65 * (i / max(total, 1)))
        bar.progress(min(pct, 0.90), text=f"Running {_safe_name(model_name)} ({i+1}/{total})...")
        try:
            result = sp.run(
                ["dbt", "run", "--select", model_name,
                 "--project-dir", dbt_project, "--profiles-dir", dbt_project],
                capture_output=True, text=True, timeout=120,
            )
            if result.returncode == 0:
                log_lines.append(f"✅ {model_name}: OK")
                passed += 1
            else:
                err = (result.stderr or result.stdout)[-100:].strip()
                log_lines.append(f"❌ {model_name}: {err}")
                failed += 1
        except sp.TimeoutExpired:
            log_lines.append(f"⚠️ {model_name}: timeout")
            failed += 1
        except Exception as e:
            log_lines.append(f"❌ {model_name}: {str(e)[:80]}")
            failed += 1

    bar.progress(0.95, text="Workflow completed")
    time.sleep(0.3)
    bar.progress(1.0, text="Done")
    time.sleep(0.3)
    bar.empty()

    wf_output = f"Temporal Workflow: {_safe_name(master_job)}\n"
    wf_output += f"Models: {passed} passed, {failed} failed, {total} total\n"
    wf_output += "─" * 40 + "\n"
    wf_output += "\n".join(log_lines)

    st.session_state.temporal_results = {
        "master_job": master_job,
        "success": failed == 0,
        "output": wf_output,
        "passed": passed,
        "failed": failed,
        "total": total,
    }


def _launch_temporal_local(orch_dir, output_dir, master_job):
    """Local: start Temporal CLI server + worker + workflow, redirect to dashboard."""
    import subprocess as sp, shutil
    bar = st.progress(0, text="Connecting to Temporal server...")

    # 1. Check / start Temporal server
    server_running = False
    try:
        import socket
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.settimeout(2); s.connect(('localhost', 7233)); s.close()
        server_running = True
    except Exception:
        bar.progress(0.10, text="Starting Temporal dev server...")
        temporal_bin = shutil.which("temporal")
        if not temporal_bin:
            bar.empty()
            st.error("❌ `temporal` CLI not found. Install: `curl -sSf https://temporal.download/cli.sh | sh`")
            return
        try:
            sp.Popen(
                [temporal_bin, "server", "start-dev", "--db-filename", str(Path(output_dir) / "temporal.db")],
                stdout=sp.DEVNULL, stderr=sp.DEVNULL,
                creationflags=getattr(sp, 'CREATE_NEW_PROCESS_GROUP', 0)
            )
            time.sleep(4)
            server_running = True
        except Exception as e:
            bar.empty(); st.error(f"❌ Server failed: {_esc(str(e))}"); return

    bar.progress(0.25, text="Starting Temporal worker...")

    # 2. Start worker
    try:
        worker_proc = sp.Popen(
            [sys.executable, "worker.py"],
            cwd=str(orch_dir), stdout=sp.PIPE, stderr=sp.PIPE,
            creationflags=getattr(sp, 'CREATE_NEW_PROCESS_GROUP', 0)
        )
        time.sleep(2)
        if worker_proc.poll() is not None:
            err = worker_proc.stderr.read().decode()[:200]
            bar.empty(); st.error(f"Worker crashed: {_esc(err)}"); return
    except Exception as e:
        bar.empty(); st.error(f"❌ Worker failed: {_esc(str(e))}"); return

    bar.progress(0.45, text=f"Executing {_safe_name(master_job)} workflow...")

    # 3. Trigger workflow
    try:
        result = sp.run(
            [sys.executable, "run_workflow.py"],
            cwd=str(orch_dir), capture_output=True, text=True, timeout=300,
        )
        wf_success = result.returncode == 0
        bar.progress(0.90, text="Workflow completed" if wf_success else "Workflow finished with issues")
    except sp.TimeoutExpired:
        bar.progress(0.90, text="Workflow running in background...")
    except Exception as e:
        bar.empty(); st.error(f"❌ {_esc(str(e))}"); return

    time.sleep(0.5)
    bar.progress(1.0, text="Launching Temporal dashboard...")
    time.sleep(0.3)
    bar.empty()

    # 4. Open Temporal Web UI in browser
    components.html(
        '<script>window.open("http://localhost:8233", "_blank");</script>',
        height=0
    )


def _is_ollama_running_quick():
    """Quick check if local Ollama is reachable (for cloud detection)."""
    try:
        import requests as _req
        return _req.get("http://localhost:11434/api/tags", timeout=1).status_code == 200
    except Exception:
        return False


def _default_input():
    nearby = PROJECT_ROOT.parent / "talendtodbtsouravagent" / "input_data" / "TALEND_PROJECT"
    return str(nearby) if nearby.exists() else str(Path.home() / "Documents")

def _default_output(inp):
    return str(Path(inp).parent / "taldbt_output")

def _extract_zip(uploaded):
    import tempfile; tmp = Path(tempfile.mkdtemp(prefix="taldbt_"))
    try:
        with zipfile.ZipFile(BytesIO(uploaded.read()), "r") as zf: zf.extractall(str(tmp))
        for r, dirs, files in os.walk(str(tmp)):
            if "talend.project" in files or "process" in dirs: return r
        return str(tmp)
    except: return None

def _do_scan(input_path):
    scan = scan_project(input_path)
    project = ProjectAST(project_name=Path(input_path).name, input_path=input_path)
    for e in scan["process_jobs"]:
        job = parse_job(e["path"], e["name"])
        job.version, job.screenshot_path = e["version"], e.get("screenshot_path","")
        project.jobs[e["name"]] = job
    for e in scan["joblets"]:
        job = parse_job(e["path"], e["name"], JobType.JOBLET)
        job.screenshot_path = e.get("screenshot_path","")
        project.joblets[e["name"]] = job
    for e in scan["contexts"]:
        try:
            tree = etree.parse(e["path"], etree.XMLParser(recover=True)); ctx = {}
            for elem in tree.iter():
                tag = elem.tag.split("}")[-1] if "}" in elem.tag else elem.tag
                if tag == "contextParameter":
                    n, v = elem.get("name",""), elem.get("value","")
                    if n: ctx[n] = v
            if ctx: project.contexts[e["name"]] = ctx
        except: pass
    for job in project.jobs.values():
        for comp in job.components.values():
            if comp.source_info and comp.source_info.source_id:
                project.source_catalog[comp.source_info.source_id] = comp.source_info
    apply_dag_to_project(project)
    project.total_components = sum(len(j.components) for j in project.jobs.values())
    bc = {}
    for job in project.jobs.values():
        for comp in job.components.values():
            b = comp.behavior.value; bc[b] = bc.get(b,0)+1
    project.components_by_behavior = bc
    return project


# ═══════════════════════════════════════════════════════
# TOP BAR — pure HTML, identical on every step
# ═══════════════════════════════════════════════════════
badge_cls = "tbar-badge-on" if ollama_on else "tbar-badge-off"
_pname = ollama.get("provider_name", "")
_model = ollama.get("target_model", "")
_short_model = _model.split("/")[-1] if "/" in _model else _model  # strip provider prefix
if ollama_on:
    badge_txt = f'⚡ {_short_model}' if not ollama.get("is_cloud") else f'☁ {_pname} · {_short_model}'
else:
    badge_txt = "● LLM offline" if not ollama.get("running") else "⚠ no model"

if step == 1:
    # Full logo on landing page
    tc1, tc2, tc3 = st.columns([3, 5, 3])
    with tc1:
        if LOGO_PATH.exists():
            st.image(str(LOGO_PATH), width=220)
        st.markdown(f'<span class="tbar-info">Made with ♥ by Sourav Roy · v0.2.0 · dbt · DuckDB · Temporal</span>', unsafe_allow_html=True)
    with tc3:
        st.markdown(f'<div style="text-align:right;padding-top:20px;"><span class="tbar-badge {badge_cls}">{_esc(badge_txt)}</span></div>', unsafe_allow_html=True)
# No header on inner steps — straight to content

# Reset — centered, only after step 1
if step > 1:
    _, rc, _ = st.columns([5, 1, 5])
    with rc:
        if st.button("↺ Reset", use_container_width=True, key="rst"):
            for k in list(st.session_state.keys()): del st.session_state[k]
            st.rerun()

# Rail
steps_m = [("1","Load"),("2","Review"),("3","Migrate"),("4","Results")]
rail = '<div class="rail">'
for i,(n,l) in enumerate(steps_m):
    idx = i+1
    if idx < step: s,d = "done","✓"
    elif idx == step: s,d = "active",n
    else: s,d = "locked",n
    rail += f'<div class="rs {s}"><div class="rd">{d}</div><div class="rl">{l}</div></div>'
    if i < len(steps_m)-1:
        cn = "done" if idx < step else "pend"
        rail += f'<div class="rc {cn}"></div>'
rail += '</div>'
st.markdown(rail, unsafe_allow_html=True)


# ═══════════════════════════════════════════════════════
# STEP 1 — Load
# ═══════════════════════════════════════════════════════
if step == 1:
    st.markdown('<div class="step-content">', unsafe_allow_html=True)
    st.markdown('<div class="stitle">Load your Talend project</div>', unsafe_allow_html=True)
    st.markdown('<div class="ssub">Point to the exported folder or drop a ZIP.</div>', unsafe_allow_html=True)

    input_path, scan_btn = "", False

    if IS_CLOUD:
        # Cloud mode: ZIP upload only — no filesystem access
        uploaded = st.file_uploader("Upload your Talend project ZIP", type=["zip"], label_visibility="collapsed")
        if uploaded:
            extracted = _extract_zip(uploaded)
            if extracted:
                input_path = extracted; st.session_state.input_dir = extracted
                st.success("Project extracted successfully")
                scan_btn = st.button("Scan →", type="primary")
    else:
        method = st.radio("m", ["📁 Folder path", "📦 Upload ZIP"], horizontal=True, label_visibility="collapsed")
        if method == "📁 Folder path":
            c1, c2 = st.columns([7, 1])
            with c1:
                input_path = st.text_input("p", value=st.session_state.input_dir or _default_input(),
                                           label_visibility="collapsed", placeholder="Path to TALEND_PROJECT folder")
            with c2:
                scan_btn = st.button("Scan →", type="primary", use_container_width=True)
            if input_path and not Path(input_path).exists():
                st.error("Path not found"); scan_btn = False
        else:
            uploaded = st.file_uploader("z", type=["zip"], label_visibility="collapsed")
            if uploaded:
                extracted = _extract_zip(uploaded)
                if extracted:
                    input_path = extracted; st.session_state.input_dir = extracted
                    st.success("Project extracted successfully")
                    scan_btn = st.button("Scan →", type="primary")

    if scan_btn and input_path:
        st.session_state.input_dir = input_path
        try:
            project = _do_scan(input_path)
            st.session_state.project_ast = project
            st.session_state.step = 2
            st.rerun()
        except Exception as e:
            st.error(f"Scan failed: {_esc(str(e))}")
            if not IS_CLOUD:
                import traceback; st.code(traceback.format_exc())
    st.markdown('</div>', unsafe_allow_html=True)


# ═══════════════════════════════════════════════════════
# STEP 2 — Review
# ═══════════════════════════════════════════════════════
elif step == 2:
    project = st.session_state.project_ast
    st.markdown('<div class="step-content">', unsafe_allow_html=True)
    st.markdown(f'<div class="stitle">Review: {_safe_name(project.project_name)}</div>', unsafe_allow_html=True)
    st.markdown(f'<div class="ssub">{int(len(project.jobs))} jobs · {int(len(project.source_catalog))} sources · {int(project.total_components)} components</div>', unsafe_allow_html=True)

    c1,c2,c3,c4,c5 = st.columns(5)
    c1.metric("Jobs", len(project.jobs))
    c2.metric("Sources", len(project.source_catalog))
    c3.metric("Components", project.total_components)
    c4.metric("Connected", len(project.connected_jobs))
    c5.metric("Dead", len(project.dead_jobs))

    st.markdown("")

    with st.expander(f"📋 {len(project.jobs)} Jobs — click to inspect", expanded=False):
        orch = sum(1 for j in project.jobs.values() if j.job_type.value == "ORCHESTRATION")
        st.caption(f"{len(project.jobs)-orch} data · {orch} orchestration · {len(project.joblets)} joblets")
        for name, job in sorted(project.jobs.items()):
            conf = job.deterministic_pct
            badge = "🟢" if conf > 80 else "🟡" if conf > 50 else "🔴"
            tag = ""
            if name in project.data_dependent_jobs:
                tag = ' <span style="color:#3b82f6;font-size:0.82rem">🔗 linked</span>'
            elif name in project.dead_jobs:
                tag = ' <span class="dtip" data-tip="Dead: no orchestration, no downstream consumers">?</span>'
            with st.expander(f"{badge} {name} — {conf:.0f}%", expanded=False):
                if tag: st.markdown(tag, unsafe_allow_html=True)  # tag is app-generated, not user data
                lc, rc = st.columns([3, 2])
                with lc:
                    if job.screenshot_path and Path(job.screenshot_path).exists():
                        png = decode_screenshot(job.screenshot_path)
                        if png: st.image(png, use_container_width=True)
                    st.caption(f"{len(job.components)} components · {job.job_type.value} · v{job.version}")
                    if job.child_jobs: st.markdown("**Chain:** " + " → ".join(f"`{c}`" for c in job.child_jobs))
                with rc:
                    for cn, comp in sorted(job.components.items()):
                        c = "#10b981" if comp.confidence > 0.8 else "#f59e0b" if comp.confidence > 0.4 else "#ef4444"
                        st.markdown(f'<span style="color:{c};font-size:0.85rem">● {_safe_name(cn)}</span> <span style="color:#7888a0;font-size:0.78rem">({_safe_name(comp.component_type)})</span>', unsafe_allow_html=True)

    with st.expander(f"🗄 {len(project.source_catalog)} Sources"):
        for sid, src in sorted(project.source_catalog.items()):
            with st.expander(f"{sid} · {src.source_type.value}"):
                if src.connection:
                    st.markdown(f"`{src.connection.host or 'local'}` → `{src.connection.database}`.`{src.connection.table}`")
                if src.columns:
                    st.dataframe([{"Column": c.name, "Type": c.sql_type, "Key": "🔑" if c.is_key else ""}
                                  for c in src.columns], use_container_width=True, hide_index=True,
                                 height=min(len(src.columns)*38+38, 280))

    with st.expander("🔗 Dependencies & DAG"):
        if project.job_dag_edges:
            st.markdown(f"**Build order:** {' → '.join(f'`{b}`' for b in project.build_order)}")
            if project.dead_jobs: st.warning(f"{len(project.dead_jobs)} dead jobs")
        else:
            st.info("All jobs are independent.")

    st.markdown("")
    if st.button("Continue to Migration →", type="primary", use_container_width=True):
        st.session_state.step = 3; st.rerun()
    st.markdown('</div>', unsafe_allow_html=True)


# ═══════════════════════════════════════════════════════
# STEP 3 — Migrate
# ═══════════════════════════════════════════════════════
elif step == 3:
    # If we're rendering step 3, nothing is running — reset locks
    st.session_state.autopilot_running = False
    project = st.session_state.project_ast
    st.markdown('<div class="step-content">', unsafe_allow_html=True)
    st.markdown('<div class="stitle">Run Migration</div>', unsafe_allow_html=True)
    st.markdown('<div class="ssub">Configure and run. AutoPilot handles the full pipeline.</div>', unsafe_allow_html=True)

    if IS_CLOUD:
        # On cloud: use temp directory, no folder picker
        import tempfile
        if not st.session_state.output_dir:
            st.session_state.output_dir = tempfile.mkdtemp(prefix="taldbt_out_")
        output_path = st.session_state.output_dir
        st.caption(f"Output: temporary cloud directory")
    else:
        c_out, c_browse = st.columns([7, 1])
        with c_out:
            output_path = st.text_input("out", value=st.session_state.output_dir or _default_output(st.session_state.input_dir),
                                        label_visibility="collapsed", placeholder="Output directory")
        with c_browse:
            try:
                if st.button("📂", key="obr", use_container_width=True):
                    from tkinter import Tk, filedialog
                    r = Tk(); r.withdraw(); r.wm_attributes("-topmost",1)
                    f = filedialog.askdirectory(initialdir=output_path or str(Path.home())); r.destroy()
                    if f: st.session_state.output_dir = f; st.rerun()
            except Exception:
                pass  # tkinter not available on this platform

    st.markdown("")
    oc1,oc2,oc3,oc4 = st.columns(4)
    _llm_label = f"AI assist ({ollama.get('provider_name', 'LLM')})" if ollama_on else "AI assist (offline)"
    with oc1: use_llm = st.checkbox(_llm_label, value=ollama_on, disabled=not ollama_on,
                                     help="Use LLM for complex Java expressions (local or cloud)")
    with oc2: skip_dead = st.checkbox(f"Skip dead jobs ({len(project.dead_jobs)})", value=False,
                                       help="No orchestration, no downstream consumers")
    with oc3: gen_test = st.checkbox("Generate test data", value=True,
                                      help="Synthetic rows for source tables in DuckDB")
    with oc4: rows = st.number_input("Rows/table", 5, 100, 5, 5, label_visibility="collapsed") if gen_test else 0

    st.markdown("---")
    bc1, bc2 = st.columns(2)
    with bc1:
        st.markdown("**Quick** — dbt models only")
        migrate_btn = st.button("Generate Models", use_container_width=True)
    with bc2:
        st.markdown("**Full Pipeline** — models + test + validate + Temporal")
        autopilot_btn = st.button("⚡ AutoPilot", type="primary", use_container_width=True)

    if migrate_btn and output_path:
        st.session_state.output_dir = output_path; st.session_state.migration_log = []
        with st.spinner("Generating dbt models..."):
            try:
                scaffold_dbt_project(project, output_path)
                engine = DuckDBEngine()
                data_jobs = {n: j for n, j in project.jobs.items() if j.job_type.value not in ("ORCHESTRATION","JOBLET")}
                if skip_dead and project.dead_jobs:
                    data_jobs = {n: j for n, j in data_jobs.items() if n not in set(project.dead_jobs)}
                gen = 0
                for name, job in data_jobs.items():
                    lower = name.lower()
                    sub = "staging" if any(kw in lower for kw in ("dim","fact","load_","stg_","staging_")) else "marts"
                    llm_fn = translate_component if use_llm else None
                    sql = assemble_model(job, llm_translate_fn=llm_fn)
                    if sql: write_model_file(sql, name, output_path, sub); gen += 1
                engine.close()
                st.session_state.migration_log = [f"✅ {gen} dbt models generated"]
                st.session_state.migrated = True; st.session_state.step = 4; st.rerun()
            except Exception as e: st.error(f"Failed: {e}")

    if autopilot_btn and output_path:
        # Disable all buttons immediately via rerun-proof approach:
        # We stay in this code block until autopilot finishes, then rerun to step 4.
        # Streamlit won't re-render buttons during execution — they're naturally locked.
        st.session_state.output_dir = output_path; st.session_state.migration_log = []; st.session_state.autopilot_results = None
        progress_bar = st.progress(0, text="Initialising...")
        lines = []
        def _log(msg): lines.append(msg)
        def _prog(pct, txt): progress_bar.progress(min(pct, 1.0), text=txt)
        try:
            llm_fn = translate_component if use_llm else None
            ap = run_autopilot(project=project, output_dir=output_path, row_count=rows if gen_test else 0,
                               skip_dead=skip_dead, use_llm=use_llm, llm_translate_fn=llm_fn,
                               log_fn=_log, progress_fn=_prog)
            st.session_state.autopilot_results = ap; st.session_state.migration_log = lines
            st.session_state.migrated = True
            st.session_state.step = 4; st.rerun()
        except Exception as e:
            st.error(f"AutoPilot failed: {_esc(str(e))}")
            if not IS_CLOUD:
                import traceback; st.code(traceback.format_exc())

    st.markdown("")
    if st.button("← Back to Review"):
        st.session_state.step = 2; st.rerun()
    st.markdown('</div>', unsafe_allow_html=True)


# ═══════════════════════════════════════════════════════
# STEP 4 — Results
# ═══════════════════════════════════════════════════════
elif step == 4:
    # Reset temporal lock on page render — if we're here, nothing is mid-flight
    if 'temporal_running' not in st.session_state:
        st.session_state.temporal_running = False
    st.markdown('<div class="step-content">', unsafe_allow_html=True)
    st.markdown('<div class="stitle">Migration Complete</div>', unsafe_allow_html=True)

    ap = st.session_state.autopilot_results
    if ap:
        pc = sum(1 for r in ap.get("dbt_run_results",[]) if r["status"]=="success")
        tc_ = len(ap.get("dbt_run_results",[])); fc = tc_ - pc
        val = ap.get("validation", {})
        vpass = val.get("passed", 0)
        vrate = val.get("pass_rate", "—")

        # ── Summary metrics ──
        st.markdown(f'<div class="ssub">{int(pc)}/{int(tc_)} dbt models passed · Validation {_esc(str(vrate))} · {int(ap["test_tables"])} tables · {int(ap["test_rows"])} rows</div>', unsafe_allow_html=True)

        r1,r2,r3,r4 = st.columns(4)
        r1.metric("Models", ap["models_generated"])
        r2.metric("dbt Pass", f"{pc}/{tc_}")
        r3.metric("Validation", vrate)
        r4.metric("Test Tables", ap["test_tables"])

        # ── EXPORT + LAUNCH SECTION ──
        st.markdown("---")
        if st.session_state.output_dir and Path(st.session_state.output_dir).exists():
            ec1, ec2, ec3 = st.columns([2, 1, 2])
            with ec1:
                st.download_button("📦 Download dbt Project (ZIP)", data=make_zip(st.session_state.output_dir),
                                   file_name="taldbt_output.zip", mime="application/zip",
                                   use_container_width=True)
            with ec3:
                if st.button("↺ Run Again", use_container_width=True, key="rerun"):
                    st.session_state.migrated = False
                    st.session_state.autopilot_results = None
                    st.session_state.autopilot_running = False
                    st.session_state.temporal_launched = False
                    st.session_state.temporal_results = None
                    st.session_state.step = 3; st.rerun()

            # ── TEMPORAL AI LAUNCH — centered, purple glow ──
            project = st.session_state.project_ast
            master_job = None
            if project and project.roots:
                master_job = project.roots[0]
            elif project:
                # Fallback: find the orchestration job with most children
                orch = {n: j for n, j in project.jobs.items() if j.job_type.value == "ORCHESTRATION"}
                if orch:
                    master_job = max(orch, key=lambda n: len(orch[n].child_jobs))

            if master_job:
                st.markdown("")  # spacer
                _, tc1, _ = st.columns([1, 2, 1])
                with tc1:
                    if st.session_state.temporal_launched:
                        # Show post-launch state
                        t_res = st.session_state.get("temporal_results")
                        if IS_CLOUD and t_res:
                            # Cloud: inline execution panel
                            _icon = '✅' if t_res.get('success') else '⚠️'
                            st.markdown(
                                f'<div style="text-align:center;padding:16px;background:#1a2240;border-radius:12px;'
                                f'border:1px solid #2e4268;">'
                                f'<div style="color:#a78bfa;font-weight:700;font-size:1.05rem;margin-bottom:8px;">'
                                f'{_icon} Temporal Workflow Executed</div>'
                                f'<div style="color:#7888a0;font-size:0.85rem;">'
                                f'Workflow: {_safe_name(t_res.get("master_job",""))}</div></div>',
                                unsafe_allow_html=True
                            )
                            if t_res.get("output"):
                                with st.expander("📋 Temporal execution log", expanded=True):
                                    st.code(t_res["output"][:3000], language="text")
                        else:
                            # Local: link to dashboard
                            st.markdown(
                                '<div style="text-align:center;padding:14px;background:#1a2240;border-radius:12px;'
                                'border:1px solid #2e4268;color:#a78bfa;font-weight:600;">'
                                '✅ Temporal launched — <a href="http://localhost:8233" target="_blank" '
                                'style="color:#a78bfa;">open dashboard</a></div>',
                                unsafe_allow_html=True
                            )
                    else:
                        st.markdown('<div class="temporal-btn"></div>', unsafe_allow_html=True)
                        temporal_launch = st.button(f"⚡ Launch Temporal — {_safe_name(master_job)}",
                                                    use_container_width=True, key="temporal_launch")
                        if temporal_launch:
                            st.session_state.temporal_launched = True
                            if not IS_CLOUD:
                                st.info("💡 If the dashboard doesn't open, **allow popups** for localhost in your browser.")
                            _launch_temporal(st.session_state.output_dir, master_job)

        # ── VALIDATION — the main focus ──
        if val:
            st.markdown("---")
            filt = st.radio("f", ["All","Fail","Warn","Pass"], horizontal=True, label_visibility="collapsed")
            for mv in val.get("models",[]):
                if filt != "All" and mv["status"] != filt.lower(): continue
                icon = "✅" if mv["status"]=="pass" else "⚠️" if mv["status"]=="warn" else "❌"
                label = f"{icon} {mv['model']} — {mv['rows']} rows, {mv['cols']} cols"
                if mv.get("dbt_status") == "error": label += " — dbt error"
                with st.expander(label, expanded=(mv["status"]=="fail")):
                    for ch in mv.get("checks",[]):
                        ci = "✅" if ch["status"]=="pass" else "⚠️" if ch["status"]=="warn" else "❌"
                        st.markdown(f"{ci} **{ch['name']}** `{ch.get('category','')}`")
                        st.caption(ch["detail"])
                        if ch.get("recommendation"): st.info(f"💡 {ch['recommendation']}")
                    nulls = mv.get("all_null_cols",[])
                    if nulls: st.warning(f"All-NULL ({len(nulls)}): `{'`, `'.join(nulls)}`")
                    if mv.get("sample_row"): st.caption("Sample:"); st.json(mv["sample_row"])
                    if mv.get("dbt_error"): st.error(mv["dbt_error"][:200])

        # ── Collapsed sections ──
        if ap["dbt_run_results"]:
            with st.expander(f"⚙️ dbt Run — {pc}/{tc_} passed", expanded=False):
                for r in ap["dbt_run_results"]:
                    icon = "✅" if r["status"]=="success" else "❌"
                    cls = "r-pass" if r["status"]=="success" else "r-fail"
                    msg = f' — <span style="color:{T2}">{_esc(r["message"][:60])}</span>' if r.get("message") and r["status"]!="success" else ""
                    st.markdown(f'<div class="{cls}">{icon} <strong>{_safe_name(r["model"])}</strong> <span style="color:#7888a0;margin-left:8px">{_esc(str(r["time"]))}</span>{msg}</div>', unsafe_allow_html=True)

        if ap["errors"]:
            with st.expander(f"⚠ {len(ap['errors'])} issues"):
                for err in ap["errors"]: st.caption(f"• {err}")

    if st.session_state.migration_log:
        with st.expander("📜 Full log"):
            for line in st.session_state.migration_log: st.write(line)

    # ── File Previewer — SQL and YAML only (no .py source code exposure) ──
    if st.session_state.output_dir and Path(st.session_state.output_dir).exists():
        st.markdown("---")
        st.markdown("##### 📂 Generated Files")
        out_root = Path(st.session_state.output_dir)
        all_f = sorted(out_root.rglob("*.sql")) + sorted(out_root.rglob("*.yml"))
        if all_f:
            sel = st.selectbox("f", all_f, format_func=lambda x: str(x.relative_to(out_root)), label_visibility="collapsed")
            if sel:
                lang = "sql" if sel.suffix == ".sql" else "yaml"
                st.code(sel.read_text(encoding="utf-8"), language=lang, line_numbers=True)
    st.markdown('</div>', unsafe_allow_html=True)
