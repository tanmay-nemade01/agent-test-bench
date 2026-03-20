"""
Streamlit app to build Docker containers from task folders,
run solve.sh solutions, and validate outputs with test_outputs.py.
"""

import os
import queue
import subprocess
import threading
from pathlib import Path

import streamlit as st

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------
BASE_DIR = Path(__file__).resolve().parent
TASKS_DIR = BASE_DIR / "tasks"
TASK_FOLDERS = sorted(
    [
        d.name
        for d in TASKS_DIR.iterdir()
        if d.is_dir()
        and (d / "environment" / "Dockerfile").exists()
        and (d / "solution" / "solve.sh").exists()
        and (d / "tests" / "test.sh").exists()
        and (d / "tests" / "test_outputs.py").exists()
    ]
) if TASKS_DIR.exists() else []

WINDOWS_DOCKER_HOST_CANDIDATES = [
    "npipe:////./pipe/docker_engine",
    "npipe:////./pipe/dockerDesktopLinuxEngine",
    "npipe:////./pipe/dockerDesktopWindowsEngine",
]


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _looks_like_docker_daemon_error(output: str) -> bool:
    text = (output or "").lower()
    return any(
        msg in text
        for msg in [
            "failed to connect to the docker api",
            "is the docker daemon running",
            "error during connect",
            "open //./pipe",
            "dockerdesktoplinuxengine",
            "cannot find the file specified",
        ]
    )


def _docker_env_candidates() -> list[dict[str, str]]:
    """Return env maps to try for Docker commands."""
    base_env = os.environ.copy()

    if os.name != "nt":
        return [base_env]

    candidates: list[dict[str, str]] = []
    seen_hosts: set[str] = set()

    current_host = base_env.get("DOCKER_HOST")
    if current_host:
        candidates.append(base_env.copy())
        seen_hosts.add(current_host)

    for host in WINDOWS_DOCKER_HOST_CANDIDATES:
        if host in seen_hosts:
            continue
        env = base_env.copy()
        env["DOCKER_HOST"] = host
        candidates.append(env)

    if not candidates:
        candidates.append(base_env)

    return candidates


def _stream_process(cmd: list[str], output_queue: queue.Queue, **kwargs):
    """Run a subprocess and push stdout/stderr lines into a queue."""
    try:
        proc = subprocess.Popen(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            encoding="utf-8",
            errors="replace",
            bufsize=1,
            **kwargs,
        )
        for line in iter(proc.stdout.readline, ""):
            output_queue.put(line)
        proc.stdout.close()
        proc.wait()
        output_queue.put(None)  # sentinel
        output_queue.put(proc.returncode)
    except Exception as exc:
        output_queue.put(f"ERROR: {exc}\n")
        output_queue.put(None)
        output_queue.put(1)


def run_command_with_live_output(cmd: list[str], placeholder=None, **kwargs) -> tuple[int, str]:
    """
    Execute *cmd*, stream output into the Streamlit *placeholder*,
    and return (returncode, full_output).
    """
    if placeholder is None:
        placeholder = st.empty()
    q: queue.Queue = queue.Queue()
    thread = threading.Thread(target=_stream_process, args=(cmd, q), kwargs=kwargs)
    thread.start()

    lines: list[str] = []
    while True:
        try:
            item = q.get(timeout=0.2)
        except queue.Empty:
            placeholder.code("".join(lines), language="text")
            continue
        if item is None:
            break
        lines.append(item)
        placeholder.code("".join(lines[-200:]), language="text")

    returncode = q.get()
    thread.join()
    full_output = "".join(lines)
    placeholder.code(full_output[-8000:] if len(full_output) > 8000 else full_output, language="text")
    return returncode, full_output


def run_docker_command_with_fallback(
    docker_cmd: list[str], *, live: bool = False, placeholder=None
) -> tuple[int, str, str | None]:
    """Run docker command with fallback DOCKER_HOST pipes on Windows."""
    last_rc = 1
    last_output = ""
    last_host = None

    for env in _docker_env_candidates():
        host = env.get("DOCKER_HOST")
        try:
            if live:
                rc, output = run_command_with_live_output(docker_cmd, placeholder=placeholder, env=env)
            else:
                proc = subprocess.run(
                    docker_cmd,
                    capture_output=True,
                    text=True,
                    encoding="utf-8",
                    errors="replace",
                    env=env,
                )
                rc = proc.returncode
                output = (proc.stdout or "") + (proc.stderr or "")
        except FileNotFoundError:
            return 1, "Docker CLI not found. Install Docker Desktop and ensure 'docker' is on PATH.", host
        except Exception as exc:
            rc = 1
            output = f"ERROR: {exc}"

        last_rc, last_output, last_host = rc, output, host

        if rc == 0:
            return rc, output, host

        if _looks_like_docker_daemon_error(output):
            continue

        return rc, output, host

    return last_rc, last_output, last_host


def docker_run_quiet(docker_cmd: list[str]) -> tuple[int, str, str | None]:
    return run_docker_command_with_fallback(docker_cmd, live=False)


def docker_is_ready() -> tuple[bool, str, str | None]:
    rc, output, host = docker_run_quiet(["docker", "version", "--format", "{{.Server.Version}}"])
    if rc == 0:
        version = output.strip().splitlines()[-1] if output.strip() else "unknown"
        host_label = host or "default"
        return True, f"Docker daemon reachable (server {version}, host={host_label})", host

    msg = (
        "Could not connect to Docker daemon. Start Docker Desktop and wait until Engine is running. "
        "If you're on Windows, check context/engine in Docker Desktop settings."
    )
    details = (output or "").strip()
    if details:
        msg = f"{msg}\n\nDetails:\n{details[-1200:]}"
    return False, msg, host


def docker_image_name(task: str) -> str:
    return f"task-runner-{task}".lower().replace(" ", "-")


def docker_container_name(task: str) -> str:
    return f"task-container-{task}".lower().replace(" ", "-")


def prepare_container(container_name: str, image_name: str, task_dir: Path) -> tuple[int, str]:
    """Create/start container and copy scripts into it."""
    docker_run_quiet(["docker", "rm", "-f", container_name])

    rc, out, _ = docker_run_quiet(["docker", "create", "--name", container_name, image_name, "sleep", "infinity"])
    if rc != 0:
        return rc, out

    rc, out, _ = docker_run_quiet(["docker", "start", container_name])
    if rc != 0:
        return rc, out

    rc, out, _ = docker_run_quiet(["docker", "exec", container_name, "mkdir", "-p", "/solution", "/tests"])
    if rc != 0:
        return rc, out

    solve_src = str(task_dir / "solution" / "solve.sh")
    test_sh_src = str(task_dir / "tests" / "test.sh")
    test_py_src = str(task_dir / "tests" / "test_outputs.py")

    for src, dst in [
        (solve_src, f"{container_name}:/solution/solve.sh"),
        (test_sh_src, f"{container_name}:/tests/test.sh"),
        (test_py_src, f"{container_name}:/tests/test_outputs.py"),
    ]:
        rc, out, _ = docker_run_quiet(["docker", "cp", src, dst])
        if rc != 0:
            return rc, out

    rc, out, _ = docker_run_quiet(["docker", "exec", container_name, "chmod", "+x", "/solution/solve.sh", "/tests/test.sh"])
    if rc != 0:
        return rc, out

    return 0, ""


def test_outputs_command(container_name: str) -> list[str]:
    """
    Execute test_outputs.py through pytest so test functions actually run.
    Falls back across common invocation styles.
    """
    return [
        "docker",
        "exec",
        container_name,
        "bash",
        "-lc",
        "pytest -rA -s /tests/test_outputs.py || python3 -m pytest -rA -s /tests/test_outputs.py || python -m pytest -rA -s /tests/test_outputs.py",
    ]


# ---------------------------------------------------------------------------
# Streamlit UI
# ---------------------------------------------------------------------------

st.set_page_config(page_title="Task Runner", page_icon="🐳", layout="wide")
st.title("🐳 Docker Task Runner")

if not TASK_FOLDERS:
    st.error("No valid task folders found under ./tasks. Each task folder needs environment/Dockerfile, solution/solve.sh, tests/test.sh, and tests/test_outputs.py.")
    st.stop()

with st.sidebar:
    st.header("Settings")
    selected_task = st.selectbox("Select Task", TASK_FOLDERS)
    task_dir = TASKS_DIR / selected_task
    st.divider()

    toml_path = task_dir / "task.toml"
    if toml_path.exists():
        st.subheader("Task Info")
        toml_text = toml_path.read_text(encoding="utf-8")
        for line in toml_text.splitlines():
            line = line.strip()
            if line.startswith("name"):
                val = line.split("=", 1)[1].strip().strip('"')
                st.markdown(f"**Name:** {val}")
            elif line.startswith("difficulty"):
                val = line.split("=", 1)[1].strip().strip('"')
                st.markdown(f"**Difficulty:** {val}")
            elif line.startswith("category"):
                val = line.split("=", 1)[1].strip().strip('"')
                st.markdown(f"**Category:** {val}")
            elif line.startswith("timeout_sec"):
                val = line.split("=", 1)[1].strip()
                st.markdown(f"**Timeout:** {val}s")

    st.divider()
    st.caption("Workflow: Build → Solve → Test")

tab_instructions, tab_run, tab_files = st.tabs(["📄 Instructions", "▶️ Run & Test", "📁 Files"])

with tab_instructions:
    instruction_path = task_dir / "instruction.md"
    if instruction_path.exists():
        st.markdown(instruction_path.read_text(encoding="utf-8"))
    else:
        st.info("No instruction.md found for this task.")

with tab_files:
    col1, col2, col3 = st.columns(3)
    with col1:
        st.subheader("solve.sh")
        st.code((task_dir / "solution" / "solve.sh").read_text(encoding="utf-8"), language="bash")
    with col2:
        st.subheader("test.sh")
        st.code((task_dir / "tests" / "test.sh").read_text(encoding="utf-8"), language="bash")
    with col3:
        st.subheader("test_outputs.py")
        st.code((task_dir / "tests" / "test_outputs.py").read_text(encoding="utf-8"), language="python")

with tab_run:
    image_name = docker_image_name(selected_task)
    container_name = docker_container_name(selected_task)
    docker_ok, docker_status_msg, _ = docker_is_ready()

    if docker_ok:
        st.success(docker_status_msg)
    else:
        st.error(docker_status_msg)
        st.info("Start Docker Desktop and wait for Engine running, then retry.")

    state_key = f"state_{selected_task}"
    if state_key not in st.session_state:
        st.session_state[state_key] = {
            "image_built": False,
            "solve_done": False,
            "test_done": False,
            "build_log": "",
            "solve_log": "",
            "test_log": "",
            "test_passed": None,
        }
    ts = st.session_state[state_key]

    c1, c2, c3 = st.columns(3)
    c1.metric("1. Build Image", "✅" if ts["image_built"] else "⬜")
    c2.metric("2. Run solve.sh", "✅" if ts["solve_done"] else "⬜")
    c3.metric("3. Run tests", "✅ Passed" if ts["test_passed"] is True else ("❌ Failed" if ts["test_passed"] is False else "⬜"))

    st.divider()
    st.subheader("Step 1 — Build Docker Image")
    if st.button("🔨 Build Image", key="btn_build", disabled=not docker_ok):
        ts["image_built"] = False
        ts["solve_done"] = False
        ts["test_done"] = False
        ts["test_passed"] = None

        dockerfile_dir = str(task_dir / "environment")
        with st.spinner("Building Docker image..."):
            placeholder = st.empty()
            rc, log, _ = run_docker_command_with_fallback(
                ["docker", "build", "-t", image_name, dockerfile_dir],
                live=True,
                placeholder=placeholder,
            )
            ts["build_log"] = log

        if rc == 0:
            ts["image_built"] = True
            st.success(f"Image **{image_name}** built successfully.")
        else:
            st.error(f"Build failed (exit code {rc}). Check the log above.")
        st.rerun()

    if ts["build_log"]:
        with st.expander("Build log", expanded=False):
            st.code(ts["build_log"][-6000:], language="text")

    st.subheader("Step 2 — Run solve.sh")
    if st.button("▶️ Run solve.sh", key="btn_solve", disabled=not (ts["image_built"] and docker_ok)):
        ts["solve_done"] = False
        ts["test_done"] = False
        ts["test_passed"] = None

        with st.spinner("Starting container & running solve.sh ..."):
            rc, prep_out = prepare_container(container_name, image_name, task_dir)
            if rc != 0:
                ts["solve_log"] = prep_out
                st.error("Failed to prepare container. Check log below.")
                st.rerun()

            placeholder = st.empty()
            rc, log, _ = run_docker_command_with_fallback(
                ["docker", "exec", container_name, "bash", "/solution/solve.sh"],
                live=True,
                placeholder=placeholder,
            )
            ts["solve_log"] = log

        if rc == 0:
            ts["solve_done"] = True
            st.success("solve.sh completed successfully.")
        else:
            st.error(f"solve.sh failed (exit code {rc}). Check the log above.")
        st.rerun()

    if ts["solve_log"]:
        with st.expander("solve.sh log", expanded=False):
            st.code(ts["solve_log"][-6000:], language="text")

    st.subheader("Step 3 — Run test_outputs.py")
    if st.button("🧪 Run test_outputs.py", key="btn_test", disabled=not (ts["solve_done"] and docker_ok)):
        ts["test_done"] = False
        ts["test_passed"] = None

        with st.spinner("Running test_outputs.py inside container..."):
            placeholder = st.empty()
            rc, log, _ = run_docker_command_with_fallback(
                test_outputs_command(container_name),
                live=True,
                placeholder=placeholder,
            )
            ts["test_log"] = log

        ts["test_done"] = True
        ts["test_passed"] = rc == 0
        if rc == 0:
            st.success("All tests passed! ✅")
            st.balloons()
        else:
            st.error(f"Tests failed (exit code {rc}). See log below.")
        st.rerun()

    if ts["test_log"]:
        with st.expander("Test log", expanded=not ts.get("test_passed", True)):
            st.code(ts["test_log"][-6000:], language="text")

    st.divider()
    if st.button("🗑️ Cleanup Container", key="btn_cleanup", disabled=not docker_ok):
        docker_run_quiet(["docker", "rm", "-f", container_name])
        ts["solve_done"] = False
        ts["test_done"] = False
        ts["test_passed"] = None
        ts["solve_log"] = ""
        ts["test_log"] = ""
        st.success(f"Container **{container_name}** removed.")
        st.rerun()

    st.divider()
    st.subheader("⚡ One-Click Run All")
    if st.button("🚀 Build → Solve → Test", key="btn_run_all", type="primary", disabled=not docker_ok):
        ts["image_built"] = False
        ts["solve_done"] = False
        ts["test_done"] = False
        ts["test_passed"] = None
        ts["build_log"] = ""
        ts["solve_log"] = ""
        ts["test_log"] = ""

        dockerfile_dir = str(task_dir / "environment")

        st.subheader("Building image…")
        placeholder_build = st.empty()
        rc, log, _ = run_docker_command_with_fallback(
            ["docker", "build", "-t", image_name, dockerfile_dir],
            live=True,
            placeholder=placeholder_build,
        )
        ts["build_log"] = log
        if rc != 0:
            st.error("Build failed. Aborting.")
            st.stop()
        ts["image_built"] = True
        st.success("Image built ✅")

        rc, prep_out = prepare_container(container_name, image_name, task_dir)
        if rc != 0:
            ts["solve_log"] = prep_out
            st.error("Failed to prepare container. Aborting.")
            st.stop()

        st.subheader("Running solve.sh…")
        placeholder_solve = st.empty()
        rc, log, _ = run_docker_command_with_fallback(
            ["docker", "exec", container_name, "bash", "/solution/solve.sh"],
            live=True,
            placeholder=placeholder_solve,
        )
        ts["solve_log"] = log
        if rc != 0:
            st.error("solve.sh failed. Continuing to tests anyway…")
        else:
            ts["solve_done"] = True
            st.success("solve.sh completed ✅")

        st.subheader("Running test_outputs.py…")
        placeholder_test = st.empty()
        rc, log, _ = run_docker_command_with_fallback(
            test_outputs_command(container_name),
            live=True,
            placeholder=placeholder_test,
        )
        ts["test_log"] = log
        ts["test_done"] = True
        ts["test_passed"] = rc == 0

        if rc == 0:
            st.success("All tests passed! ✅")
            st.balloons()
        else:
            st.error(f"Tests failed (exit code {rc}).")

        st.rerun()
