"""
Streamlit app to build Docker containers from task folders,
run solve.sh solutions, and validate them with test.sh.
"""

import streamlit as st
import subprocess
import os
import time
import threading
import queue
from pathlib import Path

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------
BASE_DIR = Path(__file__).resolve().parent
TASK_FOLDERS = sorted(
    [
        d.name
        for d in BASE_DIR.iterdir()
        if d.is_dir()
        and (d / "environment" / "Dockerfile").exists()
        and (d / "solution" / "solve.sh").exists()
        and (d / "tests" / "test.sh").exists()
        and (d / "tests" / "test_outputs.py").exists()
    ]
)

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _stream_process(cmd: list[str], output_queue: queue.Queue, **kwargs):
    """Run a subprocess and push stdout/stderr lines into a queue."""
    try:
        proc = subprocess.Popen(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
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
            # re-render what we have so far
            placeholder.code("".join(lines), language="text")
            continue
        if item is None:
            break
        lines.append(item)
        placeholder.code("".join(lines[-200:]), language="text")  # keep last 200 lines visible

    # next item is the return code
    returncode = q.get()
    thread.join()
    full_output = "".join(lines)
    placeholder.code(full_output[-8000:] if len(full_output) > 8000 else full_output, language="text")
    return returncode, full_output


def docker_image_name(task: str) -> str:
    return f"task-runner-{task}".lower().replace(" ", "-")


def docker_container_name(task: str) -> str:
    return f"task-container-{task}".lower().replace(" ", "-")


# ---------------------------------------------------------------------------
# Streamlit UI
# ---------------------------------------------------------------------------

st.set_page_config(page_title="Task Runner", page_icon="🐳", layout="wide")
st.title("🐳 Docker Task Runner")

if not TASK_FOLDERS:
    st.error("No valid task folders found. Each task folder needs environment/Dockerfile, solution/solve.sh, tests/test.sh, and tests/test_outputs.py.")
    st.stop()

# ---- Sidebar: task selection & settings ----
with st.sidebar:
    st.header("Settings")
    selected_task = st.selectbox("Select Task", TASK_FOLDERS)
    task_dir = BASE_DIR / selected_task
    st.divider()

    # Show task metadata if task.toml exists
    toml_path = task_dir / "task.toml"
    if toml_path.exists():
        st.subheader("Task Info")
        toml_text = toml_path.read_text(encoding="utf-8")
        # simple parse for display
        for line in toml_text.splitlines():
            line = line.strip()
            if line.startswith("name"):
                val = line.split('=', 1)[1].strip().strip('"')
                st.markdown(f"**Name:** {val}")
            elif line.startswith("difficulty"):
                val = line.split('=', 1)[1].strip().strip('"')
                st.markdown(f"**Difficulty:** {val}")
            elif line.startswith("category"):
                val = line.split('=', 1)[1].strip().strip('"')
                st.markdown(f"**Category:** {val}")
            elif line.startswith("timeout_sec"):
                val = line.split('=', 1)[1].strip()
                st.markdown(f"**Timeout:** {val}s")

    st.divider()
    st.caption("Workflow: Build → Solve → Test")

# ---- Main area: tabs ----
tab_instructions, tab_run, tab_files = st.tabs(["📄 Instructions", "▶️ Run & Test", "📁 Files"])

# ---- Tab 1: Instructions ----
with tab_instructions:
    instruction_path = task_dir / "instruction.md"
    if instruction_path.exists():
        st.markdown(instruction_path.read_text(encoding="utf-8"))
    else:
        st.info("No instruction.md found for this task.")

# ---- Tab 3: Files ----
with tab_files:
    col1, col2, col3 = st.columns(3)
    with col1:
        st.subheader("solve.sh")
        st.code(
            (task_dir / "solution" / "solve.sh").read_text(encoding="utf-8"),
            language="bash",
        )
    with col2:
        st.subheader("test.sh")
        st.code(
            (task_dir / "tests" / "test.sh").read_text(encoding="utf-8"),
            language="bash",
        )
    with col3:
        st.subheader("test_outputs.py")
        st.code(
            (task_dir / "tests" / "test_outputs.py").read_text(encoding="utf-8"),
            language="python",
        )

# ---- Tab 2: Run & Test ----
with tab_run:
    image_name = docker_image_name(selected_task)
    container_name = docker_container_name(selected_task)

    # Initialize session state for this task
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

    # Step indicators
    c1, c2, c3 = st.columns(3)
    c1.metric("1. Build Image", "✅" if ts["image_built"] else "⬜")
    c2.metric("2. Run solve.sh", "✅" if ts["solve_done"] else "⬜")
    c3.metric("3. Run tests", "✅ Passed" if ts["test_passed"] is True else ("❌ Failed" if ts["test_passed"] is False else "⬜"))

    st.divider()

    # ---- Step 1: Build Docker image ----
    st.subheader("Step 1 — Build Docker Image")
    if st.button("🔨 Build Image", key="btn_build"):
        ts["image_built"] = False
        ts["solve_done"] = False
        ts["test_done"] = False
        ts["test_passed"] = None

        dockerfile_dir = str(task_dir / "environment")
        with st.spinner("Building Docker image..."):
            placeholder = st.empty()
            rc, log = run_command_with_live_output(
                ["docker", "build", "-t", image_name, dockerfile_dir],
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

    # ---- Step 2: Run solve.sh ----
    st.subheader("Step 2 — Run solve.sh")
    build_ready = ts["image_built"]
    if st.button("▶️ Run solve.sh", key="btn_solve", disabled=not build_ready):
        ts["solve_done"] = False
        ts["test_done"] = False
        ts["test_passed"] = None

        # Remove old container if it exists
        subprocess.run(
            ["docker", "rm", "-f", container_name],
            capture_output=True,
        )

        # Create and start container
        with st.spinner("Starting container & running solve.sh ..."):
            # Create container
            subprocess.run(
                [
                    "docker", "create",
                    "--name", container_name,
                    image_name,
                    "sleep", "infinity",
                ],
                capture_output=True,
                text=True,
            )
            # Start container
            subprocess.run(
                ["docker", "start", container_name],
                capture_output=True,
            )

            # Copy files into container
            solve_src = str(task_dir / "solution" / "solve.sh")
            test_sh_src = str(task_dir / "tests" / "test.sh")
            test_py_src = str(task_dir / "tests" / "test_outputs.py")

            subprocess.run(["docker", "cp", solve_src, f"{container_name}:/solution/solve.sh"], capture_output=True)
            subprocess.run(["docker", "exec", container_name, "mkdir", "-p", "/tests"], capture_output=True)
            subprocess.run(["docker", "cp", test_sh_src, f"{container_name}:/tests/test.sh"], capture_output=True)
            subprocess.run(["docker", "cp", test_py_src, f"{container_name}:/tests/test_outputs.py"], capture_output=True)

            # Make scripts executable
            subprocess.run(
                ["docker", "exec", container_name, "chmod", "+x", "/solution/solve.sh", "/tests/test.sh"],
                capture_output=True,
            )

            # Run solve.sh
            placeholder = st.empty()
            rc, log = run_command_with_live_output(
                ["docker", "exec", container_name, "bash", "/solution/solve.sh"],
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

    # ---- Step 3: Run tests ----
    st.subheader("Step 3 — Run test.sh")
    solve_ready = ts["solve_done"]
    if st.button("🧪 Run Tests", key="btn_test", disabled=not solve_ready):
        ts["test_done"] = False
        ts["test_passed"] = None

        with st.spinner("Running test.sh inside container..."):
            placeholder = st.empty()
            rc, log = run_command_with_live_output(
                ["docker", "exec", container_name, "bash", "/tests/test.sh"],
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

    # ---- Cleanup button ----
    st.divider()
    if st.button("🗑️ Cleanup Container", key="btn_cleanup"):
        subprocess.run(["docker", "rm", "-f", container_name], capture_output=True)
        ts["solve_done"] = False
        ts["test_done"] = False
        ts["test_passed"] = None
        ts["solve_log"] = ""
        ts["test_log"] = ""
        st.success(f"Container **{container_name}** removed.")
        st.rerun()

    # ---- Run All button ----
    st.divider()
    st.subheader("⚡ One-Click Run All")
    if st.button("🚀 Build → Solve → Test", key="btn_run_all", type="primary"):
        ts["image_built"] = False
        ts["solve_done"] = False
        ts["test_done"] = False
        ts["test_passed"] = None
        ts["build_log"] = ""
        ts["solve_log"] = ""
        ts["test_log"] = ""

        dockerfile_dir = str(task_dir / "environment")

        # --- Build ---
        st.subheader("Building image…")
        placeholder_build = st.empty()
        rc, log = run_command_with_live_output(
            ["docker", "build", "-t", image_name, dockerfile_dir],
        )
        ts["build_log"] = log
        if rc != 0:
            st.error("Build failed. Aborting.")
            st.stop()
        ts["image_built"] = True
        st.success("Image built ✅")

        # --- Solve ---
        subprocess.run(["docker", "rm", "-f", container_name], capture_output=True)
        subprocess.run(
            ["docker", "create", "--name", container_name, image_name, "sleep", "infinity"],
            capture_output=True, text=True,
        )
        subprocess.run(["docker", "start", container_name], capture_output=True)

        # Copy files
        solve_src = str(task_dir / "solution" / "solve.sh")
        test_sh_src = str(task_dir / "tests" / "test.sh")
        test_py_src = str(task_dir / "tests" / "test_outputs.py")
        subprocess.run(["docker", "cp", solve_src, f"{container_name}:/solution/solve.sh"], capture_output=True)
        subprocess.run(["docker", "exec", container_name, "mkdir", "-p", "/tests"], capture_output=True)
        subprocess.run(["docker", "cp", test_sh_src, f"{container_name}:/tests/test.sh"], capture_output=True)
        subprocess.run(["docker", "cp", test_py_src, f"{container_name}:/tests/test_outputs.py"], capture_output=True)
        subprocess.run(
            ["docker", "exec", container_name, "chmod", "+x", "/solution/solve.sh", "/tests/test.sh"],
            capture_output=True,
        )

        st.subheader("Running solve.sh…")
        placeholder_solve = st.empty()
        rc, log = run_command_with_live_output(
            ["docker", "exec", container_name, "bash", "/solution/solve.sh"],
        )
        ts["solve_log"] = log
        if rc != 0:
            st.error("solve.sh failed. Continuing to tests anyway…")
        else:
            ts["solve_done"] = True
            st.success("solve.sh completed ✅")

        # --- Test ---
        st.subheader("Running test.sh…")
        placeholder_test = st.empty()
        rc, log = run_command_with_live_output(
            ["docker", "exec", container_name, "bash", "/tests/test.sh"],
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
