"""Streamlit UI orchestration for Task Runner."""

import os

import streamlit as st

from .agent_ops import (
    fetch_instruction_from_isolated_container,
    generate_solve_script_with_mistral,
    write_generated_solve_script,
)
from .config import TASK_FOLDERS, TASKS_DIR
from .docker_ops import (
    docker_container_name,
    docker_image_name,
    docker_is_ready,
    docker_run_quiet,
    instruction_container_name,
    run_docker_command_with_fallback,
)
from .execution import prepare_container, test_outputs_command


def render_app():
    """Render the full Streamlit application UI."""
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

        st.subheader("Agentic AI")
        use_agent_solver = st.toggle("Use agentic solver", value=False)
        st.selectbox("Model provider", ["Mistral"], disabled=not use_agent_solver)
        agent_model = st.selectbox(
            "Model",
            ["mistral-small-latest", "mistral-medium-latest", "mistral-large-latest"],
            index=0,
            disabled=not use_agent_solver,
        )
        default_mistral_key = os.environ.get("MISTRAL_API_KEY", "")
        agent_api_key = st.text_input(
            "API Key",
            type="password",
            value=default_mistral_key,
            disabled=not use_agent_solver,
            help="Used only for agentic solution generation.",
        )
        st.caption("Agent mode creates an isolated instruction-only container, then generates solve.sh from instruction.md.")
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
                "agent_generated": False,
                "solve_done": False,
                "test_done": False,
                "agent_log": "",
                "generated_solve_sh": "",
                "build_log": "",
                "solve_log": "",
                "test_log": "",
                "test_passed": None,
            }
        ts = st.session_state[state_key]

        c1, c2, c3 = st.columns(3)
        c1.metric("1. Build Image", "✅" if ts["image_built"] else "⬜")
        c2_label = "2. Agent solve.sh" if use_agent_solver else "2. Run solve.sh"
        c2_value = "✅" if (ts["agent_generated"] if use_agent_solver else ts["solve_done"]) else "⬜"
        c2.metric(c2_label, c2_value)
        c3.metric("3. Run tests", "✅ Passed" if ts["test_passed"] is True else ("❌ Failed" if ts["test_passed"] is False else "⬜"))

        st.divider()
        st.subheader("Step 1 — Build Docker Image")
        if st.button("🔨 Build Image", key="btn_build", disabled=not docker_ok):
            ts["image_built"] = False
            ts["agent_generated"] = False
            ts["solve_done"] = False
            ts["test_done"] = False
            ts["agent_log"] = ""
            ts["generated_solve_sh"] = ""
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

        if use_agent_solver:
            st.subheader("Step 2 — Generate solve.sh with Agent")
            if st.button("🤖 Generate solve.sh", key="btn_agent_generate", disabled=not (ts["image_built"] and docker_ok)):
                ts["agent_generated"] = False
                ts["solve_done"] = False
                ts["test_done"] = False
                ts["test_passed"] = None
                ts["agent_log"] = ""
                ts["generated_solve_sh"] = ""

                if not agent_api_key or agent_api_key == "your_mistral_api_key_here":
                    st.error("Please provide a valid Mistral API key in the sidebar.")
                    st.stop()

                with st.spinner("Preparing isolated instruction container..."):
                    rc, instruction_or_error = fetch_instruction_from_isolated_container(selected_task, image_name, task_dir)
                    if rc != 0:
                        ts["agent_log"] = instruction_or_error
                        st.error("Failed to read instruction.md from isolated container.")
                        st.rerun()

                with st.spinner("Generating solve.sh with Mistral..."):
                    rc, generated_or_error = generate_solve_script_with_mistral(
                        selected_task,
                        agent_model,
                        agent_api_key,
                        instruction_or_error,
                    )
                    if rc != 0:
                        ts["agent_log"] = generated_or_error
                        st.error("Agent generation failed. See log below.")
                        st.rerun()

                    ts["generated_solve_sh"] = generated_or_error
                    ts["agent_generated"] = True
                    ts["agent_log"] = "Agent solve.sh generated successfully."
                    st.success("Agent generated solve.sh successfully.")
                    st.rerun()

            if ts["agent_log"]:
                with st.expander("Agent log", expanded=False):
                    st.code(ts["agent_log"][-6000:], language="text")

            if ts["generated_solve_sh"]:
                with st.expander("Generated solve.sh preview", expanded=False):
                    st.code(ts["generated_solve_sh"][-8000:], language="bash")

        run_solve_disabled = not (ts["image_built"] and docker_ok and (ts["agent_generated"] if use_agent_solver else True))
        st.subheader("Step 3 — Run solve.sh" if use_agent_solver else "Step 2 — Run solve.sh")
        if st.button("▶️ Run solve.sh", key="btn_solve", disabled=run_solve_disabled):
            ts["solve_done"] = False
            ts["test_done"] = False
            ts["test_passed"] = None

            with st.spinner("Starting container & running solve.sh ..."):
                solve_script_path = None
                if use_agent_solver:
                    solve_script_path = write_generated_solve_script(selected_task, ts["generated_solve_sh"])

                rc, prep_out = prepare_container(container_name, image_name, task_dir, solve_script_path=solve_script_path)
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

        st.subheader("Step 4 — Run test_outputs.py" if use_agent_solver else "Step 3 — Run test_outputs.py")
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
            docker_run_quiet(["docker", "rm", "-f", instruction_container_name(selected_task)])
            ts["agent_generated"] = False
            ts["solve_done"] = False
            ts["test_done"] = False
            ts["test_passed"] = None
            ts["agent_log"] = ""
            ts["generated_solve_sh"] = ""
            ts["solve_log"] = ""
            ts["test_log"] = ""
            st.success(f"Container **{container_name}** removed.")
            st.rerun()

        st.divider()
        st.subheader("⚡ One-Click Run All")
        if st.button("🚀 Build → Solve → Test", key="btn_run_all", type="primary", disabled=not docker_ok):
            ts["image_built"] = False
            ts["agent_generated"] = False
            ts["solve_done"] = False
            ts["test_done"] = False
            ts["test_passed"] = None
            ts["agent_log"] = ""
            ts["generated_solve_sh"] = ""
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

            solve_script_path = None
            if use_agent_solver:
                if not agent_api_key or agent_api_key == "your_mistral_api_key_here":
                    st.error("Please provide a valid Mistral API key in the sidebar.")
                    st.stop()

                st.subheader("Generating solve.sh with agent…")
                rc, instruction_or_error = fetch_instruction_from_isolated_container(selected_task, image_name, task_dir)
                if rc != 0:
                    ts["agent_log"] = instruction_or_error
                    st.error("Failed to read instruction.md from isolated container. Aborting.")
                    st.stop()

                rc, generated_or_error = generate_solve_script_with_mistral(
                    selected_task,
                    agent_model,
                    agent_api_key,
                    instruction_or_error,
                )
                if rc != 0:
                    ts["agent_log"] = generated_or_error
                    st.error("Agent generation failed. Aborting.")
                    st.stop()

                ts["generated_solve_sh"] = generated_or_error
                ts["agent_generated"] = True
                ts["agent_log"] = "Agent solve.sh generated successfully."
                solve_script_path = write_generated_solve_script(selected_task, ts["generated_solve_sh"])

            rc, prep_out = prepare_container(container_name, image_name, task_dir, solve_script_path=solve_script_path)
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
