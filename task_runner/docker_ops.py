"""Docker process and connectivity helpers."""

import os
import queue
import subprocess
import threading

import streamlit as st

WINDOWS_DOCKER_HOST_CANDIDATES = [
    "npipe:////./pipe/docker_engine",
    "npipe:////./pipe/dockerDesktopLinuxEngine",
    "npipe:////./pipe/dockerDesktopWindowsEngine",
]


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
        output_queue.put(None)
        output_queue.put(proc.returncode)
    except Exception as exc:
        output_queue.put(f"ERROR: {exc}\\n")
        output_queue.put(None)
        output_queue.put(1)


def run_command_with_live_output(cmd: list[str], placeholder=None, **kwargs) -> tuple[int, str]:
    """Execute cmd, stream output into Streamlit placeholder, return (rc, output)."""
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
        msg = f"{msg}\\n\\nDetails:\\n{details[-1200:]}"
    return False, msg, host


def docker_image_name(task: str) -> str:
    return f"task-runner-{task}".lower().replace(" ", "-")


def docker_container_name(task: str) -> str:
    return f"task-container-{task}".lower().replace(" ", "-")


def instruction_container_name(task: str) -> str:
    return f"instruction-only-{task}".lower().replace(" ", "-")
