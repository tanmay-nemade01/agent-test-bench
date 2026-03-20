"""Task execution helpers: container prep and tests command."""

from pathlib import Path

from .docker_ops import docker_run_quiet


def prepare_container(container_name: str, image_name: str, task_dir: Path, solve_script_path: Path | None = None) -> tuple[int, str]:
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

    solve_src = str(solve_script_path if solve_script_path is not None else (task_dir / "solution" / "solve.sh"))
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
    """Execute test_outputs.py through pytest so test functions actually run."""
    return [
        "docker",
        "exec",
        container_name,
        "bash",
        "-lc",
        "pytest -rA -s /tests/test_outputs.py || python3 -m pytest -rA -s /tests/test_outputs.py || python -m pytest -rA -s /tests/test_outputs.py",
    ]
