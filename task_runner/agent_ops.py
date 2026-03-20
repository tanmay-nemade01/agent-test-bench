"""Agentic model helpers (instruction isolation + Mistral generation)."""

import json
import re
import urllib.error
import urllib.request
from pathlib import Path

from .config import AGENT_OUTPUT_DIR
from .docker_ops import docker_run_quiet, instruction_container_name


def _extract_code_block(text: str) -> str:
    """Extract bash code from markdown code fences if present."""
    if "```" not in text:
        return text.strip()
    match = re.search(r"```(?:bash|sh)?\\s*(.*?)```", text, flags=re.DOTALL | re.IGNORECASE)
    if match:
        return match.group(1).strip()
    return text.replace("```", "").strip()


def fetch_instruction_from_isolated_container(task: str, image_name: str, task_dir: Path) -> tuple[int, str]:
    """
    Create a dedicated container that receives only instruction.md,
    then read the instruction content from inside that container.
    """
    instr_container = instruction_container_name(task)
    instruction_path = task_dir / "instruction.md"
    if not instruction_path.exists():
        return 1, "instruction.md not found for selected task."

    docker_run_quiet(["docker", "rm", "-f", instr_container])

    rc, out, _ = docker_run_quiet(["docker", "create", "--name", instr_container, image_name, "sleep", "infinity"])
    if rc != 0:
        return rc, out

    rc, out, _ = docker_run_quiet(["docker", "start", instr_container])
    if rc != 0:
        return rc, out

    rc, out, _ = docker_run_quiet(["docker", "exec", instr_container, "mkdir", "-p", "/project"])
    if rc != 0:
        return rc, out

    rc, out, _ = docker_run_quiet(["docker", "cp", str(instruction_path), f"{instr_container}:/project/instruction.md"])
    if rc != 0:
        return rc, out

    rc, out, _ = docker_run_quiet(["docker", "exec", instr_container, "bash", "-lc", "cat /project/instruction.md"])
    if rc != 0:
        return rc, out

    docker_run_quiet(["docker", "rm", "-f", instr_container])
    return 0, out


def generate_solve_script_with_mistral(task: str, model: str, api_key: str, instruction_text: str) -> tuple[int, str]:
    """Generate solve.sh content from task instructions using Mistral chat completions API."""
    if not api_key:
        return 1, "Mistral API key is required."

    prompt = (
        f"You are solving task '{task}'. Use only the provided instructions to generate a complete /solution/solve.sh script. "
        "Return bash script only with no explanation and no markdown fences. "
        "The script must be executable, deterministic, and should produce outputs required for /tests/test_outputs.py to pass."
    )

    payload = {
        "model": model,
        "messages": [
            {"role": "system", "content": "You are an expert data engineering coding agent. Output only shell script content."},
            {"role": "user", "content": f"Instruction file content:\\n\\n{instruction_text}\\n\\n{prompt}"},
        ],
        "temperature": 0.1,
    }

    request = urllib.request.Request(
        "https://api.mistral.ai/v1/chat/completions",
        data=json.dumps(payload).encode("utf-8"),
        headers={
            "Authorization": f"Bearer {api_key}",
            "Content-Type": "application/json",
        },
        method="POST",
    )

    try:
        with urllib.request.urlopen(request, timeout=120) as response:
            body = response.read().decode("utf-8", errors="replace")
    except urllib.error.HTTPError as exc:
        body = exc.read().decode("utf-8", errors="replace") if exc.fp else str(exc)
        return 1, f"Mistral API HTTP error: {exc.code}\\n{body}"
    except Exception as exc:
        return 1, f"Mistral API request failed: {exc}"

    try:
        parsed = json.loads(body)
        content = parsed["choices"][0]["message"]["content"]
    except Exception as exc:
        return 1, f"Failed to parse Mistral response: {exc}\\nRaw response:\\n{body[:2000]}"

    solve_script = _extract_code_block(content)
    if not solve_script.strip():
        return 1, "Mistral returned an empty script."

    if not solve_script.lstrip().startswith("#!"):
        solve_script = "#!/bin/bash\\nset -e\\n\\n" + solve_script

    return 0, solve_script


def write_generated_solve_script(task: str, script_content: str) -> Path:
    out_dir = AGENT_OUTPUT_DIR / task
    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / "solve.sh"
    out_path.write_text(script_content, encoding="utf-8")
    return out_path
