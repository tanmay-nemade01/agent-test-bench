# Docker Task Runner (Streamlit)

A production-oriented Streamlit application for running data/analytics tasks in isolated Docker containers, executing task solutions, and validating outputs with automated tests.

This project is designed for **task benchmarking and verification workflows** where each task is self-contained (environment, solution script, tests) and should run reproducibly across machines.

---

## Overview

The app provides a guided UI workflow for each task:

1. Select a task from available task folders.
2. Review task instructions and metadata.
3. Build a Docker image from the task's Dockerfile.
4. Run the task solution (`solve.sh`) inside the container.
5. Run verification tests (`test_outputs.py` via `pytest`) and display logs/results directly in Streamlit.

The application includes robust Docker handling for Windows and Linux/macOS environments, real-time command logs, and per-task session state tracking.

---

## Key Features

- **Task discovery from a dedicated `tasks/` directory**
- **Interactive task selection** and instruction rendering
- **Docker image build orchestration** per task
- **Container lifecycle management** (create/start/copy/cleanup)
- **Agentic solve mode** with model selection and API-key based execution
- **Mistral model support** (`mistral-small/medium/large-latest`)
- **Instruction-only container pattern** for agent generation isolation
- **Solution execution** (`/solution/solve.sh`) with live logs
- **Test execution via pytest** (`/tests/test_outputs.py`) with visible output
- **One-click Build → Solve → Test flow**
- **Windows Docker named-pipe fallback logic** for improved reliability
- **UTF-8 subprocess decoding with safe fallback** for resilient log rendering

---

## Project Structure

```text
agent-test-bench/
├─ app.py
├─ README.md
├─ requirements.txt
└─ tasks/
	├─ customer_churn_cohorts/
	│  ├─ instruction.md
	│  ├─ task.toml
	│  ├─ environment/
	│  │  └─ Dockerfile
	│  ├─ solution/
	│  │  └─ solve.sh
	│  └─ tests/
	│     ├─ test.sh
	│     └─ test_outputs.py
	├─ dbt_multicurrency_lifo/
	│  └─ ... (same structure)
	└─ dbt_session_attribution/
		└─ ... (same structure)
```

---

## Task Contract

Each task folder under `tasks/` must include:

- `environment/Dockerfile`
- `solution/solve.sh`
- `tests/test.sh`
- `tests/test_outputs.py`
- (Optional) `instruction.md`, `task.toml`

If any required files are missing, the task is excluded from selection.

---

## Runtime Workflow

### 1) Build Image
Builds a Docker image from:

- `tasks/<task_name>/environment/Dockerfile`

Image naming convention:

- `task-runner-<task_name>`

### 2) Run Solution
Creates and starts a long-lived container, copies scripts into it, then executes:

- `/solution/solve.sh`

Container naming convention:

- `task-container-<task_name>`

### Agentic Solve Path (Optional)

When **Use agentic solver** is enabled in the sidebar:

1. The app creates a **separate container** for instruction access only.
2. Only `instruction.md` is copied into that container.
3. The instruction content is sent to the selected Mistral model.
4. The generated script is saved as a runtime `solve.sh` artifact.
5. Tests validate the outputs produced by this agent-generated solve script.

Instruction-only container naming convention:

- `instruction-only-<task_name>`

### 3) Run Tests
Runs `test_outputs.py` using `pytest` in-container (not plain Python execution), ensuring test functions are actually executed and results are visible in the UI.

---

## Docker Compatibility Notes

The app includes Docker connectivity safeguards:

- Detects daemon availability before running actions.
- Retries with Windows `DOCKER_HOST` named-pipe candidates when needed:
  - `npipe:////./pipe/docker_engine`
  - `npipe:////./pipe/dockerDesktopLinuxEngine`
  - `npipe:////./pipe/dockerDesktopWindowsEngine`
- Provides user-facing diagnostics when Docker Desktop is not running.

---

## Installation

### Prerequisites

- Python 3.10+
- Docker Desktop / Docker Engine
- Access to the `docker` CLI from your shell/terminal

### Setup

Install dependencies from `requirements.txt` and launch Streamlit:

1. Install project dependencies.
2. Start the app with Streamlit.
3. Open the local Streamlit URL displayed in your terminal.

### API Keys (`.env`)

For agent mode, define your Mistral key in project root `.env`:

- `MISTRAL_API_KEY=...`

The app will also let you enter/override the key directly in the sidebar.

---

## Usage Guide

1. Open the app.
2. Select a task in the sidebar.
3. Review instructions in **Instructions** tab.
4. Optionally inspect scripts in **Files** tab.
5. In **Run & Test** tab:
	- Click **Build Image**
	- *(Optional: agent mode)* Click **Generate solve.sh**
	- Click **Run solve.sh**
	- Click **Run test_outputs.py**
6. Review logs in expandable sections.
7. Use **Cleanup Container** when done.

For convenience, use **One-Click Run All** to execute the full flow in sequence.

---

## Error Handling & Observability

The UI surfaces:

- Build logs
- Solution execution logs
- Test output logs
- Pass/fail status indicators
- Docker readiness status and failure hints

This makes debugging task-level failures straightforward without leaving the app.

---

## Extending with New Tasks

To add a new task:

1. Create `tasks/<new_task_name>/`
2. Add required subfolders/files (`environment`, `solution`, `tests`)
3. Provide `instruction.md` and `task.toml` for richer UI metadata
4. Restart or refresh the app

The task will be auto-discovered if the required files are present.

---

## Security & Isolation Considerations

- Task execution happens inside Docker containers for isolation.
- The app does not require direct host execution of task scripts.
- Use trusted task definitions and Dockerfiles in controlled environments.

---

## Typical Use Cases

- Data transformation challenge evaluation
- Reproducible analytics benchmark runs
- Candidate solution verification
- Internal task scoring pipelines with human-readable logs

---