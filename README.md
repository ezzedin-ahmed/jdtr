# JDTR - Just a Durable Task Runner

[![Python 3.12+](https://img.shields.io/badge/python-3.12+-blue.svg)](https://www.python.org/downloads/)
[![Tests](https://img.shields.io/badge/tests-passing-brightgreen.svg)]()
[![Coverage](https://img.shields.io/badge/coverage-88%25-green.svg)]()
[![License](https://img.shields.io/badge/license-MIT-blue.svg)]()

A lightweight, persistent workflow engine for Python that enables building complex async workflows with automatic state management, resumability, and type safety.

## Features

- 🔄 **Automatic Resumption** - Workflows resume from the last completed step after crashes or restarts
- 🔒 **Type-Safe** - Type checking between workflow steps
- 💾 **Persistent State** - Uses RocksDB for reliable state storage
- ⚡ **Async-First** - Built on asyncio for high-performance concurrent execution
- 🚀 **FastAPI Integration** - Expose workflows as REST APIs with one line of code
- 🔍 **Progress Tracking** - Monitor workflow execution and debug failures

## Installation

```bash
git clone https://github.com/ezzedin-ahmed/jdtr
pip install -e ./jdtr
```

## Quick Start

```python
from jdtr import Workflow, Database

# Define workflow steps. Each step's output feeds the next step's input.
async def validate_order(order_id: str, amount: float) -> tuple[str, float]:
    if amount <= 0:
        raise ValueError("Invalid amount")
    return (order_id, amount)

async def process_payment(order_id: str, amount: float) -> str:
    # Simulate payment processing
    return f"txn_{order_id}"

async def send_confirmation(txn_id: str) -> str:
    return f"Confirmation sent for {txn_id}"

# Create workflow
db = Database("./workflow_data")
workflow = Workflow(
    workflow_id="order_processing",
    steps=[validate_order, process_payment, send_confirmation],
    db=db,
)

# Resume any runs left unfinished by a previous crash, then start a new run.
await workflow.initialize()
await workflow.run("123", 99.99)
```

## Core Concepts

### Workflows

A workflow is a sequence of async functions (steps) that execute in order. Each step's output becomes the next step's input.

```python
# Single return value → single parameter
async def step1(input: str) -> int:
    return 42

async def step2(value: int) -> str:
    return str(value)

# Tuple return → multiple parameters
async def step3() -> tuple[str, int]:
    return ("hello", 123)

async def step4(message: str, count: int) -> None:
    print(f"{message} x {count}")
```

### Type Safety

The workflow engine validates type compatibility at initialization:

```python
# ✅ Compatible types
async def step_a() -> int:
    return 1

async def step_b(x: int) -> str:
    return str(x)

workflow = Workflow("test", [step_a, step_b], db)  # OK

# ❌ Incompatible types
async def step_c(x: str) -> None:
    pass

workflow = Workflow("test", [step_a, step_c], db)  # Raises TypeError
```

### Persistence & Resumption

Workflows automatically save progress after each step. If your application crashes, workflows resume from the last completed step:

```python
# First run - crashes after step 2
workflow = Workflow("data_pipeline", steps, db)
await workflow.initialize()
await workflow.run(data)  # Completes steps 1, 2, crashes at 3

# Restart application
workflow = Workflow("data_pipeline", steps, db)
await workflow.initialize()  # Automatically resumes from step 3
```

## FastAPI Integration

Expose workflows as REST endpoints:

```python
from fastapi import FastAPI

app = FastAPI()

# Add workflow as a router
router = workflow.as_router(prefix="/api")
app.include_router(router)

# Start server
# uvicorn main:app --reload
```

Trigger workflows via HTTP:

```bash
curl -X POST http://localhost:8000/api/order_processing/ \
  -H "Content-Type: application/json" \
  -d '{"order_id": "123", "amount": 99.99}'
```

## Advanced Usage

### Error Handling

Workflows handle errors gracefully, leaving the run in a resumable state:

```python
async def risky_step(data: str) -> str:
    if not data:
        raise ValueError("Empty data")
    return data.upper()

# If risky_step fails, the run stops and logs the error.
# Progress is saved at the last successful step.
# Calling initialize() again retries from the failed step.
```

A run that keeps failing is retried at most `max_retries` times (default `3`)
before being marked permanently **failed** and skipped by future
`initialize()` calls, so a poison run can't loop forever:

```python
workflow = Workflow("orders", steps, db, max_retries=5)
```

### Multiple Workflows

Run multiple workflows in the same database:

```python
db = Database("./data")

order_wf = Workflow("orders", order_steps, db)
await order_wf.initialize()

inventory_wf = Workflow("inventory", inventory_steps, db)
await inventory_wf.initialize()

# Workflows are isolated by workflow_id
```

### Custom Progress Tracking

Access run state directly:

```python
from jdtr import Run

# Get all unfinished runs for a specific workflow
unfinished = Run.get_unfinished(db, workflow_id="order_processing")

for run in unfinished:
    progress = run.get_progress()
    print(f"Run at step {progress}, attempts={run.get_attempts()}")

# Omit workflow_id to inspect unfinished runs across every workflow
all_unfinished = Run.get_unfinished(db)
```

## Examples

### Data Processing Pipeline

```python
async def load_data(csv_path: str) -> list[dict]:
    # Read CSV file
    return [{"id": 1, "value": 100}, {"id": 2, "value": 200}]

async def transform_data(data: list[dict]) -> list[dict]:
    return [{"id": d["id"], "value": d["value"] * 2} for d in data]

async def save_results(data: list[dict]) -> int:
    # Save data
    return len(data)

pipeline = Workflow("etl_pipeline", [load_data, transform_data, save_results], db)
await pipeline.initialize()
await pipeline.run("data.csv")
```

### Notification System

```python
async def fetch_user(user_id: str, message: str) -> tuple[str, str, str]:
    # Get from database
    email = f"user{user_id}@example.com"
    return (user_id, email, message)

async def send_email(user_id: str, email: str, message: str) -> str:
    # Send via SMTP
    return f"email_sent_{user_id}"

async def log_notification(confirmation: str) -> None:
    print(f"Logged: {confirmation}")

notifier = Workflow("notifications", [fetch_user, send_email, log_notification], db)
```


## Architecture

```
┌─────────────┐
│   FastAPI   │  HTTP endpoints
└──────┬──────┘
       │
┌──────▼──────┐
│  Workflow   │  Step orchestration & type checking
└──────┬──────┘
       │
┌──────▼──────┐
│     Run     │  Progress tracking & state management
└──────┬──────┘
       │
┌──────▼──────┐
│  Database   │  Persistent storage (RocksDB)
└─────────────┘
```

## Requirements

- Python 3.12+
- rocksdict
- pydantic
- fastapi (optional, for HTTP endpoints)

## Execution Semantics

- **At-least-once, not exactly-once.** After a crash a run resumes from the
  last *persisted* step, so a step interrupted mid-execution runs again. Keep
  steps **idempotent**, especially those with side effects (payments, emails).
- **Step outputs must be JSON-serializable** (or pydantic models, which are
  stored via `model_dump`). On resume, pydantic models are restored as plain
  `dict`s — re-validate them inside the step if you need typed models.
- **Single-process locking only.** Resumption is serialized within one process
  via an in-memory lock. Running two processes against the same database can
  execute the same run concurrently; run a single instance per database.

## Limitations

- Steps must be async functions
- RocksDB is single-process (no distributed execution)
- No built-in scheduling or cron support
- Finished-run state is retained (no automatic garbage collection)

## Development

This project uses [uv](https://docs.astral.sh/uv/) for dependency management.

```bash
uv sync                              # install deps (incl. dev tools)
uv run pytest                        # run tests with coverage
uv run ruff check src tests          # lint
uv run ruff format src tests         # format
uv run pyright src                   # type-check
```

CI runs all of the above on Python 3.12 and 3.13 (see `.github/workflows/ci.yml`).

## Contributing

Contributions welcome! Especially enhancing type hinting.

## License

MIT License - see LICENSE file for details

---

Built with ❤️ for async workflows
