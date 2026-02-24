"""
Kenya Law Parallel Coordinator - Orchestrates multiple workers in parallel.

Usage:
  uv run python parallel_coordinator.py              # Run with default 4 workers
  uv run python parallel_coordinator.py --workers 4  # Run with 4 workers
  uv run python parallel_coordinator.py --resume     # Resume from checkpoint
"""

import subprocess
import os
import sys
import json
import time
import argparse
import logging
from concurrent.futures import ProcessPoolExecutor, as_completed
from dataclasses import dataclass
from typing import Optional
import psutil
import fcntl
import signal

# Configuration
DEFAULT_WORKERS = 2
MAX_RETRIES = 3
MEMORY_THRESHOLD = 85  # Pause if memory exceeds this percentage
CHECKPOINT_FILE = "checkpoint.json"

# Task definitions - ordered by priority (most recent first)
TASKS = [
    # 2025: October, November, December (3 tasks)
    {"year": "2025", "month": 10},
    {"year": "2025", "month": 11},
    {"year": "2025", "month": 12},
    # 2024: All months (12 tasks)
    {"year": "2024", "month": 1},
    {"year": "2024", "month": 2},
    {"year": "2024", "month": 3},
    {"year": "2024", "month": 4},
    {"year": "2024", "month": 5},
    {"year": "2024", "month": 6},
    {"year": "2024", "month": 7},
    {"year": "2024", "month": 8},
    {"year": "2024", "month": 9},
    {"year": "2024", "month": 10},
    {"year": "2024", "month": 11},
    {"year": "2024", "month": 12},
    # 2023: All months (12 tasks)
    {"year": "2023", "month": 1},
    {"year": "2023", "month": 2},
    {"year": "2023", "month": 3},
    {"year": "2023", "month": 4},
    {"year": "2023", "month": 5},
    {"year": "2023", "month": 6},
    {"year": "2023", "month": 7},
    {"year": "2023", "month": 8},
    {"year": "2023", "month": 9},
    {"year": "2023", "month": 10},
    {"year": "2023", "month": 11},
    {"year": "2023", "month": 12},
    # 2022: All months (12 tasks)
    {"year": "2022", "month": 1},
    {"year": "2022", "month": 2},
    {"year": "2022", "month": 3},
    {"year": "2022", "month": 4},
    {"year": "2022", "month": 5},
    {"year": "2022", "month": 6},
    {"year": "2022", "month": 7},
    {"year": "2022", "month": 8},
    {"year": "2022", "month": 9},
    {"year": "2022", "month": 10},
    {"year": "2022", "month": 11},
    {"year": "2022", "month": 12},
    # 2021: May through December (8 tasks)
    {"year": "2021", "month": 5},
    {"year": "2021", "month": 6},
    {"year": "2021", "month": 7},
    {"year": "2021", "month": 8},
    {"year": "2021", "month": 9},
    {"year": "2021", "month": 10},
    {"year": "2021", "month": 11},
    {"year": "2021", "month": 12},
    # 2020: December only (1 task)
    {"year": "2020", "month": 12},
]

MONTH_NAMES = {
    1: 'January', 2: 'February', 3: 'March', 4: 'April',
    5: 'May', 6: 'June', 7: 'July', 8: 'August',
    9: 'September', 10: 'October', 11: 'November', 12: 'December'
}

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [COORDINATOR] %(levelname)s: %(message)s",
    handlers=[
        logging.FileHandler("logs/coordinator.log"),
        logging.StreamHandler()
    ]
)

# Track running processes for cleanup
running_processes = []
shutdown_requested = False


def signal_handler(sig, frame):
    """Handle shutdown signals gracefully."""
    global shutdown_requested
    shutdown_requested = True
    logging.info("Shutdown requested, stopping workers...")
    for proc in running_processes:
        try:
            proc.terminate()
        except Exception:
            pass
    sys.exit(0)


signal.signal(signal.SIGINT, signal_handler)
signal.signal(signal.SIGTERM, signal_handler)


@dataclass
class TaskResult:
    year: str
    month: int
    success: bool
    error: Optional[str] = None
    attempts: int = 1


def load_checkpoint() -> dict:
    """Load checkpoint data."""
    if not os.path.exists(CHECKPOINT_FILE):
        return {}
    try:
        with open(CHECKPOINT_FILE, "r") as f:
            fcntl.flock(f, fcntl.LOCK_SH)
            data = json.load(f)
            fcntl.flock(f, fcntl.LOCK_UN)
            return data
    except Exception:
        return {}


def save_checkpoint(data: dict):
    """Save checkpoint data."""
    try:
        with open(CHECKPOINT_FILE, "w") as f:
            fcntl.flock(f, fcntl.LOCK_EX)
            json.dump(data, f, indent=2)
            fcntl.flock(f, fcntl.LOCK_UN)
    except Exception as e:
        logging.warning(f"Failed to save checkpoint: {e}")


def get_task_status(checkpoint: dict, year: str, month: int) -> tuple[str, str]:
    """Get status and start letter for a task."""
    key = f"{year}_{month}"
    if key not in checkpoint:
        return "pending", "a"

    status = checkpoint[key].get("status", "pending")
    letter = checkpoint[key].get("letter", "a")

    if status == "complete" and letter == "z":
        return "complete", "a"

    # If in progress or incomplete, resume from next letter
    if status == "in_progress" or (status == "complete" and letter != "z"):
        next_letter = chr(min(ord(letter) + 1, ord('z')))
        return "incomplete", next_letter

    return "pending", "a"


def check_memory() -> bool:
    """Check if system has enough memory to start new workers."""
    mem = psutil.virtual_memory()
    if mem.percent > MEMORY_THRESHOLD:
        logging.warning(f"Memory at {mem.percent:.1f}%, threshold is {MEMORY_THRESHOLD}%")
        return False
    return True


def run_worker(year: str, month: int, start_letter: str = "a") -> TaskResult:
    """Run a single worker process."""
    global running_processes

    month_name = MONTH_NAMES.get(month, str(month))
    logging.info(f"Starting worker for {year}/{month_name} (from letter '{start_letter}')")

    env = os.environ.copy()
    env["SCRAPER_YEAR"] = year
    env["SCRAPER_MONTH"] = str(month)
    env["SCRAPER_START_LETTER"] = start_letter

    try:
        proc = subprocess.Popen(
            ["python", "worker.py"],
            env=env,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            cwd=os.path.dirname(os.path.abspath(__file__)) or "."
        )
        running_processes.append(proc)

        # Stream output
        for line in proc.stdout:
            # Just log to file, don't print to console (too noisy with 12 workers)
            pass

        proc.wait()
        running_processes.remove(proc)

        if proc.returncode == 0:
            logging.info(f"Worker completed: {year}/{month_name}")
            return TaskResult(year=year, month=month, success=True)
        else:
            logging.error(f"Worker failed: {year}/{month_name} (exit code {proc.returncode})")
            return TaskResult(year=year, month=month, success=False, error=f"Exit code {proc.returncode}")

    except Exception as e:
        logging.error(f"Worker error: {year}/{month_name} - {e}")
        return TaskResult(year=year, month=month, success=False, error=str(e))


def run_worker_with_retry(task: dict, checkpoint: dict, max_retries: int = MAX_RETRIES) -> TaskResult:
    """Run a worker with automatic retry on failure."""
    year = task["year"]
    month = task["month"]

    # Get start letter from checkpoint
    status, start_letter = get_task_status(checkpoint, year, month)

    if status == "complete":
        logging.info(f"Skipping {year}/{month} - already complete")
        return TaskResult(year=year, month=month, success=True)

    for attempt in range(1, max_retries + 1):
        if shutdown_requested:
            return TaskResult(year=year, month=month, success=False, error="Shutdown requested")

        # Wait for memory to be available
        while not check_memory():
            if shutdown_requested:
                return TaskResult(year=year, month=month, success=False, error="Shutdown requested")
            logging.info("Waiting for memory to free up...")
            time.sleep(30)

        result = run_worker(year, month, start_letter)
        result.attempts = attempt

        if result.success:
            return result

        if attempt < max_retries:
            logging.warning(f"Retrying {year}/{month} (attempt {attempt + 1}/{max_retries})")
            time.sleep(5)

    return result


def print_progress(completed: int, total: int, failed: list):
    """Print progress summary."""
    pct = (completed / total) * 100 if total > 0 else 0
    logging.info(f"Progress: {completed}/{total} tasks ({pct:.1f}%)")

    if failed:
        logging.warning(f"Failed tasks: {len(failed)}")
        for task in failed:
            logging.warning(f"  - {task['year']}/{MONTH_NAMES.get(task['month'], task['month'])}")


def main():
    parser = argparse.ArgumentParser(description="Kenya Law Parallel Scraper Coordinator")
    parser.add_argument("--workers", type=int, default=DEFAULT_WORKERS, help=f"Number of parallel workers (default: {DEFAULT_WORKERS})")
    parser.add_argument("--resume", action="store_true", help="Resume from checkpoint")
    args = parser.parse_args()

    num_workers = args.workers

    logging.info("=" * 60)
    logging.info("Kenya Law Parallel Scraper Coordinator")
    logging.info("=" * 60)
    logging.info(f"Workers: {num_workers}")
    logging.info(f"Total tasks: {len(TASKS)}")
    logging.info(f"Memory threshold: {MEMORY_THRESHOLD}%")

    # Load checkpoint
    checkpoint = load_checkpoint() if args.resume else {}
    if checkpoint:
        logging.info(f"Loaded checkpoint with {len(checkpoint)} entries")

    # Filter out completed tasks
    pending_tasks = []
    for task in TASKS:
        status, _ = get_task_status(checkpoint, task["year"], task["month"])
        if status != "complete":
            pending_tasks.append(task)
        else:
            logging.info(f"Skipping {task['year']}/{task['month']} - already complete")

    if not pending_tasks:
        logging.info("All tasks already complete!")
        return

    logging.info(f"Pending tasks: {len(pending_tasks)}")
    logging.info("=" * 60)

    # Check memory before starting
    mem = psutil.virtual_memory()
    logging.info(f"Available memory: {mem.available / (1024**3):.1f} GB ({100 - mem.percent:.1f}% free)")

    completed = 0
    failed = []

    # Use ProcessPoolExecutor for parallel execution
    # Note: We use subprocess inside each worker, so this is really just for scheduling
    with ProcessPoolExecutor(max_workers=num_workers) as executor:
        futures = {
            executor.submit(run_worker_with_retry, task, checkpoint): task
            for task in pending_tasks
        }

        for future in as_completed(futures):
            task = futures[future]
            try:
                result = future.result()
                if result.success:
                    completed += 1
                    logging.info(f"Task completed: {task['year']}/{task['month']} (attempts: {result.attempts})")
                else:
                    failed.append(task)
                    logging.error(f"Task failed: {task['year']}/{task['month']} - {result.error}")
            except Exception as e:
                failed.append(task)
                logging.error(f"Task exception: {task['year']}/{task['month']} - {e}")

            print_progress(completed, len(pending_tasks), failed)

    # Final summary
    logging.info("=" * 60)
    logging.info("FINAL SUMMARY")
    logging.info("=" * 60)
    logging.info(f"Completed: {completed}/{len(pending_tasks)}")
    logging.info(f"Failed: {len(failed)}")

    if failed:
        logging.warning("Failed tasks:")
        for task in failed:
            logging.warning(f"  - {task['year']}/{MONTH_NAMES.get(task['month'], task['month'])}")
        logging.info("Run with --resume to retry failed tasks")


if __name__ == "__main__":
    os.makedirs("logs", exist_ok=True)
    main()
