#!/usr/bin/env python
"""
Distributed Crawler Worker
Continuously leases tasks from the dashboard and executes Scrapy spiders.
"""

import os
import sys
import time
import json
import socket
import logging
import subprocess
import asyncio
from typing import Optional, Dict, Any

import httpx
from dotenv import load_dotenv

load_dotenv()

# Configuration
DASHBOARD_URL = os.getenv("DASHBOARD_URL", "http://dashboard_service:8000")
WORKER_ID = os.getenv("WORKER_ID", f"worker_{socket.gethostname()}_{os.getpid()}")
POLL_INTERVAL = int(os.getenv("POLL_INTERVAL", "15"))  # seconds
SCRAPY_PROJECT = os.getenv("SCRAPY_PROJECT", "jimmy_crawler")
RUN_ID_FILTER = os.getenv("RUN_ID_FILTER")  # If set, worker only processes this run

# Logging
logging.basicConfig(
    level=logging.INFO,
    format='[%(asctime)s] %(levelname)s [%(name)s] %(message)s'
)
logger = logging.getLogger("crawler_worker")


class CrawlerWorker:
    """
    Worker that leases tasks and runs Scrapy spiders.
    """

    def __init__(self):
        self.worker_id = WORKER_ID
        self.dashboard_url = DASHBOARD_URL
        self.run_id_filter = RUN_ID_FILTER
        self.client = httpx.AsyncClient(timeout=60.0)
        self.running = True

        logger.info(f"Worker initialized: {self.worker_id}")
        logger.info(f"Dashboard URL: {self.dashboard_url}")
        if self.run_id_filter:
            logger.info(f"Run ID filter: {self.run_id_filter} (dedicated worker)")
        else:
            logger.info("No run filter (shared worker pool)")

    async def run(self):
        """
        Main worker loop: lease task -> execute -> complete/fail
        """
        logger.info("Worker started. Polling for tasks...")

        while self.running:
            try:
                # 1. Lease a task
                task = await self.lease_task()

                if not task:
                    # No tasks available, sleep
                    logger.debug(f"No tasks available. Sleeping {POLL_INTERVAL}s...")
                    await asyncio.sleep(POLL_INTERVAL)
                    continue

                # 2. Execute the task with heartbeat
                logger.info(f"Leased task: {task['task_id']} (type: {task['task_type']})")
                logger.info(f"Lease expires at: {task.get('lease_expires_at')}")

                # Get heartbeat interval from response
                heartbeat_interval = task.get('heartbeat_interval', 120)

                success, error_code, error_message = await self.execute_task_with_heartbeat(
                    task, heartbeat_interval
                )

                # 3. Report result
                if success:
                    await self.complete_task(task['task_id'])
                else:
                    await self.fail_task(task['task_id'], error_code, error_message)

            except KeyboardInterrupt:
                logger.info("Worker interrupted. Shutting down...")
                self.running = False
                break

            except Exception as e:
                logger.error(f"Unexpected error in worker loop: {e}", exc_info=True)
                await asyncio.sleep(5)

        await self.client.aclose()
        logger.info("Worker stopped.")

    async def lease_task(self) -> Optional[Dict[str, Any]]:
        """
        Request a task from the dashboard.
        If run_id_filter is set, only lease tasks for that run.
        """
        try:
            body = {"worker_id": self.worker_id}

            # If this is a dedicated worker for a specific run
            if self.run_id_filter:
                body["run_id"] = self.run_id_filter

            response = await self.client.post(
                f"{self.dashboard_url}/api/tasks/lease",
                json=body
            )

            if response.status_code == 204:
                # No tasks available
                # If dedicated worker and no tasks, exit (Job complete)
                if self.run_id_filter:
                    logger.info(f"No tasks for run {self.run_id_filter}, worker exiting")
                    self.running = False
                return None

            response.raise_for_status()
            return response.json()

        except Exception as e:
            logger.error(f"Failed to lease task: {e}")
            return None

    async def execute_task_with_heartbeat(
            self, task: Dict[str, Any], heartbeat_interval: int
    ) -> tuple:
        """
        Execute task and send periodic heartbeats to keep lease alive.
        Returns: (success: bool, error_code: str, error_message: str)
        """
        task_id = task['task_id']

        # Start heartbeat task
        heartbeat_task = asyncio.create_task(
            self._heartbeat_loop(task_id, heartbeat_interval)
        )

        try:
            # Execute spider
            success, error_code, error_message = await self.execute_task(task)
            return success, error_code, error_message
        finally:
            # Stop heartbeat
            heartbeat_task.cancel()
            try:
                await heartbeat_task
            except asyncio.CancelledError:
                pass

    async def _heartbeat_loop(self, task_id: str, interval: int):
        """
        Send heartbeat every `interval` seconds to extend lease.
        """
        try:
            while True:
                await asyncio.sleep(interval)
                try:
                    response = await self.client.post(
                        f"{self.dashboard_url}/api/tasks/{task_id}/heartbeat"
                    )
                    if response.status_code == 200:
                        data = response.json()
                        logger.debug(f"Heartbeat sent for {task_id}, lease extended to {data.get('lease_expires_at')}")
                    else:
                        logger.warning(f"Heartbeat failed for {task_id}: {response.status_code}")
                except Exception as e:
                    logger.error(f"Heartbeat error for {task_id}: {e}")
        except asyncio.CancelledError:
            logger.debug(f"Heartbeat stopped for {task_id}")
            raise

    async def execute_task(self, task: Dict[str, Any]) -> tuple:
        """
        Execute a Scrapy spider with per-task logging.
        Returns: (success: bool, error_code: str, error_message: str)
        """
        task_id = task['task_id']
        task_type = task['task_type']
        payload = task['payload']
        portal_id = payload['portal_id']
        run_id = payload['run_id']

        logger.info(f"Executing task {task_id}: portal={portal_id}, run={run_id}")

        # Create log directory
        log_dir = f"/app/logs/{portal_id}/{run_id}"
        os.makedirs(log_dir, exist_ok=True)
        log_file = f"{log_dir}/{task_id}.log"

        logger.info(f"Logs will be written to: {log_file}")

        try:
            # Build Scrapy command
            spider_name = await self._get_spider_name(portal_id)

            # Serialize payload as JSON argument
            payload_json = json.dumps(payload)

            cmd = [
                "scrapy", "crawl", spider_name,
                "-a", f"task_payload={payload_json}",
                "-a", f"portal_id={portal_id}",
                "-a", f"run_id={run_id}",
                "-a", f"task_id={task_id}",
                "-a", f"task_type={task_type}",
                "--logfile", log_file,
                "--loglevel", "INFO"
            ]

            logger.info(f"Running: {' '.join(cmd[:5])}... (full command in logs)")

            # Execute WITHOUT capture_output so logs go to stdout AND file
            # Use Popen for async execution
            process = await asyncio.create_subprocess_exec(
                *cmd,
                cwd=f"/app/{SCRAPY_PROJECT}",
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE
            )

            # Wait for completion with timeout (2 hours)
            try:
                stdout, stderr = await asyncio.wait_for(
                    process.communicate(),
                    timeout=7200
                )
            except asyncio.TimeoutError:
                process.kill()
                await process.wait()
                logger.error(f"Task {task_id} timed out")
                return False, "timeout", "Spider execution timed out after 2 hours"

            # Log output
            if stdout:
                logger.info(f"STDOUT:\n{stdout.decode('utf-8', errors='ignore')[-1000:]}")
            if stderr:
                logger.error(f"STDERR:\n{stderr.decode('utf-8', errors='ignore')[-1000:]}")

            if process.returncode == 0:
                logger.info(f"Task {task_id} completed successfully")
                logger.info(f"View logs: {log_file}")
                return True, None, None
            else:
                logger.error(f"Task {task_id} failed with return code {process.returncode}")

                # Read last 100 lines of log file for error context
                error_context = ""
                try:
                    with open(log_file, 'r') as f:
                        lines = f.readlines()
                        error_context = ''.join(lines[-100:])
                except Exception:
                    pass

                return False, "spider_error", f"Exit code: {process.returncode}\n{error_context[:500]}"

        except Exception as e:
            logger.error(f"Task {task_id} execution failed: {e}", exc_info=True)
            return False, "execution_error", str(e)[:500]

    async def complete_task(self, task_id: str):
        """
        Mark task as completed.
        """
        try:
            response = await self.client.post(
                f"{self.dashboard_url}/api/tasks/{task_id}/complete",
                json={}
            )
            response.raise_for_status()
            logger.info(f"Task {task_id} marked as completed")

        except Exception as e:
            logger.error(f"Failed to complete task {task_id}: {e}")

    async def fail_task(self, task_id: str, error_code: str, error_message: str):
        """
        Mark task as failed.
        """
        try:
            # Classify if retryable
            retryable = self._is_retryable_error(error_code or "")

            response = await self.client.post(
                f"{self.dashboard_url}/api/tasks/{task_id}/fail",
                json={
                    "error_code": error_code or "unknown_error",
                    "error_message": error_message or "No error message provided",
                    "retryable": retryable
                }
            )
            response.raise_for_status()

            result = response.json()
            logger.info(f"Task {task_id} failed: {result.get('status')}")

        except Exception as e:
            logger.error(f"Failed to report task failure {task_id}: {e}")

    async def _get_spider_name(self, portal_id: str) -> str:
        """
        Fetch spider name from portal config.
        """
        try:
            response = await self.client.get(
                f"{self.dashboard_url}/api/portals/{portal_id}"
            )
            response.raise_for_status()
            portal = response.json()
            return portal.get('spider_name', portal_id)

        except Exception as e:
            logger.error(f"Failed to get spider name for {portal_id}: {e}")
            # Fallback: use portal_id as spider name
            return portal_id

    def _is_retryable_error(self, error_code: str) -> bool:
        """
        Determine if an error should trigger a retry.
        """
        retryable_codes = {
            "timeout", "connection_reset", "dns_error", "429",
            "500", "502", "503", "504", "network_error"
        }
        return any(code in error_code.lower() for code in retryable_codes)


async def main():
    """
    Entry point for the worker.
    """
    worker = CrawlerWorker()
    await worker.run()


if __name__ == "__main__":
    asyncio.run(main())
