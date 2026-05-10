#!/usr/bin/env python3
"""Collect Flink task distribution and subtask ownership via the REST API."""

import argparse
import csv
import logging
import time
from datetime import datetime
from typing import Any, Dict, List, Optional

import requests

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('/tmp/task_distribution_collector.log'),
        logging.StreamHandler(),
    ],
)
logger = logging.getLogger(__name__)


class TaskDistributionCollector:
    def __init__(self, flink_rest_url: str = 'http://localhost:8083'):
        self.flink_rest_url = flink_rest_url.rstrip('/')
        self.rows: List[Dict[str, Any]] = []
        self.start_time = None
        self.taskmanager_by_host: Dict[str, str] = {}
        self.restart_loop_started_at: Optional[float] = None
        self.restart_loop_job_id: Optional[str] = None
        self.vertex_query_retry_after: float = 0.0
        self.last_404_job_id: Optional[str] = None
        logger.info(f"Connected to Flink REST API at {self.flink_rest_url}")

    def query_rest(self, path: str) -> Optional[Dict[str, Any]]:
        try:
            response = requests.get(f"{self.flink_rest_url}{path}", timeout=10)
            response.raise_for_status()
            return response.json()
        except requests.exceptions.HTTPError as exc:
            if exc.response.status_code == 404:
                logger.debug(f"404 Not Found on {path} - job may have restarted")
            else:
                logger.error(f"HTTP error querying {path}: {exc}")
            return None
        except Exception as exc:
            logger.error(f"Error querying Flink REST {path}: {exc}")
            return None

    @staticmethod
    def _extract_list(payload: Optional[Dict[str, Any]], keys: List[str]) -> List[Dict[str, Any]]:
        if not payload:
            return []
        for key in keys:
            value = payload.get(key)
            if isinstance(value, list):
                return value
        return []

    @staticmethod
    def _first_value(data: Dict[str, Any], keys: List[str], default: Any = '') -> Any:
        for key in keys:
            if key in data and data[key] not in (None, ''):
                return data[key]
        return default

    def get_running_job(self, job_name: str = 'CIC Kafka Streaming Job') -> Optional[Dict[str, Any]]:
        """Fetch a RUNNING job - strict, no fallbacks."""
        payload = self.query_rest('/jobs/overview')
        jobs = self._extract_list(payload, ['jobs'])
        if not jobs:
            logger.debug("No jobs found in /jobs/overview, trying /jobs endpoint")
            payload = self.query_rest('/jobs')
            jobs = self._extract_list(payload, ['jobs'])
        if not jobs:
            logger.debug("No jobs available at all")
            return None

        # Filter only RUNNING jobs - strict requirement
        running_jobs = [job for job in jobs if str(job.get('state', '')).upper() == 'RUNNING']
        if not running_jobs:
            other_states = [f"{job.get('name')}({job.get('state')})" for job in jobs[:3]]
            logger.info(f"No RUNNING jobs found. Other jobs: {other_states}")
            return None

        # Prefer job by name
        preferred_jobs = [job for job in running_jobs if job_name.lower() in str(job.get('name', '')).lower()]
        if preferred_jobs:
            job_id = preferred_jobs[0].get('id') or preferred_jobs[0].get('jid')
            logger.debug(f"Found preferred RUNNING job: {job_id}")
            return preferred_jobs[0]
        
        # Use any running job
        job_id = running_jobs[0].get('id') or running_jobs[0].get('jid')
        logger.debug(f"Using RUNNING job (generic): {job_id}")
        return running_jobs[0]

    def get_latest_job_id(self) -> Optional[str]:
        """Always fetch the latest running job ID without caching."""
        job = self.get_running_job()
        if not job:
            logger.warning('No running Flink job found')
            return None

        job_id = str(job.get('jid') or job.get('jobid') or job.get('id') or '')
        if not job_id:
            logger.warning('Could not determine job id from Flink REST response')
            return None
        
        logger.debug(f"Latest job ID: {job_id}")
        return job_id

    def get_job_details(self, job_id: str) -> Optional[Dict[str, Any]]:
        payload = self.query_rest(f'/jobs/{job_id}')
        if payload is None:
            return None
        return payload

    def get_job_state(self, job_id: str) -> str:
        payload = self.get_job_details(job_id)
        if not payload:
            return 'UNKNOWN'
        return str(self._first_value(payload, ['state', 'executionState'], 'UNKNOWN')).upper()

    def handle_restart_state(self, job_id: str, job_state: str) -> bool:
        if job_state != 'RESTARTING':
            self.restart_loop_started_at = None
            self.restart_loop_job_id = None
            return False

        now = time.time()
        if self.restart_loop_job_id != job_id:
            self.restart_loop_job_id = job_id
            self.restart_loop_started_at = now

        elapsed = now - (self.restart_loop_started_at or now)
        if elapsed > 20:
            logger.debug('WARNING restart loop detected')
        return True

    def should_pause_vertex_queries(self, job_id: str) -> bool:
        now = time.time()
        if now < self.vertex_query_retry_after and self.last_404_job_id == job_id:
            return True
        return False

    def note_vertex_404(self, job_id: str, pause_seconds: int = 3) -> None:
        self.last_404_job_id = job_id
        self.vertex_query_retry_after = time.time() + pause_seconds

    def get_vertices(self, job_id: str) -> Optional[List[Dict[str, Any]]]:
        payload = self.query_rest(f'/jobs/{job_id}/vertices')
        if payload is None:
            logger.debug(f"Failed to get vertices for job {job_id}, may need to refresh job ID")
            return None
        return self._extract_list(payload, ['vertices'])

    def refresh_taskmanager_mapping(self) -> None:
        payload = self.query_rest('/taskmanagers')
        taskmanagers = self._extract_list(payload, ['taskmanagers'])
        for taskmanager in taskmanagers:
            taskmanager_id = str(self._first_value(taskmanager, ['id', 'taskmanager-id', 'taskmanagerId'], ''))
            host = str(self._first_value(taskmanager, ['host', 'hostname', 'address', 'path'], ''))
            if host and taskmanager_id:
                self.taskmanager_by_host[host] = taskmanager_id

    def select_vertex(self, vertices: List[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
        if not vertices:
            return None

        preferred_names = ['kafka source', 'kafka-source', 'processing-map', 'kafka-source']
        for vertex in vertices:
            name = str(vertex.get('name', '')).lower()
            if any(token in name for token in preferred_names) and int(vertex.get('parallelism', 0) or 0) >= 2:
                return vertex

        for vertex in vertices:
            if int(vertex.get('parallelism', 0) or 0) >= 2:
                return vertex

        return vertices[0]

    def get_subtasks(self, job_id: str, vertex_id: str) -> List[Dict[str, Any]]:
        payload = self.query_rest(f'/jobs/{job_id}/vertices/{vertex_id}/subtasks')
        return self._extract_list(payload, ['subtasks'])

    def get_subtask_detail(self, job_id: str, vertex_id: str, subtask_index: int) -> Dict[str, Any]:
        payload = self.query_rest(f'/jobs/{job_id}/vertices/{vertex_id}/subtasks/{subtask_index}')
        return payload or {}

    def collect_once(self) -> List[Dict[str, Any]]:
        # Always fetch the latest job ID - no caching
        job_id = self.get_latest_job_id()
        if not job_id:
            logger.warning('No running Flink job found on first attempt')
            return []

        job_state = self.get_job_state(job_id)
        if self.handle_restart_state(job_id, job_state):
            return []

        if self.should_pause_vertex_queries(job_id):
            return []

        # Try to get vertices, with smart retry on 404
        vertices = self.get_vertices(job_id)
        if vertices is None:
            # 404 error - job may be restarting or the REST API is not ready yet
            self.note_vertex_404(job_id, pause_seconds=5)
            return []
        
        if not vertices:
            logger.debug('No vertices found for job')
            return []

        vertex = self.select_vertex(vertices)
        if not vertex:
            logger.warning('No suitable vertex found for task distribution collection')
            return []

        vertex_id = str(vertex.get('id') or vertex.get('vertexid') or vertex.get('vertex_id') or '')
        if not vertex_id:
            logger.warning('Could not determine vertex id from Flink REST response')
            return []

        subtasks = self.get_subtasks(job_id, vertex_id)
        if not subtasks:
            parallelism = int(vertex.get('parallelism', 0) or 0)
            subtasks = [{'subtask': index} for index in range(parallelism)]

        timestamp = datetime.now().isoformat()
        self.refresh_taskmanager_mapping()
        collected_rows: List[Dict[str, Any]] = []
        for subtask in subtasks:
            subtask_index = int(self._first_value(subtask, ['subtask', 'subtask_index', 'subtaskIndex', 'index'], 0))
            detail = subtask
            if not any(key in subtask for key in ['host', 'taskmanager-id', 'taskmanagerId', 'status']):
                detail = self.get_subtask_detail(job_id, vertex_id, subtask_index)

            status = str(self._first_value(detail, ['status', 'state', 'executionState'], 'UNKNOWN'))
            host = str(self._first_value(detail, ['host', 'assignedHost', 'taskmanager-host', 'taskmanagerHost'], ''))
            taskmanager_id = str(self._first_value(detail, ['taskmanager-id', 'taskmanagerId', 'taskmanager_id', 'tmid'], ''))
            if not taskmanager_id and host in self.taskmanager_by_host:
                taskmanager_id = self.taskmanager_by_host[host]
            if not taskmanager_id:
                taskmanager_id = host

            row = {
                'timestamp': timestamp,
                'taskmanager_id': taskmanager_id,
                'subtask_index': subtask_index,
                'host': host,
                'status': status,
            }
            collected_rows.append(row)

            logger.info(
                'timestamp=%s taskmanager_id=%s subtask_index=%s host=%s status=%s',
                timestamp,
                taskmanager_id,
                subtask_index,
                host,
                status,
            )

        return collected_rows

    def start_collection(self, duration: int = 120, interval: int = 1) -> None:
        logger.info(f'Starting task distribution collection for {duration}s with {interval}s interval')
        self.start_time = time.time()
        self.rows = []

        elapsed = 0.0
        while elapsed < duration:
            self.rows.extend(self.collect_once())
            time.sleep(interval)
            elapsed = time.time() - self.start_time

        logger.info(f'Collection complete. Collected {len(self.rows)} task distribution rows')

    def save_csv(self, output_file: str) -> None:
        if not self.rows:
            logger.warning('No task distribution rows to save')
            return

        with open(output_file, 'w', newline='') as file_handle:
            writer = csv.DictWriter(
                file_handle,
                fieldnames=['timestamp', 'taskmanager_id', 'subtask_index', 'host', 'status'],
            )
            writer.writeheader()
            writer.writerows(self.rows)
        logger.info(f'Task distribution saved to {output_file}')


def main() -> None:
    parser = argparse.ArgumentParser(description='Collect Flink task distribution from REST API')
    parser.add_argument('--flink-rest', default='http://localhost:8083', help='Flink REST base URL')
    parser.add_argument('--output', required=True, help='Output CSV file')
    parser.add_argument('--duration', type=int, default=120, help='Collection duration in seconds')
    parser.add_argument('--interval', type=int, default=1, help='Collection interval in seconds')

    args = parser.parse_args()

    collector = TaskDistributionCollector(flink_rest_url=args.flink_rest)
    collector.start_collection(duration=args.duration, interval=args.interval)
    collector.save_csv(args.output)


if __name__ == '__main__':
    main()