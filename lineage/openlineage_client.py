"""
lineage/openlineage_client.py

Reusable OpenLineage event emitter for the Tax Analytics Platform.
POSTs RunEvent JSON to the Marquez API.

Usage (from any job wrapper or script):
    from lineage.openlineage_client import LineageClient
    from lineage.lineage_schemas import kafka_dataset, minio_dataset, pg_dataset

    client = LineageClient()
    run_id = client.start(
        job_name="job_01_raw_to_minio",
        namespace="flink",
        inputs=[kafka_dataset("tax-applications")],
        outputs=[minio_dataset("raw-tax", "/date=2024-01-01/")],
    )
    # ... do work ...
    client.complete(run_id, job_name="job_01_raw_to_minio", namespace="flink")
    # or on failure:
    client.fail(run_id, job_name="job_01_raw_to_minio", namespace="flink", error=str(e))
"""

import logging
import os
import uuid
from datetime import datetime, timezone
from typing import Optional

import requests
from dotenv import load_dotenv

load_dotenv()

log = logging.getLogger(__name__)

MARQUEZ_URL  = os.getenv("MARQUEZ_API_URL", "http://marquez:5000")
PRODUCER_URL  = f"{MARQUEZ_URL}/api/v1/lineage"
NAMESPACES_URL = f"{MARQUEZ_URL}/api/v1/namespaces"
PLATFORM     = "tax-analytics-platform"


class LineageClient:
    """
    Thin wrapper around the OpenLineage HTTP transport.
    Builds and POSTs RunEvent payloads to Marquez.

    All methods are fire-and-forget — errors are logged but never raised
    so that a Marquez outage never breaks the data pipeline.
    """

    def __init__(self, producer_url: str = PRODUCER_URL):
        self.url = producer_url

    # ── Public API ────────────────────────────────────────────────────────────

    def start(
        self,
        job_name: str,
        namespace: str,
        inputs:  list[dict] | None = None,
        outputs: list[dict] | None = None,
        run_id:  str | None = None,
    ) -> str:
        """
        Emit a START event. Returns the run_id so it can be passed to
        complete() or fail().
        """
        run_id = run_id or str(uuid.uuid4())
        self._emit("START", run_id, job_name, namespace, inputs or [], outputs or [])
        return run_id

    def complete(
        self,
        run_id:  str,
        job_name: str,
        namespace: str,
        inputs:  list[dict] | None = None,
        outputs: list[dict] | None = None,
    ) -> None:
        """Emit a COMPLETE event for an existing run_id."""
        self._emit("COMPLETE", run_id, job_name, namespace, inputs or [], outputs or [])

    def fail(
        self,
        run_id:   str,
        job_name: str,
        namespace: str,
        error:    str = "",
        inputs:  list[dict] | None = None,
        outputs: list[dict] | None = None,
    ) -> None:
        """Emit a FAIL event for an existing run_id."""
        self._emit(
            "FAIL", run_id, job_name, namespace,
            inputs or [], outputs or [],
            error_message=error,
        )

    # ── Internal ──────────────────────────────────────────────────────────────

    def _emit(
        self,
        event_type:    str,
        run_id:        str,
        job_name:      str,
        namespace:     str,
        inputs:        list[dict],
        outputs:       list[dict],
        error_message: str = "",
    ) -> None:
        """Build and POST an OpenLineage RunEvent."""
        payload = {
            "eventType": event_type,
            "eventTime": datetime.now(timezone.utc).isoformat(),
            "run": {
                "runId": run_id,
                "facets": {
                    "errorMessage": {"message": error_message} if error_message else {},
                },
            },
            "job": {
                "namespace": namespace,
                "name":      job_name,
                "facets": {
                    "documentation": {
                        "_producer": PLATFORM,
                        "_schemaURL": "https://openlineage.io/spec/facets/1-0-0/DocumentationJobFacet.json",
                        "description": f"{namespace}/{job_name}",
                    }
                },
            },
            "inputs":  inputs,
            "outputs": outputs,
            "producer": PLATFORM,
            "schemaURL": "https://openlineage.io/spec/2-0-2/OpenLineage.json",
        }

        try:
            resp = requests.post(
                self.url,
                json=payload,
                timeout=5,
                headers={"Content-Type": "application/json"},
            )
            if resp.status_code not in (200, 201, 204):
                log.warning(
                    "Lineage emit returned HTTP %d for %s/%s [%s]",
                    resp.status_code, namespace, job_name, event_type,
                )
            else:
                log.debug(
                    "Lineage %s → %s/%s (run=%s)",
                    event_type, namespace, job_name, run_id[:8],
                )
        except requests.exceptions.ConnectionError:
            log.warning("Marquez not reachable — lineage event dropped (%s/%s)", namespace, job_name)
        except Exception as exc:
            log.warning("Lineage emit error: %s", exc)
