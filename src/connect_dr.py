#!/usr/bin/env python3
"""Confluent Cloud — Disaster Recovery Utility for Managed Connectors.

Run coordinated disaster-recovery (DR) operations against many Confluent
Cloud managed connectors with a single command.

ACTIONS
-------
failover
    Move connectors from the SOURCE Kafka cluster to the DR Kafka cluster.
    Optional per-connector configuration overrides (for example
    region-specific bucket names or JDBC URLs) can be supplied via
    --config-overrides-file. After a successful failover the connector
    is running in the DR cluster.

failback
    Reverse a previous failover: move connectors from the DR Kafka cluster
    back to the SOURCE Kafka cluster.

status
    Read-only. For every connector in the SOURCE Kafka cluster (or the
    subset named via --connectors), calls the per-connector DR status
    endpoint and prints the active region, active Kafka cluster, DR
    connector ID, and any in-flight DR operation. Connectors that have
    never been part of a DR operation are reported as "no DR state"
    rather than as errors. --dr-environment, --dr-kafka-cluster-id,
    --config-overrides-file and --dry-run are ignored in this mode.

--dry-run
    Sends `?dry_run=true` with each failover/failback request. Confluent
    Cloud runs every eligibility check and reports what would happen, but
    no connector is modified. Recommended before any real failover.
    Ignored by `status`.

AUTHENTICATION
--------------
HTTP Basic against https://api.confluent.cloud. The Cloud API key is the
username and the Cloud API secret is the password. Both are supplied at
run time via --secret-key and --secret-value.

QUICK START
-----------
Preview a failover (eligibility checks only, no changes made)::

    python connect_dr.py failover \\
        --environment           env-abc123 \\
        --kafka-cluster-id      lkc-source \\
        --dr-environment        env-abc123 \\
        --dr-kafka-cluster-id   lkc-dr \\
        --secret-key            "$CCLOUD_API_KEY" \\
        --secret-value          "$CCLOUD_API_SECRET" \\
        --dry-run

Execute the failover (drop --dry-run when you are ready)::

    python connect_dr.py failover \\
        --environment           env-abc123 \\
        --kafka-cluster-id      lkc-source \\
        --dr-environment        env-abc123 \\
        --dr-kafka-cluster-id   lkc-dr \\
        --secret-key            "$CCLOUD_API_KEY" \\
        --secret-value          "$CCLOUD_API_SECRET"

Check the DR status of every connector in a Kafka cluster::

    python connect_dr.py status \\
        --environment       env-abc123 \\
        --kafka-cluster-id  lkc-source \\
        --secret-key        "$CCLOUD_API_KEY" \\
        --secret-value      "$CCLOUD_API_SECRET"

Run `python connect_dr.py --help` for the full flag reference.

OUTPUT
------
For failover and failback, the script prints a table — one row per connector:

    NAME                             PRIMARY ID           DR ID                RESULT    DETAIL
    --------------------------------------------------------------------------------------------------
    my-s3-sink-prod                  lcc-abc12            lcc-xyz98            SUCCESS
    my-jdbc-source                   lcc-def34            -                    FAILED    Connector is paused...

For `status`, the script prints a table — one row per connector:

    NAME                             ID                   STATE        ACTIVE LKC           DR
    --------------------------------------------------------------------------------------------------
    my-s3-sink-prod                  lcc-xyz98            STOPPED      lkc-dr-region        Active in DR
    my-jdbc-source                   lcc-def34            RUNNING      -                    No DR
    my-broken-conn                   lcc-aaaa             FAILED       -                    ERROR (HTTP 500): server error

A summary block is printed at the end with totals (and, for failures or
errors, every field returned by the API on its own line).

Every run also streams a per-connector CSV to disk. The path is
generated automatically as `dr_<action>_<UTC-timestamp>.csv` in the
current working directory — for example, `dr_failover_20260529T143012Z.csv`.
The absolute path is printed in the pre-run confirmation summary and
again when streaming starts, so it can be copy-pasted from the terminal.
Rows are flushed to disk as each connector finishes, so a mid-run crash,
Ctrl-C, or kill still leaves the rows that already completed on disk.
Consecutive runs never overwrite each other because every file has its
own timestamp.
"""

from __future__ import annotations

import argparse
import csv
import json
import os
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional
from urllib.parse import quote

try:
    import requests
    from requests.auth import HTTPBasicAuth
except ImportError:
    sys.stderr.write(
        "error: the 'requests' library is required but not installed.\n"
        "       Install it with:  pip install requests\n"
    )
    sys.exit(1)


# =============================================================================
#  Constants and configuration
# =============================================================================

# Confluent Cloud public API base URL. Used for both connector discovery and
# the DR endpoints.
API_BASE_URL = "https://api.confluent.cloud"

# Connectors are processed in strict, non-overlapping batches of BATCH_SIZE:
# all requests in a batch are dispatched in parallel, and the next batch is
# not submitted until every request in the current batch has returned. This
# bounds in-flight load on the Confluent Cloud API and provides a clear
# "batch N of M" progress signal in the output.
BATCH_SIZE = 2

# Per-request HTTP timeout. A single DR operation is a multi-step
# orchestration server-side, so up to 5 minutes per call is allowed.
REQUEST_TIMEOUT_SECONDS = 5 * 60


def _path(segment: str) -> str:
    """Percent-encode a single URL path segment.

    `safe=""` means even `/` is escaped, which is what we want — connector
    names, env IDs, and cluster IDs are single path segments and must never
    introduce additional path components into the URL.
    """
    return quote(segment, safe="")


# =============================================================================
#  Errors
# =============================================================================


class CLIError(Exception):
    """Raised for clean, user-facing CLI errors.

    The message is printed to stderr without a Python traceback and the
    process exits with code 1. Use this for any failure the user can act on
    (bad credentials, missing file, malformed JSON, unreachable API, etc.).
    """


# =============================================================================
#  Result types
# =============================================================================


@dataclass
class ConnectorOutcome:
    name: str
    status: str  # "SUCCESS" or "FAILED"
    http_status: Optional[int] = None
    primary_connector_id: str = ""
    dr_connector_id: str = ""
    error_message: str = ""
    duration_seconds: float = 0.0


@dataclass
class RunSummary:
    """Aggregate of every per-connector outcome across the run."""

    action: str
    total: int = 0
    succeeded: int = 0
    failed: int = 0
    outcomes: List[ConnectorOutcome] = field(default_factory=list)


@dataclass
class ConnectorStatusOutcome:
    name: str
    outcome_kind: str  # "HAS_DR_STATE" | "NO_DR_STATE" | "ERROR"
    http_status: Optional[int] = None
    connector_id: str = ""
    connector_type: str = ""
    current_state: str = ""
    active_region: str = ""
    active_lkc_id: str = ""
    dr_connector_id: str = ""
    dr_operation_state: str = ""
    dr_operation_started_at: str = ""
    error_message: str = ""
    duration_seconds: float = 0.0


@dataclass
class DiscoveredConnector:
    name: str
    connector_id: str = ""
    state: str = ""


@dataclass
class StatusRunSummary:
    """Aggregate of every per-connector status outcome across the run."""

    total: int = 0
    has_dr_state: int = 0
    no_dr_state: int = 0
    errored: int = 0
    # Region tallies (over connectors with HAS_DR_STATE only).
    by_active_region: Dict[str, int] = field(default_factory=dict)
    # In-flight operation tallies (over connectors with HAS_DR_STATE and a
    # non-empty dr_operation_state).
    by_operation_state: Dict[str, int] = field(default_factory=dict)
    outcomes: List[ConnectorStatusOutcome] = field(default_factory=list)


# =============================================================================
#  Command-line interface
# =============================================================================


def parse_args() -> argparse.Namespace:
    # add_help=False places -h/--help into its own "flags" group at the bottom
    # of the help output, keeping script-specific arguments grouped neatly
    # under "required arguments" / "optional arguments".
    parser = argparse.ArgumentParser(
        prog="connect_dr.py",
        # Override the default usage line so the action (failover/failback/
        # status) appears first, matching how the script is actually invoked.
        usage=(
            "python connect_dr.py {failover,failback,status}\n"
            "                --environment ENV_ID --kafka-cluster-id LKC_ID\n"
            "                [--dr-environment ENV_ID --dr-kafka-cluster-id LKC_ID]\n"
            "                --secret-key API_KEY --secret-value API_SECRET\n"
            "                [--connectors NAMES] [--config-overrides-file PATH]\n"
            "                [--dry-run] [-h]\n\n"
            "  --dr-environment / --dr-kafka-cluster-id are required for\n"
            "  failover and failback; they are ignored by status."
        ),
        description=(
            "Run a Confluent Cloud disaster-recovery failover, failback, or "
            "read-only status check across many managed connectors."
        ),
        formatter_class=argparse.RawDescriptionHelpFormatter,
        add_help=False,
    )

    parser.add_argument(
        "action", choices=("failover", "failback", "status"),
        help=(
            "Operation to perform on each connector: 'failover' or 'failback' "
            "(both mutating), or 'status' (read-only DR state query)."
        ),
    )

    required = parser.add_argument_group("required arguments")
    required.add_argument(
        "--environment", required=True, metavar="ENV_ID",
        help=(
            "Source environment ID. failover moves connectors OUT of it, "
            "failback returns them TO it, and status queries are read from it."
        ),
    )
    required.add_argument(
        "--kafka-cluster-id", required=True, metavar="LKC_ID",
        help=(
            "Source Kafka cluster ID. failover moves connectors OUT of it, "
            "failback returns them TO it, and status queries are read from it."
        ),
    )
    # --dr-environment / --dr-kafka-cluster-id are required for failover/failback only;
    # the read-only status query is keyed by source env+lkc+connector and doesn't need
    # them. They're parsed as optional here and enforced post-parse so the help output
    # still groups them with the other required args.
    required.add_argument(
        "--dr-environment", default=None, metavar="ENV_ID",
        help="Target DR environment. Required for failover/failback. Ignored for status.",
    )
    required.add_argument(
        "--dr-kafka-cluster-id", default=None, metavar="LKC_ID",
        help="Target DR Kafka cluster. Required for failover/failback. Ignored for status.",
    )
    required.add_argument(
        "--secret-key", required=True, metavar="API_KEY",
        help="Confluent Cloud API key.",
    )
    required.add_argument(
        "--secret-value", required=True, metavar="API_SECRET",
        help="Confluent Cloud API secret.",
    )

    optional = parser.add_argument_group("optional arguments")
    optional.add_argument(
        "--connectors", default="", metavar="my-s3-sink,my-jdbc-source",
        help=(
            "Comma-separated connector names. If omitted, every connector in "
            "the source Kafka cluster is auto-discovered (any state). Custom "
            "connectors (non-`lcc-` IDs) are always skipped."
        ),
    )
    optional.add_argument(
        "--config-overrides-file", default=None, metavar="PATH",
        help=(
            'JSON file with per-connector overrides for failover. Example: '
            '{"my-s3-sink": {"s3.bucket.name": "dr-bucket"}, '
            '"my-jdbc-source": {"connection.url": "jdbc:postgresql://dr-db:5432/orders"}}. '
            "Ignored for failback."
        ),
    )
    optional.add_argument(
        "--dry-run", action="store_true",
        help="Preview only — runs eligibility checks but does not move any connector.",
    )

    flags = parser.add_argument_group("flags")
    flags.add_argument(
        "-h", "--help", action="help",
        help="Show this help message and exit.",
    )
    args = parser.parse_args()

    # Enforce conditional requirements that argparse can't express via
    # `required=True` alone: failover/failback need the DR target; status
    # doesn't. parser.error() prints usage and exits with code 2.
    if args.action in ("failover", "failback"):
        missing = []
        if not args.dr_environment:
            missing.append("--dr-environment")
        if not args.dr_kafka_cluster_id:
            missing.append("--dr-kafka-cluster-id")
        if missing:
            parser.error(
                f"the following arguments are required for action '{args.action}': "
                + ", ".join(missing)
            )
    elif args.action == "status":
        # Surface flags that are silently ignored by `status` so they can be
        # removed from runbooks.
        ignored = []
        if args.dr_environment:
            ignored.append("--dr-environment")
        if args.dr_kafka_cluster_id:
            ignored.append("--dr-kafka-cluster-id")
        if args.config_overrides_file:
            ignored.append("--config-overrides-file")
        if args.dry_run:
            ignored.append("--dry-run")
        if ignored:
            print(
                "  warning: the following flags are ignored for action 'status': "
                + ", ".join(ignored),
                file=sys.stderr,
                )
    return args


# =============================================================================
#  Config-overrides file
# =============================================================================


def load_config_overrides(path: Optional[str]) -> Dict[str, Dict[str, str]]:
    """Load and validate the optional --config-overrides-file payload.

    The file must be a JSON object whose keys are connector names and whose
    values are flat string→string maps. Returns an empty dict when no path is
    supplied so callers don't have to special-case "no file".

    Raises CLIError on any I/O failure, JSON-decoding failure, or schema
    violation so the caller can surface a clean error message.
    """
    if not path:
        return {}

    try:
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)
    except FileNotFoundError as e:
        raise CLIError(f"--config-overrides-file: file not found: {path}") from e
    except OSError as e:
        raise CLIError(f"--config-overrides-file: cannot read {path}: {e}") from e
    except json.JSONDecodeError as e:
        raise CLIError(f"--config-overrides-file: invalid JSON in {path}: {e}") from e

    if not isinstance(data, dict):
        raise CLIError(
            "--config-overrides-file must contain a JSON object at the top level"
        )
    for connector_name, overrides in data.items():
        if not isinstance(overrides, dict):
            raise CLIError(
                f"--config-overrides-file: entry for {connector_name!r} must be a JSON object"
            )
        for k, v in overrides.items():
            if not isinstance(k, str) or not isinstance(v, str):
                raise CLIError(
                    f"--config-overrides-file: entry for {connector_name!r} must contain "
                    f"only string keys and string values (got {k!r} -> {v!r})"
                )
    return data


# =============================================================================
#  Confluent Cloud HTTP client
# =============================================================================


def build_session(api_key: str, api_secret: str) -> requests.Session:
    """Build an authenticated `requests.Session` for Confluent Cloud.

    Authentication: HTTP Basic. The Cloud API key is used as the username and
    the Cloud API secret is used as the password. `requests` sets the
    `Authorization: Basic <base64(key:secret)>` header automatically; the raw
    credentials are stored only on the Session object and discarded when the
    process exits. They are not printed, logged, or written to any file.
    """
    session = requests.Session()
    session.auth = HTTPBasicAuth(api_key, api_secret)
    session.headers.update(
        {
            "Content-Type": "application/json",
            "Accept": "application/json",
        }
    )
    return session


def list_active_connectors(
        session: requests.Session,
        env_id: str,
        kafka_cluster_id: str,
        exclude_custom_connectors: bool = False,
) -> List[DiscoveredConnector]:
    """Discover connectors in the source Kafka cluster.

    Calls the Confluent Cloud "List of Connectors with Expansions" endpoint:

        GET /connect/v1/environments/{env}/clusters/{lkc}/connectors?expand=status,id

    Reference:
        https://docs.confluent.io/cloud/current/ccloud/list-connectv-1-connectors-with-expansions/

    The unexpanded variant returns only connector names — useless here since we
    need each connector's runtime state and ID to populate the discovery rows
    and to detect custom connectors. The expanded variant returns an object
    keyed by connector name, where each value carries:
      - `status.connector.state` — runtime state, surfaced in the result rows
                                   so the operator can see what was acted on.
      - `id.id`                  — used to detect custom connectors. Regular
                                   managed connectors have IDs prefixed
                                   `lcc-`; custom-connector IDs use a
                                   different prefix (e.g. `clcc-`). Custom
                                   connectors cannot be failed over (their
                                   plugin artifact and runtime are uploaded
                                   per-region), so when discovering
                                   connectors for a failover or failback we
                                   skip them up-front instead of letting the
                                   per-connector API call reject them later.
    The endpoint is not paginated; every connector in the cluster is returned
    in a single response.

    When `exclude_custom_connectors=True`, any connector whose `id.id` does
    not start with `lcc-` is dropped from the returned list with a clear
    stderr message. The default is False (no filter), but every caller in
    this script passes True — custom connectors are never relevant for DR
    actions (their plugin artifacts are region-scoped).

    No state filtering is performed. Connectors in any state are returned —
    failback callers need post-failover STOPPED connectors, status callers
    want DR state for STOPPED/PAUSED connectors too, and failover lets the
    server-side DR API reject ineligible ones with a clean per-connector
    error (which is more informative than a client-side prefilter).

    Returns the sorted list of connectors, with each connector's `id.id` and
    runtime state populated from the response. Any connector skipped by the
    custom-connector filter is reported on stderr so the caller can see
    exactly what was excluded and why.

    Raises CLIError on HTTP/transport failure or an unexpected response shape
    so the caller can surface a clean error message instead of a traceback.
    """
    url = (
        f"{API_BASE_URL}/connect/v1/environments/{_path(env_id)}"
        f"/clusters/{_path(kafka_cluster_id)}/connectors"
    )
    try:
        resp = session.get(
            url, params={"expand": "status,id"}, timeout=REQUEST_TIMEOUT_SECONDS
        )
    except requests.RequestException as e:
        raise CLIError(
            f"failed to list connectors in cluster {kafka_cluster_id} "
            f"(environment {env_id}): {e}"
        ) from e

    if not 200 <= resp.status_code < 300:
        raise CLIError(
            f"failed to list connectors in cluster {kafka_cluster_id} "
            f"(environment {env_id}): HTTP {resp.status_code} "
            f"{extract_error_text(resp)}"
        )

    try:
        payload = resp.json()
    except ValueError as e:
        raise CLIError(
            f"failed to parse list-connectors response from cluster "
            f"{kafka_cluster_id} (environment {env_id}): {e}"
        ) from e

    if not isinstance(payload, dict):
        raise CLIError(
            "Unexpected list-connectors response shape: expected a JSON object "
            f"keyed by connector name, got {type(payload).__name__}. The request "
            "must include expand=status,id."
        )

    active: List[DiscoveredConnector] = []
    for connector_name, expansion in payload.items():
        state = ""
        connector_id = ""
        if isinstance(expansion, dict):
            status_obj = expansion.get("status")
            if isinstance(status_obj, dict):
                connector_obj = status_obj.get("connector")
                if isinstance(connector_obj, dict):
                    state = connector_obj.get("state", "") or ""
            # The `id` expansion is documented as {"id": "...", "id_type": "..."}.
            # Guard against any other shape (string, list, missing) so the script
            # cannot crash on an unexpected API response.
            id_obj = expansion.get("id")
            if isinstance(id_obj, dict):
                connector_id = id_obj.get("id", "") or ""

        # Custom-connector filter: skip when the caller asked us to AND the
        # ID is present and clearly non-managed (anything not prefixed
        # `lcc-`). An empty / missing ID is not enough to skip — we keep the
        # connector in the returned list so an incomplete API response does
        # not silently drop work that the operator expects to see.
        if (
                exclude_custom_connectors
                and connector_id
                and not connector_id.startswith("lcc-")
        ):
            print(
                f"  skip {connector_name}: appears to be a custom connector "
                f"(id={connector_id}; expected `lcc-` prefix). Custom "
                f"connectors cannot be failed over.",
                file=sys.stderr,
            )
            continue

        active.append(DiscoveredConnector(
            name=connector_name,
            connector_id=connector_id,
            state=state,
        ))
    active.sort(key=lambda d: d.name)
    return active


def call_dr_endpoint(
        session: requests.Session,
        action: str,
        env_id: str,
        kafka_cluster_id: str,
        connector_name: str,
        body: Dict[str, Any],
        dry_run: bool,
) -> requests.Response:
    """POST the per-connector DR API and return the raw Response.

    The response is returned without raising on non-2xx — the caller inspects
    the status code and the body so per-connector failures can be captured in
    the run summary instead of aborting the whole batch.
    """
    url = (
        f"{API_BASE_URL}/connect/v1/environments/{_path(env_id)}"
        f"/clusters/{_path(kafka_cluster_id)}"
        f"/connectors/{_path(connector_name)}/disaster-recovery:{action}"
    )
    params = {"dry_run": "true"} if dry_run else None
    return session.post(url, params=params, json=body, timeout=REQUEST_TIMEOUT_SECONDS)


def call_status_endpoint(
        session: requests.Session,
        env_id: str,
        kafka_cluster_id: str,
        connector_name: str,
) -> requests.Response:
    """GET the per-connector DR status endpoint and return the raw Response.

    The endpoint path differs from the failover/failback URLs: it uses the
    trailing `/disaster-recovery/status` sub-resource form rather than the
    `:action` action-verb form.

    Returned without raising on non-2xx so the caller can distinguish:
        404 -> connector has never been part of a DR operation
        2xx -> connector has DR state; body is the DR-status JSON payload
        anything else -> transient / auth / server error
    """
    url = (
        f"{API_BASE_URL}/connect/v1/environments/{_path(env_id)}"
        f"/clusters/{_path(kafka_cluster_id)}"
        f"/connectors/{_path(connector_name)}/disaster-recovery/status"
    )
    return session.get(url, timeout=REQUEST_TIMEOUT_SECONDS)


def build_request_body(
        args: argparse.Namespace,
        overrides_for_connector: Optional[Dict[str, str]],
) -> Dict[str, Any]:
    """Construct the JSON body for a single failover/failback API call."""
    body: Dict[str, Any] = {
        "dr_environment_id": args.dr_environment,
        "dr_kafka_cluster_id": args.dr_kafka_cluster_id,
    }
    if args.action == "failover" and overrides_for_connector:
        body["config_overrides"] = overrides_for_connector
    return body


def extract_error_text(resp: requests.Response) -> str:
    """Pull a human-readable error string from a non-2xx response.

    Confluent Cloud returns errors as
        {"errors": [{"detail": "...", "title": "...", ...}]}
    per the Confluent API Design Guide. The first non-empty `detail` (or
    `title`) is preferred. Falls back to the raw response body when the
    envelope is missing.
    """
    try:
        payload = resp.json()
    except ValueError:
        return resp.text[:500]
    if isinstance(payload, dict):
        errors = payload.get("errors")
        if isinstance(errors, list) and errors:
            details = [
                e.get("detail") or e.get("title") or ""
                for e in errors
                if isinstance(e, dict)
            ]
            joined = "; ".join(d for d in details if d)
            if joined:
                return joined
    return json.dumps(payload)[:500]


# =============================================================================
#  Per-connector worker
# =============================================================================


def process_one(
        session: requests.Session,
        args: argparse.Namespace,
        connector_name: str,
        overrides_for_connector: Optional[Dict[str, str]],
) -> ConnectorOutcome:
    """Run the chosen action on a single connector and translate the response.

    The DR API returns HTTP 200 with a JSON body that carries its own
    "status" field ("SUCCESS" / "FAILED") plus an "error_message". A 4xx/5xx
    means the request never reached the per-connector flow at all (auth
    failure, bad request, server outage, etc.) — that case is captured
    separately with whatever the error body says.
    """
    start = time.monotonic()
    try:
        body = build_request_body(args, overrides_for_connector)
        resp = call_dr_endpoint(
            session,
            args.action,
            args.environment,
            args.kafka_cluster_id,
            connector_name,
            body,
            args.dry_run,
        )
        elapsed = time.monotonic() - start

        if 200 <= resp.status_code < 300:
            try:
                payload = resp.json() if resp.content else {}
            except ValueError as e:
                return ConnectorOutcome(
                    name=connector_name,
                    status="FAILED",
                    http_status=resp.status_code,
                    error_message=f"unparseable response body: {e}",
                    duration_seconds=elapsed,
                )
            body_status = (payload.get("status") or "").upper()
            # Capture every field the DR response carries — useful for the
            # JSON summary and for the per-connector log line. The response's
            # connector_name is also returned but matches what we sent, so
            # we reuse the request-side `connector_name` for clarity.
            return ConnectorOutcome(
                name=connector_name,
                status=body_status or "SUCCESS",
                http_status=resp.status_code,
                primary_connector_id=payload.get("primary_connector_id", ""),
                dr_connector_id=payload.get("dr_connector_id", ""),
                error_message=payload.get("error_message", ""),
                duration_seconds=elapsed,
            )

        return ConnectorOutcome(
            name=connector_name,
            status="FAILED",
            http_status=resp.status_code,
            error_message=extract_error_text(resp),
            duration_seconds=elapsed,
        )
    except requests.RequestException as e:
        return ConnectorOutcome(
            name=connector_name,
            status="FAILED",
            error_message=f"network error: {e}",
            duration_seconds=time.monotonic() - start,
        )


def process_status_one(
        session: requests.Session,
        env_id: str,
        kafka_cluster_id: str,
        discovered: DiscoveredConnector,
) -> ConnectorStatusOutcome:
    """Query DR status for one connector and translate the response.

    Classification:
      - 2xx body present     -> HAS_DR_STATE (populated from the response)
      - 404 NotFound         -> NO_DR_STATE (connector has never failed over)
      - any other non-2xx    -> ERROR with the response body / status code
      - transport exception  -> ERROR with the exception message

    `discovered` carries the per-connector metadata collected during the
    discovery step (name, lcc id, runtime state). It is merged into every
    outcome so the table output can show id and runtime state even when the
    DR-status endpoint returns 404 or an error.
    """
    connector_name = discovered.name
    start = time.monotonic()
    try:
        resp = call_status_endpoint(
            session, env_id, kafka_cluster_id, connector_name
        )
        elapsed = time.monotonic() - start

        if 200 <= resp.status_code < 300:
            try:
                payload = resp.json() if resp.content else {}
            except ValueError as e:
                return ConnectorStatusOutcome(
                    name=connector_name,
                    outcome_kind="ERROR",
                    http_status=resp.status_code,
                    connector_id=discovered.connector_id,
                    current_state=discovered.state,
                    error_message=f"unparseable response body: {e}",
                    duration_seconds=elapsed,
                )
            # Prefer the DR response's connector_id (authoritative), fall
            # back to the discovered id if the server omits it for any reason.
            return ConnectorStatusOutcome(
                name=connector_name,
                outcome_kind="HAS_DR_STATE",
                http_status=resp.status_code,
                connector_id=payload.get("connector_id", "") or discovered.connector_id,
                connector_type=payload.get("connector_type", ""),
                current_state=discovered.state,
                active_region=payload.get("active_region", ""),
                active_lkc_id=payload.get("active_lkc_id", ""),
                dr_connector_id=payload.get("dr_connector_id", ""),
                dr_operation_state=payload.get("dr_operation_state", ""),
                dr_operation_started_at=payload.get("dr_operation_started_at", "") or "",
                duration_seconds=elapsed,
            )

        if resp.status_code == 404:
            # Service returns 404 with "Connector X is not failed over to DR
            # region" when no DR backup row exists. That's informational, not
            # an error — many connectors will be in this state in steady-state.
            return ConnectorStatusOutcome(
                name=connector_name,
                outcome_kind="NO_DR_STATE",
                http_status=resp.status_code,
                connector_id=discovered.connector_id,
                current_state=discovered.state,
                duration_seconds=elapsed,
            )

        return ConnectorStatusOutcome(
            name=connector_name,
            outcome_kind="ERROR",
            http_status=resp.status_code,
            connector_id=discovered.connector_id,
            current_state=discovered.state,
            error_message=extract_error_text(resp),
            duration_seconds=elapsed,
        )
    except requests.RequestException as e:
        return ConnectorStatusOutcome(
            name=connector_name,
            outcome_kind="ERROR",
            connector_id=discovered.connector_id,
            current_state=discovered.state,
            error_message=f"network error: {e}",
            duration_seconds=time.monotonic() - start,
        )


# =============================================================================
#  Orchestration
# =============================================================================


def _chunked(items: List[Any], size: int) -> List[List[Any]]:
    """Split `items` into consecutive non-overlapping chunks of at most `size`.

    Returned as a list (not a generator) so callers can compute `len(...)` for
    progress reporting without exhausting it. Element type is not enforced —
    callers can pass list of str (failover/failback) or list of
    DiscoveredConnector (status).
    """
    return [items[i:i + size] for i in range(0, len(items), size)]


def resolve_connector_set(
        session: requests.Session, args: argparse.Namespace
) -> List[DiscoveredConnector]:
    """Either use the explicit --connectors list or discover connectors via the API.

    Discovery rules — uniform across all three actions:
      * Custom connectors (non-`lcc-` IDs) are filtered out. Their plugin
        artifacts are region-scoped, so they cannot participate in DR for
        any action.
      * Every other connector is included regardless of runtime state.
        - status callers want DR state for STOPPED/PAUSED connectors too;
          post-failover the source-region connector is paused.
        - failback callers especially need this: after a successful
          failover, the source-region connector is no longer RUNNING, so a
          RUNNING-only filter would hide every failback candidate.
        - failover callers ask the script to TRY every connector; the DR
          API rejects ineligible ones per-connector with a clean error,
          which is more informative than client-side prefiltering.

    The explicit --connectors list is never auto-filtered; if a caller names
    a connector that cannot be acted on, the relevant API rejects it cleanly.
    Names from the explicit list come back with empty id/state because the
    script does not call the listing endpoint in that path.
    """
    if args.connectors.strip():
        names = [c.strip() for c in args.connectors.split(",") if c.strip()]
        print(
            f"Targeting {len(names)} connectors from --connectors flag",
            file=sys.stderr,
        )
        return [DiscoveredConnector(name=n) for n in names]

    print(
        "Discovering connectors in the source Kafka cluster...",
        file=sys.stderr,
    )
    discovered = list_active_connectors(
        session,
        args.environment,
        args.kafka_cluster_id,
        exclude_custom_connectors=True,
    )
    print(f"  found {len(discovered)} connectors", file=sys.stderr)
    return discovered


def run(args: argparse.Namespace) -> RunSummary:
    overrides_map = load_config_overrides(args.config_overrides_file)
    if overrides_map and args.action != "failover":
        print(
            "  warning: --config-overrides-file is set but the action is not "
            "'failover'; the file will be ignored",
            file=sys.stderr,
        )
        overrides_map = {}

    with build_session(args.secret_key, args.secret_value) as session:
        discovered = resolve_connector_set(session, args)
        # Failover/failback orchestration only needs the names; the
        # discovered id/state metadata is consumed by the status path.
        connectors = [d.name for d in discovered]
        summary = RunSummary(action=args.action, total=len(connectors))
        if not connectors:
            return summary

        # Warn about overrides that don't match any target connector — these
        # are almost always typos in the JSON file.
        unknown = set(overrides_map) - set(connectors)
        if unknown:
            print(
                f"  warning: --config-overrides-file has entries for unknown "
                f"connectors (ignored): {sorted(unknown)}",
                file=sys.stderr,
            )

        total_batches = (len(connectors) + BATCH_SIZE - 1) // BATCH_SIZE
        print(
            f"\nIssuing {args.action} requests in batches of {BATCH_SIZE} "
            f"({total_batches} batch{'es' if total_batches != 1 else ''} total, "
            f"dry_run={args.dry_run})...\n",
            file=sys.stderr,
        )
        # One-shot table header for the per-connector rows.
        print(_format_outcome_header(), file=sys.stderr)

        # Open the per-connector CSV (if requested) BEFORE any DR work runs.
        # This doubles as a write-permission pre-flight check — a bad path
        # fails fast with a CLIError, no connectors mutated. Rows are then
        # streamed and flushed as each connector completes, so a mid-run
        # crash or Ctrl-C still leaves whatever finished on disk.
        csv_writer, csv_file = _open_streaming_csv(args.output_result, _OUTCOME_CSV_COLUMNS)
        try:
            # Strict batched fan-out: dispatch BATCH_SIZE requests in parallel,
            # wait for every one to return, then move to the next batch. A
            # single slow connector therefore holds up the rest of its batch —
            # that is intentional, so the output shows a clear "batch N done"
            # boundary.
            with ThreadPoolExecutor(max_workers=BATCH_SIZE) as pool:
                for batch in _chunked(connectors, BATCH_SIZE):
                    futures = [
                        pool.submit(
                            process_one,
                            session,
                            args,
                            connector_name,
                            overrides_map.get(connector_name),
                        )
                        for connector_name in batch
                    ]
                    for future in as_completed(futures):
                        outcome = future.result()
                        summary.outcomes.append(outcome)
                        if outcome.status == "SUCCESS":
                            summary.succeeded += 1
                        else:
                            summary.failed += 1
                        print(_format_outcome_line(outcome), file=sys.stderr)
                        _stream_csv_row(csv_writer, csv_file, outcome, _OUTCOME_CSV_COLUMNS)
        finally:
            if csv_file is not None:
                csv_file.close()

    return summary


def run_status(args: argparse.Namespace) -> StatusRunSummary:
    """Read-only DR status query for every connector in the source cluster.

    Mirrors the structure of `run()` (discovery + bounded fan-out + per-call
    log line) but issues GET requests against the disaster-recovery/status
    endpoint and produces a StatusRunSummary instead of a RunSummary.
    """
    with build_session(args.secret_key, args.secret_value) as session:
        discovered = resolve_connector_set(session, args)
        summary = StatusRunSummary(total=len(discovered))
        if not discovered:
            return summary

        total_batches = (len(discovered) + BATCH_SIZE - 1) // BATCH_SIZE
        print(
            f"\nQuerying DR status in batches of {BATCH_SIZE} "
            f"({total_batches} batch{'es' if total_batches != 1 else ''} total)...\n",
            file=sys.stderr,
        )
        # One-shot table header (per-row output goes to stderr too).
        print(_format_status_header(), file=sys.stderr)

        # See note in run() about streaming CSV — same pre-flight + per-row
        # flush pattern, just with the status column set.
        csv_writer, csv_file = _open_streaming_csv(args.output_result, _STATUS_CSV_COLUMNS)
        try:
            with ThreadPoolExecutor(max_workers=BATCH_SIZE) as pool:
                for batch in _chunked(discovered, BATCH_SIZE):
                    futures = [
                        pool.submit(
                            process_status_one,
                            session,
                            args.environment,
                            args.kafka_cluster_id,
                            d,
                        )
                        for d in batch
                    ]
                    for future in as_completed(futures):
                        outcome = future.result()
                        summary.outcomes.append(outcome)
                        if outcome.outcome_kind == "HAS_DR_STATE":
                            summary.has_dr_state += 1
                            # Region/state tallies only over connectors that
                            # actually have DR state — NO_DR_STATE rows would
                            # skew "active_region".
                            if outcome.active_region:
                                summary.by_active_region[outcome.active_region] = (
                                        summary.by_active_region.get(outcome.active_region, 0) + 1
                                )
                            if outcome.dr_operation_state:
                                summary.by_operation_state[outcome.dr_operation_state] = (
                                        summary.by_operation_state.get(outcome.dr_operation_state, 0) + 1
                                )
                        elif outcome.outcome_kind == "NO_DR_STATE":
                            summary.no_dr_state += 1
                        else:
                            summary.errored += 1
                        print(
                            _format_status_line(outcome, source_lkc=args.kafka_cluster_id),
                            file=sys.stderr,
                        )
                        _stream_csv_row(csv_writer, csv_file, outcome, _STATUS_CSV_COLUMNS)
        finally:
            if csv_file is not None:
                csv_file.close()

    return summary


# Fixed-width column layout for the per-connector status table. Tuned so the
# typical row (connector names ~20 chars, lcc-/clcc- IDs ~16 chars, LKC IDs
# ~14 chars) fits in ~120 columns without truncation.
_STATUS_COL_NAME = 32
_STATUS_COL_ID = 20
_STATUS_COL_STATE = 12
_STATUS_COL_LKC = 18


def _format_status_header() -> str:
    """One-shot column header printed before the per-connector rows."""
    header = (
        f"  {'NAME':<{_STATUS_COL_NAME}} "
        f"{'ID':<{_STATUS_COL_ID}} "
        f"{'STATE':<{_STATUS_COL_STATE}} "
        f"{'ACTIVE LKC':<{_STATUS_COL_LKC}} "
        f"DR"
    )
    rule = "  " + "-" * (len(header) - 2 + 24)  # ~24 chars of room for DR column
    return f"{header}\n{rule}"


def _dr_summary(o: ConnectorStatusOutcome) -> str:
    """Human-readable summary of the DR column for a single outcome."""
    if o.outcome_kind == "NO_DR_STATE":
        return "No DR"
    if o.outcome_kind == "ERROR":
        # Surface the HTTP code only when it's actually informative — drop
        # the bare "http=N" prefix that was hard to read in the old format.
        if o.http_status is not None:
            return f"ERROR (HTTP {o.http_status}): {o.error_message}"
        return f"ERROR: {o.error_message}"
    # HAS_DR_STATE — show where the connector is currently active, plus
    # any in-flight operation state.
    region = o.active_region or "?"
    base = f"Active in {region}"
    if o.dr_operation_state:
        base = f"{base} (op: {o.dr_operation_state})"
    return base


def _format_status_line(o: ConnectorStatusOutcome, source_lkc: str = "") -> str:
    """One per-connector row in the status table.

    Columns: NAME | ID | STATE | ACTIVE LKC | DR. Empty fields render as `-`
    so column boundaries stay visible.

    `source_lkc` is the Kafka cluster the script is querying. It is used as
    the displayed "active LKC" for NO_DR_STATE and ERROR rows — those
    connectors live on the source cluster by definition (no DR move has
    happened), and showing the cluster ID is more useful than `-`.
    """
    name = o.name
    cid = o.connector_id or "-"
    state = o.current_state or "-"
    # HAS_DR_STATE: trust the DR-status response's active_lkc_id. Otherwise
    # fall back to the source LKC the script is querying.
    lkc = o.active_lkc_id or source_lkc or "-"
    return (
        f"  {name:<{_STATUS_COL_NAME}} "
        f"{cid:<{_STATUS_COL_ID}} "
        f"{state:<{_STATUS_COL_STATE}} "
        f"{lkc:<{_STATUS_COL_LKC}} "
        f"{_dr_summary(o)}"
    ).rstrip()


# Column layout for the per-connector failover/failback table. Widths match
# the status table where possible so all three actions look consistent.
_OUTCOME_COL_NAME = 32
_OUTCOME_COL_PRIMARY = 20
_OUTCOME_COL_DR = 20
_OUTCOME_COL_RESULT = 9


def _format_outcome_header() -> str:
    """One-shot column header printed before the per-connector rows."""
    header = (
        f"  {'NAME':<{_OUTCOME_COL_NAME}} "
        f"{'PRIMARY ID':<{_OUTCOME_COL_PRIMARY}} "
        f"{'DR ID':<{_OUTCOME_COL_DR}} "
        f"{'RESULT':<{_OUTCOME_COL_RESULT}} "
        f"DETAIL"
    )
    rule = "  " + "-" * (len(header) - 2 + 40)
    return f"{header}\n{rule}"


def _format_outcome_line(o: ConnectorOutcome) -> str:
    """One per-connector row in the failover/failback table.

    Columns: NAME | PRIMARY ID | DR ID | RESULT | DETAIL. Empty fields
    render as `-` so column boundaries stay visible. DETAIL carries the
    error message for FAILED rows; for SUCCESS rows it is omitted so the
    table reads cleanly.
    """
    name = o.name
    primary = o.primary_connector_id or "-"
    dr = o.dr_connector_id or "-"
    result = o.status or "-"
    detail = ""
    if o.status != "SUCCESS":
        # Prefix the HTTP code on transport-level failures (4xx/5xx), which
        # is the case where http_status is set but the DR API never returned
        # a per-connector status body.
        if o.http_status is not None and o.http_status >= 400:
            detail = f"HTTP {o.http_status}: {o.error_message}"
        else:
            detail = o.error_message
    return (
        f"  {name:<{_OUTCOME_COL_NAME}} "
        f"{primary:<{_OUTCOME_COL_PRIMARY}} "
        f"{dr:<{_OUTCOME_COL_DR}} "
        f"{result:<{_OUTCOME_COL_RESULT}} "
        f"{detail}"
    ).rstrip()


# =============================================================================
#  Output
# =============================================================================


def print_summary(s: RunSummary) -> None:
    print()
    print("=" * 72)
    print(f"DR {s.action} summary")
    print("=" * 72)
    print(f"  total      : {s.total}")
    print(f"  succeeded  : {s.succeeded}")
    print(f"  failed     : {s.failed}")
    if s.failed:
        print()
        print("Failed connectors (every field returned by the DR API):")
        # Sort by name so the failed block is deterministic across runs;
        # per-line progress logging stays in completion order on stderr.
        for o in sorted(s.outcomes, key=lambda x: x.name):
            if o.status != "SUCCESS":
                print(f"  - {o.name}")
                print(f"      status:               {o.status}")
                print(f"      http_status:          {o.http_status if o.http_status is not None else '-'}")
                print(f"      primary_connector_id: {o.primary_connector_id or '-'}")
                print(f"      dr_connector_id:      {o.dr_connector_id or '-'}")
                print(f"      error_message:        {o.error_message or '(no error message)'}")
                print(f"      duration_seconds:     {o.duration_seconds:.2f}")


def print_status_summary(s: StatusRunSummary) -> None:
    print()
    print("=" * 72)
    print("DR status summary")
    print("=" * 72)
    print(f"  total           : {s.total}")
    print(f"  with DR state   : {s.has_dr_state}")
    print(f"  no DR state     : {s.no_dr_state}")
    print(f"  errored         : {s.errored}")
    if s.by_active_region:
        print()
        print("  by active region (only connectors with DR state):")
        for region, count in sorted(s.by_active_region.items()):
            print(f"    {region:<10}  {count}")
    if s.by_operation_state:
        print()
        print("  by dr_operation_state (in-flight or last-known DR op):")
        for state, count in sorted(s.by_operation_state.items()):
            print(f"    {state:<25}  {count}")
    if s.errored:
        print()
        print("Errored connectors (every field returned by the DR status API):")
        # Sort by name so the errored block is deterministic across runs;
        # per-line progress logging stays in completion order on stderr.
        for o in sorted(s.outcomes, key=lambda x: x.name):
            if o.outcome_kind != "ERROR":
                continue
            print(f"  - {o.name}")
            print(f"      http_status:       {o.http_status if o.http_status is not None else '-'}")
            print(f"      error_message:     {o.error_message or '(no error message)'}")
            print(f"      duration_seconds:  {o.duration_seconds:.2f}")


# Column orderings for output CSV. Defined as module-level constants so
# the help text, write helpers, and any downstream consumer agree on the schema.
_OUTCOME_CSV_COLUMNS: List[str] = [
    "name",
    "status",
    "primary_connector_id",
    "dr_connector_id",
    "http_status",
    "error_message",
    "duration_seconds",
]

_STATUS_CSV_COLUMNS: List[str] = [
    "name",
    "outcome_kind",
    "connector_id",
    "connector_type",
    "current_state",
    "active_region",
    "active_lkc_id",
    "dr_connector_id",
    "dr_operation_state",
    "dr_operation_started_at",
    "http_status",
    "error_message",
    "duration_seconds",
]


def _csv_cell(value: Any) -> str:
    """Render a dataclass field value as a CSV cell.

    None -> empty string (so downstream tools don't see the literal "None").
    Floats are formatted to 2dp to match the per-row console output.
    Everything else is str()'d. csv.writer handles quoting / escaping.
    """
    if value is None:
        return ""
    if isinstance(value, float):
        return f"{value:.2f}"
    return str(value)


def _open_streaming_csv(
        path: Optional[str], columns: List[str]
) -> tuple:
    """Open the result CSV for streaming writes and emit the header.

    Returns (writer, file_handle) when `path` is set, else (None, None). The
    file is opened in "w" mode up-front so a bad path fails before any DR
    work runs (pre-flight write-permission check). Rows are written by
    `_stream_csv_row` as connectors complete; the caller is responsible for
    closing the file in a `finally`.
    """
    if not path:
        return None, None
    try:
        # newline="" is required by the csv module on all platforms; without
        # it, csv.writer emits a stray \r on Windows.
        handle = open(path, "w", encoding="utf-8", newline="")
    except OSError as e:
        raise CLIError(f"results csv: cannot write {path}: {e}") from e
    writer = csv.writer(handle)
    writer.writerow(columns)
    handle.flush()
    print(f"Streaming results to {path}", file=sys.stderr)
    return writer, handle


def _stream_csv_row(
        writer, handle, outcome: Any, columns: List[str]
) -> None:
    """Write one row for `outcome` to the streaming CSV and flush to disk.

    No-op when `writer` is None (i.e. no CSV was opened). Flush
    after every row so partial results survive a mid-run crash, Ctrl-C, or
    a parent process kill.
    """
    if writer is None:
        return
    writer.writerow(_csv_cell(getattr(outcome, col)) for col in columns)
    handle.flush()


# =============================================================================
#  Pre-run confirmation
# =============================================================================


def confirm_parameters(args: argparse.Namespace) -> bool:
    """Show the parsed parameters and ask the user to confirm before running.

    Returns True only when the answer read from stdin is `y` / `yes`
    (case-insensitive). Any other answer, an empty answer, or EOF returns
    False.

    Note that stdin is not required to be a TTY: piping an
    answer in (`echo y | connect_dr.py ...`) does confirm the run.

    The API key is shown so the user can verify they're authenticating with
    the right credential; the secret is never printed — only a placeholder.
    """
    print("Run summary:", file=sys.stderr)
    print(f"  action:               {args.action}", file=sys.stderr)
    print(f"  source environment:   {args.environment}", file=sys.stderr)
    print(f"  source kafka cluster: {args.kafka_cluster_id}", file=sys.stderr)
    if args.action in ("failover", "failback"):
        print(f"  DR environment:       {args.dr_environment}", file=sys.stderr)
        print(f"  DR kafka cluster:     {args.dr_kafka_cluster_id}", file=sys.stderr)
    if args.connectors.strip():
        print(f"  connectors:           {args.connectors}", file=sys.stderr)
    else:
        print("  connectors:           (auto-discover every connector in the source cluster)", file=sys.stderr)
    if args.action == "failover" and args.config_overrides_file:
        print(f"  config overrides:     {args.config_overrides_file}", file=sys.stderr)
    if args.action != "status":
        print(f"  dry run:              {args.dry_run}", file=sys.stderr)
    # The CSV path is generated by main() before we get here, so it's always
    # set; surface it so the operator knows where to look for results.
    print(f"  results csv:          {args.output_result}", file=sys.stderr)
    print(f"  API key:              {args.secret_key}", file=sys.stderr)
    print("  API secret:           (provided, not displayed)", file=sys.stderr)
    print(f"  API base URL:         {API_BASE_URL}", file=sys.stderr)
    print(file=sys.stderr)

    try:
        answer = input(f"Proceed with {args.action}? [y/N]: ").strip().lower()
    except EOFError:
        # stdin is closed or empty (e.g. `< /dev/null`, a closed pipe) — treat
        # as "No" rather than hanging or proceeding silently. A pipe that
        # carries an actual answer does not land here; it is read normally.
        print("(no input; aborting)", file=sys.stderr)
        return False
    return answer in ("y", "yes")


# =============================================================================
#  Entry point
# =============================================================================


def _default_output_path(action: str) -> str:
    """Build a unique CSV path for this run.

    Pattern: `dr_<action>_<UTC-timestamp>.csv` in the current working directory,
    returned as an absolute path so users can copy-paste it from the
    confirmation prompt. The timestamp guarantees consecutive runs never
    overwrite each other.
    """
    ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    return os.path.abspath(f"dr_{action}_{ts}.csv")


def main() -> int:
    # parse_args() calls sys.exit(2) directly on argument errors, so it is
    # outside the try/except below.
    args = parse_args()
    # Always write a CSV result file; the path is auto-generated from the
    # action and a UTC timestamp so it never collides with previous runs and
    # the user doesn't have to choose (or accidentally overwrite) a path.
    args.output_result = _default_output_path(args.action)
    try:
        if not confirm_parameters(args):
            print("Aborted.", file=sys.stderr)
            return 0
        if args.action == "status":
            status_summary = run_status(args)
            print_status_summary(status_summary)
            # The results CSV is streamed inside run_status() as connectors
            # complete; nothing to do here.
            # Read-only command — exit non-zero only if any per-connector status
            # query errored. Connectors with NO_DR_STATE are not failures.
            return 0 if status_summary.errored == 0 else 1

        summary = run(args)
        print_summary(summary)
        # The results CSV is streamed inside run() as connectors complete.

        # Exit non-zero if any connector failed so this command is safe to chain
        # into automation (e.g. a runbook that aborts on first failure).
        return 0 if summary.failed == 0 else 1
    except CLIError as e:
        print(f"error: {e}", file=sys.stderr)
        return 1
    except KeyboardInterrupt:
        # Standard SIGINT exit code (128 + 2).
        print("\nInterrupted.", file=sys.stderr)
        return 130


if __name__ == "__main__":
    sys.exit(main())
