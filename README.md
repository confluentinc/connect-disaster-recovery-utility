# Connect Disaster Recovery Utility

A command-line utility for running disaster-recovery (DR) operations across many
Confluent Cloud managed connectors with a single command.

This repository is part of the Confluent organization on GitHub. It is public and
open to contributions from the community.

## Overview

This utility builds on the Confluent Cloud connector disaster-recovery API to
drive DR across an entire Kafka cluster from a single command. It auto-discovers
the connectors in the cluster, issues the DR operation in small parallel batches,
continues through individual connector failures, and writes every result to a
timestamped CSV for review and audit.

There are three actions:

| Action | Mutating? | What it does |
| --- | --- | --- |
| `failover` | Yes | Moves connectors from the source Kafka cluster to the DR Kafka cluster. Supports per-connector config overrides. |
| `failback` | Yes | Reverses a previous failover, back to the source cluster. |
| `status` | No | Reports each connector's active region, active cluster, and any in-flight DR operation. |

## Prerequisites

- **Your Confluent Cloud organization must be allowlisted for connector
  disaster recovery.** Contact the Confluent team to have the org you intend to
  run this against enabled. The DR endpoints are unavailable until that is done.
- Python 3.7 or later
- A Confluent Cloud API key and secret with org admin permissions.
- Network access to `https://api.confluent.cloud`.

## Installation

```bash
git clone https://github.com/confluentinc/connect-disaster-recovery-utility.git
cd connect-disaster-recovery-utility
pip install -r requirements.txt
```

[`requests`](https://pypi.org/project/requests/) is the only third-party
dependency.

## Usage

```bash
python src/connect_dr.py --help
```

The help output documents all available functionality — every flag, which action
it applies to, and the config-overrides file format.

## Output

Per-connector progress and warnings stream to stderr; a summary with totals is
printed to stdout at the end. Every run also writes
`dr_<action>_<UTC-timestamp>.csv` to the current working directory, flushed row
by row so an interrupted run still leaves completed results on disk. The path is
shown in the confirmation prompt.

## Contributing

Contributions are welcome. See [LICENSE](LICENSE) for contribution terms and
[CHANGELOG.md](CHANGELOG.md) for recent updates.
