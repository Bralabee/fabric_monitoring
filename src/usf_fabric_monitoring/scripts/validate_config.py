"""Config validation CLI.

This module backs:
- console script: `usf-validate-config`
- Makefile target: `make validate-config`

It validates JSON configuration files under `config/` using the schemas in
`usf_fabric_monitoring.core.config_validation`.

This is intentionally lightweight and safe to run offline.
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Iterable, Optional

from usf_fabric_monitoring.core.config_validation import (
    SCHEMAS_BY_FILENAME,
    validate_config_dir,
)


def _default_config_dir() -> Path:
    # repo_root/src/usf_fabric_monitoring/scripts/validate_config.py -> repo_root
    return Path(__file__).resolve().parents[3] / "config"


def parse_args(argv: Optional[Iterable[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Validate usf_fabric_monitoring JSON config files",
    )
    parser.add_argument(
        "config_dir",
        nargs="?",
        default=_default_config_dir(),
        type=Path,
        help="Config directory to validate (default: repo_root/config)",
    )
    parser.add_argument(
        "--all-json",
        action="store_true",
        help=(
            "Validate all *.json files, not only known config filenames "
            f"({', '.join(sorted(SCHEMAS_BY_FILENAME))})."
        ),
    )
    parser.add_argument(
        "--json",
        dest="json_output",
        action="store_true",
        help="Print results as JSON (always printed to stdout).",
    )
    return parser.parse_args(argv)


def main(argv: Optional[Iterable[str]] = None) -> int:
    args = parse_args(argv)

    results = validate_config_dir(args.config_dir, only_known_files=not args.all_json)

    if args.json_output:
        print(json.dumps(results, indent=2))
    else:
        if not results:
            print(f"✅ Config valid: {args.config_dir}")
        else:
            print(f"❌ Config validation errors in: {args.config_dir}\n")
            for filename, errors in results.items():
                print(filename)
                for err in errors:
                    print(f"  - {err}")
                print()

    return 0 if not results else 1


if __name__ == "__main__":
    raise SystemExit(main())
