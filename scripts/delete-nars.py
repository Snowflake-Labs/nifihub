#!/usr/bin/env python3
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
"""
Delete NAR files from an Apache NiFi instance.

Reads NAR identifiers (one per line) from a file and deletes each via
the NiFi Controller API.

Usage:
    python scripts/delete-nars.py --nar-ids-file /tmp/nar_ids.txt \
        --runtime-url https://localhost:8443 --token <token>
"""

import argparse
import os
import sys
from pathlib import Path

import nipyapi

sys.path.insert(0, str(Path(__file__).resolve().parent / "cd"))
from manage_flows import configure_nifi
from nifi_config import get_nifi_auth


def delete_nars(nar_ids_file, runtime_url, pat=None):
    """Delete NARs listed in the given file."""
    configure_nifi(runtime_url, pat=pat, nifi_auth=get_nifi_auth())

    with open(nar_ids_file, "r") as f:
        nar_ids = [line.strip() for line in f if line.strip()]

    if not nar_ids:
        print("No NAR IDs to delete.", file=sys.stderr)
        return

    api = nipyapi.nifi.ControllerApi()
    for nar_id in nar_ids:
        print("Deleting NAR %s ..." % nar_id, file=sys.stderr)
        try:
            api.delete_nar(nar_id, force=True)
            print("  Deleted.", file=sys.stderr)
        except Exception as exc:
            print("  Failed to delete NAR %s: %s" % (nar_id, exc), file=sys.stderr)

    print("NAR cleanup complete.", file=sys.stderr)


def main():
    parser = argparse.ArgumentParser(
        description="Delete NAR files from an Apache NiFi instance"
    )
    parser.add_argument(
        "--nar-ids-file", required=True, help="File containing NAR identifiers (one per line)"
    )
    parser.add_argument(
        "--runtime-url", default=os.environ.get("NIFI_URL"), required=not os.environ.get("NIFI_URL"), help="Existing Apache NiFi URL"
    )
    parser.add_argument(
        "--token", default=os.environ.get("NIFI_TOKEN", ""), help="NiFi bearer token (or use NIFI_USERNAME/NIFI_PASSWORD)"
    )
    args = parser.parse_args()

    try:
        delete_nars(args.nar_ids_file, args.runtime_url, args.token)
    except Exception as exc:
        print("NAR deletion failed: %s" % exc, file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
