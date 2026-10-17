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
Upload all NAR files from a directory to an Apache NiFi instance.

Finds every *.nar file under the given directory.  For each NAR, reads
its coordinates (group, artifact, version) from META-INF/MANIFEST.MF and
checks whether an identical NAR is already installed on the runtime.  If
so the upload is skipped.  Otherwise the NAR is uploaded and we wait for
it to reach the "Installed" state.

Writes the list of *newly uploaded* NAR identifiers to stdout (one per
line) so the caller can record them for later cleanup.

Usage:
    python scripts/upload-nars.py --nar-dir extensions/ \
        --runtime-url https://localhost:8443 --token <token>
"""

import argparse
import glob
import os
import sys
from pathlib import Path
import time
import zipfile

import nipyapi

sys.path.insert(0, str(Path(__file__).resolve().parent / "cd"))
from manage_flows import configure_nifi
from nifi_config import get_nifi_auth


def _read_nar_coordinates(nar_path):
    """Extract (group, artifact, version) from a NAR's MANIFEST.MF."""
    with zipfile.ZipFile(nar_path) as zf:
        manifest = zf.read("META-INF/MANIFEST.MF").decode("utf-8")
    coords = {}
    for line in manifest.splitlines():
        for key, field in [("Nar-Group:", "group"), ("Nar-Id:", "artifact"), ("Nar-Version:", "version")]:
            if line.startswith(key):
                coords[field] = line[len(key):].strip()
    return coords.get("group"), coords.get("artifact"), coords.get("version")


def _get_installed_nars(api):
    """Return a dict mapping (group, artifact, version) -> nar_id for all installed NARs."""
    result = api.get_nar_summaries()
    installed = {}
    for entry in (result.nar_summaries or []):
        s = entry.nar_summary
        if s.state and s.state.lower() == "installed" and s.coordinate:
            key = (s.coordinate.group, s.coordinate.artifact, s.coordinate.version)
            installed[key] = s.identifier
    return installed


def _wait_for_nar_install(api, nar_id, timeout=15):
    """Poll until a NAR reaches Installed state or timeout."""
    deadline = time.time() + timeout
    state = None
    while time.time() < deadline:
        summary = api.get_nar_summary(nar_id)
        state = summary.nar_summary.state or ""
        if state.lower() == "installed":
            return summary
        if state.lower() in ("failed", "missing"):
            msg = summary.nar_summary.failure_message or "unknown error"
            raise Exception("NAR %s install failed (%s): %s" % (nar_id, state, msg))
        time.sleep(1)
    raise Exception("Timed out waiting for NAR %s to install (last state: %s)" % (nar_id, state))


def upload_nars(nar_dir, runtime_url, pat=None):
    """Upload all .nar files found under nar_dir and return their identifiers."""
    configure_nifi(runtime_url, pat=pat, nifi_auth=get_nifi_auth())

    nar_files = sorted(glob.glob(os.path.join(nar_dir, "**", "*.nar"), recursive=True))
    if not nar_files:
        print("No .nar files found under %s" % nar_dir, file=sys.stderr)
        return []

    print("Found %d NAR(s) to upload" % len(nar_files), file=sys.stderr)
    api = nipyapi.nifi.ControllerApi()

    installed = _get_installed_nars(api)
    uploaded_ids = []

    for nar_path in nar_files:
        filename = os.path.basename(nar_path)
        group, artifact, version = _read_nar_coordinates(nar_path)
        coords = (group, artifact, version)
        print("Processing %s (%s:%s:%s) ..." % (filename, group, artifact, version), file=sys.stderr)

        if coords in installed:
            print("  Already installed (id: %s), skipping." % installed[coords], file=sys.stderr)
            continue

        with open(nar_path, "rb") as fh:
            result = api.upload_nar(body=fh.read(), filename=filename)

        nar_id = result.nar_summary.identifier
        state = result.nar_summary.state or ""
        print("  Uploaded — id: %s, state: %s" % (nar_id, state), file=sys.stderr)

        if state.lower() != "installed":
            print("  Waiting for install ...", file=sys.stderr)
            _wait_for_nar_install(api, nar_id)

        print("  Installed.", file=sys.stderr)
        uploaded_ids.append(nar_id)
        print(nar_id)

    print("Done. %d NAR(s) uploaded, %d already present." % (
        len(uploaded_ids), len(nar_files) - len(uploaded_ids)), file=sys.stderr)
    return uploaded_ids


def main():
    parser = argparse.ArgumentParser(
        description="Upload NAR files to an Apache NiFi instance"
    )
    parser.add_argument(
        "--nar-dir", required=True, help="Directory to search for .nar files"
    )
    parser.add_argument(
        "--runtime-url", default=os.environ.get("NIFI_URL"), required=not os.environ.get("NIFI_URL"), help="Existing Apache NiFi URL"
    )
    parser.add_argument(
        "--token", default=os.environ.get("NIFI_TOKEN", ""), help="NiFi bearer token (or use NIFI_USERNAME/NIFI_PASSWORD)"
    )
    args = parser.parse_args()

    try:
        upload_nars(args.nar_dir, args.runtime_url, args.token)
    except Exception as exc:
        print("NAR upload failed: %s" % exc, file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
