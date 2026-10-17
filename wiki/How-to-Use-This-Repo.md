# How to Use This Repo

This page explains how to consume the artifacts published by NiFi Hub — extension NARs and flow definitions — without contributing code.

---

## Using an Extension Bundle

Extension bundles are published as NARs to [GitHub Releases](../../releases). Each bundle has its own independent release history.

### Installing a NAR in NiFi

1. Go to the [Releases](../../releases) page and find the NAR you want
2. Download the `.nar` file from the release assets
3. Copy the NAR into NiFi's `lib/` directory (or the configured NAR directory for your installation)
4. Restart NiFi (or use NiFi's NAR auto-load feature if enabled)
5. The processors and controller services from that NAR will appear in the NiFi component palette

### Understanding NAR Versions

Each bundle uses Maven CI-friendly versions. The version is in the NAR filename (e.g., `nifi-example-nar-0.3.0.nar`). Versions follow semantic versioning. Check the [Components](Components) section for documentation on what each component does.

---

## Using a Flow Definition

Flow definitions under `flows/` can be imported directly into any NiFi instance.

### Manual Import

1. Browse to `flows/<bucket>/` and download the `.json` file for the flow you want
2. In NiFi, right-click on the canvas and select **Upload Template** (NiFi 1.x) or **Import Process Group** (NiFi 2.x)
3. Select the downloaded JSON file
4. The process group will appear on the canvas — configure any parameters and start it

### Via the Git Registry Client

If your NiFi instance is configured with a Git Flow Registry Client pointing at this repository:

1. In NiFi, drag a **Process Group** onto the canvas
2. In the dialog, switch to the registry tab
3. Browse to the flow you want and select a version
4. Click **Import** — the flow is placed on your canvas at the selected version

You can then track whether the running flow has drifted from the registered version and upgrade to newer versions through the NiFi UI.

See [GitHub Registry Client](Introduction-and-Concepts--GitHub-Registry-Client) for how to configure the client.

---

## Managing Your Own Environment

Configure existing NiFi URLs and resources in `environments/<name>/config.yaml`.
Set NiFi username/password or bearer token authentication, preview the live plan,
then reconcile resources with `scripts/run-cd.py`. GitHub Actions uses the same
pipeline. See [the configuration reference](../environments/README.md).
