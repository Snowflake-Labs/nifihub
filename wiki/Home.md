# NiFi Hub

A community-driven repository of independently-versioned Apache NiFi extension bundles and versioned flow definitions, with a full CI/CD pipeline for quality gates, deployment, and environment management.

---

## Where do you want to go?

| I want to… | Go to |
|---|---|
| Understand how this repo works | [Introduction & Concepts](Introduction-and-Concepts) |
| Learn how flows are versioned with Git | [GitHub Registry Client](Introduction-and-Concepts--GitHub-Registry-Client) |
| Understand what CI checks run on PRs | [CI Pipeline](Introduction-and-Concepts--CI) |
| Deploy NiFi infrastructure declaratively | [CD Pipeline](Introduction-and-Concepts--CD) |
| Run the CD pipeline locally or against a local NiFi | [Running CD Locally](How-to-Run-CD-Locally) |
| Use the pipeline with a non-Snowflake NiFi instance | [Non-SOM Runtimes](Introduction-and-Concepts--Non-SOM-Runtimes) |
| Download a NAR or import a flow | [How to Use This Repo](How-to-Use-This-Repo) |
| Add a new extension bundle or flow | [How to Contribute](How-to-Contribute) |
| Browse processor and controller service docs | [Components](Components) |
| Browse available flow definitions | [Flows](Flows) |

---

## What's in This Repo

**Extension Bundles** (`extensions/`) — Reusable NiFi processors, controller services, and reporting tasks packaged as NARs. Each bundle has its own version and release lifecycle and is published to GitHub Releases as a downloadable NAR.

**Flow Definitions** (`flows/`) — Versioned NiFi flow definitions exported as JSON, with companion documentation and automated validation tests. Flows can be imported directly into NiFi or deployed to a Snowflake Openflow runtime.

**Environments** (`environments/`) — GitOps configuration for managing Openflow deployments, runtimes, and flow assignments declaratively. Changes to `config.yaml` files are automatically applied to Snowflake via GitHub Actions.
