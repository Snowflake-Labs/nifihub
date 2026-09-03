# Introduction and Concepts

This section explains the key technologies and systems that make up NiFi Hub. Read these pages to understand what the repo provides before using or contributing to it.

---

## Key Concepts

### Apache NiFi

[Apache NiFi](https://nifi.apache.org/) is a data flow automation platform. Flows are built from **processors** (data transformation units) connected by **relationships**, grouped into **process groups**. Flows can be exported as JSON definitions and imported into any NiFi instance.

### Extension Bundles (NARs)

NiFi extensions are packaged as **NARs** (NiFi Archive files) — the NiFi equivalent of a JAR with an isolated classloader. A NAR bundles one or more processors, controller services, or reporting tasks along with their dependencies. NiFi Hub publishes NARs to GitHub Releases; you download and drop them into NiFi's NAR directory.

### Flow Definitions

Flow definitions are JSON files exported from NiFi's UI. They capture the complete structure of a process group: processors, connections, controller services, parameters, and their configuration. NiFi Hub stores flows as versioned JSON alongside documentation and validation tests.

### Flow Registry and Versioning

NiFi has a built-in **Flow Registry** concept that lets flows be versioned in an external store. NiFi Hub uses a **Git-based registry client** so that the `flows/` directory acts as the registry — each flow version maps to a Git commit or tag. See [GitHub Registry Client](Introduction-and-Concepts--GitHub-Registry-Client).

---

## Topics in This Section

| Page | What it covers |
|------|---------------|
| [GitHub Registry Client](Introduction-and-Concepts--GitHub-Registry-Client) | How NiFi's Git registry client works and how flows in this repo can be versioned and imported via it |
| [Snowflake Flow Diff Action](Introduction-and-Concepts--Snowflake-Flow-Diff-GitHub-Action) | The GitHub Action that produces human-readable flow diffs and checkstyle reports on PRs |
| [CI Pipeline](Introduction-and-Concepts--CI) | What automated checks run on every pull request |
| [CD Pipeline](Introduction-and-Concepts--CD) | How flows are deployed to Snowflake runtimes and how Openflow infrastructure is managed as code |
| [Non-SOM Runtimes](Introduction-and-Concepts--Non-SOM-Runtimes) | How to use the same NiFi reconciliation features against pre-provisioned or open-source NiFi runtimes |
