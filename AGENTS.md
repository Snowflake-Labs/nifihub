# NiFi Hub - Agent Instructions

## Repository Overview

NiFi Hub is a repository of independently-versioned Apache NiFi extension bundles and versioned flow definitions. All Java code is under package `com.snowflake.nifihub`.

Deployment automation manages resources on existing Apache NiFi instances through the NiFi REST API. Server installation, scaling, storage, and networking are operated separately. Environment files use a top-level `runtimes` list with a unique `name` and explicit `url` for each instance.

## Repository Structure

- `extensions/<category>/<sub-category>/nifi-<name>-bundle/` -- NiFi extension bundles (Java, Maven)
- `flows/<bucket>/<flow-name>.json` -- NiFi flow definitions (JSON)
- `flows/<bucket>/<flow-name>.md` -- Flow documentation (Markdown)
- `flows/<bucket>/tests/test_<flow-name>.py` -- Flow validation tests (Python, nipyapi)
- `pom.xml` -- Root parent POM (no modules, defines shared config)
- `environments/<name>/config.yaml` -- Desired NiFi resources; validated against `environments/schema.json`
- `scripts/cd/` -- Live resource discovery, diffing, and reconciliation
- `scripts/ci/` -- Flow integration tests on existing NiFi instances, using isolated process groups and parameter contexts
- `checkstyle.xml` -- Checkstyle rules (NiFi conventions)
- `pmd-ruleset.xml` -- PMD rules (NiFi conventions)

## Adding a New Bundle

1. Create the directory: `extensions/<category>/<sub-category>/nifi-<name>-bundle/`
2. Create the bundle POM with `<parent>` pointing to root POM via relativePath
3. Create `nifi-<name>-processors/` with jar packaging and `nifi-<name>-nar/` with nar packaging
4. Java package: `com.snowflake.nifihub.<category>.<name>`
5. Register processors in `META-INF/services/org.apache.nifi.processor.Processor`
6. Write unit tests using `nifi-mock` (`TestRunner`)
7. Create a `SKILL.md` at the bundle root
8. Build: `./mvnw clean verify -Pcontrib-check -f <bundle-path>/pom.xml`

## Bundle Versioning (CI-Friendly Versions)

Each bundle uses Maven CI-friendly versions via the `${revision}` property. The version is defined **only once** in the bundle aggregator POM:

```xml
<version>${revision}</version>
<properties>
    <revision>0.1.0-SNAPSHOT</revision>
</properties>
```

Child modules (processors, NARs, etc.) reference the parent version as `${revision}` and inherit it automatically:

```xml
<parent>
    <groupId>com.snowflake.nifihub</groupId>
    <artifactId>nifi-<name>-bundle</artifactId>
    <version>${revision}</version>
</parent>
```

**To change a bundle's version**, update the `<revision>` property in the bundle's aggregator `pom.xml` only. The `flatten-maven-plugin` ensures that installed/deployed artifacts contain the resolved version.

**To override the version at build time** (without editing files), pass `-Drevision=<version>` on the command line.

## Adding a New Flow

1. Export the flow definition as JSON from NiFi
2. Place it at `flows/<bucket>/<flow-name>.json`
3. Create a companion `flows/<bucket>/<flow-name>.md` explaining the flow
4. Create validation tests at `flows/<bucket>/tests/test_<flow-name>.py`
5. Tests should validate JSON structure; integration tests require `NIFI_URL` env var

## Build Commands

```bash
# Build a specific bundle
./mvnw clean verify -f extensions/<category>/<sub-category>/nifi-<name>-bundle/pom.xml

# Build with quality checks (checkstyle + PMD + RAT)
./mvnw clean verify -Pcontrib-check -f <bundle-path>/pom.xml

# Build with code coverage
./mvnw clean verify -Preport-code-coverage -f <bundle-path>/pom.xml

# Run flow tests
pip install -r flows/requirements.txt
pytest flows/<bucket>/tests/ -v

# Run CD regression tests and offline flow tests
pytest scripts/cd/tests flows/ --import-mode=importlib -q

# Preview resource changes on an existing NiFi instance
python scripts/run-cd.py environments/local/config.yaml --dry-run
```

## Code Style

- All variables that can be `final` must be `final`
- No star imports
- No underscores in class names, variables, or filenames
- 200 character line width
- 4-space indentation
- Use SLF4J loggers, never System.out.println
- Use `.formatted()` for string formatting, not concatenation
- Include Apache 2.0 license header in all source files

## Testing

- Unit tests use `nifi-mock` framework via `TestRunner`
- Tests are named `Test<ClassName>.java` and live in `src/test/java/`
- Code coverage must be >= 80%
- Flow tests use `pytest` with `nipyapi`
- Live flow tests require `NIFI_URL`, `DEPLOYED_PG_ID`, and NiFi authentication through `NIFI_USERNAME`/`NIFI_PASSWORD` or `NIFI_TOKEN`
- CI cleanup removes only the temporary test process group and its isolated parameter contexts; installed NARs and explicitly configured shared services/providers remain available
