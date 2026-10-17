# Hello World Flow

Generates a FlowFile, adds a configurable attribute with the example extension,
and logs its attributes:

`GenerateFlowFile → ExampleProcessor → LogAttribute`

Install the standard NiFi processors and `com.snowflake.nifihub:nifi-example-nar:0.3.0`
before importing this snapshot. The custom processor is implemented in the
example extension bundle. Referenced standard processor bundle versions must be
available on the target NiFi instance.

The flow has no parameter context. Import the JSON into NiFi or configure the
GitHub Flow Registry Client and deploy through an environment YAML. Structural
tests run offline. Live tests use an existing dedicated NiFi instance, with
`NIFI_URL`, NiFi credentials, and `DEPLOYED_PG_ID` set by the integration workflow.
The test YAML lists the example NAR release to install before importing the flow.
