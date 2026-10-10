# AWS ORT Execution

Managed execution and worker placement for AWS ORT (Oxidized Runtime).

The execution component owns workers and provides access to their execution
partitions. Hardware discovery and placement resources belong to
`aws-ort-topology`, re-exported here as `topology`. This boundary keeps execution
independent of HTTP transport and AWS service clients.
