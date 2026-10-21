# aws-smithy-managed-http

HTTP connection pooling with explicit managed execution placement.

The client module combines the native Smithy connection pool with a managed
runtime. Transport settings stay on the native pool builder, and callers obtain
its existing clients for explicitly chosen execution partitions. These clients
can be installed in AWS SDK configurations using Smithy's shared conversion.

Client construction requires the `client` feature. For HTTPS, enable a TLS provider
on `aws-smithy-http-client` and select it on the supplied native builder. Runtime
ownership and shutdown remain with the application.
