# aws-smithy-managed-http

HTTP connection pooling with explicit managed execution placement.

The client module combines the native Smithy connection pool with a managed
runtime. Transport settings stay on the native pool builder, and callers obtain
its existing clients for explicitly chosen execution partitions. The pool also
implements Smithy's HTTP client interface and can be installed directly in AWS
SDK configurations. Already configured native pools can be composed with an
execution handle through `ManagedHttpPool::from_pool`.

Client construction requires the `client` feature. For HTTPS, enable a TLS provider
on `aws-smithy-http-client` and select it on the supplied native builder. Runtime
ownership and shutdown remain with the application.
