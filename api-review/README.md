# tm-api-review

Customer-scenario code for the transfer manager API review. Each file in `examples/` is one
scenario, written the way a customer would write it against the public API.

This is a standalone workspace (excluded from the repository workspace) so it resolves dependencies and
features like a downstream consumer.

```sh
cargo build --examples --manifest-path api-review/Cargo.toml
```
