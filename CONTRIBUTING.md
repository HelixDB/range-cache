# Contributing

Contributions are welcome. Please include focused tests for behavioral changes
and run the same checks as CI:

```bash
cargo fmt --all -- --check
cargo test --no-default-features
cargo test --all-features
cargo clippy --all-targets --all-features -- -D warnings
RUSTDOCFLAGS="-D warnings" cargo test --doc --all-features
cargo publish --dry-run
```

For coverage, install `cargo-llvm-cov` and run:

```bash
cargo llvm-cov --all-features --workspace --fail-under-lines 95
```

Use a current stable toolchain for development. Changes must remain compatible
with the package MSRV declared in `Cargo.toml`.

