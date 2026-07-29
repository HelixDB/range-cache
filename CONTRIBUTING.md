# Contributing

Contributions are welcome. Please include focused tests for behavioral changes
and run the same checks as CI:

```bash
cargo fmt --all -- --check
cargo test --lib --tests --no-default-features
cargo test --all-features
cargo clippy --all-targets --all-features -- -D warnings
rustfmt --edition 2024 --check $(rg --files -g '*.rs' -g '!target/**' -g '!fuzz/target/**')
RUSTDOCFLAGS="-D warnings" cargo test --doc --no-default-features
RUSTDOCFLAGS="-D warnings" cargo test --doc --all-features
cargo bench --all-features --bench range_cache -- --test
cargo publish --dry-run
```

## Coverage

Install `cargo-llvm-cov`, then generate and check the source-normalized report:

```bash
cargo llvm-cov --all-features --workspace --json --output-path target/coverage.json
node scripts/check-source-coverage.mjs target/coverage.json
```

The checker requires 100% source function, line, and region coverage. LLVM emits
generic Rust functions once per test binary and raw summaries can count a source
region as missed in one monomorphization even when another executes it. The
checker collapses only identical source coordinates, using their maximum count;
it does not exclude files, functions, or regions.

## Fuzzing

The state-machine target uses only the public core API and checks every
operation against an independent dense reference model. Install the pinned tools
and reproduce the PR run with:

```bash
rustup toolchain install nightly-2026-02-17 --profile minimal
cargo install cargo-fuzz --version 0.13.2 --locked
cargo +nightly-2026-02-17 fuzz run range_cache_state_machine fuzz/corpus/range_cache_state_machine -- -runs=10000 -seed=0 -timeout=5 -max_len=4096
```

Reproduce and minimize a crash with:

```bash
cargo +nightly-2026-02-17 fuzz run range_cache_state_machine fuzz/artifacts/range_cache_state_machine/crash-...
cargo +nightly-2026-02-17 fuzz tmin range_cache_state_machine fuzz/artifacts/range_cache_state_machine/crash-...
```

Promote the minimized input into `fuzz/corpus/range_cache_state_machine/` and
add a readable deterministic regression test that captures the same failure.

## Miri

Run the core and private-invariant tests under the pinned nightly:

```bash
rustup toolchain install nightly-2026-02-17 --profile minimal --component miri,rust-src
cargo +nightly-2026-02-17 miri test --no-default-features --lib --test core
```

Use a current stable toolchain for development. Changes must remain compatible
with the package MSRV declared in `Cargo.toml`.
