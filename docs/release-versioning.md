# Release and Versioning

Version is stored in `Cargo.toml`.

## Bump version

```bash
./scripts/bump_version.sh patch
./scripts/bump_version.sh minor
./scripts/bump_version.sh major
./scripts/bump_version.sh 0.1.0
```

The script requires a clean workspace, updates the `ingest4x` version in `Cargo.toml`, and updates `Cargo.lock` when present.

Validation before bump:

```bash
cargo fmt --all -- --check
cargo clippy --all-targets --all-features -- -D warnings
cargo test
```

After checks pass, the script creates a version commit and pushes the current branch.

See `./scripts/bump_version.sh --help` for optional flags.

## Create release

```bash
./scripts/release.sh
```

Release script steps:

1. Ensure workspace is clean.
2. Read version from `Cargo.toml` and generate `vX.Y.Z` tag.
3. Check that no same tag exists locally or remotely.
4. Push branch and tag.
5. Create GitHub Release via GitHub CLI.

Binary artifacts are built by GitHub Actions and attached to the release; this script handles tagging and release creation.

See `./scripts/release.sh --help` for optional flags.
