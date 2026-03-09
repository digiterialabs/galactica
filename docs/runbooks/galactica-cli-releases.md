# Galactica CLI Releases

`galactica-cli` release assets are published by `.github/workflows/release.yml`.

## Publish a release

Create and push a semver tag:

```bash
git tag v0.1.1
git push origin v0.1.1
```

That workflow builds native `galactica-cli` binaries for:

- `aarch64-apple-darwin`
- `x86_64-apple-darwin`
- `x86_64-pc-windows-msvc`
- `x86_64-unknown-linux-gnu`

It packages them into release assets named like:

- `galactica-cli-v0.1.1-aarch64-apple-darwin.tar.gz`
- `galactica-cli-v0.1.1-x86_64-pc-windows-msvc.zip`

The publish job also uploads `SHA256SUMS.txt`.

## Installer behavior

`cargo run -p galactica-cli -- self-install` now tries to install from the latest GitHub Release first.

It only uses the release asset when:

- the repo's `workspace.package.repository` points at a GitHub repository
- the latest release tag matches `workspace.package.version`
- there is an asset matching the current host OS and CPU architecture

If any of those checks fail, `self-install` falls back to `cargo install --path rust/galactica-cli`.
