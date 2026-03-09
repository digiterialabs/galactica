# Galactica CLI Releases

`galactica-cli` release assets are published by `.github/workflows/release.yml`.

## One-line install

From a Galactica checkout on Apple Silicon macOS or Linux:

```bash
curl -fsSL https://raw.githubusercontent.com/digiterialabs/galactica/main/scripts/install.sh | sh
```

From a Galactica checkout on Windows PowerShell:

```powershell
irm https://raw.githubusercontent.com/digiterialabs/galactica/main/scripts/install.ps1 | iex
```

These bootstrap scripts:

- auto-detect the current host OS and CPU architecture
- resolve the matching `galactica-cli` asset from the latest GitHub Release
- install `galactica-cli` into a user-local `bin` directory
- create a `galactica` launcher bound to the current repo checkout

Current release coverage:

- Apple Silicon macOS
- Windows x64
- Linux x64

## Publish a release

Create and push a semver tag:

```bash
git tag v0.1.4
git push origin v0.1.4
```

That workflow builds native `galactica-cli` binaries for:

- `aarch64-apple-darwin`
- `x86_64-pc-windows-msvc`
- `x86_64-unknown-linux-gnu`

It packages them into release assets named like:

- `galactica-cli-v0.1.4-aarch64-apple-darwin.tar.gz`
- `galactica-cli-v0.1.4-x86_64-pc-windows-msvc.zip`

The publish job also uploads `SHA256SUMS.txt`.

## CLI installer behavior

`cargo run -p galactica-cli -- self-install` now tries to install from the latest GitHub Release first.

It only uses the release asset when:

- the repo's `workspace.package.repository` points at a GitHub repository
- the latest release tag matches `workspace.package.version`
- there is an asset matching the current host OS and CPU architecture

If any of those checks fail, `self-install` falls back to `cargo install --path rust/galactica-cli`.

At the moment, GitHub Releases publish assets for Apple Silicon macOS, Windows x64, and Linux x64. Intel macOS hosts still work, but `self-install` will fall back to `cargo install` because there is no x86_64 macOS release asset in this repo's workflow.
