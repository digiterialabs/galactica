$ErrorActionPreference = "Stop"

$RepoRoot = $env:GALACTICA_REPO_ROOT
$InstallRoot = $env:GALACTICA_INSTALL_ROOT
$Repo = if ($env:GALACTICA_INSTALL_REPO) { $env:GALACTICA_INSTALL_REPO } else { "digiterialabs/galactica" }
$ReleaseApiUrl = $env:GALACTICA_CLI_RELEASE_API_URL

function Find-RepoRoot {
    param([string]$StartDir)

    $dir = Get-Item -LiteralPath $StartDir
    while ($null -ne $dir) {
        $cargoToml = Join-Path $dir.FullName "Cargo.toml"
        $cliDir = Join-Path $dir.FullName "rust/galactica-cli"
        if ((Test-Path -LiteralPath $cargoToml) -and (Test-Path -LiteralPath $cliDir)) {
            return $dir.FullName
        }
        $dir = $dir.Parent
    }
    return $null
}

if (-not $RepoRoot) {
    $RepoRoot = Find-RepoRoot -StartDir (Get-Location).Path
}

if (-not $RepoRoot) {
    throw "Failed to find the Galactica repo root. Run this from the checkout or set GALACTICA_REPO_ROOT."
}

if (-not (Test-Path -LiteralPath (Join-Path $RepoRoot "Cargo.toml"))) {
    throw "Repo root does not look like a Galactica checkout: $RepoRoot"
}

if (-not $InstallRoot) {
    $InstallRoot = Join-Path $env:LOCALAPPDATA "Galactica"
}

if (-not $ReleaseApiUrl) {
    $ReleaseApiUrl = "https://api.github.com/repos/$Repo/releases/latest"
}

$headers = @{
    "Accept" = "application/vnd.github+json"
}
if ($env:GITHUB_TOKEN) {
    $headers["Authorization"] = "Bearer $($env:GITHUB_TOKEN)"
}

$arch = [System.Runtime.InteropServices.RuntimeInformation]::OSArchitecture.ToString().ToLowerInvariant()
switch ($arch) {
    "x64" { $target = "x86_64-pc-windows-msvc"; $suffix = ".zip" }
    default { throw "Unsupported Windows architecture: $arch" }
}

$release = Invoke-RestMethod -Headers $headers -Uri $ReleaseApiUrl
$asset = $release.assets |
    Where-Object { $_.name -like "*galactica-cli*" -and $_.name -like "*$target*" -and $_.name -like "*$suffix" } |
    Select-Object -First 1
if (-not $asset) {
    throw "Failed to resolve a galactica-cli release asset for $target"
}

$checksumAsset = $release.assets | Where-Object { $_.name -eq "SHA256SUMS.txt" } | Select-Object -First 1

$tempDir = Join-Path ([System.IO.Path]::GetTempPath()) ("galactica-install-" + [System.Guid]::NewGuid().ToString("N"))
New-Item -ItemType Directory -Force -Path $tempDir | Out-Null
try {
    $archivePath = Join-Path $tempDir $asset.name
    $checksumPath = Join-Path $tempDir "SHA256SUMS.txt"
    Write-Host "Downloading $($asset.name) from $($release.tag_name)..."
    Invoke-WebRequest -Headers $headers -Uri $asset.browser_download_url -OutFile $archivePath
    if ($checksumAsset) {
        Invoke-WebRequest -Headers $headers -Uri $checksumAsset.browser_download_url -OutFile $checksumPath
    }

    if (Test-Path -LiteralPath $checksumPath) {
        $expected = Select-String -Path $checksumPath -Pattern ([regex]::Escape($asset.name) + '$') | Select-Object -First 1
        if ($expected) {
            $expectedHash = ($expected.Line -split '\s+')[0].Trim()
            $actualHash = (Get-FileHash -Algorithm SHA256 -LiteralPath $archivePath).Hash.ToLowerInvariant()
            if ($actualHash -ne $expectedHash.ToLowerInvariant()) {
                throw "Checksum mismatch for $($asset.name)"
            }
        }
    }

    $extractDir = Join-Path $tempDir "extract"
    Expand-Archive -LiteralPath $archivePath -DestinationPath $extractDir -Force
    $binary = Get-ChildItem -LiteralPath $extractDir -Recurse -Filter "galactica-cli.exe" | Select-Object -First 1
    if (-not $binary) {
        throw "Failed to find galactica-cli.exe in $($asset.name)"
    }

    $binDir = Join-Path $InstallRoot "bin"
    $binaryPath = Join-Path $binDir "galactica-cli.exe"
    $launcherPath = Join-Path $binDir "galactica.cmd"
    New-Item -ItemType Directory -Force -Path $binDir | Out-Null
    Copy-Item -LiteralPath $binary.FullName -Destination $binaryPath -Force

    @"
@echo off
"$binaryPath" --repo-root "$RepoRoot" %*
"@ | Set-Content -LiteralPath $launcherPath -Encoding ASCII

    Write-Host "Installed galactica-cli to $binaryPath"
    Write-Host "Installed galactica launcher to $launcherPath"
    if (($env:PATH -split ';') -notcontains $binDir) {
        Write-Host "Add $binDir to PATH:"
        Write-Host "  setx PATH `"%PATH%;$binDir`""
    }
    Write-Host "Next:"
    Write-Host "  galactica detect"
    Write-Host "  galactica doctor"
    Write-Host "  galactica install"
}
finally {
    if (Test-Path -LiteralPath $tempDir) {
        Remove-Item -LiteralPath $tempDir -Force -Recurse
    }
}
