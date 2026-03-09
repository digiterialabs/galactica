param(
    [string]$EnrollmentToken = $env:GALACTICA_ENROLLMENT_TOKEN,
    [string]$EnrollmentTokenFile = "",
    [string]$ControlPlaneAddr = $(if ($env:GALACTICA_CONTROL_PLANE_ADDR) { $env:GALACTICA_CONTROL_PLANE_ADDR } else { "http://127.0.0.1:9090" }),
    [string]$AgentEndpoint = $(if ($env:GALACTICA_AGENT_ENDPOINT) { $env:GALACTICA_AGENT_ENDPOINT } else { "http://127.0.0.1:50061" })
)

$ErrorActionPreference = "Stop"

$ScriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
$RepoRoot = Resolve-Path (Join-Path $ScriptDir "..\..")
$ConfigPath = Join-Path $RepoRoot "config\dev\windows-node-agent.toml"

if (-not $EnrollmentToken -and $EnrollmentTokenFile) {
    $EnrollmentToken = (Get-Content -Raw $EnrollmentTokenFile).Trim()
}

if (-not $EnrollmentToken) {
    throw "Missing enrollment token. Pass -EnrollmentToken, set GALACTICA_ENROLLMENT_TOKEN, or use -EnrollmentTokenFile."
}

Push-Location $RepoRoot
try {
    cargo run -p galactica-node-agent -- `
        --config $ConfigPath `
        --enrollment-token $EnrollmentToken `
        --control-plane-addr $ControlPlaneAddr `
        --agent-endpoint $AgentEndpoint
}
finally {
    Pop-Location
}
