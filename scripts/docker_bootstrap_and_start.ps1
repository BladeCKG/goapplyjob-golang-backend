$ErrorActionPreference = "Stop"
$RootDir = (Resolve-Path (Join-Path $PSScriptRoot "..")).Path
Set-Location $RootDir

if (-not (Get-Command docker -ErrorAction SilentlyContinue)) {
    throw "docker is required but not found in PATH."
}

if (-not (Test-Path ".env") -and (Test-Path ".env.example")) {
    Copy-Item ".env.example" ".env"
    Write-Host "Created .env from .env.example. Review values before production use."
}

New-Item -ItemType Directory -Force -Path "logs" | Out-Null
New-Item -ItemType Directory -Force -Path "watcher_output" | Out-Null
if (-not (Test-Path "watcher_state.json")) {
    "{}" | Out-File -FilePath "watcher_state.json" -Encoding utf8
}

docker compose --profile workers up -d --build
