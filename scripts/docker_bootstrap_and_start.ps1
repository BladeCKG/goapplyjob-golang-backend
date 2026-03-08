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

$dbUrl = $env:DATABASE_URL
if ([string]::IsNullOrWhiteSpace($dbUrl) -and (Test-Path ".env")) {
    $line = Get-Content ".env" | Where-Object { $_ -match "^DATABASE_URL=" } | Select-Object -Last 1
    if ($null -ne $line) {
        $dbUrl = ($line -replace "^DATABASE_URL=", "").Trim()
    }
}
if ([string]::IsNullOrWhiteSpace($dbUrl) -or -not ($dbUrl -match "^postgres(ql)?://")) {
    throw "DATABASE_URL must be set to a PostgreSQL URL in .env before docker bootstrap."
}

New-Item -ItemType Directory -Force -Path "logs" | Out-Null

docker compose run --rm api /app/migrate
docker compose --profile workers up -d --build
