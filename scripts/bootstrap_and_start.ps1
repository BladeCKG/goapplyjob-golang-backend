param(
    [switch]$ForceRebuild
)

$ErrorActionPreference = "Stop"
$RootDir = (Resolve-Path (Join-Path $PSScriptRoot "..")).Path
Set-Location $RootDir

if (-not (Get-Command go -ErrorAction SilentlyContinue)) {
    throw "go is required but not found in PATH."
}

if (-not (Test-Path ".env") -and (Test-Path ".env.example")) {
    Copy-Item ".env.example" ".env"
    Write-Host "Created .env from .env.example. Review values before production use."
}

New-Item -ItemType Directory -Force -Path "logs" | Out-Null

go run ./cmd/migrate

if (Test-Path "logs\processes.json") {
    try {
        $existing = Get-Content "logs\processes.json" -ErrorAction Stop | ConvertFrom-Json
        foreach ($proc in @($existing)) {
            if ($null -ne $proc.pid) {
                Stop-Process -Id ([int]$proc.pid) -ErrorAction SilentlyContinue
                Write-Host ("Stopped previous process PID: {0}" -f $proc.pid)
            }
        }
    }
    catch {
        Write-Host "Could not parse logs\processes.json, continuing startup."
    }
}

if ($ForceRebuild) {
    go build ./cmd/api
    go build ./cmd/watcher
}

$services = @(
    @{ Name = "api"; Out = "logs\api.out.log"; Err = "logs\api.err.log"; Args = @("run", "./cmd/api") },
    @{ Name = "watcher"; Out = "logs\watcher.out.log"; Err = "logs\watcher.err.log"; Args = @("run", "./cmd/watcher") }
)

$started = @()
foreach ($svc in $services) {
    $proc = Start-Process -FilePath "go" -ArgumentList $svc.Args -WorkingDirectory $RootDir -RedirectStandardOutput $svc.Out -RedirectStandardError $svc.Err -PassThru
    $started += [pscustomobject]@{
        pid  = $proc.Id
        name = $svc.Name
    }
    Write-Host ("Started {0} (PID: {1})" -f $svc.Name, $proc.Id)
}

$started | ConvertTo-Json | Out-File -FilePath "logs\processes.json" -Encoding utf8
Write-Host ""
Write-Host "All services started."
Write-Host "PID file: logs\processes.json"
Write-Host "Stop all:"
Write-Host '$p = Get-Content logs\processes.json | ConvertFrom-Json; $p | ForEach-Object { Stop-Process -Id $_.pid -ErrorAction SilentlyContinue }'
