Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

function Invoke-Step {
  param(
    [Parameter(Mandatory = $true)][string]$Name,
    [Parameter(Mandatory = $true)][scriptblock]$Script
  )

  Write-Host "==> $Name"
  & $Script
}

$scriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
$repoRoot = Resolve-Path (Join-Path $scriptDir "..\..")
Set-Location $repoRoot

Invoke-Step "Go build all packages" {
  go build ./...
}

Invoke-Step "Go test all packages" {
  go test ./...
}

$docker = Get-Command docker -ErrorAction SilentlyContinue
if ($null -eq $docker) {
  Write-Host "==> Docker not found; skipping Docker image build checks"
  exit 0
}

Invoke-Step "Docker build api image" {
  docker build -f Dockerfile -t goapplyjob-api-precommit .
}

Invoke-Step "Docker build workerchain image" {
  docker build -f Dockerfile.workerchain -t goapplyjob-workerchain-precommit .
}

Write-Host "All pre-commit checks passed."
