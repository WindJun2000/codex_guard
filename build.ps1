# Build the Codex Guard plugin for Windows.
# Requires Go 1.26+, CGO enabled, and a C compiler (gcc/clang).

$ErrorActionPreference = "Stop"

$env:CGO_ENABLED = "1"
$outDir = Join-Path $PSScriptRoot "dist"
New-Item -ItemType Directory -Force -Path $outDir | Out-Null

Write-Host "Building codex-guard.dll ..."
go build -buildmode=c-shared -o (Join-Path $outDir "codex-guard.dll") .

Write-Host "Done: $outDir\codex-guard.dll"
