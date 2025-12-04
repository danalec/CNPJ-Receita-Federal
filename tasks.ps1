$ErrorActionPreference = "Stop"
Set-StrictMode -Version Latest

[CmdletBinding()] param(
  [Parameter(Mandatory = $true, Position = 0)]
  [ValidateSet("install","lint","check","test","pipeline","step")]
  [string]$Task,
  [Parameter(Mandatory = $false, Position = 1)]
  [string]$STEP
)

switch ($Task) {
  "install" {
    & poetry install --with dev
  }
  "lint" {
    & poetry run ruff check .
  }
  "check" {
    & poetry run python -m compileall -q src
    & poetry run python -m compileall -q main.py
  }
  "test" {
    & poetry run pytest -q
  }
  "pipeline" {
    & poetry run python main.py
  }
  "step" {
    if (-not $STEP) { Write-Error "STEP is required"; exit 2 }
    & poetry run python main.py --step $STEP
  }
}

