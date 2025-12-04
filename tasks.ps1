param(
  [Parameter(Mandatory = $true, Position = 0)]
  [ValidateSet("install","lint","check","test","pipeline","etl","step","ci")]
  [string]$Task,
  [Parameter(Mandatory = $false, Position = 1)]
  [string]$STEP
)

$ErrorActionPreference = "Stop"

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
  "etl" {
    $sw = [System.Diagnostics.Stopwatch]::StartNew()
    & poetry run python main.py --step download
    & poetry run python main.py --step extract
    & poetry run python main.py --step consolidate
    & poetry run python main.py --step load
    & poetry run python main.py --step constraints
    $sw.Stop()
    Write-Host ("ETL concluído em {0:hh\:mm\:ss\.fff}" -f $sw.Elapsed)
  }
  "step" {
    if (-not $STEP) { Write-Error "STEP is required"; exit 2 }
    & poetry run python main.py --step $STEP
  }
  "ci" {
    try { py -3.10 -m venv .venv310 } catch {}
    if (Test-Path .venv310\Scripts\Activate.ps1) { . .\.venv310\Scripts\Activate.ps1 } else { python -m venv .venv; . .\.venv\Scripts\Activate.ps1 }
    python -m pip install --upgrade pip
    pip install -r requirements.txt -r requirements-dev.txt
    pytest -q -m "not integration" --maxfail=1 --disable-warnings --cov=src --cov-report=term-missing
    ruff check .
    mypy src
    docker build -t cnpj-receita-federal:ci .
    docker compose up -d db
    docker compose exec db pg_isready -U cnpj -d cnpj
    $env:PG_INTEGRATION = "1"
    $env:POSTGRES_USER = "cnpj"
    $env:POSTGRES_PASSWORD = "cnpj"
    $env:POSTGRES_HOST = "127.0.0.1"
    $env:POSTGRES_PORT = "5432"
    $env:POSTGRES_DATABASE = "cnpj"
    pytest -q -m integration --maxfail=1 --disable-warnings
    docker compose down -v
  }
}

