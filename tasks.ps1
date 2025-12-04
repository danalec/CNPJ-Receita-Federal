param(
  [Parameter(Mandatory = $true, Position = 0)]
  [ValidateSet("install","lint","check","test","pipeline","etl","step","ci")]
  [string]$Task,
  [Parameter(Mandatory = $false, Position = 1)]
  [string]$STEP
)

$ErrorActionPreference = "Stop"

function Write-Section([string]$text, [string]$color = 'Cyan') {
  Write-Host ("==== $text ====") -ForegroundColor $color
}

function Invoke-Step([string]$name, [ScriptBlock]$action) {
  Write-Host ("» Iniciando $name") -ForegroundColor Yellow
  $swStep = [System.Diagnostics.Stopwatch]::StartNew()
  & $action
  $exit = $LASTEXITCODE
  $swStep.Stop()
  if (-not $exit) { $exit = 0 }
  if ($exit -ne 0) {
    Write-Host ("✖ $name falhou (código $exit) em {0:mm\:ss\.fff}" -f $swStep.Elapsed) -ForegroundColor Red
    exit $exit
  } else {
    Write-Host ("✔ $name concluído em {0:mm\:ss\.fff}" -f $swStep.Elapsed) -ForegroundColor Green
  }
}

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
    Write-Section "ETL"
    Invoke-Step "download" { poetry run python -m src --step download }
    Invoke-Step "extract" { poetry run python -m src --step extract }
    Invoke-Step "consolidate" { poetry run python -m src --step consolidate }
    Invoke-Step "load" { poetry run python -m src --step load }
    Invoke-Step "constraints" { poetry run python -m src --step constraints }
    $sw.Stop()
    Write-Host ("==== ETL concluído em {0:hh\:mm\:ss\.fff} ====" -f $sw.Elapsed) -ForegroundColor Magenta
  }
  "step" {
    if (-not $STEP) { Write-Error "STEP is required"; exit 2 }
    Write-Section ("Executando step: $STEP")
    Invoke-Step $STEP { poetry run python -m src --step $STEP }
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

