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

function Use-Poetry() { return [bool](Get-Command poetry -ErrorAction SilentlyContinue) }

function PyRun([string[]]$args) {
  if (Use-Poetry) { & poetry run python @args } else { & python @args }
}

function Set-EnvIfMissing([string]$name, [string]$value) {
  $current = [Environment]::GetEnvironmentVariable($name, "Process")
  if (-not $current -or $current -eq "") { [Environment]::SetEnvironmentVariable($name, $value, "Process") }
}

function Setup-PostgresEnv() {
  Set-EnvIfMissing "POSTGRES_USER" "cnpj"
  Set-EnvIfMissing "POSTGRES_PASSWORD" "cnpj"
  Set-EnvIfMissing "POSTGRES_HOST" "127.0.0.1"
  Set-EnvIfMissing "POSTGRES_PORT" "5432"
  Set-EnvIfMissing "POSTGRES_DATABASE" "cnpj"
  Set-EnvIfMissing "postgres_user" $env:POSTGRES_USER
  Set-EnvIfMissing "postgres_password" $env:POSTGRES_PASSWORD
  Set-EnvIfMissing "postgres_host" $env:POSTGRES_HOST
  Set-EnvIfMissing "postgres_port" $env:POSTGRES_PORT
  Set-EnvIfMissing "postgres_database" $env:POSTGRES_DATABASE
  Set-EnvIfMissing "PYTHONIOENCODING" "utf-8"
}

function Resolve-TargetDate() {
  try {
    $baseUrl = "https://arquivos.receitafederal.gov.br/dados/cnpj/dados_abertos_cnpj/"
    $resp = Invoke-WebRequest -Uri $baseUrl -UseBasicParsing -TimeoutSec 30
    $dates = @()
    foreach ($l in $resp.Links) {
      $href = $l.href
      if ($href -match '^[0-9]{4}-[0-9]{2}/?$') { $dates += ($href.TrimEnd('/')) }
    }
    if ($dates.Count -gt 0) {
      $sorted = $dates | Sort-Object -Descending
      return $sorted[0]
    }
  } catch {}
  return (Get-Date).ToString('yyyy-MM')
}

function Update-DotEnvTargetDate([string]$Value) {
  $path = Join-Path (Get-Location) ".env"
  if (Test-Path $path) {
    $lines = Get-Content -Path $path -Encoding UTF8
    $found = $false
    for ($i=0; $i -lt $lines.Count; $i++) {
      if ($lines[$i] -match '^\s*target_date\s*=') {
        $lines[$i] = "target_date=$Value"
        $found = $true
        break
      }
    }
    if (-not $found) { $lines += "target_date=$Value" }
    Set-Content -Path $path -Value ($lines -join [Environment]::NewLine) -Encoding UTF8
  } else {
    Set-Content -Path $path -Value ("target_date=$Value") -Encoding UTF8
  }
}

function Setup-TargetDate() {
  $td = Resolve-TargetDate
  [Environment]::SetEnvironmentVariable("TARGET_DATE", $td, "Process")
  [Environment]::SetEnvironmentVariable("target_date", $td, "Process")
  Update-DotEnvTargetDate -Value $td
  Write-Host ("[INFO] target_date = {0}" -f $td) -ForegroundColor Cyan
}

function Ensure-DbUp() {
  Write-Section "Database" "Cyan"
  docker compose up -d db
  $max = 30
  for ($i = 0; $i -lt $max; $i++) {
    docker compose exec -T db pg_isready -U cnpj -d cnpj | Out-Null
    if ($LASTEXITCODE -eq 0) { break }
    Start-Sleep -Seconds 2
  }
}

function Write-DotEnvIfMissing() {
  $path = Join-Path (Get-Location) ".env"
  if (-not (Test-Path $path)) {
    $content = @(
      "postgres_user=$($env:POSTGRES_USER)"
      "postgres_password=$($env:POSTGRES_PASSWORD)"
      "postgres_host=$($env:POSTGRES_HOST)"
      "postgres_port=$($env:POSTGRES_PORT)"
      "postgres_database=$($env:POSTGRES_DATABASE)"
    ) -join [Environment]::NewLine
    Set-Content -Path $path -Value $content -Encoding UTF8
  }
}

function Invoke-Step([string]$name, [ScriptBlock]$action, [bool]$AllowFail = $false) {
  Write-Host ("[START] $name") -ForegroundColor Yellow
  $swStep = [System.Diagnostics.Stopwatch]::StartNew()
  & $action
  $exit = $LASTEXITCODE
  $swStep.Stop()
  if (-not $exit) { $exit = 0 }
  if ($exit -ne 0) {
    Write-Host ("[FAIL] $name (codigo $exit) em {0:mm\:ss\.fff}" -f $swStep.Elapsed) -ForegroundColor Red
    if (-not $AllowFail) { exit $exit }
  } else {
    Write-Host ("[OK] $name em {0:mm\:ss\.fff}" -f $swStep.Elapsed) -ForegroundColor Green
  }
}

switch ($Task) {
  "install" {
    if (Use-Poetry) { poetry install --with dev } else {
      python -m pip install --upgrade pip
      pip install -r requirements.txt -r requirements-dev.txt
    }
  }
  "lint" {
    if (Use-Poetry) { poetry run ruff check . } else { ruff check . }
  }
  "check" {
    if (Use-Poetry) { poetry run python -m compileall -q src } else { python -m compileall -q src }
    if (Use-Poetry) { poetry run python -m compileall -q main.py } else { python -m compileall -q main.py }
  }
  "test" {
    if (Use-Poetry) { poetry run pytest -q } else { pytest -q }
  }
  "pipeline" {
    if (Use-Poetry) { poetry run python main.py } else { python main.py }
  }
  "etl" {
    $sw = [System.Diagnostics.Stopwatch]::StartNew()
    Setup-PostgresEnv
    Setup-TargetDate
    Ensure-DbUp
    Write-DotEnvIfMissing
    Write-Section "ETL"
    Invoke-Step "download" { if (Use-Poetry) { poetry run python -m src --step download } else { python -m src --step download } } $true
    Invoke-Step "extract" { if (Use-Poetry) { poetry run python -m src --step extract } else { python -m src --step extract } }
    Invoke-Step "consolidate" { if (Use-Poetry) { poetry run python -m src --step consolidate } else { python -m src --step consolidate } }
    Invoke-Step "load" { if (Use-Poetry) { poetry run python -m src --step load } else { python -m src --step load } }
    Invoke-Step "constraints" { if (Use-Poetry) { poetry run python -m src --step constraints } else { python -m src --step constraints } }
    $sw.Stop()
    Write-Host ("==== ETL concluido em {0:hh\:mm\:ss\.fff} ====" -f $sw.Elapsed) -ForegroundColor Magenta
  }
  "step" {
    if (-not $STEP) { Write-Error "STEP is required"; exit 2 }
    Write-Section ("Executando step: $STEP")
    Invoke-Step $STEP { if (Use-Poetry) { poetry run python -m src --step $STEP } else { python -m src --step $STEP } }
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
    $maxTries = 10
    for ($i = 0; $i -lt $maxTries; $i++) {
      docker compose exec db pg_isready -U cnpj -d cnpj
      if ($LASTEXITCODE -eq 0) { break }
      Start-Sleep -Seconds 3
    }
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

