<#
.SYNOPSIS
    Helper commands for the real-time fraud detection stack (Windows PowerShell 5.1+ / PowerShell 7).

.DESCRIPTION
    .\run.ps1 setup            Check Docker, create .env (random DB password), check dataset, build images
    .\run.ps1 download-data    Download the Kaggle dataset (needs %USERPROFILE%\.kaggle\kaggle.json)
    .\run.ps1 train            Retrain the model (forces a new run; restarts streaming if it is up)
    .\run.ps1 up               Start everything (first start trains the model) and wait until healthy
    .\run.ps1 down             Stop the stack (data, model and Kafka/Postgres volumes are kept)
    .\run.ps1 reset [-Force]   Stop and DELETE Kafka/Postgres/checkpoint volumes (asks first)
    .\run.ps1 test             Unit tests + coverage (in Docker)
    .\run.ps1 e2e              End-to-end test against the running stack
    .\run.ps1 logs [service]   Follow logs (all services or one)
    .\run.ps1 status           Show container status / health
    .\run.ps1 screenshots      Capture dashboard screenshots to docs\screenshots
    .\run.ps1 diagrams         Render docs\figures\architecture.mmd to SVG/PNG

.EXAMPLE
    .\run.ps1 setup
    .\run.ps1 up
    .\run.ps1 logs stream-processor
#>
[CmdletBinding()]
param(
    [Parameter(Position = 0)]
    [ValidateSet('help', 'setup', 'download-data', 'train', 'up', 'down', 'reset', 'test', 'e2e',
                 'logs', 'status', 'screenshots', 'diagrams')]
    [string]$Command = 'help',

    [Parameter(Position = 1)]
    [string]$Service = '',

    [switch]$Force
)

$ErrorActionPreference = 'Stop'
Set-Location -Path $PSScriptRoot

$LongRunningServices = @('kafka', 'postgres', 'kafka-ui', 'producer', 'stream-processor', 'dashboard')
$DatasetPath = Join-Path $PSScriptRoot 'data\raw\creditcard.csv'
$KaggleUrl = 'https://www.kaggle.com/datasets/mlg-ulb/creditcardfraud'

# ----------------------------------------------------------------------------- output helpers
function Write-Step([string]$Message) { Write-Host "==> $Message" -ForegroundColor Cyan }
function Write-Ok([string]$Message) { Write-Host "    [OK] $Message" -ForegroundColor Green }
function Write-Note([string]$Message) { Write-Host "    [!] $Message" -ForegroundColor Yellow }

# ----------------------------------------------------------------------------- native command helpers
function Invoke-Native {
    param([string]$Exe, [string[]]$Arguments)
    & $Exe @Arguments
    if ($LASTEXITCODE -ne 0) {
        throw "'$Exe $($Arguments -join ' ')' failed with exit code $LASTEXITCODE"
    }
}

function Invoke-Compose([string[]]$Arguments) {
    Invoke-Native -Exe 'docker' -Arguments (@('compose') + $Arguments)
}

function Get-NativeOutput {
    # Runs a native command, returns stdout lines (stderr discarded) and never throws.
    param([string]$Exe, [string[]]$Arguments)
    $previous = $ErrorActionPreference
    $ErrorActionPreference = 'Continue'
    try {
        $output = & $Exe @Arguments 2>$null
        return @{ Ok = ($LASTEXITCODE -eq 0); Lines = @($output) }
    } finally {
        $ErrorActionPreference = $previous
    }
}

# ----------------------------------------------------------------------------- checks
function Assert-Docker {
    if (-not (Get-Command docker -ErrorAction SilentlyContinue)) {
        throw 'Docker CLI not found. Install Docker Desktop (WSL2 backend): https://www.docker.com/products/docker-desktop/'
    }
    $info = Get-NativeOutput -Exe 'docker' -Arguments @('info', '--format', '{{.ServerVersion}}')
    if (-not $info.Ok) {
        throw 'Docker Desktop is not running. Start it and wait for "Engine running", then try again.'
    }
}

function Assert-EnvFile {
    if (-not (Test-Path (Join-Path $PSScriptRoot '.env'))) {
        throw '.env not found. Run: .\run.ps1 setup'
    }
}

function Test-Dataset([switch]$Required) {
    if (Test-Path $DatasetPath) {
        $sizeMb = [math]::Round((Get-Item $DatasetPath).Length / 1MB, 1)
        Write-Ok "Dataset found: data\raw\creditcard.csv ($sizeMb MB)"
        return $true
    }
    $message = "Dataset missing: data\raw\creditcard.csv`n" +
               "    1. Download it from $KaggleUrl (free Kaggle account).`n" +
               "    2. Extract creditcard.csv from archive.zip into data\raw\.`n" +
               "    Or, with a Kaggle API token: .\run.ps1 download-data"
    if ($Required) { throw $message }
    Write-Note $message
    return $false
}

function Test-ModelTrained {
    return (Test-Path (Join-Path $PSScriptRoot 'models\metrics.json')) -and
           (Test-Path (Join-Path $PSScriptRoot 'models\best_model\metadata'))
}

function Get-EnvValue([string]$Key, [string]$Default = '') {
    $envFile = Join-Path $PSScriptRoot '.env'
    if (-not (Test-Path $envFile)) { return $Default }
    $line = Get-Content $envFile | Where-Object { $_ -match "^\s*$Key\s*=" } | Select-Object -First 1
    if ($null -eq $line) { return $Default }
    $value = ($line -split '=', 2)[1].Trim()
    if ($value) { return $value }
    return $Default
}

# ----------------------------------------------------------------------------- .env creation
function New-EnvFile {
    $envFile = Join-Path $PSScriptRoot '.env'
    if (Test-Path $envFile) {
        Write-Ok '.env already exists (kept as is)'
        return
    }
    $bytes = New-Object byte[] 24
    [System.Security.Cryptography.RandomNumberGenerator]::Create().GetBytes($bytes)
    $password = ([Convert]::ToBase64String($bytes)) -replace '[^A-Za-z0-9]', ''

    $content = Get-Content (Join-Path $PSScriptRoot '.env.example') -Raw
    $content = $content -replace 'POSTGRES_PASSWORD=change-me', "POSTGRES_PASSWORD=$password"

    # Never ask Compose for more CPUs than Docker Desktop has.
    $ncpu = Get-NativeOutput -Exe 'docker' -Arguments @('info', '--format', '{{.NCPU}}')
    if ($ncpu.Ok -and $ncpu.Lines.Count -gt 0) {
        $cpus = [int]$ncpu.Lines[0]
        if ($cpus -lt 4) { $content = $content -replace 'TRAINER_CPUS=4', "TRAINER_CPUS=$cpus" }
        if ($cpus -lt 2) { $content = $content -replace 'STREAM_CPUS=2', "STREAM_CPUS=$cpus" }
    }

    # LF line endings, UTF-8 without BOM (Compose reads a BOM as part of the first key).
    $content = $content -replace "`r`n", "`n"
    [System.IO.File]::WriteAllText($envFile, $content, (New-Object System.Text.UTF8Encoding $false))
    Write-Ok '.env created with a random POSTGRES_PASSWORD'
}

# ----------------------------------------------------------------------------- health waiting
function Get-ServiceStates {
    $result = Get-NativeOutput -Exe 'docker' -Arguments @('compose', 'ps', '--all', '--format', 'json')
    $states = @{}
    if (-not $result.Ok) { return $states }
    $text = ($result.Lines -join "`n").Trim()
    if (-not $text) { return $states }
    if ($text.StartsWith('[')) {
        $items = $text | ConvertFrom-Json
    } else {
        $items = $result.Lines | Where-Object { $_ -and $_.Trim() } | ForEach-Object { $_ | ConvertFrom-Json }
    }
    foreach ($item in $items) { $states[$item.Service] = $item }
    return $states
}

function Wait-Healthy([int]$TimeoutSeconds = 900) {
    $deadline = (Get-Date).AddSeconds($TimeoutSeconds)
    $lastReport = ''
    while ((Get-Date) -lt $deadline) {
        $states = Get-ServiceStates
        $pending = @()
        foreach ($name in $LongRunningServices) {
            $state = $states[$name]
            if ($null -eq $state) { $pending += "$name (not created)"; continue }
            if ($state.State -eq 'exited' -or $state.State -eq 'dead') {
                throw "Service '$name' stopped (exit code $($state.ExitCode)). See: .\run.ps1 logs $name"
            }
            if ($state.State -ne 'running') { $pending += "$name ($($state.State))"; continue }
            if ($state.Health -and $state.Health -ne 'healthy') { $pending += "$name ($($state.Health))" }
        }
        if ($pending.Count -eq 0) {
            Write-Ok 'All services are running and healthy'
            return
        }
        $report = $pending -join ', '
        if ($report -ne $lastReport) { Write-Host "    waiting for: $report"; $lastReport = $report }
        Start-Sleep -Seconds 5
    }
    throw "Timed out after $TimeoutSeconds s waiting for healthy services. Check: .\run.ps1 status / .\run.ps1 logs"
}

function Show-Urls {
    $dashboard = Get-EnvValue 'DASHBOARD_HOST_PORT' '8501'
    $kafkaUi = Get-EnvValue 'KAFKA_UI_HOST_PORT' '8081'
    $sparkUi = Get-EnvValue 'SPARK_UI_HOST_PORT' '4040'
    $kafka = Get-EnvValue 'KAFKA_EXTERNAL_HOST_PORT' '9094'
    $pg = Get-EnvValue 'POSTGRES_HOST_PORT' '15432'
    Write-Host ''
    Write-Host '  Dashboard   : ' -NoNewline; Write-Host "http://localhost:$dashboard" -ForegroundColor Green
    Write-Host '  Kafka UI    : ' -NoNewline; Write-Host "http://localhost:$kafkaUi" -ForegroundColor Green
    Write-Host '  Spark UI    : ' -NoNewline; Write-Host "http://localhost:$sparkUi" -ForegroundColor Green
    Write-Host "  Kafka (host): localhost:$kafka     PostgreSQL (host): localhost:$pg"
    Write-Host ''
}

# ----------------------------------------------------------------------------- commands
function Invoke-Setup {
    Write-Step 'Checking Docker'
    Assert-Docker
    Write-Ok 'Docker Desktop is running'
    Write-Step 'Creating .env'
    New-EnvFile
    Write-Step 'Checking dataset'
    [void](Test-Dataset)
    Write-Step 'Building images (first build downloads ~1.5 GB, a few minutes)'
    Invoke-Compose @('build')
    Write-Ok 'Setup complete. Next: .\run.ps1 up'
}

function Invoke-DownloadData {
    Assert-Docker
    $kaggleDir = Join-Path $env:USERPROFILE '.kaggle'
    if (-not ((Test-Path (Join-Path $kaggleDir 'kaggle.json')) -or (Test-Path (Join-Path $kaggleDir 'access_token')))) {
        throw "No Kaggle API token in $kaggleDir. Create one at https://www.kaggle.com/settings (API section), or download the dataset manually from $KaggleUrl"
    }
    $env:KAGGLE_CONFIG_HOST_DIR = $kaggleDir
    Write-Step 'Downloading creditcard.csv with the Kaggle API'
    Invoke-Compose @('--profile', 'tools', 'run', '--rm', 'downloader')
    [void](Test-Dataset -Required)
}

function Invoke-Train {
    Assert-Docker; Assert-EnvFile
    [void](Test-Dataset -Required)
    # The producer keeps the test split in memory and the stream processor keeps the model.
    # Stop both first (this also frees memory for the 3 GB trainer) and recreate them after
    # training, so neither can keep replaying or scoring with stale artifacts.
    $states = Get-ServiceStates
    $streamingServices = @('producer', 'stream-processor')
    $wasRunning = @($streamingServices | Where-Object { $states[$_] -and $states[$_].State -eq 'running' })
    if ($wasRunning.Count -gt 0) {
        Write-Step "Stopping $($wasRunning -join ', ') during training"
        Invoke-Compose (@('stop') + $wasRunning)
    }
    Write-Step 'Training (4 models x 2 imbalance strategies; about 10 min)'
    Invoke-Compose @('run', '--rm', '--build', 'trainer', 'python', '-m', 'src.training.train_model', '--force')
    Write-Ok 'Model saved to models\ (results: docs\RESULTS.md, figures: docs\figures\)'
    if ($wasRunning.Count -gt 0) {
        Write-Step 'Recreating the producer and stream processor with the new model and test split'
        Invoke-Compose (@('up', '-d', '--force-recreate', '--no-deps') + $streamingServices)
    }
}

function Invoke-Up {
    Assert-Docker; Assert-EnvFile
    if (-not (Test-ModelTrained)) {
        [void](Test-Dataset -Required)
        Write-Note 'No trained model yet: the trainer runs first (about 10-15 min) before streaming starts.'
    }
    Write-Step 'Starting the stack'
    Invoke-Compose @('up', '-d', '--build')
    Write-Step 'Waiting for services to become healthy'
    Wait-Healthy
    Show-Urls
}

function Invoke-Reset {
    Assert-Docker
    if (-not $Force) {
        Write-Note 'This deletes the Kafka topics/messages, all stored predictions and the Spark checkpoint.'
        Write-Note 'Your dataset (data\) and trained model (models\) are NOT touched.'
        $answer = Read-Host '    Type "yes" to continue'
        if ($answer -ne 'yes') { Write-Host '    Cancelled.'; return }
    }
    Invoke-Compose @('down', '--volumes', '--remove-orphans')
    Write-Ok 'Stack stopped and volumes removed. Start again with: .\run.ps1 up'
}

function Invoke-Test {
    Assert-Docker; Assert-EnvFile
    Write-Step 'Running unit tests in Docker (coverage report: reports\coverage\index.html)'
    New-Item -ItemType Directory -Force -Path (Join-Path $PSScriptRoot 'reports') | Out-Null
    Invoke-Compose @('--profile', 'test', 'run', '--rm', '--build', 'tests')
}

function Invoke-E2E {
    Assert-Docker; Assert-EnvFile
    Write-Step 'Checking the stack is healthy'
    Wait-Healthy -TimeoutSeconds 300
    Write-Step 'Running end-to-end tests'
    Invoke-Compose @('--profile', 'e2e', 'run', '--rm', '--build', 'e2e')
}

function Invoke-Logs {
    $arguments = @('logs', '-f', '--tail', '200')
    if ($Service) { $arguments += $Service }
    Invoke-Compose $arguments
}

switch ($Command) {
    'help'          { Get-Help $PSCommandPath -Detailed }
    'setup'         { Invoke-Setup }
    'download-data' { Invoke-DownloadData }
    'train'         { Invoke-Train }
    'up'            { Invoke-Up }
    'down'          { Assert-Docker; Invoke-Compose @('down'); Write-Ok 'Stopped (volumes kept)' }
    'reset'         { Invoke-Reset }
    'test'          { Invoke-Test }
    'e2e'           { Invoke-E2E }
    'logs'          { Invoke-Logs }
    'status'        { Assert-Docker; Invoke-Compose @('ps', '--all') }
    'screenshots'   { Assert-Docker; Wait-Healthy -TimeoutSeconds 120; Invoke-Compose @('--profile', 'tools', 'run', '--rm', 'screenshots') }
    'diagrams'      { Assert-Docker; Invoke-Compose @('--profile', 'tools', 'run', '--rm', 'diagrams') }
}
