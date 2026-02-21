# SchemaLens AI — PowerShell Startup Script
# Run this from the SchemaAI directory: .\start.ps1

Write-Host ""
Write-Host "=======================================" -ForegroundColor Cyan
Write-Host "   SchemaLens AI — Starting Up" -ForegroundColor Cyan
Write-Host "=======================================" -ForegroundColor Cyan
Write-Host ""

# Check Python
if (-not (Get-Command python -ErrorAction SilentlyContinue)) {
    Write-Host "ERROR: Python not found. Please install Python 3.9+" -ForegroundColor Red
    exit 1
}

$ScriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path

# Install dependencies
Write-Host "📦 Installing Python dependencies..." -ForegroundColor Yellow
pip install -r "$ScriptDir\backend\requirements.txt" -q

Write-Host ""
Write-Host "🚀 Starting backend on http://localhost:8000 ..." -ForegroundColor Green
Write-Host "   (Loading Olist CSVs into SQLite — takes ~20s on first run)" -ForegroundColor Gray
Write-Host ""
Write-Host "📊 Open the app at: http://localhost:8000" -ForegroundColor Cyan
Write-Host "   (Or open frontend\index.html directly if you prefer)" -ForegroundColor Gray
Write-Host ""
Write-Host "Press Ctrl+C to stop the server." -ForegroundColor Gray
Write-Host ""

# Start backend from project root so imports work
Set-Location $ScriptDir
python -m uvicorn backend.main:app --host 0.0.0.0 --port 8000 --reload
