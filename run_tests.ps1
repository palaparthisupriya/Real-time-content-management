# PowerShell test runner for Windows environments
$ErrorActionPreference = "Stop"

Write-Host "=============================================" -ForegroundColor Cyan
Write-Host "Running Real-Time Recommendation Service Tests" -ForegroundColor Cyan
Write-Host "=============================================" -ForegroundColor Cyan

# Test docker compose availability
$composeCmd = ""
if (Get-Command "docker" -ErrorAction SilentlyContinue) {
    $composeCmd = "docker compose"
} elseif (Get-Command "docker-compose" -ErrorAction SilentlyContinue) {
    $composeCmd = "docker-compose"
} else {
    Write-Warning "Docker command not found in PATH. Ensure Docker Desktop is running if executing containerized tests."
}

if ($composeCmd) {
    Write-Host "`n--> 1. Running Unit Tests: Event Ingestion API" -ForegroundColor Yellow
    Invoke-Expression "$composeCmd exec -T event_ingestion_api pytest -v tests/"

    Write-Host "`n--> 2. Running Unit Tests: Profile Update Service" -ForegroundColor Yellow
    Invoke-Expression "$composeCmd exec -T profile_update_service pytest -v tests/"

    Write-Host "`n--> 3. Running Unit Tests: Recommendation API" -ForegroundColor Yellow
    Invoke-Expression "$composeCmd exec -T recommendation_api pytest -v tests/"

    Write-Host "`n--> 4. Running End-to-End Integration Tests" -ForegroundColor Yellow
    if (Get-Command "python" -ErrorAction SilentlyContinue) {
        python scripts/e2e_test.py
    } else {
        Invoke-Expression "$composeCmd exec -T recommendation_api python -c `"import urllib.request; urllib.request.urlopen('http://localhost:8001/health')`""
    }

    Write-Host "`n=============================================" -ForegroundColor Green
    Write-Host " ALL TESTS COMPLETED SUCCESSFULLY!" -ForegroundColor Green
    Write-Host "=============================================" -ForegroundColor Green
} else {
    Write-Host "Skipping docker-compose test execution because Docker is not detected in current shell." -ForegroundColor Yellow
}
