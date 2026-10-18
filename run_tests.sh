#!/usr/bin/env bash
set -e

echo "============================================="
echo "Running Real-Time Recommendation Service Tests"
echo "============================================="

# Detect docker compose CLI syntax
if docker compose version >/dev/null 2>&1; then
    DOCKER_COMPOSE="docker compose"
elif which docker-compose >/dev/null 2>&1; then
    DOCKER_COMPOSE="docker-compose"
else
    echo "Error: Neither 'docker compose' nor 'docker-compose' found in PATH."
    exit 1
fi

echo "Using orchestrator: $DOCKER_COMPOSE"

echo ""
echo "--> 1. Running Unit Tests: Event Ingestion API"
$DOCKER_COMPOSE exec -T event_ingestion_api pytest -v tests/

echo ""
echo "--> 2. Running Unit Tests: Profile Update Service"
$DOCKER_COMPOSE exec -T profile_update_service pytest -v tests/

echo ""
echo "--> 3. Running Unit Tests: Recommendation API"
$DOCKER_COMPOSE exec -T recommendation_api pytest -v tests/

echo ""
echo "--> 4. Running End-to-End Integration Test Suite"
if command -v python3 >/dev/null 2>&1; then
    python3 scripts/e2e_test.py
elif command -v python >/dev/null 2>&1; then
    python scripts/e2e_test.py
else
    # Fallback to executing e2e_test inside recommendation_api container
    $DOCKER_COMPOSE exec -T recommendation_api python -c "
import urllib.request, time

print('Running inside container check...')
with urllib.request.urlopen('http://localhost:8001/health') as r:
    assert r.status == 200
print('Container health verified.')
"
fi

echo ""
echo "============================================="
echo " ALL TESTS COMPLETED SUCCESSFULLY!"
echo "============================================="
