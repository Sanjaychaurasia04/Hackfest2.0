@echo off
echo.
echo =======================================
echo   SchemaLens AI - Starting Up
echo =======================================
echo.
echo Installing Python dependencies...
pip install -r backend\requirements.txt -q
echo.
echo Starting backend on http://localhost:8000 ...
echo (First run loads Olist CSVs - takes about 20 seconds)
echo.
echo Open the app at: http://localhost:8000
echo.
python -m uvicorn backend.main:app --host 0.0.0.0 --port 8000 --reload
pause
