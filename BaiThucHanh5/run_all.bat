@echo off
title Camera Person Counting System Controller
:menu
cls
echo ========================================================
echo       CAMERA PERSON COUNTING SYSTEM CONTROLLER
echo ========================================================
echo  [1] Start Docker Infrastructure (Kafka, MongoDB, MongoExpress)
echo  [2] Setup Virtual Environment ^& Install Dependencies
echo  [3] Run Camera Server (Kafka Producer)
echo  [4] Run Processing Server (YOLOv8 Detector)
echo  [5] Run Storage Server (MongoDB Consumer)
echo  [6] Run Streamlit Dashboard (Real-time Web UI)
echo  [7] Run PySpark Analytics Script
echo  [8] Stop Docker Infrastructure (docker-compose down)
echo  [9] Exit
echo ========================================================
set /p choice="Enter your choice (1-9): "

if "%choice%"=="1" goto start_docker
if "%choice%"=="2" goto setup_env
if "%choice%"=="3" goto run_camera
if "%choice%"=="4" goto run_processing
if "%choice%"=="5" goto run_storage
if "%choice%"=="6" goto run_dashboard
if "%choice%"=="7" goto run_spark
if "%choice%"=="8" goto stop_docker
if "%choice%"=="9" goto end
goto menu

:start_docker
echo Starting Docker containers...
docker compose up -d
echo.
echo Verification:
docker ps
pause
goto menu

:setup_env
echo Creating virtual environment (venv) if not exists...
if not exist venv (
    python -m venv venv
)
echo Installing/Updating dependencies from requirements.txt...
call .\venv\Scripts\pip install -r requirements.txt
echo Dependencies installation completed.
pause
goto menu

:run_camera
echo Starting Camera Server...
echo Choose camera source:
echo  [1] Sample Video (Automatically downloaded if not exists)
echo  [2] Webcam (Device 0)
echo  [3] Synthetic Generator (Shapes fallback)
set /p src_choice="Select source (1-3): "
set src_arg=video
if "%src_choice%"=="2" set src_arg=webcam
if "%src_choice%"=="3" set src_arg=synthetic

cls
echo Starting Camera Server with source: %src_arg%...
call .\venv\Scripts\python camera_server.py --source %src_arg%
pause
goto menu

:run_processing
echo Starting Processing Server (YOLOv8 detector)...
call .\venv\Scripts\python processing_server.py
pause
goto menu

:run_storage
echo Starting Storage Server (MongoDB consumer)...
call .\venv\Scripts\python storage_server.py
pause
goto menu

:run_dashboard
echo Starting Streamlit Dashboard...
call .\venv\Scripts\streamlit run dashboard.py
pause
goto menu

:run_spark
echo Running PySpark Analytics Script...
call .\venv\Scripts\python spark_analytics.py
pause
goto menu

:stop_docker
echo Stopping Docker containers...
docker compose down
pause
goto menu

:end
echo Exiting. Goodbye!
