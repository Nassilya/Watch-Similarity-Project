@echo off

echo   Watch Similarity Project

set ROOT=%~dp0

echo.
echo [1/4] Scala MS1 - Parsing + Image Processing (Spark)...
pushd "%ROOT%microservice-1-scala"
call sbt run
if %errorlevel% neq 0 ( echo FAILED at Step 1 & exit /b 1 )
popd

echo.
echo [2/4] Python - CNN Feature Extraction...
pushd "%ROOT%python"
python model.py
if %errorlevel% neq 0 ( echo FAILED at Step 2 & exit /b 1 )
popd

echo.
echo [3/4] Scala MS2 - Scoring + Similarity (Spark)...
pushd "%ROOT%microservice-2-scala"
call sbt run
if %errorlevel% neq 0 ( echo FAILED at Step 3 & exit /b 1 )
popd

echo.
echo [4/4] Starting Streamlit...
pushd "%ROOT%frontend"
streamlit run streamlit_app.py
