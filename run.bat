@echo off
setlocal enabledelayedexpansion

REM Script to run the Spark Connect example job against a running Spark Connect server on Windows
set "PROJECT_DIR=%~dp0"
set "JAR_FILE=%PROJECT_DIR%target\sparkconnect-1.0.0-SNAPSHOT-jar-with-dependencies.jar"
if not exist "%JAR_FILE%" (
    set "JAR_FILE=%PROJECT_DIR%target\sparkconnect-1.0.0-SNAPSHOT.jar"
)

set "INPUT_CSV=%PROJECT_DIR%src\main\resources\test.csv"
set "OUTPUT_DIR=%TEMP%\spark-connect-example-output"

if not defined SPARK_CONNECT_URL (
    set "SPARK_CONNECT_URL=sc://localhost:15002"
)

if not exist "%JAR_FILE%" (
    echo Jar file not found at: %JAR_FILE%
    echo Please build the project first: mvn clean package
    exit /b 1
)

where spark-submit >nul 2>nul
if %errorlevel% neq 0 (
    if defined SPARK_HOME (
        set "SPARK_SUBMIT_CMD=%SPARK_HOME%\bin\spark-submit.cmd"
    ) else (
        echo Error: spark-submit not found in PATH and SPARK_HOME is not set.
        echo Ensure SPARK_HOME is set and SPARK_HOME\bin is added to your PATH.
        exit /b 1
    )
) else (
    set "SPARK_SUBMIT_CMD=spark-submit"
)

if exist "%OUTPUT_DIR%" (
    echo Cleaning previous output directory at %OUTPUT_DIR%...
    rmdir /s /q "%OUTPUT_DIR%"
)

echo Connecting to Spark Connect Server at %SPARK_CONNECT_URL%...
echo Running Spark Connect job...
%SPARK_SUBMIT_CMD% --remote "%SPARK_CONNECT_URL%" --class com.uj.example.Example "%JAR_FILE%" "file:///%INPUT_CSV:\=/%" "%OUTPUT_DIR:\=/%"

if %errorlevel% equ 0 (
    echo Job finished successfully! Output written to: %OUTPUT_DIR%
) else (
    echo Spark job encountered an error. Exit code: %errorlevel%
    exit /b %errorlevel%
)
endlocal
