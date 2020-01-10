@ECHO OFF
setlocal
set TESTPATH=%~dp0
set PYTHONPATH=%TESTPATH%\..
set TESTSCRIPT=%TESTPATH%\run-tests.py
ECHO.
ECHO testing locally
python %TESTSCRIPT% %*
endlocal
