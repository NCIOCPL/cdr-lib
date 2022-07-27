@ECHO OFF
ECHO.
ECHO testing tunneling
setlocal
set TESTPATH=%~dp0
set PYTHONPATH=%TESTPATH%\..
set TESTSCRIPT=%TESTPATH%\run-tests.py
set TEST_MODE=remote
python -X dev %TESTSCRIPT% %*
endlocal
