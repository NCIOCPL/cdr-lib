@ECHO OFF
ECHO.
ECHO testing tunneling with Python 2.7
setlocal
set TESTPATH=%~dp0
set PYTHONPATH=%TESTPATH%\..
set TESTSCRIPT=%TESTPATH%\run-tests.py
set TEST_CDR_TIER=DEV
python %TESTSCRIPT% %*
ECHO.
ECHO testing tunneling with Python 3.6
py %TESTSCRIPT% %*
endlocal
