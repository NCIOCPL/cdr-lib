@ECHO OFF
setlocal
set TESTPATH=%~dp0
set PYTHONPATH=%TESTPATH%\..
set TESTSCRIPT=%TESTPATH%\run-tests.py
ECHO.
ECHO testing locally with Python 2.7
python %TESTSCRIPT% %*
ECHO.
ECHO testing locally with Python 3.6
py %TESTSCRIPT% %*
endlocal
