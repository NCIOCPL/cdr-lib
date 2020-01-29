@ECHO OFF
IF "%~1"=="" (GOTO USAGE) ELSE GOTO RUNTESTS
:USAGE
ECHO usage: test-tier.cmd TIER
GOTO END
:RUNTESTS
ECHO.
ECHO testing tunneling
setlocal
set TESTPATH=%~dp0
set PYTHONPATH=%TESTPATH%\..
set TESTSCRIPT=%TESTPATH%\run-tests.py
set TEST_TIER=%~1
python %TESTSCRIPT%
:END
endlocal
