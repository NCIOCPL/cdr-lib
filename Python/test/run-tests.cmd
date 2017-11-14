@ECHO OFF
setlocal
set TESTPATH=%~dp0
CALL %TESTPATH%\test-local.cmd %*
CALL %TESTPATH%\test-remote.cmd %*
