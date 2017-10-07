@ECHO OFF
ECHO testing tunneling with Python 2.7
setlocal
set TEST_CDR_TIER=DEV
python test-cdr.py %*
ECHO testing tunneling with Python 3.6
py test-cdr.py %*
endlocal
