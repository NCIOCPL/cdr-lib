@ECHO OFF
ECHO testing locally with Python 2.7
python test-cdr.py %*
ECHO testing locally with Python 3.6
py test-cdr.py %*
