:: setcdrmake.bat
::
:: Sets up variables for installing on my local machine. 

:: IMPORTANT: After extracting this file from CVS, rename it
::  before modifying it.  Otherwise you risk having CVS put your 
::  changes back to the repository and/or have someone else's changes
::  (who didn't read these instructions) clobber yours.

:: Uncomment and modify any of these if not using these include
::  files and libraries on mmdb2 (or wherever the CDR resides).
rem set CDR=n:/cdr

:: If any of the above variables are set, then you must either uncomment
::   the following line to tell make/nmake to override makefile
::   variables with environment variables (be careful about the
::   rest of your environment!), or use
::      make -e
rem set MAKEFLAGS=e

:: DRV sets a drive path before all of the above variables.
:: If finding CDR, XML4C, SABLOT, REGEX, BISON, BISON_SIMPLE on mmdb2, d:
::   just set DRV=<your__mapped_drive_leter:>.
:: DRV is NOT set in the makefile, so this is not an override and
::   does not require make -e or MAKEFLAGS.
set DRV=n:
