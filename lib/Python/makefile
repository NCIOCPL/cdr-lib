#--------------------------------------------------------------------------
#
# $Id: makefile,v 1.1 2002-01-30 00:02:38 mruben Exp $
#
# $Log: not supported by cvs2svn $
#
#--------------------------------------------------------------------------

#--------------------------------------------------------------------------
# Makefile variables
#--------------------------------------------------------------------------

CDR		= $(DRV)/cdr
CVSUPD		= cvs update
CDRPYLIB	= $(CDR)/Lib/Python

CP		= cp -f
RM		= rm -f

PYTHON_MODULES	= cdr.py cdrdb.py cdrpub.py publish.py UnicodeToLatex.py
PYTHON_LIB_MODS	= $(CDRPYLIB)/cdr.pyc $(CDRPYLIB)/cdrdb.pyc \
		  $(CDRPYLIB)/cdrpub.pyc $(CDRPYLIB)/publish.pyc \
		  $(CDRPYLIB)/UnicodeToLatex.pyc

# phony all to prevent accidental installation by simply typing make
all:

cvsupdate:
	$(CVSUPD)

install:	cvsupdate
	$(MAKE) _install

_install:
	$(CP) $(PYTHON_MODULES) $(CDRPYLIB)
	$(RM) $(PYTHON_LIB_MODS)

