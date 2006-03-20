#!/usr/bin/python
# ******************************************************************
# File Name: get_stats.py
#            ------------
# Script that gets the changes file from the FTP server for the 
# latest full load and creates a formatted TXT file for inclusion
# with the vendor notification message.
# Intermediate files are stored in the $PDQLOG directory.
# ------------------------------------------------------------------
# $Author: venglisc $      
# Created:              Volker Englisch  - 2006-03-20
# $Locker:  $
# 
# $Source: /usr/local/cvsroot/production/prod/bin/get_stats.py,v $
# $Revision: 1.1 $
#
# History:
# $Log: not supported by cvs2svn $
# ******************************************************************
import sys, os, time

# Setting the variables
# ---------------------
tmpDir  = '/tmp'
pdqLog  = '/pdq/prod/log'
ftpFile = '%s/getchanges.ftp' % tmpDir
pubDir  = '/u/ftp/pub/pdq/monthly'
FTPHOST = 'cipsftp.nci.nih.gov'

relDate = time.strftime("%Y-%b", time.localtime())
relDateHdr = time.strftime("%b %Y", time.localtime())
rchanges= '%s.changes'     % relDate
lchanges= '%s_changes.txt' % relDate

class CommandResult:                                                            
    def __init__(self, code, output):                                           
        self.code   = code                                                      
        self.output = output                                                    

def runCommand(command):                                                        
    commandStream = os.popen('%s 2>&1' % command)                               
    output = commandStream.read()                                               
    code = commandStream.close()                                                
    return CommandResult(code, output)         

# Creating the ftp files to perform the download
# ----------------------------------------------
print 'Building FTP file ...'

ftpCmd = open(ftpFile, 'w')
ftpCmd.write('debug -o %s/get_status.debug\n' % pdqLog)
ftpCmd.write('open %s\n' % FTPHOST)
ftpCmd.write('user operator mars56\n')
ftpCmd.write('cd %s\n' % pubDir)
ftpCmd.write('lcd %s\n' % pdqLog)
ftpCmd.write('get %s -o %s/%s \n' % (rchanges, pdqLog, rchanges))
ftpCmd.write('bye\n')
ftpCmd.close()

os.chmod(ftpFile, 0600)
print 'Ftp starting...'

doFtp = '/usr/bin/lftp -f ' + ftpFile
myCmd = runCommand(doFtp)

# Reading the data in
# -------------------
file = open(pdqLog + '/' + rchanges, 'r')
records = file.read()
file.close()

# Manipulating the data to create a formatted output file
# -------------------------------------------------------
lines = records.split()

stat = {}
change = {}
i = 0
for line in lines:
    i += 1
    mysplit = line.split(':')
    change[mysplit[1]] = mysplit[2]
    stat[mysplit[0]] = change
    if i % 3 == 0:
       change = {}


# Write the data to the log directory
# -----------------------------------
print 'Writing formatted changes file...'
sf = open(pdqLog + '/' + lchanges, 'w')
sf.write('\n\n       Changed Documents for %s\n' % relDateHdr)
sf.write('       ==============================\n\n')
sf.write('Document Type            added  modified  removed\n')
sf.write('---------------------  -------  --------  -------\n')

docType = stat.keys()
docType.sort()

for docs in docType:
   sf.write('%20s:  %7s  %8s  %7s\n' % (docs.replace('.' + relDate, ''), 
                                 stat[docs]['added'], 
                                 stat[docs]['modified'], 
                                 stat[docs]['removed']))
sf.write('\n')
sf.close()
print 'Done.'
