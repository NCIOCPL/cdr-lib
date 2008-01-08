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
# $Revision: 1.3 $
#
# History:
# $Log: not supported by cvs2svn $
# Revision 1.2  2007/09/07 16:17:53  venglisc
# Replaced FTP process with Python ftplib commands.  Adjusting for more
# frequent publishing.
#
# Revision 1.1  2006/03/20 17:51:19  venglisc
# Initial copy of the script retrieving the statistics file for PDQ updates
# and reformatting its output to be included in the vendor notification
# message.
#
# ******************************************************************
import sys, os, ftplib, time

# Setting the variables
# ---------------------
tmpDir  = '/tmp'
pdqLog  = '/pdq/prod/log'
ftpFile = '%s/getchanges.ftp' % tmpDir
pubDir  = '/u/ftp/pub/pdq/full'

FTPSERVER = 'cipsftp.nci.nih.gov'
FTPUSER   = 'operator'
FTPPWD    = 'mars56'

now     = time.time()
lastWk  = time.time() - 5 * 24 * 60 * 60
relDate = time.strftime("%Y%V", time.localtime(lastWk))
relDateHdr = time.strftime("Week %V, %Y", time.localtime(lastWk))
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
print 'Getting the statistics files...'

try:
    ftpDir = '/u/ftp/pub/pdq/full'
    ftpFile = '%s' % (rchanges)
    ftp = ftplib.FTP(FTPSERVER)
    ftp.login(FTPUSER, FTPPWD)
    chCwd = ftp.cwd(pubDir)
    print ftp.pwd()
    # ftp.dir()
    print "%s" % chCwd
    os.chdir(pdqLog)
    print "FtpFile: %s" % ftpFile

    file = open(ftpFile, 'w')
    a = ftp.retrbinary('RETR %s' % ftpFile, file.write) # , file.write())
    print a
    file.close()
    print "Bytes transfered %d" % ftp.size(ftpFile)
except ftplib.Error, msg:
    print '*** FTP Error ***\n%s' % msg
    sys.exit(1)

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
sf.write('       ===================================\n\n')
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
