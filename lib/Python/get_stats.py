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
# $Revision: 1.5 $
#
# History:
# $Log: not supported by cvs2svn $
# Revision 1.4  2009/01/05 20:18:31  venglisc
# Corrected the calculation for 'lastWk' since it displayed last year instead
# of currect year (200801 instead 200901).
#
# Revision 1.3  2008/01/08 19:27:48  venglisc
# Modified Week format for weeks to display between 01...53 instead of
# 00...53.
#
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
import sys, os, ftplib, time, shutil, cdrutil, datetime

# Setting the variables
# ---------------------
tmpDir  = '/tmp'
PDQLOG  = '/home/cdroperator/prod/log/pdq'
FTPBASE = '/u/ftp/cdr'
# FTPBASE = '/home/cdroperator/test'
FTPDIR  = '%s/pub/pdq/full' % FTPBASE
ftpFile = '%s/getchanges.ftp' % tmpDir
# pubDir  = '/u/ftp/pub/pdq/full'

today = datetime.date.today()
one_day = datetime.timedelta(1)
one_week = datetime.timedelta(7)
last_week = today - one_week
year, week, weekday = last_week.isocalendar()
# year, week, weekday = today.isocalendar()
WEEK = "%04d%02d" % (year, week)
WEEKHDR = "Week %02d, %04d" % (week, year)

rchanges= '%s.changes'     % WEEK
lchanges= '%s_changes.txt' % WEEK

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

ftpFile = '%s' % (rchanges)
os.chdir(PDQLOG)
print "FtpFile: %s" % ftpFile

try:
    shutil.copy2('%s/%s' % (FTPDIR, ftpFile), '%s/%s' % (PDQLOG, ftpFile))
except:
    print '***Error in get_stats'
    print '***   stats-file not found: %s' % ftpFile
    print '***   Run fixISOweek in /u/ftp/cdr to recover'
    sys.exit(1)

# Reading the data in
# -------------------
file = open(PDQLOG + '/' + rchanges, 'r')
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
sf = open(PDQLOG + '/' + lchanges, 'w')
sf.write('\n\n       Changed Documents for %s\n' % WEEKHDR)
sf.write('       ===================================\n\n')
sf.write('Document Type            added  modified  removed\n')
sf.write('---------------------  -------  --------  -------\n')

docType = stat.keys()
docType.sort()

for docs in docType:
   sf.write('%20s:  %7s  %8s  %7s\n' % (docs.replace('.' + WEEK, ''), 
                                 stat[docs]['added'], 
                                 stat[docs]['modified'], 
                                 stat[docs]['removed']))
sf.write('\n')
sf.close()
print 'Done.'
