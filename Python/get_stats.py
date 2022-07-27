#!/usr/bin/python
# ******************************************************************
# File Name: get_stats.py
#            ------------
# Script that gets the changes file from the FTP server for the
# latest full load and creates a formatted TXT file for inclusion
# with the vendor notification message.
# Intermediate files are stored in the $PDQLOG directory.
# ------------------------------------------------------------------
# Created:              Volker Englisch  - 2006-03-20
# ******************************************************************
import datetime
import os
import shutil
import sys

# Setting the variables
# ---------------------
tmpDir = '/tmp'
PDQLOG = '/home/cdroperator/prod/log/pdq'
FTPBASE = '/u/ftp/cdr'
# FTPBASE = '/home/cdroperator/test'
FTPDIR = '%s/pub/pdq/full' % FTPBASE
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

rchanges = '%s.changes' % WEEK
lchanges = '%s_changes.txt' % WEEK


class CommandResult:
    def __init__(self, code, output):
        self.code = code
        self.output = output


def runCommand(command):
    commandStream = os.popen('%s 2>&1' % command)
    output = commandStream.read()
    code = commandStream.close()
    return CommandResult(code, output)


# Creating the ftp files to perform the download
# ----------------------------------------------
print('Getting the statistics files...')

ftpFile = '%s' % (rchanges)
os.chdir(PDQLOG)
print("FtpFile: %s" % ftpFile)

try:
    shutil.copy2('%s/%s' % (FTPDIR, ftpFile), '%s/%s' % (PDQLOG, ftpFile))
except Exception:
    print('***Error in get_stats')
    print('***   stats-file not found: %s' % ftpFile)
    print('***   Run fixISOweek in /u/ftp/cdr to recover')
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
print('Writing formatted changes file...')
sf = open(PDQLOG + '/' + lchanges, 'w')
sf.write('\n\n       Changed Documents for %s\n' % WEEKHDR)
sf.write('       ===================================\n\n')
sf.write('Document Type            added  modified  removed\n')
sf.write('---------------------  -------  --------  -------\n')

docType = sorted(stat.keys())

for docs in docType:
    args = (
        docs.replace('.' + WEEK, ''),
        stat[docs]['added'],
        stat[docs]['modified'],
        stat[docs]['removed'],
    )
    sf.write('%20s:  %7s  %8s  %7s\n' % args)
sf.write('\n')
sf.close()
print('Done.')
