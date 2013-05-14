#!/usr/bin/python

#----------------------------------------------------------------------
#
# $Id: cdrutil.py 11558 2013-03-19 01:54:40Z volker $
#
#----------------------------------------------------------------------
import sys, time

# -----------------------------------------------
# -----------------------------------------------
def isProductionHost():
    fp = file('/etc/emailers.rc')
    rc = fp.read()
    fp.close();
    return rc.lower().find('production') != -1

# -----------------------------------------------
# -----------------------------------------------
def getEnvironment():
    try:
        fp = file('/etc/cdrenv.rc')
        rc = fp.read()
        return rc.upper().strip()
    except:
        return 'OCE'

# -----------------------------------------------
# -----------------------------------------------
def getTier():
    try:
        fp = file('/etc/cdrtier.rc')
        rc = fp.read()
        return rc.upper().strip()
    except:
        return 'DEV'

DEBUG_LOG        = 1
OPERATOR         = ['***REMOVED***']
HTML_BASE        = "/PDQUpdate"
CGI_BASE         = HTML_BASE + "/cgi-bin"
IS_PROD_HOST     = isProductionHost()
PROD_HOST        = 'pdqupdate.cancer.gov'
DEV_HOST         = 'verdi.nci.nih.gov'
WEB_HOST         = IS_PROD_HOST and PROD_HOST or DEV_HOST
SMTP_RELAY       = "MAILFWD.NIH.GOV"


# Reading the hostnames for the given CBIIT/OCE tier
# --------------------------------------------------
class AppHost:
    def __init__(self, org, tier, filename = '/etc/cdrapphosts.rc'):
        self.org = org   # CBIIT or OCE
        self.tier = tier # DEV, QA, CA, or PROD
        self.host = {}   # stores hostnames for the given tier

        # Read file (default: /etc/apphost) and filter by org/tier
        # --------------------------------------------------------
        try:
            f = open(filename, "r" )
            for row in f:
                try:
                    (organization, tier, use, name, domain) = \
                                                  row.strip().split( ":" )
                    if self.org == organization:
                        if self.tier == tier:
                            self.host[use] = [name, domain]
                except ValueError:
                    # XXX Should this pass?
                    pass

            f.close()
        except:
            raise Exception('Unable to read values from apphost file')


#----------------------------------------------------------------------
# Send email to a list of recipients.
#----------------------------------------------------------------------
def sendMail(sender, recips, subject = "", body = "", html = 0):
    if not recips:
        raise Exception("sendMail: no recipients specified")
    if type(recips) != type([]) and type(recips) != type(()):
        return Exception("sendMail: recipients must be a "
                         "list of email addresses")
    recipList = recips[0]
    for recip in recips[1:]:
        recipList += (",\n  %s" % recip)

    # Headers
    message = """\
From: %s
To: %s
Subject: %s
""" % (sender, recipList, subject)

    # Set content type for html
    if html:
        message += "Content-type: text/html; charset=utf-8\n"

    # Separator line + body
    message += "\n%s" % body

    # Send it
    import smtplib
    server = smtplib.SMTP(SMTP_RELAY)
    server.sendmail(sender, recips, message)
    server.quit()
    return message

#----------------------------------------------------------------------
# Write to a debug log.
#----------------------------------------------------------------------
def log(what, logfile='/weblogs/glossifier/glossifier.log'):
    logFile = logfile
    if DEBUG_LOG:
        f = open('%s' % logFile, 'a')
        f.write("%s: %s\n" % (time.strftime("%Y-%m-%d %H:%M:%S"), what))
        f.close()


#----------------------------------------------------------------------
# Wrap CGI fields in a dictionary (if not done already).
#----------------------------------------------------------------------
def wrapFieldsInMap(fields):
    if type(fields) == type({}):
        return fields
    fieldMap = {}
    #for key in fields.keys():
    #    fieldMap[key] = unicode(fields.getvalue(key, ""), "utf-8")
    # Faster version; if Guido ever hides the 'list' member, then
    # we'll have to revert to the version above:
    for field in fields.list:
        fieldMap[field.name] = unicode(field.value, "utf-8")
    return fieldMap

#----------------------------------------------------------------------
# Connect to the emailers database.
#----------------------------------------------------------------------
def getConnection(db = 'emailers'):

    # Don't call getConnection unless we're dealing with MySQL.
    import MySQLdb

    if getEnvironment() == 'CBIIT':
        h = AppHost(getEnvironment(), getTier(),
                               filename = '/etc/cdrapphosts.rc')
        conn = MySQLdb.connect(user = 'emailers',
                               host = h.host['DBNIX'][0], #'***REMOVED***-d',
                               port = 3600,
                               passwd = '***REMOVED***',
                               db = db)
    else:
        conn = MySQLdb.connect(user = 'emailers',
                               passwd = '***REMOVED***',
                               db = db)

    conn.cursor().execute("SET NAMES utf8")
    return conn

#----------------------------------------------------------------------
# Print page to standard output and exit.
#----------------------------------------------------------------------
def sendPage(page, textType = 'html'):
    output = u"""\
Content-type: text/%s; charset=utf-8

%s""" % (textType, page)
    print output.encode('utf-8')
    sys.exit(0)

#----------------------------------------------------------------------
# Used by Genetics Professional emailers to generate obfuscated URL.
#----------------------------------------------------------------------
def base36(n):
    if n == 0:
        return "0"
    elif n < 0:
        return '-' + base36(abs(n))
    lowestDigit = "0123456789abcdefghijklmnopqrstuvwxyz"[n % 36]
    higherDigits = n // 36
    if higherDigits == 0:
        return lowestDigit
    else:
        return base36(higherDigits) + lowestDigit
