#----------------------------------------------------------------------
#
# $Id: cdrutil.py -1   $
#
#----------------------------------------------------------------------
import MySQLdb, sys, time, os

# -----------------------------------------------
#
# -----------------------------------------------
def isProductionHost():
    fp = file('/etc/cdrtier.rc')
    rc = fp.read()
    fp.close()
    return rc.find('PROD') != -1

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
#def log(what, logfile='/weblogs/glossifier/glossifier.log'):
def log(what, logfile='/usr/local/cdr/log/cdr.log'):
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

#----------------------------------------------------------------------
# Run an external command.
#----------------------------------------------------------------------
def runCommand(command):
    commandStream = os.popen('%s 2>&1' % command)
    output = commandStream.read()
    code = commandStream.close()
    return CommandResult(code, output)


#----------------------------------------------------------------------
# Object for results of an external command.
#----------------------------------------------------------------------
class CommandResult:
    def __init__(self, code, output, error = None):
        self.code   = code
        self.output = output
        self.error  = error


# -----------------------------------------------------------
# Extract the valid document types for licensees from the
# driver document
# -----------------------------------------------------------
def getDocTypes(filename = '/home/cdroperator/prod/lib/pdq_files.txt'):
    sourceDoc = filename
    fileText = open(sourceDoc, 'r').readlines()

    docTypes = []

    for record in fileText:
         if record.split(':')[0] == 'FTPCDRPUB':
             docTypes.append(record.split(':')[1])

    return docTypes

#----------------------------------------------------------------------
# Extract the text content of a DOM element.
#----------------------------------------------------------------------
def getTextContent(node, recurse=False, separator=''):
    """
    Get text content for a node, possibly including sub nodes.

    Pass:
        node      - Node to be checked.
        recurse   - Flag indicating that all subnodes must be processed.
        separator - If multiple nodes, put this between them if we wish
                    to avoid jamming words together.
                    The separator is applied even if recurse=False.  It
                    also appears after the end of the last node.

    Return:
        Text content as a single string.
    """
    text = ''
    for child in node.childNodes:
        if child.nodeType in (child.TEXT_NODE, child.CDATA_SECTION_NODE):
            text = text + child.nodeValue + separator
        elif recurse and child.nodeType == child.ELEMENT_NODE:
            text = text + getTextContent(child, recurse, separator)
    return text


