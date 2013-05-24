#----------------------------------------------------------------------
#
# $Id: cdrutil.py -1   $
#
#----------------------------------------------------------------------
import MySQLdb, sys, time, os, socket

def translateTier(tier):
    """
    Convert internal system names (composers) to logical names of "tiers".

    If the name is unknown or if it's already okay, it's left alone.

    Pass:
        tier - name to look up.
    """
    # Resolve synomyms from OCE -> CBIIT names
    hiTier = tier.upper()
    if hiTier == 'BACH':
        tier = 'PROD'
    elif hiTier == 'MAHLER':
        tier = 'DEV'
    elif hiTier == 'FRANCK':
        tier = 'QA'

    # Return translated name, or original if no translation occurred
    return tier

#-----------------------------------------------------------
# Class describing all of the attributes of a host
#-----------------------------------------------------------
class HostProp:
    """
    Instead of having different functions to return different properties
    of a host connection, it would seem to make sense to have all of them
    just return an object that can be queried for anything desired.

    If more properties are added later, e.g., passwords, alternate names,
    etc., we can add them to the HostProp class without any change at
    all to the function interfaces.
    """
    def __init__(self, org, tier, use, name, domain):
        """
        Always initialize with all required fields
        """
        self.__org    = org
        self.__tier   = tier
        self.__use    = use
        self.__name   = name
        self.__domain = domain

    @property
    def org(self):    return self.__org

    @property
    def tier(self):   return self.__tier

    @property
    def use(self):    return self.__use

    @property
    def name(self):   return self.__name

    @property
    def domain(self): return self.__domain

    # This one does a bit of work, returning name.domain
    # e.g., "foobar.cdr.nih.gov"
    @property
    def qname(self):
        return "%s.%s" % (self.__name, self.__domain)


#-----------------------------------------------------------
# Class holding all network server names
#-----------------------------------------------------------
class AppHost:
    """
    Holds all of the network configuration information we have enabling
    us to use the correct network server name for each organization (CBIIT
    or OCE), each tier (DEV, QA, PROD, etc.) and each use (APP, DBxx, etc.)

    Instantiate this only once.  Anything else is redundant unless it uses
    a different config file.  Multiple instantiations are probably safe
    because Environment and Tier are independent of this config file, but
    it's hard to see a use case for multiple instances.

    Importing cdr.py will automatically instantiate it.  If cdr is imported,
    reference the instance using the module object "cdr.h".  Otherwise,
    instantiate it separately.
    """

    # Static dictionary, only loaded once, with all name config info
    #   key = (org, tier, use)
    #   value = (name, domain)
    lookup = {}

    # These variables are accessible through class or instance
    # Do NOT change them after __init__() runs unless you have a really
    #  good reason (I can't think what that could be.)
    org  = None  # 'CBIIT' or 'OCE'
    tier = None  # 'DEV', 'QA', 'PROD', maybe others
    host = {}    # '***REMOVED***', etc.

    # The actual names of the current machine, as reported by the OS
    # This variable doesn't come from the config file
    localhost   = None
    localname   = None
    localdomain = None

    def __init__(self, org, tier, filename = '/etc/cdrapphosts.rc'):
        """
        Load the lookup dictionary.
        Set variables for the current environment.
        """
        # Prevent multiple instantiations?
        # if AppHost.lookup:
        #   raise Exception("Attempt to instantiate multiple AppHost objects)

        # If the dictionary isn't loaded, load it
        if not AppHost.lookup:

            # These are class variables.
            AppHost.org  = org
            AppHost.tier = tier

            # Here's what the local OS tells us
            AppHost.localhost   = socket.getfqdn()
            dotPos              = AppHost.localhost.index('.')
            AppHost.localname   = AppHost.localhost[0:dotPos]
            AppHost.localdomain = AppHost.localhost[dotPos+1:]

            try:
                f = open(filename)
                while True:
                    row = f.readline()

                    # At the end
                    if not row:
                        break

                    # Remove whitespace, including trailing \n
                    row = row.strip()

                    # Skip blank lines and comments
                    if not row or row.startswith('#'):
                        continue

                    # Parse and store in the dictionary
                    # XXX Do I need to normalize case?
                    #     Or can I assume file is OK?
                    org, tier, use, name, domain = row.strip().split(":")
                    AppHost.lookup[(org,tier,use)] = (name, domain)

                    # Also update the shortcut dictionary for this host
                    # We can remove this later if everything using it
                    #  is converted to another technique.  The info is
                    #  already in the lookup dictionary.
                    if self.org == org and self.tier == tier:
                        AppHost.host[use] = (name, domain)

                f.close()
            except:
                raise Exception('Unable to read values from apphost file')


    def getAnyHostNames(self, org, tier, use):
        """
        Find and return the names for any desired host, anywhere.

        This is not normally used by applications.  See getHostNames()
        for more usable function.

        Pass:
            org  = CBIIT or OCE
            tier = DEV, QA, PROD, or synomyms 'mahler', 'franck', 'bach'
            use  = See cdrapphosts.rc

            Names are case insensitive.

        Return:
            HostProp (q.v.) instance with all info.
            If lookup fails:
                Return None.
        """
        # Resolve synomyms from OCE -> CBIIT names
        tier = translateTier(tier)

        # The HostProp object to return
        # Caller gets this if lookup fails and not 'localhost'
        retObj = None

        # Lookup
        try:
            namePair = AppHost.lookup[(org, tier, use)]
            retObj = HostProp(org, tier, use, namePair[0], namePair[1])
        except KeyError:
            # 'localhost' is a special case.  We don't want it in the
            #  config file because we'd need a different file for every
            #  server and, worse, if we got it wrong we'd be really sorry
            if tier == 'localhost':
                # "use" is just echoing back caller's parm.  There aren't
                # separate socket names for separate uses.
                retObj = HostProp(getEnvironment(), getTier(), use,
                                  AppHost.localname, AppHost.localdomain)

        # Results
        return retObj

    def getTierHostNames(self, tier, use):
        """
        Get an application host name for the current network environment.

        This is a front end to getAnyHostNames that defaults the org to
        CBIIT or OCE based on where it runs.  This will be the most
        common case.  When OCE goes away we can remove getAnyHostNames()
        and move the logic to here.

        Pass:
            See getAnyHostNames().

        Return:
            See getAnyHostNames().
        """
        return self.getAnyHostNames(getEnvironment(), tier, use)

    def getHostNames(self, use):
        """
        Most common case of getting names, current network environment,
        current server, just get the names for this use.  Another front-end
        to getAnyHostNames().

        Pass:
            See getAnyHostNames().

        Return:
            See getAnyHostNames().
        """
        return self.getAnyHostNames(getEnvironment(), getTier(), use)


#-----------------------------------------------------------
# Functions for characterizing the localhost
#-----------------------------------------------------------
def isProductionHost():
    """
    Is this server the production server?
    """
    if getTier() == 'PROD':
        return True
    return False

def getEnvironment():
    """
    Returns the name of the organization running the infrastructure
    on this server.  Currently either 'OCE' or 'CBIIT'

    Caches the file lookup in an AppHost class variable.
    """
    if not AppHost.org:
        try:
            fp = file('/etc/cdrenv.rc')
            rc = fp.read()
            AppHost.org = rc.upper().strip()
        except:
            AppHost.org = 'OCE'
    return AppHost.org

def getTier():
    """
    Returns the tier, 'DEV', 'QA', 'PROD', maybe others, of the current
    server.

    Caches the file lookup in an AppHost class variable.
    """
    if not AppHost.tier:
        try:
            fp = file('/etc/cdrtier.rc')
            rc = fp.read()
            AppHost.tier = rc.upper().strip()
        except:
            AppHost.tier = 'DEV'
    return AppHost.tier


#-----------------------------------------------------------
# Module constants
#-----------------------------------------------------------
DEBUG_LOG        = 1
OPERATOR         = ['***REMOVED***']
HTML_BASE        = "/PDQUpdate"
CGI_BASE         = HTML_BASE + "/cgi-bin"
IS_PROD_HOST     = isProductionHost()
PROD_HOST        = 'pdqupdate.cancer.gov'
DEV_HOST         = 'verdi.nci.nih.gov'
WEB_HOST         = IS_PROD_HOST and PROD_HOST or DEV_HOST
SMTP_RELAY       = "MAILFWD.NIH.GOV"

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


