#----------------------------------------------------------------------
#
# Wrappers for identifying server configuration values, and utilities
# specific to the CDR Linux servers.
#
#----------------------------------------------------------------------
import MySQLdb
import time
import os
import socket
import cdrpw
import urllib2
import urllib
import json
import pkg_resources
import hashlib
from email.mime.text import MIMEText
from email.header import Header
from email.utils import parseaddr, formataddr
import smtplib

class Settings:
    def __init__(self, db=None):
        self.db = db
        self.org = getEnvironment()
        self.tier = getTier()
        self.environ = dict(os.environ)
        self.path = [p for p in os.environ["PATH"].split(":")]
        self.hosts = self.get_hosts()
        self.python = self.get_python_settings()
        self.release = open("/etc/redhat-release").read().strip()
        self.mysql = self.get_mysql_settings()
        self.system = self.get_system_info()
        self.files = self.get_files()
    def get_files(self):
        files = {}
        site = { "emailers": "gpmailers" }.get(self.db, self.db)
        self.walk(files, "/web/%s" % site)
        self.walk(files, "/usr/local/cdr/lib/Python")
        return files
    def walk(self, files, path):
        for path, dirs, filenames in os.walk(path):
            directory = files
            for name in path.split("/")[1:]:
                if name not in directory:
                    directory[name] = {}
                directory = directory[name]
            for name in filenames:
                if not name.endswith(".pyc"):
                    self.add_file(path, name, directory)
    def add_file(self, path, name, files):
        try:
            path = "%s/%s" % (path, name)
            fp = open(path, "rb")
            bytes = fp.read()
            fp.close()
            md5 = hashlib.md5()
            md5.update(bytes)
            md5 = md5.hexdigest().lower()
        except Exception as e:
            md5 = "unreadable"
        files[name] = md5
    def get_system_info(self):
        info = {}
        for name in ("kernel-release", "kernel-version", "processor"):
            command = "uname --%s" % name
            try:
                value = runCommand(command).output.strip()
                info[name] = value
            except Exception as e:
                log("%s: %s" % (command, e))
        return info
    def get_hosts(self):
        try:
            hosts = {}
            h = AppHost(self.org, self.tier)
            for key in h.lookup:
                org, tier, use = key
                host = ".".join(h.lookup[key])
                if org not in hosts:
                    hosts[org] = {}
                if tier not in hosts[org]:
                    hosts[org][tier] = {}
                hosts[org][tier][use] = host
            return hosts
        except Exception as e:
            log("Settings.get_hosts(): %s" % e)
            return {}
    def get_python_settings(self):
        env = pkg_resources.Environment()
        settings = {}
        for name in env:
            for package in env[name]:
                settings[package.project_name] = package.version
        return settings
    def get_mysql_settings(self):
        if not self.db:
            return {}
        try:
            cursor = getConnection(self.db).cursor()
            cursor.execute("SHOW VARIABLES")
            return dict(cursor.fetchall())
        except Exception as e:
            log("Settings.get_mysql_settings(): %s" % e)
            return {}
    def serialize(self, indent=None):
        return json.dumps({
            "release": self.release,
            "environ": self.environ,
            "python": self.python,
            "path": self.path,
            "mysql": self.mysql,
            "tier": self.tier,
            "hosts": self.hosts,
            "org": self.org,
            "system": self.system,
            "files": self.files
        }, indent=indent)


def can_do(session, action, doctype="", tier=None):
    """
    Authorization check used on CDR Linux servers. Ask the Windows
    server for this tier to make the determination.

    Pass:
        session - ID of the user's current CDR session login
        action - what the user wants to do
        doctype - optional document type the user wants to do it with

    Return:
        True if the user can perform the specified action
        Otherwise false
    """

    org = getEnvironment()
    tier = tier or getTier()
    app_host = AppHost(org, tier)
    url = app_host.makeCdrCgiUrl(tier, "check-auth.py")
    parms = urllib.urlencode({
        "Session": session,
        "action": action,
        "doctype": doctype
    })
    try:
        response = urllib2.urlopen(url, parms)
        return response.read().strip() == "Y"
    except Exception as e:
        log("can_do(%s, %s, %s): %s" % (repr(session), repr(action),
                                        repr(doctype), e))
        log("url=%s" % repr(url))
        return False

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

    As new software is developed, use the cdrapi.settings.Tier class
    for this functionality.
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
    host = {}

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

    def makeCdrCgiUrl(self, tier, program, ssl='default', use='APPC'):
        """
        Make a URL that works for a particular tier and program.
        This uses the "APPC" name of the host.  URL's created with this
        routine won't work on our actual web servers unless forced to
        do so with the optional "use" parameter.

        Example:
            makeCdrCgiUrl('PROD', 'CTGov.py', 'Y')

        Pass:
            tier    - One of 'PROD', 'DEV', 'bach', etc.
            program - Name of the python script.
            ssl     - 'Y' or 'y' or 'yes' = Yes, use https.
                      'N' or 'n' or 'no'  = No, use http.
                      'default'           = Whatever default rule is in place.
            use     - One of the server use synonyms in cdrapphosts.rc.
                      default is 'APPC', the name used outside the internal
                      server infrastructure.
        Return:
            URL string.
            If we can't resolve the host, return an error message.
        """
        # Resolve names
        hostInfo = self.getTierHostNames(tier, use)
        if not hostInfo:
            return "*** Unable to resolve web host for tier=%s ***" % tier

        # Resolve protocol
        ssl = ssl.lower()[0]
        if ssl == 'y':
            protocol = "https"
        elif ssl == 'n':
            protocol = "http"

        # Default
        else:
            protocol = "https"

        return "%s://%s/cgi-bin/cdr/%s" % (protocol, hostInfo.qname, program)


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
    on this server.  Currently this is always 'CBIIT'
    """

    return "CBIIT"

def getTier(drive_prefix=""):
    """
    Returns the tier, 'DEV', 'QA', 'PROD', maybe others, of the current
    server.

    Caches the file lookup in an AppHost class variable.
    """
    if not AppHost.tier:
         drives = "DCEFGHIJKLMNOPQRSTUVWXYZ"
         drive_prefixes = [drive_prefix] + [drive + ":" for drive in drives]
         for drive_prefix in drive_prefixes:
             try:
                 with open(drive_prefix + "/etc/cdrtier.rc") as fp:
                     AppHost.tier = fp.read().upper().strip()
                     break
             except:
                 pass
         if not AppHost.tier:
             raise Exception("unable to find /etc/cdrtier.rc")
    return AppHost.tier


#-----------------------------------------------------------
# Module constants
#-----------------------------------------------------------
DEBUG_LOG        = 1
HTML_BASE        = "/PDQUpdate"
CGI_BASE         = HTML_BASE + "/cgi-bin"
IS_PROD_HOST     = isProductionHost()
PROD_HOST        = 'pdqupdate.cancer.gov'
DEV_HOST         = 'verdi.nci.nih.gov'
WEB_HOST         = IS_PROD_HOST and PROD_HOST or DEV_HOST

class EmailMessage:
    """
    Encapsulates the processing for assembling and sending rfc-822 messages.

    Includes full Unicode support. Possible future enhancement would add
    an add_part() method, which would wrap the existing MIMEText object
    inside a MIMEMultipart object and append another part to the message.
    """

    SMTP_RELAY = "MAILFWD.NIH.GOV"
    HEADER_CHARSET = "ISO-8859-1"

    def __init__(self, sender, recips, subject, body, subtype="plain"):
        """
        Assemble the pieces for the message.

        Don't plug in the headers to the message object yet, in case we
        implement the enhancement for multi-part mime messages at some
        point.
        """

        self.sender = self.format_address(sender)
        self.recips = [self.format_address(recip) for recip in recips]
        self.subject = Header(self.unicode(subject), self.HEADER_CHARSET)
        self.message = self.encode_body(body, subtype)

    def send(self):
        """
        Plug in the headers and send the message through the NIH mail server.

        Returns the ascii-serialized message.
        """

        self.message["From"] = self.sender
        self.message["To"] = ", ".join(self.recips)
        self.message["Subject"] = self.subject
        message = self.message.as_string()
        server = smtplib.SMTP(self.SMTP_RELAY)
        server.sendmail(self.sender, self.recips, message)
        server.quit()
        return message

    @classmethod
    def encode_body(cls, body, subtype):
        """Wrap string in message object (plain or html text).
        """

        body = cls.unicode(body)
        for charset in 'US-ASCII', 'ISO-8859-1', 'UTF-8':
            try:
                return MIMEText(body.encode(charset), subtype, charset)
            except UnicodeError:
                pass
        raise Exception("unable to encode message body")

    @classmethod
    def unicode(cls, string):
        """If the passed string isn't already unicode, make it so.
        """

        if isinstance(string, unicode):
            return string
        for encoding in ("utf-8", "iso-8859-1", "ascii"):
            try:
                return string.decode(encoding)
            except UnicodeDecodeError:
                pass
        return string.decode("ascii", "replace")

    @classmethod
    def format_address(cls, address):
        """Make an address SMTP-ready.

        Handles addresses with display portion containing non-ASCII
        characters. Will fail, however if the mailbox portion is not
        completely ASCII (that violates the standard -- or at least
        it used to). May need to relax that restriction in the future
        if support for RFC 6532 becomes more widespread.
        See https://tools.ietf.org/html/rfc6532.
        """

        name, addr = parseaddr(cls.unicode(address))
        name = str(Header(unicode(name), cls.HEADER_CHARSET))
        addr = addr.encode("ascii")
        return formataddr((name, addr))

    @staticmethod
    def test():
        """Standalone test method.
        """

        import argparse
        parser = argparse.ArgumentParser()
        sender = u"NCI PDQ\u00ae Operator <NCIPDQOperator@mail.nih.gov>"
        subject = u"Come and get it! \u2665"
        body = u"<p>Your PDQ&reg; data are ready!</p>"
        parser.add_argument("--sender", default=sender)
        parser.add_argument("--recips", nargs="+")
        parser.add_argument("--subject", default=subject)
        parser.add_argument("--body", default=body)
        parser.add_argument("--type", default="html")
        o = parser.parse_args()
        message = EmailMessage(o.sender, o.recips, o.subject, o.body, o.type)
        message.send()

#----------------------------------------------------------------------
# Send email to a list of recipients.
#----------------------------------------------------------------------
def sendMail(sender, recips, subject="", body="", html=False):
    if not recips:
        raise Exception("sendMail: no recipients specified")
    if not isinstance(recips, (list, tuple)):
        raise Exception("sendMail: recips must be sequence of email addresses")
    subtype = html and "html" or "plain"
    return EmailMessage(sender, recips, subject, body, subtype).send()

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
    if isinstance(fields, type({})):
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
def getConnection(db='emailers', drive_prefix=""):
    env = getEnvironment()
    tier = getTier()
    pw = cdrpw.password(env, tier, db)
    host_file = drive_prefix + "/etc/cdrapphosts.rc"
    appHost = AppHost(env, tier, filename=host_file)
    port = 3631
    if db == "glossifier":
        host = appHost.host["GLOSSIFIERDB"][0]
    else:
        host = appHost.host["EMAILERSDB"][0]
    conn = MySQLdb.connect(user=db, host=host, port=port, passwd=pw, db=db)
    conn.cursor().execute("SET NAMES utf8")
    return conn

#----------------------------------------------------------------------
# Print page to standard output and exit.
#----------------------------------------------------------------------
def sendPage(page, textType = 'html'):
    output = u"""\
Content-type: text/%s; charset=utf-8

%s""" % (textType, page)
    print(output.encode('utf-8'))
    exit(0)

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


