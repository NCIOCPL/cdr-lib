#----------------------------------------------------------------------
#
# $Id: cdr.py,v 1.99 2004-11-05 05:54:35 ameyer Exp $
#
# Module of common CDR routines.
#
# Usage:
#   import cdr
#
# $Log: not supported by cvs2svn $
# Revision 1.98  2004/11/05 05:16:52  ameyer
# Added some new support for blobs, including zero length blobs (indicating
# that a blob should be deleted/dissociated from a document).
# Added makeDocBlob() function.
# Added some documentation to the Doc class.
#
# Revision 1.97  2004/10/14 22:05:20  ameyer
# If sendMail fails, log the error message as well as return it.
#
# Revision 1.96  2004/09/21 20:35:58  ameyer
# Changes to new diffXmlDoc() function and subroutines - supporting output
# of entire doc with changes in context, or differences only.
#
# Revision 1.95  2004/09/15 03:14:36  ameyer
# Changed getCDATA() to accept docs with no CDATA, returning them unmodified.
#
# Revision 1.94  2004/09/15 01:02:58  ameyer
# Added stripBlankLines() and diffXmlDocs().
#
# Revision 1.93  2004/08/27 13:47:46  bkline
# Added document status functions.
#
# Revision 1.92  2004/08/11 17:54:07  bkline
# Added new function addExternalMapping().
#
# Revision 1.91  2004/07/08 19:03:44  bkline
# Made logwrite() a little more bulletproof by ignoring all exceptions,
# not just the ones we expect.
#
# Revision 1.90  2004/06/30 20:44:56  ameyer
# Added new cacheInit() function.
#
# Revision 1.89  2004/05/17 16:17:37  bkline
# Modified getTextContent() to accomodate change in the parser's handling
# of CDATA sections.
#
# Revision 1.88  2004/05/17 15:21:11  bkline
# Added function getEmail().
#
# Revision 1.87  2004/05/06 18:40:47  ameyer
# Changed getPubPort to check for env var first, then default and batch
# pub ports.  Also checking to be sure there is a CdrServer listening on
# any port before returning it.
#
# Revision 1.86  2004/04/02 17:06:54  bkline
# Added (and used) new function _addRepDocActiveStatus().
#
# Revision 1.85  2004/03/31 13:29:04  bkline
# Made _addRepDocComment() function more robust in the face of unusual
# conditions.
#
# Revision 1.84  2004/02/26 21:03:40  bkline
# Expanded, generalized support for dynamic discovery of host name.
#
# Revision 1.83  2004/02/03 15:38:21  bkline
# Plugged in cgitb debugging help.
#
# Revision 1.82  2003/12/19 22:07:39  ameyer
# Added utf-8 encoding of filterDoc doc and parameters, in case of need.
#
# Revision 1.81  2003/11/04 17:00:18  bkline
# Added check to getErrors() to make sure we were passed a string.
#
# Revision 1.80  2003/11/04 16:55:54  bkline
# Fixed bug in regular expression to extract <Err> content.  Added
# option for extracting error strings as a sequence.
#
# Revision 1.78  2003/08/26 17:36:26  bkline
# Added new functions expandFilterSet() and expandFilterSets().
#
# Revision 1.77  2003/08/21 19:27:02  bkline
# Added functions for normalizing and comparing XML for CDR documents;
# added code to do XML escaping of character entities in control
# elements of Doc class when serializing.
#
# Revision 1.76  2003/07/29 13:02:03  bkline
# Added function to retrieve lists of valid values.  Changed CVSROOT
# to point to verdi.
#
# Revision 1.75  2003/04/26 16:32:36  bkline
# Eliminated assumptions about encoding for Doc class.
#
# Revision 1.74  2003/04/25 20:26:40  ameyer
# Added line each to addDoc, repDoc to ensure unicode->utf-8 encoding.
#
# Revision 1.73  2003/03/14 01:35:03  bkline
# Suppressed version attribute for filter in filterDoc() when version is
# empty.
#
# Revision 1.72  2003/02/24 21:18:35  bkline
# Added version attribute to Filter element.
#
# Revision 1.71  2003/02/10 17:21:40  bkline
# Added function mailerCleanup().
#
# Revision 1.70  2003/01/31 01:00:04  ameyer
# Modified _sysValue to distinguish between null value and "".
#
# Revision 1.69  2003/01/31 00:08:20  ameyer
# Added functions to add/replace/delete/get values in server sys_value table.
#
# Revision 1.68  2002/12/05 18:33:39  bkline
# Fixed some ternary logic syntax in the publish() command.
#
# Revision 1.67  2002/11/22 14:40:48  bkline
# Removed superfluous space before body in sendMail().
#
# Revision 1.66  2002/11/13 20:36:05  bkline
# Plugged in filter set mechanism for filterDoc().
#
# Revision 1.65  2002/11/13 16:57:54  bkline
# Added delFilterSet().
#
# Revision 1.64  2002/11/12 11:43:57  bkline
# Added filter set support.
#
# Revision 1.63  2002/10/29 21:00:16  pzhang
# Added allowInActive parameter to publish() to handle Hotfix-Remove.
#
# Revision 1.62  2002/10/24 19:57:19  ameyer
# Fixed bug in getQueryTermValueForId().
#
# Revision 1.61  2002/10/23 02:32:12  ameyer
# Made getQueryTermValueForId() return single sequence.
#
# Revision 1.60  2002/10/23 02:21:55  ameyer
# Added getQueryTermValueForId()
#
# Revision 1.59  2002/10/04 00:41:14  ameyer
# Enhanced logwrite to accept tuple or list.
#
# Revision 1.58  2002/10/01 21:29:21  ameyer
# Added parameter allowNonPub to publish().  Passes it to the server to
# allow the publishing system to publish documents not marked publishable.
# Fixed parameter error in call to traceback.print_exc().
#
# Revision 1.57  2002/09/19 18:04:24  ameyer
# Added check/convert for unicode comments in _addDocComment.
# Changed print_tb to print_exc to get more info in traceback log.
#
# Revision 1.56  2002/09/18 18:56:48  ameyer
# Fixed misspelling in comment.
#
# Revision 1.55  2002/09/18 18:28:43  ameyer
# Added traceback capability to logwrite.
#
# Revision 1.54  2002/09/15 16:58:53  bkline
# Replaced mmdb2 with mahler for CVS server name macro.
#
# Revision 1.53  2002/09/13 02:36:03  ameyer
# Fixed bug in valDoc, wrong attribute spelling.
#
# Revision 1.52  2002/09/12 20:59:26  bkline
# Added missing import for tempfile package.
#
# Revision 1.51  2002/09/12 20:47:49  bkline
# Added makeTempDir() function.
#
# Revision 1.50  2002/09/12 20:20:06  bkline
# Added runCommand function and accompanying CommandResult class.
#
# Revision 1.49  2002/09/12 00:46:14  bkline
# Added URDATE for final PDQ to CDR conversion of documents.
#
# Revision 1.48  2002/09/05 16:30:05  pzhang
# Added getPubPort().
#
# Revision 1.47  2002/09/02 00:37:22  bkline
# Added CVSROOT and PROD_HOST.
#
# Revision 1.46  2002/08/16 03:13:23  ameyer
# Added comment parameter to addDoc and repDoc.
# Added optional html formatting to sendMail.
# Made a number of trivial changes to reduce the number of warning messages
# produced by pychecker.
#
# Revision 1.45  2002/08/15 23:35:32  ameyer
# Added html parameter to sendMail.
# Made a number of trivial revisions to silence pychecker warnings.
#
# Revision 1.44  2002/07/31 05:03:11  ameyer
# Fixed idSessionUser.
#
# Revision 1.43  2002/07/25 17:21:30  bkline
# Added comment about the reason argument in addDoc/repDoc.
#
# Revision 1.42  2002/07/24 02:40:38  bkline
# Added PERL constant.
#
# Revision 1.41  2002/07/16 14:26:51  ameyer
# Added process id to logwrite, and changed format of message header.
#
# Revision 1.40  2002/07/11 21:04:31  ameyer
# Added date/time stamp to logwrite.
#
# Revision 1.39  2002/07/11 14:52:26  ameyer
# Added logwrite function.
#
# Revision 1.38  2002/07/05 20:55:04  bkline
# Added reindex() command.
#
# Revision 1.37  2002/07/02 23:49:21  ameyer
# Added extended cdr id normalizer exNormalize().
#
# Revision 1.36  2002/07/01 21:39:59  bkline
# Corrected filter to Filter in getDoctypes().
#
# Revision 1.35  2002/06/26 02:24:58  ameyer
# Added lastVersions().
#
# Revision 1.34  2002/06/18 22:19:16  ameyer
# Added canDo() to check authorization to do something.
#
# Revision 1.33  2002/06/08 02:02:35  bkline
# Added getCssFiles() (and changed some .* patterns to .*?).
#
# Revision 1.32  2002/05/14 12:56:55  bkline
# Added listVersions() function.
#
# Revision 1.31  2002/04/16 21:10:24  bkline
# Added missing %s argument in publish().
#
# Revision 1.30  2002/04/09 21:02:23  bkline
# Fixed typo in addDoc and repDoc (missing : after else).
#
# Revision 1.29  2002/04/09 20:19:53  bkline
# Modified addDoc() and repDoc to optionally return a tuple with the
# document ID string and any warnings.
#
# Revision 1.28  2002/04/02 20:03:17  bkline
# Added stub for pubStatus command; added docTime parameter to publish().
#
# Revision 1.27  2002/04/02 14:30:13  bkline
# Added publish command.
#
# Revision 1.26  2002/03/19 00:33:06  bkline
# Added validateOnly parameter to valDoc().
#
# Revision 1.25  2002/03/04 15:04:53  bkline
# Replaced verAttr with qual in filterDoc().
#
# Revision 1.24  2002/03/01 22:20:21  bkline
# Added docDate parameter to filterDoc() function.
#
# Revision 1.23  2002/02/27 20:27:08  bkline
# Removed extra logout function definition.
#
# Revision 1.22  2002/02/19 23:16:51  ameyer
# Eliminated SCRIPTS.  Now using BASEDIR - pointing to a more generic place.
#
# Revision 1.21  2002/02/19 22:09:40  bkline
# Added docVer parameter to filterDoc().
#
# Revision 1.20  2002/02/19 18:37:50  bkline
# Preserved docId passed to filterDoc if string.
#
# Revision 1.19  2002/02/15 06:56:31  ameyer
# Modified putLinkType to detect add/modify transactions in a different
# way.
#
# Revision 1.18  2002/02/14 21:42:14  mruben
# Fixed log comment [bkline for mruben].
#
# Revision 1.17  2002/02/14 21:25:49  mruben
# Added no_output option to filterDoc() [committed by RMK].
#
# Revision 1.16  2002/02/06 13:38:20  bkline
# Fixed definition of SCRIPTS.
#
# Revision 1.15  2002/01/31 21:39:26  bkline
# Exposed ability to pass a filter directly in memory as XML doc string.
#
# Revision 1.14  2002/01/22 22:30:59  bkline
# Added depth argument to getTree() function.
#
# Revision 1.13  2001/12/24 19:35:04  bkline
# Added valDoc function.
#
# Revision 1.12  2001/12/19 20:23:18  bkline
# Added options to doc save commands; added email support; added unlock()
# function.
#
# Revision 1.11  2001/10/04 14:34:49  bkline
# Added delDoc() function.
#
# Revision 1.10  2001/09/27 19:15:45  bkline
# Added constants for PYTHON and SCRIPTS.
#
# Revision 1.9  2001/09/17 16:08:39  bkline
# Fixed bug in filterDoc (added missing "</Parm>" tag.
#
# Revision 1.8  2001/08/08 18:23:49  mruben
# improved interface to CdrFilter
#
# Revision 1.7  2001/07/31 17:23:07  bkline
# Added versioning flag to addDoc() and repDoc() functions.
#
# Revision 1.6  2001/06/13 22:37:17  bkline
# Added DOM support.  Added QueryResult and Doc classes.  Added support
# for commands to manipulate the query_term_def table.
#
# Revision 1.5  2001/05/18 19:19:06  bkline
# Added routines for link management, schema documents, and adding
# document types.
#
# Revision 1.4  2001/05/03 20:17:11  bkline
# Stub versions of link command wrappers added.
#
# Revision 1.3  2001/04/08 22:50:06  bkline
# Replaced getTerm implementation with version that uses results from
# stored procedure.
#
# Revision 1.2  2001/04/08 16:31:53  bkline
# Added report, search, doctype, and term tree support.
#
#----------------------------------------------------------------------

#----------------------------------------------------------------------
# Import required packages.
#----------------------------------------------------------------------
import socket, string, struct, sys, re, cgi, base64, xml.dom.minidom
import os, smtplib, time, cdrdb, tempfile, traceback, difflib

#----------------------------------------------------------------------
# Set some package constants
#----------------------------------------------------------------------
DOMAIN_NAME   = 'nci.nih.gov'
PROD_NAME     = 'bach'
DEV_NAME      = 'mahler'
PROD_HOST     = '%s.%s' % (PROD_NAME, DOMAIN_NAME)
DEV_HOST      = '%s.%s' % (DEV_NAME, DOMAIN_NAME)
CVSROOT       = "verdi.nci.nih.gov:/usr/local/cvsroot"
DEFAULT_HOST  = 'localhost'
DEFAULT_PORT  = 2019
BATCHPUB_PORT = 2020
URDATE        = '2002-06-22'
LOGON_STRING  = """<CdrCommandSet><CdrCommand><CdrLogon>
                   <UserName>%s</UserName><Password>%s</Password>
                   </CdrLogon></CdrCommand>"""
LOGOFF_STRING = "<CdrCommand><CdrLogoff/></CdrCommand></CdrCommandSet>"
PYTHON        = "d:\\python\\python.exe"
PERL          = "d:\\bin\\Perl.exe"
BASEDIR       = "d:/cdr"
SMTP_RELAY    = "MAILFWD.NIH.GOV"
DEFAULT_LOGDIR  = "d:/cdr/Log"
DEFAULT_LOGFILE = DEFAULT_LOGDIR + "/debug.log"

#----------------------------------------------------------------------
# Find a port to the CdrServer, searching port numbers in the following
#
# Set TCP/IP port for publishing to value of CDRPUBPORT, if present,
# else DEFAULT_PORT, else BATCHPUB_PORT
#----------------------------------------------------------------------
def getPubPort():
    """
    Find a TCP/IP port to the CdrServer, searching port numbers in
    the following order:
        Value of environment variable "CDRPUBPORT".
            Typically used for testing/debugging software.
        DEFAULT_PORT (2019 at this time).
            The CDR is normally running on this port.
        BATCHPUB_PORT (2020 at this time).
            Typically used when 2019 is turned off to prevent users
            from running interactively during a publication job.
    Raises an error if there is no CdrServer listening on that port.
    """
    ports2check = (os.getenv("CDRPUBPORT"), DEFAULT_PORT, BATCHPUB_PORT)
    for port in ports2check:
        if port:
            try:
                # See if there's a CdrServer listening on this port
                sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                sock.connect((DEFAULT_HOST, port))
                sock.close()
                return port
            except:
                # No listener, keep trying
                pass

    # If we got here, we've tried all possibilities
    raise StandardError("No CdrServer found for publishing")

#----------------------------------------------------------------------
# Normalize a document id to form 'CDRnnnnnnnnnn'.
#----------------------------------------------------------------------
def normalize(id):
    if id is None: return None
    if type(id) == type(9):
        idNum = id
    else:
        digits = re.sub('[^\d]', '', id)
        idNum  = int(digits)
    return "CDR%010d" % idNum

#----------------------------------------------------------------------
# Extended normalization of ids
#----------------------------------------------------------------------
def exNormalize(id):
    """
    An extended form of normalize.
    Pass:
        An id in any of the following forms:
            12345
            'CDR0000012345'
            'CDR12345'
            'CDR0000012345#F1'
            '12345#F1'
            etc.
    Return:
        Tuple of:
            Full id string
            id as an integer
            Fragment as a string, or None if no fragment
        e.g.,
            'CDR0000012345#F1'
            12345
            'F1'
    Raise:
        StandardError if not a CDR ID.
    """

    if type(id) == type(9):
        # Passed a number
        idNum = id
        frag  = None

    else:
        # Parse the string
        pat = re.compile (r"(^(CDR0*)?)(?P<num>(\d+))\#?(?P<frag>(.*))$")
        # pat = re.compile (r"(?P<num>(\d+))\#?(?P<frag>(.*))")
        result = pat.search (id)

        if not result:
            raise StandardError ("Invalid CDR ID string: " + id)

        idNum = int (result.group ('num'))
        frag  = result.group ('frag')

    # Sanity check on number
    if idNum < 1:
        raise StandardError ("Invalid CDR ID number: " + str (idNum))

    # Construct full id
    fullId = "CDR%010d" % idNum
    if frag:
        fullId += '#' + frag

    return (fullId, idNum, frag)

#----------------------------------------------------------------------
# Send a set of commands to the CDR Server and return its response.
#----------------------------------------------------------------------
def sendCommands(cmds, host = DEFAULT_HOST, port = DEFAULT_PORT):

    # Connect to the CDR Server.
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.connect((host, port))

    # Send the commands to the server.
    sock.send(struct.pack('!L', len(cmds)))
    sock.send(cmds)

    # Read the server's response.
    (rlen,) = struct.unpack('!L', sock.recv(4))
    resp = ''
    while len(resp) < rlen:
        resp = resp + sock.recv(rlen - len(resp))

    # Clean up and hand the server's response back to the caller.
    sock.close()
    return resp

#----------------------------------------------------------------------
# Wrap a command in a CdrCommandSet element.
#----------------------------------------------------------------------
def wrapCommand(command, credentials):

    # If credentials is a tuple, then we have a userId/passWord pair.
    if type(credentials) == type(()):
        login = LOGON_STRING % credentials
        return "%s<CdrCommand>%s</CdrCommand>%s" % (login, command,
                                                    LOGOFF_STRING)

    # Otherwise we have a session ID for a user who's already logged in.
    cmds = """<CdrCommandSet><SessionId>%s</SessionId>
              <CdrCommand>%s</CdrCommand>
              </CdrCommandSet>""" % (credentials, command)
    return cmds

#----------------------------------------------------------------------
# Extract a single error element from XML response.
#----------------------------------------------------------------------
def checkErr(resp):
    if string.find(resp, "<Err>") != -1:
        expr = re.compile("<Err>(.*?)</Err>", re.DOTALL)
        err = expr.search(resp)
        err = err and err.group(1) or "Unknown failure"
        return err
    return None

#----------------------------------------------------------------------
# Extract error elements from XML.
#----------------------------------------------------------------------
def getErrors(xml, errorsExpected = 1, asSequence = 0):

    # Version which returns the errors in a list.
    if asSequence:
        if type(xml) not in (type(""), type(u"")):
            return []
        pattern = re.compile("<Err>(.*?)</Err>", re.DOTALL)
        errs = pattern.findall(xml)
        if errorsExpected and not errs:
            return ["Internal failure"]
        return errs

    # Make sure we have a string.
    if type(xml) not in (type(""), type(u"")):
        return ""

    # Compile the pattern for the regular expression.
    pattern = re.compile("<Errors[>\s].*</Errors>", re.DOTALL)

    # Search for the <Errors> element.
    errors  =  pattern.search(xml)
    if errors:           return errors.group()
    elif errorsExpected: return "<Errors><Err>Internal failure</Err></Errors>"
    else:                return ""

#----------------------------------------------------------------------
# Extract a piece of the CDR Server's response.
#----------------------------------------------------------------------
def extract(pattern, response):

    # Compile the regular expression.
    expr = re.compile(pattern, re.DOTALL)

    # Search for the piece we want.
    piece = expr.search(response)
    if piece: return piece.group(1)
    else:     return getErrors(response)

#----------------------------------------------------------------------
# Extract several pieces of the CDR Server's response.
#----------------------------------------------------------------------
def extract_multiple(pattern, response):

    # Compile the regular expression.
    expr = re.compile(pattern, re.DOTALL)

    # Search for the piece we want.
    piece = expr.search(response)
    if piece: return piece.groups()
    else:     return getErrors(response)

#----------------------------------------------------------------------
# Log in to the CDR Server.  Returns session ID.
#----------------------------------------------------------------------
def login(userId, passWord, host = DEFAULT_HOST, port = DEFAULT_PORT):

    # Send the login request to the server.
    cmds = LOGON_STRING % (userId, passWord) + "</CdrCommandSet>"
    resp = sendCommands(cmds, host, port)

    # Extract the session ID.
    return extract("<SessionId[^>]*>(.+)</SessionId>", resp)

#----------------------------------------------------------------------
# Identify the user associated with a session.
# Essentially a reverse login, provides info needed to re-login
# the same user with a new session.
# Note: This is a quick and dirty insecure function, but anyone with
#       access to it already has access to the same info via other means.
#       Future version may use mySession to secure this function.
# Pass:
#   mySession  - session for user doing the lookup - currently unused.
#   getSession - session to be looked up.
#
# Returns:
#   Tuple of (userid, password)
#   Or single error string.
#----------------------------------------------------------------------
def idSessionUser(mySession, getSession):

    # Direct access to db.  May replace later with secure server function.
    try:
        conn   = cdrdb.connect()
        cursor = conn.cursor()
    except cdrdb.Error, info:
        return "Unable to connect to database to get session info: %s" %\
                info[1][0]

    # Search user/session tables
    try:
        cursor.execute (\
            "SELECT u.name, u.password " \
            "  FROM usr u, session s " \
            " WHERE u.id = s.usr " \
            "   AND s.name = '%s'" % getSession)
        usrRow = cursor.fetchone()
        if type(usrRow)==type(()) or type(usrRow)==type([]):
            return usrRow
        else:
            # return "User unknown for session %s" % getSession
            return usrRow
    except cdrdb.Error, info:
        return "Error selecting usr for session: %s - %s" % \
                (getSession, info[1][0])

#----------------------------------------------------------------------
# Determine whether a session is authorized to do something
# Returns:
#   True (1) = Is authorized
#   False (0) = Is not authorized
#----------------------------------------------------------------------
def canDo (session, action, docType="",
           host=DEFAULT_HOST, port=DEFAULT_PORT):

    # Create the command
    cmd = wrapCommand ("""
 <CdrCanDo>
   <Action>%s</Action>
   <DocType>%s</DocType>
 </CdrCanDo>\n
""" % (action, docType), session)

    # Submit it
    resp = sendCommands (cmd, host, port);

    # Expected results are simple enough that we don't need DOM parse
    if resp.find ("<CdrCanDoResp>Y</CdrCanDoResp>") >= 0:
        return 1
    return 0

#----------------------------------------------------------------------
# Find information about the last versions of a document.
# Returns tuple of:
#   Last version number, or -1 if no versions
#   Last publishable version number or -1, may be same as last version.
#   Is changed information:
#     'Y' = last version is different from current working doc.
#     'N' = last version is not different.
# These are pass throughs of the response from the CdrLastVersions command.
# Single error string returned if errors.
#----------------------------------------------------------------------
def lastVersions (session, docId, host=DEFAULT_HOST, port=DEFAULT_PORT):

    # Create the command
    cmd = wrapCommand ("""
 <CdrLastVersions>
   <DocId>%s</DocId>
 </CdrLastVersions>
""" % docId, session)

    # Submit it
    resp = sendCommands (cmd, host, port)

    # Failed?
    errs = getErrors (resp, 0)
    if len (errs) > 0:
        return errs

    # Else get the parts we want
    lastAny   = extract ("<LastVersionNum>(.+)</LastVersionNum>", resp)
    lastPub   = extract ("<LastPubVersionNum>(.+)</LastPubVersionNum>", resp)
    isChanged = extract ("<IsChanged>(.+)</IsChanged>", resp)

    return (int(lastAny), int(lastPub), isChanged)

#----------------------------------------------------------------------
# Search the query term table for values
#----------------------------------------------------------------------
def getQueryTermValueForId (path, docId, conn = None):
    """
    Search for values pertaining to a particular path and id, or just
    for a particular path.
    Parameters:
        path  - query_term.path, i.e., name of an index.
        docId - limit search to specific id.
        conn  - use this connection, or create one if None.
    Return:
        Sequence of values.  May be None.
    Raises:
        StandardError if any failure.
    """
    # Create connection if none available
    if not conn:
        try:
            conn = cdrdb.connect ("CdrGuest")
        except cdrdb.Error, info:
            raise StandardError (
              "getQueryTermValueForId: can't connect to DB: %s" % info[1][0])

    # Normalize id to integer
    did = exNormalize(docId)[1]

    # Search table
    try:
        # Using % substitution because it should be completely safe and faster
        cursor = conn.cursor()
        cursor.execute (
          "SELECT value FROM query_term WHERE path = '%s' AND doc_id = %d" %
          (path, did))
        rows = cursor.fetchall()
        if len(rows) == 0:
            return None

        # Convert sequence of sequences to simple sequence
        retRows = []
        for row in rows:
            retRows.append (row[0])
        return retRows

    except cdrdb.Error, info:
        raise StandardError (
          "getQueryTermValueForId: database error: %s" % info[1][0])

#----------------------------------------------------------------------
# Extract the text content of a DOM element.
#----------------------------------------------------------------------
def getTextContent(node):
    text = ''
    for child in node.childNodes:
        if child.nodeType in (child.TEXT_NODE, child.CDATA_SECTION_NODE):
            text = text + child.nodeValue
    return text

#----------------------------------------------------------------------
# Encode a blob.
#----------------------------------------------------------------------
def makeDocBlob(blob=None, inFile=None, outFile=None, wrapper=None, attrs=""):
    """
    Encode a blob from either a string or a file in base64 with
    optional CdrDocBlob XML wrapper.

    This is a pretty trivial and probably unnecessary function, but
    it gives us a single point of control for constructing blobs.

    Parameters:
        blob=None       Blob as a string of bytes.  If None, use inFile.
                        An empty blob ("") is legal.  We return a null
                         string with the requested wrapper.
        inFile=None     Name of input file containing blob.  If None use blob.
        outFile=None    Write output to this file, overwriting whatever
                         may be there, if anything.  If None, return blob
                         as a string.
        wrapper=None    True=wrap blob in passed xml element tag.  Else not.
        attrs=None      Attribute string to include if passed wrapper.

    Returns:
        Base64 encoded blob if outFile not specified.
        Else returns empty string with output to file.

    Raises StandardError if invalid parms or bad file i/o.
    """
    # Check parms
    if blob == None and not inFile:
        raise StandardError("makeDocBlob: requires passed blob or inFile")
    if blob and inFile:
        raise StandardError("makeDocBlob: pass blob or inFile, not both")

    if inFile:
        # Get blob from file
        try:
            fp = open(inFile, "rb")
            blob = fp.read()
            fp.close()
        except IOError, info:
            raise StandardError("makeDocBlob: %s" % info)
        if not blob:
            raise StandardError("makeDocBlob: no data read from file %s" % \
                                 inFile)

    # Encode with or without wrapper
    startTag = endTag = ""
    if wrapper:
        startTag = "<" + wrapper
        if attrs:
            startTag += " " + attrs
        startTag += ">"
        endTag   = "</" + wrapper + ">"
    encodedBlob = startTag + base64.encodestring(blob) + endTag

    # Output
    if outFile:
        try:
            fp = open(outFile, "wb")
            fp.write(encodedBlob)
            fp.close()
        except IOError, info:
            raise StandardError("makeDocBlob: %s" % info)
        return ""
    return encodedBlob

#----------------------------------------------------------------------
# Object containing components of a CdrDoc element.
#
# NOTE: If the strings passed in for the constructor are encoded as
#       anything other than latin-1, you MUST provide the name of
#       the encoding used as the value of the `encoding' parameter!
#----------------------------------------------------------------------
class Doc:
    def __init__(self, x, type = None, ctrl = None, blob = None, id = None,
                 encoding = 'latin-1'):
        """
        An object encapsulating all the elements of a CDR document.

        Parameters:
            x           XML as utf-8 or Unicode string.
            type        Document type.
                         If passed, all other components of the Doc must
                          also be passed.
                         If none, then a CdrDoc must be passed with all
                          other components derived from the document string.
            ctrl        If type passed, dictionary of CdrDocCtl elements:
                         key = element name/tag
                         value = element text content
            blob        If type passed, blob, as a string of bytes will be
                          encoded as base64.  Should not be encoded already.
                         Else if CdrDocBlob is in the document string as a
                          base64 encoded string, it will be extracted as
                          the blob.
                         Else no blob.
            id          If type passed, document id, else derived from CdrDoc.
            encoding    Character encoding.  Must be accurate.  All
                         XML strings will be internally converted to utf-8.
        """
        # Two flavors for the constructor: one for passing in all the pieces:
        if type:
            self.id       = id
            self.ctrl     = ctrl or {}
            self.type     = type
            self.xml      = x
            self.blob     = blob
            self.encoding = encoding
        # ... and the other for passing in a CdrDoc element to be parsed.
        else:
            if encoding.lower() != 'utf-8':
                # Have to do this because of poor choice of parameter name
                # 'type'.  Ouch! :-<}
                if x.__class__ == u"".__class__:
                    x = x.encode('utf-8')
                else:
                    x = unicode(x, encoding).encode('utf-8')
            self.encoding = encoding
            self.ctrl     = {}
            self.xml      = ''
            self.blob     = None
            docElem       = xml.dom.minidom.parseString(x).documentElement
            self.id       = docElem.getAttribute('Id').encode('ascii') or None
            self.type     = docElem.getAttribute('Type').encode(
                                                              'ascii') or None
            for node in docElem.childNodes:
                if node.nodeType == xml.dom.minidom.Node.ELEMENT_NODE:
                    if node.nodeName == 'CdrDocCtl':
                        self.parseCtl(node)
                    elif node.nodeName == 'CdrDocXml':
                        self.xml = getTextContent(node).encode(encoding)
                    elif node.nodeName == 'CdrDocBlob':
                        self.extractBlob(node)
    def parseCtl(self, node):
        """
        Parse a CdrDocCtl node to extract all its elements into the ctrl
        dictionary.

        Pass:
            DOM node for CdrDocCtl.
        """
        for child in node.childNodes:
            if child.nodeType == xml.dom.minidom.Node.ELEMENT_NODE:
                self.ctrl[child.nodeName.encode('ascii')] = \
                    getTextContent(child).encode(self.encoding)

    def extractBlob(self, node):
        """
        Extract a base64 encoded blob from the XML string.

        Pass:
            DOM node for CdrDocBlob.
        """
        encodedBlob = getTextContent(node)
        self.blob   = base64.decodestring(encodedBlob.encode('ascii'))

    def __str__(self):
        """
        Serialize the object into a single CdrDoc XML string.

        Return:
            utf-8 encoded XML string.
        """
        alreadyUtf8 = self.encoding.lower() == "utf-8"
        rep = "<CdrDoc Type='%s'" % self.type
        if self.id: rep += " Id='%s'" % self.id
        rep += "><CdrDocCtl>"
        for key in self.ctrl.keys():
            value = self.ctrl[key]
            if not alreadyUtf8:
                value = unicode(value, self.encoding).encode('utf-8')
            rep += "<%s>%s</%s>" % (key, cgi.escape(value), key)
        rep += "</CdrDocCtl>\n"
        xml = self.xml
        if xml:
            if not alreadyUtf8:
                xml = unicode(self.xml, self.encoding).encode('utf-8')
            rep += "<CdrDocXml><![CDATA[%s]]></CdrDocXml>" % xml
        if self.blob != None:
            rep += makeDocBlob(self.blob, wrapper="CdrDocBlob")
        rep += "</CdrDoc>"
        return rep

#----------------------------------------------------------------------
# Internal subroutine to add or replace DocComment element in CdrDocCtl.
#----------------------------------------------------------------------
def _addRepDocComment(doc, comment):

    """
    Add or replace DocComment element in CdrDocCtl.
    Done by text manipulation.

    Pass:
        doc - Full document in CdrDoc format.
        comment - Comment to insert.

    Return:
        Full CdrDoc with DocComment element inserted or replaced.

    Assumptions:
        Both doc and comment must be UTF-8.  (Else must add conversions here.)
    """

    # Sanity check.
    if not doc:
        raise StandardError("_addRepDocComment(): missing doc argument")

    # Search for and delete existing DocComment
    delPat = re.compile (r"\n*<DocComment.*</DocComment>\n*", re.DOTALL)
    newDoc = delPat.sub ('', doc).replace('<DocComment/>', '')

    # Search for CdrDocCtl to insert new DocComment after it
    newDoc = newDoc.replace('<CdrDocCtl/>', '<CdrDocCtl></CdrDocCtl>')
    insPat = re.compile (r"(?P<first>.*<CdrDocCtl[^>]*>)\n*(?P<last>.*)",
                         re.DOTALL)
    insRes = insPat.search (newDoc)
    if insRes:
        parts = insRes.group ('first', 'last')
    if not insRes or len (parts) != 2:
        # Should never happen unless there's a bug
        raise StandardError ("addRepDocComment: No CdrDocCtl in doc:\n%s" %
                             doc)

    # Comment must be compatible with CdrDoc utf-8
    if type(comment) == type(u""):
        comment = comment.encode('utf-8')

    # Insert comment
    return (parts[0] + "\n<DocComment>"+comment+"</DocComment>\n" + parts[1])

#----------------------------------------------------------------------
# Internal subroutine to add or replace DocActiveStatus element in CdrDocCtl.
#----------------------------------------------------------------------
def _addRepDocActiveStatus(doc, newStatus):

    """
    Add or replace DocActiveStatus element in CdrDocCtl.
    Done by text manipulation.

    Pass:
        doc - Full document in CdrDoc format.
        newStatus - 'I' or 'A'.

    Return:
        Full CdrDoc with DocActiveStatus element inserted or replaced.

    Assumptions:
        Both doc and comment must be UTF-8.  (Else must add conversions here.)
    """

    # Sanity check.
    if not doc:
        raise StandardError("_addRepDocActiveStatus(): missing doc argument")

    # Search for and delete existing DocComment
    delPat = re.compile (r"\n*<DocActiveStatus.*</DocActiveStatus>\n*",
                         re.DOTALL)
    newDoc = delPat.sub ('', doc).replace('<DocActiveStatus/>', '')

    # Search for CdrDocCtl to insert new DocComment after it
    newDoc = newDoc.replace('<CdrDocCtl/>', '<CdrDocCtl></CdrDocCtl>')
    insPat = re.compile (r"(?P<first>.*<CdrDocCtl[^>]*>)\n*(?P<last>.*)",
                         re.DOTALL)
    insRes = insPat.search (newDoc)
    if insRes:
        parts = insRes.group ('first', 'last')
    if not insRes or len (parts) != 2:
        # Should never happen unless there's a bug
        raise StandardError ("addRepDocActiveStatus: No CdrDocCtl in doc:\n%s"
                             % doc)

    # Comment must be compatible with CdrDoc utf-8
    if type(newStatus) == type(u""):
        newStatus = newStatus.encode('utf-8')

    # Insert new status
    return (parts[0] + "\n<DocActiveStatus>" + newStatus
            + "</DocActiveStatus>\n" + parts[1])


#----------------------------------------------------------------------
# Add a new document to the CDR Server.
# If showWarnings is set to non-zero value, the caller will be given
# a tuple containing the document ID string as the first value, and
# a possibly empty string containing an "Errors" element (for example,
# for validation errors).  Otherwise (the default behavior), a single
# value is returned containing a document ID in the form "CDRNNNNNNNNNN"
# or the string for an Errors element.  We're using this parameter
# (and its default) in order to preserve compatibility with code which
# expects a simple return value.
#
# See also the comment on repDoc below regarding the 'reason' argument.
#----------------------------------------------------------------------
def addDoc(credentials, file = None, doc = None, comment = '',
           checkIn = 'N', val = 'N', reason = '', ver = 'N',
           verPublishable = 'Y', setLinks = 'Y', showWarnings = 0,
           activeStatus = None,
           host = DEFAULT_HOST, port = DEFAULT_PORT):

    # Load the document if necessary.
    if file: doc = open(file, "r").read()
    if not doc:
        if file: return "<Errors><Err>%s not found</Err></Errors>" % file
        else:    return "<Errors><Err>Document missing.</Err></Errors>"

    # Ensure that unicode appears as utf-8
    if type(doc)==type(u""): doc = doc.encode('utf-8')

    # If comment passed, filter doc to add DocComment to CdrDocCtl
    # Raises exception if fails
    if len(comment) > 0:
        doc = _addRepDocComment (doc, comment)

    # Change the active_status if requested; raises exception on failure.
    if activeStatus:
        doc = _addRepDocActiveStatus(doc, activeStatus)

    # Create the command.
    checkIn = "<CheckIn>%s</CheckIn>" % (checkIn)
    val     = "<Validate>%s</Validate>" % (val)
    reason  = "<Reason>%s</Reason>" % (reason)
    doLinks = "<SetLinks>%s</SetLinks>" % setLinks
    ver     = "<Version Publishable='%s'>%s</Version>" % (verPublishable, ver)
    cmd     = "<CdrAddDoc>%s%s%s%s%s%s</CdrAddDoc>" % (checkIn, val, ver,
                                                       doLinks, reason, doc)

    # Submit the commands.
    resp = sendCommands(wrapCommand(cmd, credentials), host, port)

    # Extract the document ID (and messages if requested).
    docId = extract("<DocId.*>(CDR\d+)</DocId>", resp)
    if not docId.startswith("CDR"):
        if showWarnings:
            return (None, docId)
        else:
            return docId
    errors = getErrors(resp, errorsExpected = 0)
    if showWarnings:
        return (docId, errors)
    else:
        return docId

#----------------------------------------------------------------------
# Replace an existing document in the CDR Server.
# See documentation of addDoc above for explanation of showWarnings
# argument.
# Note that the 'reason' argument is used to set a value in the
# audit table.  If you want to have the comment column in the
# document and doc_version tables populated, you must supply a
# DocComment child of the CdrDocCtl element inside the CdrDoc
# of the doc argument.
# [That's done for you now if you supply a 'comment' argument.]
#----------------------------------------------------------------------
def repDoc(credentials, file = None, doc = None, comment = '',
           checkIn = 'N', val = 'N', reason = '', ver = 'N',
           verPublishable = 'Y', setLinks = 'Y', showWarnings = 0,
           activeStatus = None,
           host = DEFAULT_HOST, port = DEFAULT_PORT):

    # Load the document if necessary.
    if file: doc = open(file, "r").read()
    if not doc:
        if file: return "<Errors><Err>%s not found</Err></Errors>" % file
        else:    return "<Errors><Err>Document missing.</Err></Errors>"

    # Ensure that unicode appears as utf-8
    if type(doc)==type(u""): doc = doc.encode('utf-8')

    # If comment passed, filter doc to add DocComment to CdrDocCtl
    # Raises exception if fails
    if len(comment) > 0:
        doc = _addRepDocComment (doc, comment)

    # Change the active_status if requested; raises exception on failure.
    if activeStatus:
        doc = _addRepDocActiveStatus(doc, activeStatus)

    # Create the command.
    checkIn = "<CheckIn>%s</CheckIn>" % (checkIn)
    val     = "<Validate>%s</Validate>" % (val)
    reason  = "<Reason>%s</Reason>" % (reason)
    doLinks = "<SetLinks>%s</SetLinks>" % setLinks
    ver     = "<Version Publishable='%s'>%s</Version>" % (verPublishable, ver)
    cmd     = "<CdrRepDoc>%s%s%s%s%s%s</CdrRepDoc>" % (checkIn, val, ver,
                                                       doLinks, reason, doc)

    # Submit the commands.
    resp = sendCommands(wrapCommand(cmd, credentials), host, port)

    # Extract the document ID (and messages if requested).
    docId = extract("<DocId.*>(CDR\d+)</DocId>", resp)
    if not docId.startswith("CDR"):
        if showWarnings:
            return (None, docId)
        else:
            return docId
    errors = getErrors(resp, errorsExpected = 0)
    if showWarnings:
        return (docId, errors)
    else:
        return docId

#----------------------------------------------------------------------
# Retrieve a specified document from the CDR Server.
#----------------------------------------------------------------------
def getDoc(credentials, docId, checkout = 'N', version = "Current",
           xml='Y', blob='N',
           host = DEFAULT_HOST, port = DEFAULT_PORT, getObject = 0):

    # Create the command.
    did  = normalize(docId)
    lck  = "<Lock>%s</Lock>" % (checkout)
    ver  = "<DocVersion>%s</DocVersion>" % (version)
    what = "includeXml='%s' includeBlob='%s'" % (xml, blob)
    cmd  = "<CdrGetDoc %s><DocId>%s</DocId>%s%s</CdrGetDoc>" % \
           (what, did, lck, ver)

    # Submit the commands.
    resp = sendCommands(wrapCommand(cmd, credentials), host, port)

    # Extract the document.
    doc = extract("(<CdrDoc[>\s].*</CdrDoc>)", resp)
    if doc.startswith("<Errors") or not getObject: return doc
    return Doc(doc, encoding = 'utf-8')

#----------------------------------------------------------------------
# Mark a CDR document as deleted.
#----------------------------------------------------------------------
def delDoc(credentials, docId, val = 'N', reason = '',
           host = DEFAULT_HOST, port = DEFAULT_PORT):

    # Create the command.
    docId   = "<DocId>%s</DocId>" % docId
    val     = "<Validate>%s</Validate>" % val
    reason  = reason and ("<Reason>%s</Reason>" % reason) or ''
    cmd     = "<CdrDelDoc>%s%s%s</CdrDelDoc>" % (docId, val, reason)

    # Submit the commands.
    resp = sendCommands(wrapCommand(cmd, credentials), host, port)

    # Extract the document ID.
    return extract("<DocId.*>(CDR\d+)</DocId>", resp)

#----------------------------------------------------------------------
# Validate a CDR document.
#----------------------------------------------------------------------
def valDoc(credentials, docType, docId = None, doc = None,
           valLinks = 'Y', valSchema = 'Y', validateOnly = 'Y',
           host = DEFAULT_HOST, port = DEFAULT_PORT):

    # Create the command.
    if docId:
        doc = "<DocId ValidateOnly='%s'>%s</DocId>" % (validateOnly,
                                                       normalize(docId))
    if not doc:
        raise StandardError("valDoc: no doc or docId specified")
    if valLinks == 'Y' and valSchema == 'Y':
        valTypes = "Links Schema"
    elif valLinks == 'Y':
        valTypes = "Links"
    elif valSchema == 'Y':
        valTypes = "Schema"
    else:
        raise StandardError("valDoc: no validation method specified")
    cmd     = "<CdrValidateDoc DocType='%s' "\
              "ValidationTypes='%s'>%s</CdrValidateDoc>" % (docType,
                                                            valTypes,
                                                            doc)

    # Submit the commands.
    return sendCommands(wrapCommand(cmd, credentials), host, port)

#----------------------------------------------------------------------
# Retrieve a specified document from the CDR Server using a filter.
# Returns list of [filtered_document, messages] or error_string.
# Set the inline parameter to 1 if you want the second argument to
# be recognized as the filter XML document string in memory.
#----------------------------------------------------------------------
def filterDoc(credentials, filter, docId = None, doc = None, inline=0,
              host = DEFAULT_HOST, port = DEFAULT_PORT, parm = [],
              no_output = 'N', docVer = None, docDate = None,
              filterVer = ''):

    # Create the command.
    if docId:
        qual = ''
        if docVer:
            if type(docVer) == type(9): qual = " version='%d'" % docVer
            else: qual = " version='%s'" % docVer
        elif docDate:
            qual = " docDate='%s'" % docDate
        docElem = "<Document href='%s'%s/>" % (normalize(docId), qual)
    elif doc:
        # Ensure that everything sent to host is properly encoded
        # This is belt and suspenders.  Should be encoded okay already
        if type(doc) == type(u""):
            doc = doc.encode ("utf-8")
        docElem = "<Document><![CDATA[%s]]></Document>" % doc
    else: return "<Errors><Err>Document not specified.</Err></Errors>"

    # The filter is given to us as a string containing the XML directly.
    if inline:
        filterElem = "<Filter><![CDATA[%s]]></Filter>" % filter

    # We have a list of filters given by ID or name.
    elif type(filter) is type([]):
        filterElem = ""
        for l in filter:
            filt = ""
            isSet = 0
            if l != "":
                if l.startswith("name:"):
                    filt = l[5:]
                    ref="Name"
                elif l.startswith("set:"):
                    filt = l[4:]
                    isSet = 1
                else:
                    filt = normalize(l)
                    ref="href"
            if filt != "":
                if isSet:
                    filterElem += '<FilterSet Name="%s" Version="%s"/>' % \
                        (cgi.escape(filt, 1), filterVer)
                else:
                    v = filterVer and (" version='%s'" % filterVer) or ""
                    filterElem += ("<Filter %s='%s'%s/>" %
                                   (ref,
                                    filt,
                                    v))

    # We have a single filter identified by ID.
    else:
        filt = normalize(filter)
        filterElem = ("<Filter href='%s'/>" % filt)

    parmElem = ""
    if type(parm) is type([]) or type(parm) is type(()):
        for l in parm:
            parmElem += "<Parm><Name>" + l[0] \
                      + "</Name><Value>" + l[1] \
                      + "</Value></Parm>"
    if parmElem:
        # Even parms can have non-ASCII in them and may need encoding
        if type(parmElem) == type(u""):
            parmElem = parmElem.encode ("utf-8")
        parmElem = "<Parms>%s</Parms>" % parmElem

    output = ""
    if no_output == "Y":
        output = ' Output="N"'

    cmd = "<CdrFilter%s>%s%s%s</CdrFilter>" % (output, filterElem,
                                               docElem, parmElem)

    # Submit the commands.
    resp = sendCommands(wrapCommand(cmd, credentials), host, port)

    # Extract the filtered document.
    return extract_multiple(r"<Document[>\s][^<]*<!\[CDATA\[(.*)\]\]>\s*"
                              r"</Document>"
                              r"\s*((?:<Messages>.*</Messages>)?)",
                            resp)

#----------------------------------------------------------------------
# Request the output for a CDR report.
#----------------------------------------------------------------------
def report(credentials, name, parms, host = DEFAULT_HOST, port = DEFAULT_PORT):

    # Create the command.
    cmd = "<CdrReport><ReportName>%s</ReportName>" % name

    # Add the parameters.
    if parms:
        cmd = cmd + "<ReportParams>"
        for parm in parms:
            cmd = cmd + '<ReportParam Name="%s" Value="%s"/>' % (
                cgi.escape(parm[0], 1), cgi.escape(parm[1], 1))
        cmd = cmd + "</ReportParams>"

    # Submit the commands.
    resp = sendCommands(wrapCommand(cmd + "</CdrReport>", credentials),
                        host, port)

    # Extract the report.
    return extract("(<ReportBody[>\s].*</ReportBody>)", resp)

#----------------------------------------------------------------------
# Class to contain one hit from query result set.
#----------------------------------------------------------------------
class QueryResult:
    def __init__(self, docId, docType, docTitle):
        self.docId      = docId
        self.docType    = docType
        self.docTitle   = docTitle
    def __repr__(self):
        return "%s (%s) %s\n" % (self.docId, self.docType, self.docTitle)

#----------------------------------------------------------------------
# Process a CDR query.  Returns a tuple with two members, the first of
# which is a list of tuples containing id, doctype and title for each
# document in the search result, and the second of which is an <Errors>
# element.  Exactly one of these two member of the tuple will be None.
#----------------------------------------------------------------------
def search(credentials, query, host = DEFAULT_HOST, port = DEFAULT_PORT):

    # Create the command.
    cmd = ("<CdrSearch><Query>//CdrDoc[%s]/CdrCtl/DocId</Query></CdrSearch>"
            % query)

    # Submit the search.
    resp = sendCommands(wrapCommand(cmd, credentials), host, port)

    # Check for problems.
    err = checkErr(resp)
    if err: return err

    # Extract the results.
    results = extract("<QueryResults>(.*)</QueryResults>", resp)
    qrElemsPattern  = re.compile("<QueryResult>(.*?)</QueryResult>", re.DOTALL)
    docIdPattern    = re.compile("<DocId>(.*)</DocId>", re.DOTALL)
    docTypePattern  = re.compile("<DocType>(.*)</DocType>", re.DOTALL)
    docTitlePattern = re.compile("<DocTitle>(.*)</DocTitle>", re.DOTALL)
    ret = []
    for qr in qrElemsPattern.findall(results):
        docId    = docIdPattern.search(qr).group(1)
        docType  = docTypePattern.search(qr).group(1)
        docTitle = docTitlePattern.search(qr).group(1)
        ret.append(QueryResult(docId, docType, docTitle))
    return ret

#----------------------------------------------------------------------
# Class to contain CDR document type information.
#----------------------------------------------------------------------
class dtinfo:
    def __init__(self,
                 type       = None,
                 format     = None,
                 versioning = None,
                 created    = None,
                 schema_mod = None,
                 dtd        = None,
                 schema     = None,
                 vvLists    = None,
                 comment    = None,
                 error      = None):
        self.type           = type
        self.format         = format
        self.versioning     = versioning
        self.created        = created
        self.schema_mod     = schema_mod
        self.dtd            = dtd
        self.schema         = schema
        self.vvLists        = vvLists
        self.comment        = comment
        self.error          = error
    def __repr__(self):
        if self.error: return self.error
        return """\
[CDR Document Type]
            Name: %s
          Format: %s
      Versioning: %s
         Created: %s
 Schema Modified: %s
          Schema:
%s
             DTD:
%s
         Comment:
%s
""" % (self.type or '',
       self.format or '',
       self.versioning or '',
       self.created or '',
       self.schema_mod or '',
       self.schema or '',
       self.dtd or '',
       self.comment or '')

#----------------------------------------------------------------------
# Retrieve document type information from the CDR.
#----------------------------------------------------------------------
def getDoctype(credentials, doctype, host = DEFAULT_HOST, port = DEFAULT_PORT):

    # Create the command
    cmd = "<CdrGetDocType Type='%s' GetEnumValues='Y'/>" % doctype

    # Submit the request.
    resp = sendCommands(wrapCommand(cmd, credentials), host, port)

    # Extract the response.
    results = extract("<CdrGetDocTypeResp (.*)</CdrGetDocTypeResp>", resp)
    if string.find(results, "<Err") != -1:
        return dtinfo(error = extract("<Err>(.*)</Err>", results))

    # Build the regular expressions.
    typeExpr       = re.compile("Type=['\"]([^'\"]*)['\"]")
    formatExpr     = re.compile("Format=['\"]([^'\"]*)['\"]")
    versioningExpr = re.compile("Versioning=['\"]([^'\"]*)['\"]")
    createdExpr    = re.compile("Created=['\"]([^'\"]*)['\"]")
    schemaModExpr  = re.compile("SchemaMod=['\"]([^'\"]*)['\"]")
    commentExpr    = re.compile("<Comment>(.*)</Comment>", re.DOTALL)
    dtdExpr        = re.compile(r"<DocDtd>\s*<!\[CDATA\[(.*)\]\]>\s*</DocDtd>",
                                re.DOTALL)
    schemaExpr     = re.compile(r"<DocSchema>(.*)</DocSchema>", re.DOTALL)
    enumSetExpr    = re.compile(r"""<EnumSet\s+Node\s*=\s*"""
                                r"""['"]([^'"]+)['"]\s*>(.*?)</EnumSet>""",
                                re.DOTALL)
    vvExpr         = re.compile("<ValidValue>(.*?)</ValidValue>", re.DOTALL)

    # Parse out the components.
    type       = typeExpr      .search(results)
    format     = formatExpr    .search(results)
    versioning = versioningExpr.search(results)
    created    = createdExpr   .search(results)
    schema_mod = schemaModExpr .search(results)
    dtd        = dtdExpr       .search(results)
    schema     = schemaExpr    .search(results)
    comment    = commentExpr   .search(results)
    enumSets   = enumSetExpr   .findall(results)

    # Extract the valid value lists, if any
    vvLists = []
    if enumSets:
        for enumSet in enumSets:
            vvList = vvExpr.findall(enumSet[1])
            vvLists.append((enumSet[0], vvList))

    # Return a dtinfo instance.
    return dtinfo(type       = type       and type      .group(1) or '',
                  format     = format     and format    .group(1) or '',
                  versioning = versioning and versioning.group(1) or '',
                  created    = created    and created   .group(1) or '',
                  schema_mod = schema_mod and schema_mod.group(1) or '',
                  dtd        = dtd        and dtd       .group(1) or '',
                  schema     = schema     and schema    .group(1) or '',
                  comment    = comment    and comment   .group(1) or '',
                  vvLists    = vvLists                            or None)

#----------------------------------------------------------------------
# Create a new document type for the CDR.
#----------------------------------------------------------------------
def addDoctype(credentials, info, host = DEFAULT_HOST, port = DEFAULT_PORT):

    # Create the command
    cmd = "<CdrAddDocType Type='%s' Format='%s' Versioning='%s'>"\
          "<DocSchema>%s</DocSchema>"\
        % (info.type, info.format, info.versioning, info.schema)
    if info.comment:
        cmd = cmd + "<Comment>%s</Comment>" % info.comment
    cmd = cmd + "</CdrAddDocType>"

    # Submit the request.
    resp = sendCommands(wrapCommand(cmd, credentials), host, port)
    if string.find(resp, "<Err>") != -1:
        expr = re.compile("<Err>(.*)</Err>", re.DOTALL)
        err = expr.search(resp)
        err = err and err.group(1) or "Unknown failure"
        return dtinfo(error = err)
    return getDoctype(credentials, info.type, host, port)

#----------------------------------------------------------------------
# Modify existing document type information in the CDR.
#----------------------------------------------------------------------
def modDoctype(credentials, info, host = DEFAULT_HOST, port = DEFAULT_PORT):

    # Create the command
    cmd = "<CdrModDocType Type='%s' Format='%s' Versioning='%s'>"\
          "<DocSchema>%s</DocSchema>"\
        % (info.type, info.format, info.versioning, info.schema)
    if info.comment:
        cmd = cmd + "<Comment>%s</Comment>" % info.comment
    cmd = cmd + "</CdrModDocType>"

    # Submit the request.
    resp = sendCommands(wrapCommand(cmd, credentials), host, port)
    if string.find(resp, "<Err>") != -1:
        expr = re.compile("<Err>(.*)</Err>", re.DOTALL)
        err = expr.search(resp)
        err = err and err.group(1) or "Unknown failure"
        return dtinfo(error = err)
    return getDoctype(credentials, info.type, host, port)


class Term:
    def __init__(self, id, name):
        self.id       = id
        self.name     = name
        self.parents  = []
        self.children = []

class TermSet:
    def __init__(self, error = None):
        self.terms = {}
        self.error = error

#----------------------------------------------------------------------
# Retrieve a list of valid values defined in a schema for a doctype
#----------------------------------------------------------------------
def getVVList(credentials, docType, vvName, sorted=0, putFirst=None,
              host=DEFAULT_HOST, port=DEFAULT_PORT):
    """
    Creates a list of valid values from a schema.

    Pass:
        credentials - Standard stuff.
        doctype     - String name of the document type.
        vvName      - Name of XML element for which valid vals are desired.
        sorted      - True=sort the list alphabetically, else leave alone.
        putFirst    - Optional of value(s) to move to the top of the
                      list.  Used to get some particular ordering, move
                      a default value to the top, etc.
                      Can accept list, single value, or None.
        host/port   - Standard stuff.

    Raises:
        Standard error if anything goes wrong.
    """
    # Get all info about this doctype
    # It would be more efficient to get less, but is more robust
    #   to use our standard getDoctype function for this.
    dt = getDoctype (credentials, docType, host, port)
    if type(dt)==type("") or type(dt)==type(u""):
        raise ('Error getting doctype "%s" for valid values in "%s": %s' % \
               (docType, vvName, dt))

    # Extract the valid value list from the doctype info
    vals = []
    for vvList in dt.vvLists:
        if vvList[0] == vvName:
            vals = vvList[1]
            break

    # Should never happen
    if vals == []:
        raise ('No valid value list for "%s" in doctype %s' % \
               (vvName, docType))

    # If sorting
    if sorted:
        vals.sort()

    # If user wants to put some special value(s) first
    if putFirst:
        # If scalar passed, convert it to a list
        putVals = []
        if type(putFirst) in (type(()), type([])):
            putVals = putFirst
        else:
            putVals = [putFirst]

        # Insert values at head and delete them from further on
        pos = 0
        for putVal in putVals:
            vals.insert (pos, putVal)
            pos += 1
            for i in range(pos, len(vals)):
                if vals[i] == putVal:
                    del vals[i]
                    break

    return vals

#----------------------------------------------------------------------
# Gets context information for term's position in terminology tree.
#----------------------------------------------------------------------
def getTree(credentials, docId, depth = 1,
            host = DEFAULT_HOST, port = DEFAULT_PORT):

    # Create the command
    cmd = """\
<CdrGetTree><DocId>%s</DocId><ChildDepth>%d</ChildDepth></CdrGetTree>
""" % (normalize(docId), depth)

    # Submit the request.
    resp = sendCommands(wrapCommand(cmd, credentials), host, port)
    if string.find(resp, "<Err>") != -1:
        expr = re.compile("<Err>(.*)</Err>", re.DOTALL)
        err = expr.search(resp)
        err = err and err.group(1) or "Unknown failure"
        return TermSet(error = err)

    # Parse the response.
    respExpr = re.compile("<CdrGetTreeResp>\s*"
                          "<Pairs>(.*)</Pairs>\s*"
                          "<Terms>(.*)</Terms>\s*"
                          "</CdrGetTreeResp>", re.DOTALL)
    pairExpr = re.compile("<Pair><Child>(.*?)</Child>\s*"
                          "<Parent>(.*?)</Parent></Pair>")
    termExpr = re.compile("<Term><Id>(.*?)</Id>\s*"
                          "<Name>(.*?)</Name></Term>")
    groups   = respExpr.search(resp)
    result   = TermSet()
    terms    = result.terms

    # Extract the names of all terms returned.
    for term in termExpr.findall(groups.group(2)):
        (trmId, name) = term
        terms[trmId]  = Term(id = trmId, name = name)

    # Extract the child-parent relationship pairs.
    for pair in pairExpr.findall(groups.group(1)):
        (child, parent) = pair
        terms[child].parents.append(terms[parent])
        terms[parent].children.append(terms[child])

    return result

#----------------------------------------------------------------------
# Gets the list of CDR actions which can be authorized.
#----------------------------------------------------------------------
def getActions(credentials, host = DEFAULT_HOST, port = DEFAULT_PORT):

    # Create the command
    cmd = "<CdrListActions/>"

    # Submit the request.
    resp = sendCommands(wrapCommand(cmd, credentials), host, port)
    if string.find(resp, "<Err>") != -1:
        expr = re.compile("<Err>(.*)</Err>", re.DOTALL)
        err = expr.search(resp)
        err = err and err.group(1) or "Unknown failure"
        return err

    # Parse the response.
    actions = {}
    for a in re.findall("<Action>\s*<Name>(.*?)</Name>\s*"
                        "<NeedDoctype>(.*?)</NeedDoctype>\s*</Action>", resp):
        actions[a[0]] = a[1]
    return actions

#----------------------------------------------------------------------
# Gets the list of CDR users.
#----------------------------------------------------------------------
def getUsers(credentials, host = DEFAULT_HOST, port = DEFAULT_PORT):

    # Create the command
    cmd = "<CdrListUsrs/>"

    # Submit the request.
    resp = sendCommands(wrapCommand(cmd, credentials), host, port)
    if string.find(resp, "<Err>") != -1:
        expr = re.compile("<Err>(.*)</Err>", re.DOTALL)
        err = expr.search(resp)
        err = err and err.group(1) or "Unknown failure"
        return err

    # Parse the response.
    users = re.findall("<UserName>(.*?)</UserName>", resp)
    users.sort()
    return users

#----------------------------------------------------------------------
# Gets the list of CDR authorization groups.
#----------------------------------------------------------------------
def getGroups(credentials, host = DEFAULT_HOST, port = DEFAULT_PORT):

    # Create the command
    cmd = "<CdrListGrps/>"

    # Submit the request.
    resp = sendCommands(wrapCommand(cmd, credentials), host, port)
    if string.find(resp, "<Err>") != -1:
        expr = re.compile("<Err>(.*)</Err>", re.DOTALL)
        err = expr.search(resp)
        err = err and err.group(1) or "Unknown failure"
        return err

    # Parse the response.
    groups = re.findall("<GrpName>(.*?)</GrpName>", resp)
    groups.sort()
    return groups

#----------------------------------------------------------------------
# Deletes a CDR group.
#----------------------------------------------------------------------
def delGroup(credentials, grp, host = DEFAULT_HOST, port = DEFAULT_PORT):

    # Create the command
    cmd = "<CdrDelGrp><GrpName>%s</GrpName></CdrDelGrp>" % grp

    # Submit the request.
    resp = sendCommands(wrapCommand(cmd, credentials), host, port)
    if string.find(resp, "<Err>") != -1:
        expr = re.compile("<Err>(.*)</Err>", re.DOTALL)
        err = expr.search(resp)
        err = err and err.group(1) or "Unknown failure"
        return err

    # No errors to report.
    return None

#----------------------------------------------------------------------
# Gets the list of CDR document types.
#----------------------------------------------------------------------
def getDoctypes(credentials, host = DEFAULT_HOST, port = DEFAULT_PORT):

    # Create the command
    cmd = "<CdrListDocTypes/>"

    # Submit the request.
    resp = sendCommands(wrapCommand(cmd, credentials), host, port)
    if string.find(resp, "<Err>") != -1:
        expr = re.compile("<Err>(.*)</Err>", re.DOTALL)
        err = expr.search(resp)
        err = err and err.group(1) or "Unknown failure"
        return err

    # Parse the response.
    types = re.findall("<DocType>(.*?)</DocType>", resp)
    if 'Filter' not in types: types.append('Filter')
    types.sort()
    return types

#----------------------------------------------------------------------
# Add a value to the sys_value table.
#----------------------------------------------------------------------
def addSysValue(credentials, name, value="", program=None, notes=None,
                host = DEFAULT_HOST, port = DEFAULT_PORT):
    """
    Add a value to sys_value table on server.
    Parameters:
        credentials, port, host = standard stuff.
        name    = Name of the value.
        value   = Value string, can be empty.
        program = Optional program name.
        notes   = Documentation to store with name.
    Return:
        None.
        Raises StandardError if failure.
    """
    _sysValue(credentials, "Add", name, value, program, notes, host, port)

#----------------------------------------------------------------------
# Replace a value in the sys_value table.
#----------------------------------------------------------------------
def repSysValue(credentials, name, value="", program=None, notes=None,
                host = DEFAULT_HOST, port = DEFAULT_PORT):
    """
    Replace a value to sys_value table on server.
    Parameters:
        See addSysValue
    Return:
        None.
        Raises StandardError if failure.
    """
    _sysValue(credentials, "Rep", name, value, program, notes, host, port)

#----------------------------------------------------------------------
# Replace a value in the sys_value table.
#----------------------------------------------------------------------
def delSysValue(credentials, name, host = DEFAULT_HOST, port = DEFAULT_PORT):
    """
    Delete row from sys_value table on server.
    Parameters:
        credentials, port, host = standard stuff.
        name = "name" column of the row to delete.
    Return:
        None.
        Raises StandardError if failure.
    """
    _sysValue(credentials, "Del", name, host=host, port=port)

#----------------------------------------------------------------------
# Retrieve a value from the sys_value table by its name.
#----------------------------------------------------------------------
def getSysValue(credentials, name, host = DEFAULT_HOST, port = DEFAULT_PORT):
    """
    Retrieve value from sys_value table on server by its name.
    Parameters:
        credentials, port, host = standard stuff.
          (Any credentials will do, no special authorization required.)
        name = "name" column of the row from which to retrieve value.
    Return:
        Value string:
            May be empty string.
            Returns None if NULL in database.
        Raises StandardError if failure.
    """
    return _sysValue(credentials, "Get", name, host=host, port=port)

#----------------------------------------------------------------------
# Internal routine to do the work of add/rep/del/getSysValue.
#----------------------------------------------------------------------
def _sysValue(credentials, action, name, value=None, program=None,
              notes=None, host=DEFAULT_HOST, port=DEFAULT_PORT):
    """
    Add a value to sys_value table on server.
    Parameters:
        credentials, port, host = standard stuff.
        action  = One of "Add", "Rep", "Del", "Get"
                    Unchecked - this should only be called by the four
                    front-end routines above that guarantee this.
        name    = Name of the value.
        value   = Value string, can be empty.
        program = Optional program name.
        notes   = Documentation to store with name.
    Return:
        None.
        Raises StandardError if failure.
    """
    # Required for anything
    if not credentials:
        raise StandardError ("No credentials passed to %sSysValue" % action)
    if not name:
        raise StandardError ("No name passed to %sSysValue" % action)

    # Create command
    tag = "Cdr" + action + "SysValue"
    cmd = " <%s>\n  <Name>%s</Name>\n" % (tag, name)
    if value != None:
        cmd += "  <Value>%s</Value>\n" % value
    if program:
        cmd += "  <Program>%s</Program>\n" % program
    if notes:
        cmd += "  <Notes>%s</Notes>\n" % notes
    cmd += " </%s>\n" % tag

    # Wrap with credentials and command structure
    cmd = wrapCommand (cmd, credentials)

    # Submit to server
    resp = sendCommands (cmd, host, port)

    # Did server report error?
    errs = getErrors (resp, 0)
    if len(errs) > 0:
        raise StandardError ("Server error on %sSysValue:\n%s" % (action,errs))

    # Do we need to return a value?
    if action == "Get":
        valPat   = re.compile("<Value>(.*)</Value>", re.DOTALL)
        valMatch = valPat.search (resp)
        if valMatch:
            return valMatch.group(1)

    return None

#----------------------------------------------------------------------
# Type used by getCssFiles() below.
#----------------------------------------------------------------------
class CssFile:
    def __init__(self, name, data):
        self.name = name
        self.data = data

#----------------------------------------------------------------------
# Gets the CSS files used by the client.
#----------------------------------------------------------------------
def getCssFiles(credentials, host = DEFAULT_HOST, port = DEFAULT_PORT):

    # Create the command.
    cmd = "<CdrGetCssFiles/>"

    # Submit the request.
    resp = sendCommands(wrapCommand(cmd, credentials), host, port)
    if string.find(resp, "<Err>") != -1:
        expr = re.compile("<Err>(.*)</Err>", re.DOTALL)
        err = expr.search(resp)
        err = err and err.group(1) or "Unknown failure"
        return err

    # Parse the response.
    nameExpr = re.compile("<Name>(.*)</Name>", re.DOTALL)
    dataExpr = re.compile("<Data>(.*)</Data>", re.DOTALL)
    files = []
    start = resp.find("<File>")
    if start == -1:
        return "Unable to find CSS files"
    while start != -1:
        end = resp.find("</File>", start)
        if end == -1:
            return "Missing end tag for CSS file"
        subString = resp[start:end]
        nameElem = nameExpr.search(subString)
        dataElem = dataExpr.search(subString)
        if not nameElem: return "Missing Name element"
        if not dataElem: return "Missing Data element"
        files.append(CssFile(nameElem.group(1), dataElem.group(1)))
        start = resp.find("<File>", end)
    return files

#----------------------------------------------------------------------
# Gets the list of CDR schema documents.
#----------------------------------------------------------------------
def getSchemaDocs(credentials, host = DEFAULT_HOST, port = DEFAULT_PORT):

    # Create the command
    cmd = "<CdrListSchemaDocs/>"

    # Submit the request.
    resp = sendCommands(wrapCommand(cmd, credentials), host, port)
    if string.find(resp, "<Err>") != -1:
        expr = re.compile("<Err>(.*)</Err>", re.DOTALL)
        err = expr.search(resp)
        err = err and err.group(1) or "Unknown failure"
        return err

    # Parse the response.
    return re.findall("<DocTitle>(.*?)</DocTitle>", resp)

#----------------------------------------------------------------------
# Holds information about a single CDR group.
#----------------------------------------------------------------------
class Group:
    def __init__(self, name, actions = None, users = None, comment = None):
        self.name    = name
        self.actions = actions or {}
        self.users   = users or []
        self.comment = comment

#----------------------------------------------------------------------
# Retrieves information about a CDR group.
#----------------------------------------------------------------------
def getGroup(credentials, gName, host = DEFAULT_HOST, port = DEFAULT_PORT):

    # Create the command
    cmd = "<CdrGetGrp><GrpName>%s</GrpName></CdrGetGrp>" % gName

    # Submit the request.
    resp = sendCommands(wrapCommand(cmd, credentials), host, port)
    if string.find(resp, "<Err>") != -1:
        expr = re.compile("<Err>(.*)</Err>", re.DOTALL)
        err = expr.search(resp)
        err = err and err.group(1) or "Unknown failure"
        return err

    # Parse the response.
    name     = re.findall("<GrpName>(.*?)</GrpName>", resp)[0]
    group    = Group(name)
    authExpr = re.compile("<Auth>(.*?)</Auth>", re.DOTALL)
    cmtExpr  = re.compile("<Comment>(.*?)</Comment>", re.DOTALL)
    comment  = cmtExpr.findall(resp)
    for user in re.findall("<UserName>(.*?)</UserName>", resp):
        group.users.append(user)
    group.users.sort()
    for auth in authExpr.findall(resp):
        action  = re.findall("<Action>(.*?)</Action>", auth)
        docType = re.findall("<DocType>(.*?)</DocType>", auth)
        #group.actions.append((action[0], docType and docType[0] or None))
        action  = action[0]
        docType = docType and docType[0] or None
        if not group.actions.has_key(action): group.actions[action] = []
        group.actions[action].append(docType)
    if comment: group.comment = comment[0]
    return group

#----------------------------------------------------------------------
# Stores information about a CDR group.
#----------------------------------------------------------------------
def putGroup(credentials, gName, group, host = DEFAULT_HOST,
                                        port = DEFAULT_PORT):

    # Create the command
    if gName:
        cmd = "<CdrModGrp><GrpName>%s</GrpName>" % gName
        if group.name and gName != group.name:
            cmd += "<NewGrpName>%s</NewGrpName>" % group.name
    else:
        cmd = "<CdrAddGrp><GrpName>%s</GrpName>" % group.name

    # Add the comment, if any.
    if group.comment is not None:
        cmd += "<Comment>%s</Comment>" % group.comment

    # Add the users.
    if group.users:
        for user in group.users:
            cmd += "<UserName>%s</UserName>" % user

    # Add the actions.
    if group.actions:
        actions = list(group.actions.keys())
        actions.sort()
        for action in actions:
            doctypes = group.actions[action]
            if not doctypes:
                cmd += "<Auth><Action>%s</Action></Auth>" % action
            else:
                for doctype in doctypes:
                    cmd += "<Auth><Action>%s</Action>"\
                           "<DocType>%s</DocType></Auth>" % (action, doctype)

    # Finish the command.
    if gName: cmd += "</CdrModGrp>"
    else:     cmd += "</CdrAddGrp>"

    # Submit the request.
    resp = sendCommands(wrapCommand(cmd, credentials), host, port)
    if string.find(resp, "<Err>") != -1:
        expr = re.compile("<Err>(.*)</Err>", re.DOTALL)
        err = expr.search(resp)
        err = err and err.group(1) or "Unknown failure"
        return err

    # No errors to report.
    return None

#----------------------------------------------------------------------
# Holds information about a single CDR user.
#----------------------------------------------------------------------
class User:
    def __init__(self,
                 name,
                 password,
                 fullname = None,
                 office   = None,
                 email    = None,
                 phone    = None,
                 groups   = [],
                 comment  = None):
        self.name         = name
        self.password     = password
        self.fullname     = fullname
        self.office       = office
        self.email        = email
        self.phone        = phone
        self.groups       = groups
        self.comment      = comment

#----------------------------------------------------------------------
# Retrieves information about a CDR group.
#----------------------------------------------------------------------
def getUser(credentials, uName, host = DEFAULT_HOST, port = DEFAULT_PORT):

    # Create the command
    cmd = "<CdrGetUsr><UserName>%s</UserName></CdrGetUsr>" % uName

    # Submit the request.
    resp = sendCommands(wrapCommand(cmd, credentials), host, port)
    if string.find(resp, "<Err>") != -1:
        expr = re.compile("<Err>(.*)</Err>", re.DOTALL)
        err = expr.search(resp)
        err = err and err.group(1) or "Unknown failure"
        return err

    # Parse the response.
    name     = re.findall("<UserName>(.*?)</UserName>", resp)[0]
    password = re.findall("<Password>(.*?)</Password>", resp)[0]
    user     = User(name, password)
    fullname = re.findall("<FullName>(.*?)</FullName>", resp)
    office   = re.findall("<Office>(.*?)</Office>", resp)
    email    = re.findall("<Email>(.*?)</Email>", resp)
    phone    = re.findall("<Phone>(.*?)</Phone>", resp)
    groups   = re.findall("<GrpName>(.*?)</GrpName>", resp)
    cmtExpr  = re.compile("<Comment>(.*?)</Comment>", re.DOTALL)
    comment  = cmtExpr.findall(resp)
    user.groups = groups
    if fullname: user.fullname = fullname[0]
    if office:   user.office   = office[0]
    if email:    user.email    = email[0]
    if phone:    user.phone    = phone[0]
    if comment:  user.comment  = comment[0]
    return user

#----------------------------------------------------------------------
# Stores information about a CDR user.
#----------------------------------------------------------------------
def putUser(credentials, uName, user, host = DEFAULT_HOST,
                                      port = DEFAULT_PORT):

    # Create the command
    if uName:
        cmd = "<CdrModUsr><UserName>%s</UserName>" % uName
        if user.name and uName != user.name:
            cmd += "<NewName>%s</NewName>" % user.name
    else:
        cmd = "<CdrAddUsr><UserName>%s</UserName>" % user.name

    # Add the user's password.
    cmd += "<Password>%s</Password>" % user.password

    # Add the optional single elements.
    if user.fullname is not None:
        cmd += "<FullName>%s</FullName>" % user.fullname
    if user.office is not None:
        cmd += "<Office>%s</Office>" % user.office
    if user.email is not None:
        cmd += "<Email>%s</Email>" % user.email
    if user.phone is not None:
        cmd += "<Phone>%s</Phone>" % user.phone
    if user.comment is not None:
        cmd += "<Comment>%s</Comment>" % user.comment

    # Add the groups.
    if user.groups:
        for group in user.groups:
            cmd += "<GrpName>%s</GrpName>" % group

    # Finish the command.
    if uName: cmd += "</CdrModUsr>"
    else:     cmd += "</CdrAddUsr>"

    # Submit the request.
    resp = sendCommands(wrapCommand(cmd, credentials), host, port)
    if string.find(resp, "<Err>") != -1:
        expr = re.compile("<Err>(.*)</Err>", re.DOTALL)
        err = expr.search(resp)
        err = err and err.group(1) or "Unknown failure"
        return err

    # No errors to report.
    return None

#----------------------------------------------------------------------
# Deletes a CDR user.
#----------------------------------------------------------------------
def delUser(credentials, usr, host = DEFAULT_HOST, port = DEFAULT_PORT):

    # Create the command
    cmd = "<CdrDelUsr><UserName>%s</UserName></CdrDelUsr>" % usr

    # Submit the request.
    resp = sendCommands(wrapCommand(cmd, credentials), host, port)
    if string.find(resp, "<Err>") != -1:
        expr = re.compile("<Err>(.*)</Err>", re.DOTALL)
        err = expr.search(resp)
        err = err and err.group(1) or "Unknown failure"
        return err

    # No errors to report.
    return None

#----------------------------------------------------------------------
# Holds information about a single CDR action.
#----------------------------------------------------------------------
class Action:
    def __init__(self, name, doctypeSpecific, comment  = None):
        self.name            = name
        self.doctypeSpecific = doctypeSpecific
        self.comment         = comment

#----------------------------------------------------------------------
# Retrieves information about a CDR action.
#----------------------------------------------------------------------
def getAction(credentials, name, host = DEFAULT_HOST, port = DEFAULT_PORT):

    # Create the command
    cmd = "<CdrGetAction><Name>%s</Name></CdrGetAction>" % name

    # Submit the request.
    resp = sendCommands(wrapCommand(cmd, credentials), host, port)
    if string.find(resp, "<Err>") != -1:
        expr = re.compile("<Err>(.*)</Err>", re.DOTALL)
        err = expr.search(resp)
        err = err and err.group(1) or "Unknown failure"
        return err

    # Parse the response.
    name     = re.findall("<Name>(.*)</Name>", resp)[0]
    flag     = re.findall("<DoctypeSpecific>(.*)</DoctypeSpecific>", resp)[0]
    cmtExpr  = re.compile("<Comment>(.*)</Comment>", re.DOTALL)
    comment  = cmtExpr.findall(resp)
    action   = Action(name, flag)
    if comment:  action.comment  = comment[0]
    return action

#----------------------------------------------------------------------
# Stores information for a CDR action.
#----------------------------------------------------------------------
def putAction(credentials, name, action, host = DEFAULT_HOST,
                                         port = DEFAULT_PORT):

    # Create the command
    if name:
        cmd = "<CdrRepAction><Name>%s</Name>" % name
        if action.name and name != action.name:
            cmd += "<NewName>%s</NewName>" % action.name
    else:
        cmd = "<CdrAddAction><Name>%s</Name>" % action.name

    # Add the action's doctype-specific flag.
    cmd += "<DoctypeSpecific>%s</DoctypeSpecific>" % action.doctypeSpecific

    # Add the comment, if present.
    if action.comment is not None:
        cmd += "<Comment>%s</Comment>" % action.comment

    # Finish the command.
    if name: cmd += "</CdrRepAction>"
    else:    cmd += "</CdrAddAction>"

    # Submit the request.
    resp = sendCommands(wrapCommand(cmd, credentials), host, port)
    if string.find(resp, "<Err>") != -1:
        expr = re.compile("<Err>(.*)</Err>", re.DOTALL)
        err = expr.search(resp)
        err = err and err.group(1) or "Unknown failure"
        return err

    # No errors to report.
    return None

#----------------------------------------------------------------------
# Deletes a CDR action.
#----------------------------------------------------------------------
def delAction(credentials, name, host = DEFAULT_HOST, port = DEFAULT_PORT):

    # Create the command
    cmd = "<CdrDelAction><Name>%s</Name></CdrDelAction>" % name

    # Submit the request.
    resp = sendCommands(wrapCommand(cmd, credentials), host, port)
    if string.find(resp, "<Err>") != -1:
        expr = re.compile("<Err>(.*)</Err>", re.DOTALL)
        err = expr.search(resp)
        err = err and err.group(1) or "Unknown failure"
        return err

    # No errors to report.
    return None

#----------------------------------------------------------------------
# Holds information about a single CDR link type.
#----------------------------------------------------------------------
class LinkType:
    def __init__(self, name, linkSources = None,
                             linkTargets = None,
                             linkProps   = None,
                             comment     = None):
        self.name        = name
        self.linkSources = linkSources or []
        self.linkTargets = linkTargets or []
        self.linkProps   = linkProps   or []
        self.comment     = comment
    def __str__(self):
        return "LinkType(%s,\n%s,\n%s,\n%s,\n%s)" % (self.name,
                                                 self.linkSources,
                                                 self.linkTargets,
                                                 self.linkProps,
                                                 self.comment)

#----------------------------------------------------------------------
# Holds information about a single CDR link property.
#----------------------------------------------------------------------
class LinkProp:
    def __init__(self, name, comment = None):
        self.name        = name
        self.comment     = comment

#----------------------------------------------------------------------
# Retrieves list of CDR link type names.
#----------------------------------------------------------------------
def getLinkTypes(credentials, host = DEFAULT_HOST, port = DEFAULT_PORT):

    # Create the command
    cmd = "<CdrListLinkTypes/>"

    # Submit the request.
    resp = sendCommands(wrapCommand(cmd, credentials), host, port)
    if string.find(resp, "<Err>") != -1:
        expr = re.compile("<Err>(.*)</Err>", re.DOTALL)
        err = expr.search(resp)
        err = err and err.group(1) or "Unknown failure"
        return err

    # Parse the response
    types = re.findall("<Name>(.*?)</Name>", resp)
    types.sort()
    return types

#----------------------------------------------------------------------
# Retrieves information from the CDR for a link type.
#----------------------------------------------------------------------
def getLinkType(credentials, name, host = DEFAULT_HOST, port = DEFAULT_PORT):

    # Create the command
    cmd = "<CdrGetLinkType><Name>%s</Name></CdrGetLinkType>" % name

    # Submit the request.
    resp = sendCommands(wrapCommand(cmd, credentials), host, port)
    if string.find(resp, "<Err>") != -1:
        expr = re.compile("<Err>(.*)</Err>", re.DOTALL)
        err = expr.search(resp)
        err = err and err.group(1) or "Unknown failure"
        return err
    # Parse the response
    name     = re.findall("<Name>(.*)</Name>", resp)[0]
    cmtExpr  = re.compile("<LinkTypeComment>(.*)</LinkTypeComment>", re.DOTALL)
    srcExpr  = re.compile("<LinkSource>(.*?)</LinkSource>", re.DOTALL)
    tgtExpr  = re.compile("<TargetDocType>(.*?)</TargetDocType>", re.DOTALL)
    prpExpr  = re.compile("<LinkProperties>(.*?)</LinkProperties>", re.DOTALL)
    sdtExpr  = re.compile("<SrcDocType>(.*)</SrcDocType>", re.DOTALL)
    fldExpr  = re.compile("<SrcField>(.*)</SrcField>", re.DOTALL)
    prnExpr  = re.compile("<LinkProperty>(.*)</LinkProperty>", re.DOTALL)
    prvExpr  = re.compile("<PropertyValue>(.*)</PropertyValue>", re.DOTALL)
    prcExpr  = re.compile("<PropertyComment>(.*)</PropertyComment>", re.DOTALL)
    comment  = cmtExpr.findall(resp)
    sources  = srcExpr.findall(resp)
    targets  = tgtExpr.findall(resp)
    props    = prpExpr.findall(resp)
    linkType = LinkType(name)
    if comment:  linkType.comment     = comment[0]
    if targets:  linkType.linkTargets = targets
    for source in sources:
        srcDocType  = sdtExpr.search(source).group(1)
        srcField    = fldExpr.search(source).group(1)
        linkType.linkSources.append((srcDocType, srcField))
    for prop in props:
        propName    = prnExpr.search(prop).group(1)
        propVal     = prvExpr.search(prop)
        propComment = prcExpr.search(prop)
        propVal     = propVal and propVal.group(1) or None
        propComment = propComment and propComment.group(1) or None
        linkType.linkProps.append((propName, propVal, propComment))
    return linkType

#----------------------------------------------------------------------
# Stores information for a CDR link type.
#----------------------------------------------------------------------
def putLinkType(credentials, name, linkType, linkAct,
                host = DEFAULT_HOST, port = DEFAULT_PORT):

    # Create the command
    if linkAct == "modlink":
        cmd = "<CdrModLinkType><Name>%s</Name>" % name
        if linkType.name and name != linkType.name:
            cmd += "<NewName>%s</NewName>" % linkType.name
    else:
        cmd = "<CdrAddLinkType><Name>%s</Name>" % linkType.name

    # Add the comment, if present.
    if linkType.comment is not None:
        cmd += "<Comment>%s</Comment>" % linkType.comment

    # Add the link sources.
    for src in linkType.linkSources:
        cmd += "<LinkSource><SrcDocType>%s</SrcDocType>" % src[0]
        cmd += "<SrcField>%s</SrcField></LinkSource>" % src[1]

    # Add the link targets.
    for tgt in linkType.linkTargets:
        cmd += "<TargetDocType>%s</TargetDocType>" % tgt

    # Add the link properties.
    for prop in linkType.linkProps:
        cmd += "<LinkProperties><LinkProperty>%s</LinkProperty>" % prop[0]
        cmd += "<PropertyValue>%s</PropertyValue>" % prop[1]
        cmd += "<Comment>%s</Comment></LinkProperties>" % prop[2]

    # Submit the request.
    if linkAct == "modlink":
        cmd += "</CdrModLinkType>"
    else:
        cmd += "</CdrAddLinkType>"
    resp = sendCommands(wrapCommand(cmd, credentials), host, port)
    if string.find(resp, "<Err>") != -1:
        expr = re.compile("<Err>(.*)</Err>", re.DOTALL)
        err = expr.search(resp)
        err = err and err.group(1) or "Unknown failure"
        return err

    # No errors to report if we get here.
    return None

#----------------------------------------------------------------------
# Retrieves list of CDR link properties.
#----------------------------------------------------------------------
def getLinkProps(credentials, host = DEFAULT_HOST, port = DEFAULT_PORT):

    # Create the command
    cmd = "<CdrListLinkProps/>"

    # Submit the request.
    resp = sendCommands(wrapCommand(cmd, credentials), host, port)
    if string.find(resp, "<Err>") != -1:
        expr = re.compile("<Err>(.*)</Err>", re.DOTALL)
        err = expr.search(resp)
        err = err and err.group(1) or "Unknown failure"
        return err

    # Parse the response
    propExpr = re.compile("<LinkProperty>(.*?)</LinkProperty>", re.DOTALL)
    nameExpr = re.compile("<Name>(.*)</Name>", re.DOTALL)
    cmtExpr  = re.compile("<Comment>(.*)</Comment>", re.DOTALL)
    ret      = []
    props    = propExpr.findall(resp)
    if props:
        for prop in props:
            name = nameExpr.findall(prop)[0]
            cmt  = cmtExpr.findall(prop)
            pr   = LinkProp(name)
            if cmt: pr.comment = cmt[0]
            ret.append(pr)
    return ret

#----------------------------------------------------------------------
# Returns a list of available query term rules.
#----------------------------------------------------------------------
def listQueryTermRules(session, host = DEFAULT_HOST, port = DEFAULT_PORT):

    # Create the command
    cmd = "<CdrListQueryTermRules/>"

    # Submit the request.
    resp = sendCommands(wrapCommand(cmd, session), host, port)

    # Check for problems.
    err = checkErr(resp)
    if err: return err

    # Extract the rules.
    return re.findall("<Rule>(.*?)</Rule>", resp)

#----------------------------------------------------------------------
# Returns a list of CDR query term definitions.
#----------------------------------------------------------------------
def listQueryTermDefs(session, host = DEFAULT_HOST, port = DEFAULT_PORT):

    # Create the command
    cmd = "<CdrListQueryTermDefs/>"

    # Submit the request.
    resp = sendCommands(wrapCommand(cmd, session), host, port)

    # Extract the definitions.
    defExpr      = re.compile("<Definition>(.*?)</Definition>", re.DOTALL)
    pathExpr     = re.compile("<Path>(.*)</Path>")
    ruleExpr     = re.compile("<Rule>(.*)</Rule>")
    err          = checkErr(resp)
    if err:
        return err
    definitions  = defExpr.findall(resp)
    rc           = []
    if definitions:
        for definition in definitions:
            path = pathExpr.search(definition).group(1)
            rule = ruleExpr.search(definition)
            rule = rule and rule.group(1) or None
            rc.append((path, rule))
    return rc

#----------------------------------------------------------------------
# Adds a new query term definition.
#----------------------------------------------------------------------
def addQueryTermDef(session, path, rule = None, host = DEFAULT_HOST,
                                                port = DEFAULT_PORT):

    # Create the command
    cmd = "<CdrAddQueryTermDef><Path>%s</Path>" % path
    if rule: cmd += "<Rule>%s</Rule>" % rule
    cmd += "</CdrAddQueryTermDef>"

    # Submit the request.
    resp = sendCommands(wrapCommand(cmd, session), host, port)
    return checkErr(resp)

#----------------------------------------------------------------------
# Deletes an existing query term definition.
#----------------------------------------------------------------------
def delQueryTermDef(session, path, rule = None, host = DEFAULT_HOST,
                                                port = DEFAULT_PORT):

    # Create the command
    cmd = "<CdrDelQueryTermDef><Path>%s</Path>" % path
    if rule: cmd += "<Rule>%s</Rule>" % rule
    cmd += "</CdrDelQueryTermDef>"

    # Submit the request.
    resp = sendCommands(wrapCommand(cmd, session), host, port)
    return checkErr(resp)

#----------------------------------------------------------------------
# Construct a string containing the description of the last exception.
#----------------------------------------------------------------------
def exceptionInfo():
    (eType, eValue) = sys.exc_info()[:2]
    if eType:
        eMsg = str(eType)
        if eValue:
            eMsg += (": %s" % str(eValue))
    else:
        eMsg = str(eValue) or "unable to find exception information"
    return eMsg

#----------------------------------------------------------------------
# Send email to a list of recipients.
#----------------------------------------------------------------------
def sendMail(sender, recips, subject = "", body = "", html = 0):
    if not recips:
        return "sendMail: no recipients specified"
    if type(recips) != type([]) and type(recips) != type(()):
        return "sendMail: recipients must be a list of email addresses"
    recipList = recips[0]
    for recip in recips[1:]:
        recipList += (",\n  %s" % recip)
    try:
        # Headers
        message = """\
From: %s
To: %s
Subject: %s
""" % (sender, recipList, subject)

        # Set content type for html
        if html:
            message += "Content-type: text/html; charset=iso-8859-1\n"

        # Separator line + body
        message += "\n%s" % body

        # Send it
        server = smtplib.SMTP(SMTP_RELAY)
        server.sendmail(sender, recips, message)
        server.quit()
    except:
        # Log the error and return it to caller
        msg = "sendMail failure: %s" % exceptionInfo()
        logwrite(msg)
        return msg

#----------------------------------------------------------------------
# Check in a CDR document.
#----------------------------------------------------------------------
def unlock(credentials, docId, abandon = 'Y', force = 'Y', reason = '',
           host = DEFAULT_HOST, port = DEFAULT_PORT):

    # Create the command.
    attrs   = "Abandon='%s' ForceCheckIn='%s'" % (abandon, force)
    docId   = "<DocumentId>%s</DocumentId>" % docId
    reason  = reason and ("<Comment>%s</Comment>" % reason) or ''
    cmd     = "<CdrCheckIn %s>%s%s</CdrCheckIn>" % (attrs, docId, reason)

    # Submit the commands.
    resp = sendCommands(wrapCommand(cmd, credentials), host, port)

    # Find any error messages.
    err = checkErr(resp)
    if err: return err
    return ""

#----------------------------------------------------------------------
# Get the most recent versions for a document.
#----------------------------------------------------------------------
def listVersions(credentials, docId, nVersions = -1,
                 host = DEFAULT_HOST, port = DEFAULT_PORT):

    # Create the command.
    cmd = "<CdrListVersions><DocId>%s</DocId>" \
          "<NumVersions>%d</NumVersions></CdrListVersions>" % (
          normalize(docId), nVersions)

    # Submit the commands.
    resp = sendCommands(wrapCommand(cmd, credentials), host, port)
    print resp

    # Check for failure.
    if resp.find("<Errors") != -1:
        raise StandardError(extract(r"(<Errors[\s>].*</Errors>)", resp))

    # Extract the versions.
    versions    = []
    versionExpr = re.compile("<Version>(.*?)</Version>", re.DOTALL)
    numExpr     = re.compile("<Num>(.*)</Num>")
    commentExpr = re.compile("<Comment>(.*)</Comment>", re.DOTALL)
    verList     = versionExpr.findall(resp)
    if verList:
        for ver in verList:
            numMatch     = numExpr.search(ver)
            commentMatch = commentExpr.search(ver)
            if not numMatch:
                raise StandardError("listVersions: missing Num element")
            num = int(numMatch.group(1))
            comment = commentMatch and commentMatch.group(1) or None
            versions.append((num, comment))
    return versions

#----------------------------------------------------------------------
# Reindex the specified document.
#----------------------------------------------------------------------
def reindex(credentials, docId, host = DEFAULT_HOST, port = DEFAULT_PORT):

    # Create the command.
    docId = normalize(docId)
    cmd = "<CdrReindexDoc><DocId>%s</DocId></CdrReindexDoc>" % docId

    # Submit the commands.
    resp = sendCommands(wrapCommand(cmd, credentials), host, port)

    # Check for errors.
    if resp.find("<Errors") != -1:
        return extract(r"(<Errors[\s>].*</Errors>)", resp)
    return None

#----------------------------------------------------------------------
# Create a new publishing job.
#----------------------------------------------------------------------
def publish(credentials, pubSystem, pubSubset, parms = None, docList = None,
           email = '', noOutput = 'N', allowNonPub = 'N', docTime = None,
           host = DEFAULT_HOST, port = DEFAULT_PORT, allowInActive = 'N'):

    # Create the command.
    pubSystem   = pubSystem and ("<PubSystem>%s</PubSystem>" % pubSystem) or ""
    pubSubset   = pubSubset and ("<PubSubset>%s</PubSubset>" % pubSubset) or ""
    email       = email and "<Email>%s</Email>" % email or ""
    noOutput    = noOutput and "<NoOutput>%s</NoOutput>" % noOutput
    allowNonPub = (allowNonPub == 'N') and 'N' or 'Y'
    allowNonPub = "<AllowNonPub>%s</AllowNonPub>" % allowNonPub
    allowInAct  = (allowInActive == 'N') and 'N' or 'Y'
    allowInAct  = "<AllowInActive>%s</AllowInActive>" % allowInAct
    parmElem    = ''
    docsElem    = ''
    if parms:
        parmElem = "<Parms>"
        for parm in parms:
            parmElem += "<Parm><Name>%s</Name><Value>%s</Value></Parm>" % (
                        parm[0], parm[1])
        parmElem += "</Parms>"
    if docList:
        expr = re.compile(r"CDR(\d+)(/(\d+))?")
        docsElem += "<DocList>"
        if docTime: docsElem += "<DocTime>%s</DocTime>" % docTime
        for doc in docList:
            match = expr.search(doc)
            if not match:
                return (None, "<Errors><Err>Malformed docList member '%s'"\
                              "</Err></Errors>" % doc)
            id = normalize(match.group(1))
            version = match.group(3) or "0"
            docsElem += "<Doc Id='%s' Version='%s'/>" % (id, version)
        docsElem += "</DocList>"

    cmd = "<CdrPublish>%s%s%s%s%s%s%s%s</CdrPublish>" % (pubSystem,
                                                       pubSubset,
                                                       parmElem,
                                                       docsElem,
                                                       email,
                                                       noOutput,
                                                       allowNonPub,
                                                       allowInAct)

    # Submit the commands.
    resp = sendCommands(wrapCommand(cmd, credentials), host, port)

    # Return the job ID and any warnings/errors.
    jobId  = None
    errors = None
    if resp.find("<JobId") != -1:
        jobId  = extract(r"<JobId>([^<]*)</JobId>", resp)
    if resp.find("<Errors") != -1:
        errors = extract(r"(<Errors[\s>].*</Errors>)", resp)
    return (jobId, errors)

class PubStatus:
    def __init__(self, id, pubSystem, pubSubset, parms, userName, outputDir,
                 started, completed, status, messages, email, docList,
                 errors):
        self.id        = id
        self.pubSystem = pubSystem
        self.pubSubset = pubSubset
        self.parms     = parms
        self.userName  = userName
        self.outputDir = outputDir
        self.started   = started
        self.completed = completed
        self.status    = status
        self.messages  = messages
        self.email     = email
        self.docList   = docList
        self.errors    = errors

def pubStatus(self, jobId, getDocInfo = 0):
    return "XXX this is a stub"

#----------------------------------------------------------------------
# Turn cacheing on or off in the CdrServer
#----------------------------------------------------------------------
def cacheInit(credentials, cacheOn, cacheType,
              host=DEFAULT_HOST, port=DEFAULT_PORT):
    """
    Submit a transaction to the server to turn cacheing on or off.
    At this time, cacheing is only of interest for publishing jobs,
    and the only type of cacheing we do is term denormalization
    cacheing - so that a given Term document id used in a protocol
    need only be looked up once, its document XML only parsed once,
    and the XML string for the denormalization need only be constructed
    once.  However the interface supports other types of cacheing if
    and when we create them.

    Pass:
        credentials - As usual.
        cacheOn     - true  = Turn cacheing on.
                      false = Turn it off.
        cacheType   - Currently known types are all synonyms of each other,
                      one of:
                        "term"
                        "pub"
                        "all"
        host / port - As usual.

    Return:
        Void.

    Raise:
        Standard error if error returned by host.  Possible errors
        are connection oriented, or invalid parameters.
    """
    # Attribute tells the server what to do
    cmdAttr = "off"
    if cacheOn:
        cmdAttr = "on"

    # Construct XML transaction
    cmd = "<CdrCacheing " + cmdAttr + "='" + cacheType + "'/>"

    # Send it
    resp = sendCommands(wrapCommand(cmd, credentials), host, port)

    # If error occurred, raise exception
    err = checkErr(resp)
    if err:
        raise StandardError (err)

    return None

#----------------------------------------------------------------------
# Write messages to a logfile.
#----------------------------------------------------------------------
def logwrite(msgs, logfile = DEFAULT_LOGFILE, tback = 0):
    """
    Append one or messages to a log file - closing the file when done.
    Can also record traceback information.

    Pass:
        msgs    - Single string or sequence of strings to write.
                   Should not contain binary data.
        logfile - Optional log file path, else uses default.
        tback   - True = log the latest traceback object.
                   False = do not.

    Return:
        Void.  Does nothing at all if it can't open the logfile or
          append to it.
    """
    f = None
    try:
        f = open (logfile, "a", 0)

        # Write process id and timestamp
        f.write ("!%d %s: " % (os.getpid(), time.ctime()))

        # Sequence of messages or single message
        if type(msgs) == type(()) or type(msgs) == type([]):
            for msg in msgs:
                if (type(msg)) == type(u""):
                    msg = msg.encode ('utf-8')
                f.write (msg)
                f.write ("\n")
        else:
            if (type(msgs)) == type(u""):
                msgs = msgs.encode ('utf-8')
            f.write (msgs)
            f.write ("\n")

        # If traceback is requested, include the last one
        if tback:
            try:
                traceback.print_exc (999, f)
            except:
                pass

    except:
        pass

    # Close file if opened.  This ensures that caller will see his
    #   logged messages even if his program crashes
    if f:
        try:
            f.close()
        except:
            pass


#----------------------------------------------------------------------
# Create an HTML table from a passed data
#----------------------------------------------------------------------
def tabularize (rows, tblAttrs=None):
    """
    Create an HTML table string from passed data.
    This looks like it should be in cdrcgi, but we also produce
    HTML email in batch programs - which aren't searching the
    cgi path.

    Pass:
        rows = Sequence of rows for the table, each containing
               a sequence of columns.
               If the number of columns is not the same in each row,
               then the caller gets whatever he gets, so it may be
               wise to add columns with content like "&nbsp;" if needed.
               No entity conversions are performed.

        tblAttrs = Optional string of attributes to put in table, e.g.,
               "align='center' border='1' width=95%'"

        We might add rowAttrs and colAttrs if this is worthwhile.
    Return:
        HTML as a string.
    """
    if not tblAttrs:
        html = "<table>\n"
    else:
        html = "<table " + tblAttrs + ">\n"

    for row in rows:
        html += " <tr>\n"
        for col in row:
            html += "  <td>%s</td>\n" % col
        html += " </tr>\n"
    html += "</table>"

    return html

#----------------------------------------------------------------------
# Log out from the CDR.
#----------------------------------------------------------------------
def logout(session, host = DEFAULT_HOST, port = DEFAULT_PORT):

    # Create the command
    cmd = "<CdrLogoff/>"

    # Submit the request.
    resp = sendCommands(wrapCommand(cmd, session), host, port)
    err = checkErr(resp)
    if err: return err

    # No errors to report.
    return None

#----------------------------------------------------------------------
# Object for results of an external command.
#----------------------------------------------------------------------
class CommandResult:
    def __init__(self, code, output):
        self.code   = code
        self.output = output

#----------------------------------------------------------------------
# Run an external command.
#----------------------------------------------------------------------
def runCommand(command):
    commandStream = os.popen('%s 2>&1' % command)
    output = commandStream.read()
    code = commandStream.close()
    return CommandResult(code, output)

#----------------------------------------------------------------------
# Create a temporary working area.
#----------------------------------------------------------------------
def makeTempDir(basename = "tmp", chdir = 1):
    if os.environ.has_key("TMP"):
        tempfile.tempdir = os.environ["TMP"]
    where = tempfile.mktemp(basename)
    abspath = os.path.abspath(where)
    try:
        os.mkdir(abspath)
    except:
        raise StandardError("makeTempDir",
                            "Cannot create directory %s" % abspath)
    if chdir:
        try:
            os.chdir(abspath)
        except:
            raise StandardError("makeTempDir",
                                "Cannot chdir to %s" % abspath)
    return abspath

#----------------------------------------------------------------------
# Object for ID and name (for example, of a document, in which case
# the name is the title of the document), or a filter set, or a user,
# or a group, or ....
#----------------------------------------------------------------------
class IdAndName:
    def __init__(self, id, name):
        self.id = id
        self.name = name

#----------------------------------------------------------------------
# Object representing a CdrResponseNode.
#----------------------------------------------------------------------
class CdrResponseNode:
    def __init__(self, node, when):
        self.when            = when
        self.responseWrapper = node
        self.specificElement = None
        for child in node.childNodes:
            if child.nodeType == child.ELEMENT_NODE:
                if self.specificElement:
                    raise StandardError("CdrResponseNode: too many children "
                                        "of CdrResponse element")
                self.specificElement = child
        if not self.specificElement:
            raise StandardError("No element children found for CdrResponse")
        self.elapsed         = self.specificElement.getAttribute('Elapsed')

#----------------------------------------------------------------------
# Raise an exception using the text content of Err elements.
#----------------------------------------------------------------------
def wrapException(caller, errElems):
    args = [caller]
    for elem in errElems:
        args.append(getTextContent(elem))
    exception = StandardError()
    exception.args = tuple(args)
    raise exception

#----------------------------------------------------------------------
# Extract main CdrResponse node from a response document.  This
# function will be called in one of two situations:
#  (a) a CDR session has already been established, and only
#      one CdrResponse element will be present; or
#  (b) the caller was given a login ID and password, in which case
#      there will be three CdrResponse elements present: one for
#      the CdrLogon command; one for the command submitted by the
#      original caller; and one for the CdrLogoff command.
# While we're at it, we check to make sure that the status of the
# command was success.
#----------------------------------------------------------------------
def extractResponseNode(caller, responseString):
    docElem = xml.dom.minidom.parseString(responseString).documentElement
    when = docElem.getAttribute('Time')
    cdrResponseElems = docElem.getElementsByTagName('CdrResponse')
    if not cdrResponseElems:
        errElems = docElem.getElementsByTagName('Err')
        if errElems:
            wrapException(caller, errElems)
        else:
            raise StandardError(caller, 'No CdrResponse elements found')
    if len(cdrResponseElems) == 1:
        responseElem = cdrResponseElems[0]
    elif len(cdrResponseElems) == 3:
        responseElem = cdrResponseElems[2]
        raise StandardError(caller, 'Found %d CdrResponse elements; '
                                    'expected one or three')
    if responseElem.getAttribute('Status') == 'success':
        return CdrResponseNode(responseElem, when)
    errElems = cdrResponseElems[0].getElementsByTagName('Err')
    if not errElems:
        raise StandardError(caller, 'call failed but Err elements missing')
    wrapException(caller, errElems)

    # wrapException does not return, but add a return to silence pychecker
    return None

#----------------------------------------------------------------------
# Get the list of filters in the CDR.
#----------------------------------------------------------------------
def getFilters(session, host = DEFAULT_HOST, port = DEFAULT_PORT):
    cmd          = "<CdrGetFilters/>"
    response     = sendCommands(wrapCommand(cmd, session), host, port)
    responseElem = extractResponseNode('getFilters', response)
    filters      = []
    elems        = responseElem.specificElement.getElementsByTagName('Filter')
    for elem in elems:
        docId    = elem.getAttribute('DocId')
        name     = getTextContent(elem)
        filters.append(IdAndName(docId, name))
    return filters

#----------------------------------------------------------------------
# Get the list of filter sets in the CDR.
#----------------------------------------------------------------------
def getFilterSets(session, host = DEFAULT_HOST, port = DEFAULT_PORT):
    cmd      = "<CdrGetFilterSets/>"
    response = sendCommands(wrapCommand(cmd, session), host, port)
    response = extractResponseNode('getFilterSets', response)
    sets     = []
    elems    = response.specificElement.getElementsByTagName('FilterSet')
    for elem in elems:
        id       = int(elem.getAttribute('SetId'))
        name     = getTextContent(elem)
        sets.append(IdAndName(id, name))
    return sets

#----------------------------------------------------------------------
# Object for a named set of CDR filters.
#----------------------------------------------------------------------
class FilterSet:
    def __init__(self, name, desc, notes = None, members = None):
        self.name     = name
        self.desc     = desc
        self.notes    = notes
        self.members  = members or []
        self.expanded = 0
    def __repr__(self):
        rep = "name=%s\n" % self.name
        rep += "desc=%s\n" % self.desc
        if self.notes:
            rep += "notes=%s\n" % self.notes
        if self.expanded:
            rep += "Expanded list of filters:\n"
        for member in self.members:
            if not self.expanded and type(member.id) == type(9):
                rep += "filter set %d (%s)\n" % (member.id, member.name)
            else:
                rep += "filter %s (%s)\n" % (member.id, member.name)
        return rep

#----------------------------------------------------------------------
# Pack up XML elements for a CDR filter set (common code used by
# addFilterSet() and repFilterSet()).
#----------------------------------------------------------------------
def packFilterSet(filterSet):
    elems = "<FilterSetName>%s</FilterSetName>" % filterSet.name
    elems += "<FilterSetDescription>%s</FilterSetDescription>" % \
            filterSet.desc
    if filterSet.notes is not None:
        elems += "<FilterSetNotes>%s</FilterSetNotes>" % filterSet.notes
    for member in filterSet.members:
        if type(member.id) == type(9):
            elems += "<FilterSet SetId='%d'/>" % member.id
        else:
            elems += "<Filter DocId='%s'/>" % member.id
    return elems

#----------------------------------------------------------------------
# Create a new CDR filter set.
#----------------------------------------------------------------------
def addFilterSet(session, filterSet, host = DEFAULT_HOST, port = DEFAULT_PORT):
    cmd  = "<CdrAddFilterSet>%s</CdrAddFilterSet>" % packFilterSet(filterSet)
    resp = sendCommands(wrapCommand(cmd, session), host, port)
    node = extractResponseNode('addFilterSet', resp)
    return node.specificElement.getAttribute('TotalFilters')

#----------------------------------------------------------------------
# Replace an existing CDR filter set.
#----------------------------------------------------------------------
def repFilterSet(session, filterSet, host = DEFAULT_HOST, port = DEFAULT_PORT):
    cmd  = "<CdrRepFilterSet>%s</CdrRepFilterSet>" % packFilterSet(filterSet)
    resp = sendCommands(wrapCommand(cmd, session), host, port)
    node = extractResponseNode('repFilterSet', resp)
    return node.specificElement.getAttribute('TotalFilters')

#----------------------------------------------------------------------
# Get the attributes and members of a CDR filter set.
# The members attribute in the returned object will contain a list of
# objects with id and name attributes.  For a nested filter set, the
# id will be the integer representing the primary key of the set; for
# a filter, the id will be a string containing the CDR document ID in
# the form 'CDR0000099999'.
#----------------------------------------------------------------------
def getFilterSet(session, name, host = DEFAULT_HOST, port = DEFAULT_PORT):
    cmd          = "<CdrGetFilterSet><FilterSetName>%s" \
                   "</FilterSetName></CdrGetFilterSet>" % name
    response     = sendCommands(wrapCommand(cmd, session), host, port)
    responseNode = extractResponseNode('getFilterSet', response)
    name         = None
    desc         = None
    notes        = None
    members      = []
    for node in responseNode.specificElement.childNodes:
        if node.nodeType == node.ELEMENT_NODE:
            textContent = getTextContent(node)
            if node.nodeName == 'FilterSetName':
                name = textContent
            elif node.nodeName == 'FilterSetDescription':
                desc = textContent
            elif node.nodeName == 'FilterSetNotes':
                notes = textContent or None
            elif node.nodeName == 'Filter':
                member = IdAndName(node.getAttribute('DocId'), textContent)
                members.append(member)
            elif node.nodeName == 'FilterSet':
                member = IdAndName(int(node.getAttribute('SetId')), textContent)
                members.append(member)
    return FilterSet(name, desc, notes, members)

#----------------------------------------------------------------------
# Recursively rolls out the list of filters invoked by a named filter
# set.  In contrast with getFilterSet, which returns a list of nested
# filter sets and filters intermixed, all of the members of the list
# returned by this function represent filters.  Since there is no need
# to distinguish filters from nested sets by the artifice of
# representing filter IDs as strings, the id member of each object
# in this list is an integer.
#
# Takes the name of the filter set as input.  Returns a FilterSet
# object, with the members attribute as described above.
#
# Note: since it is possible for bad data to trigger infinite
# recursion, we throw an exception if the depth of nesting exceeds
# a reasonable level.
#
# WARNING: treat the returned objects as read-only, otherwise you'll
# corrupt the cache used for future calls.
#----------------------------------------------------------------------
_expandedFilterSetCache = {}
def expandFilterSet(session, name, level = 0,
                    host = DEFAULT_HOST, port = DEFAULT_PORT):
    global _expandedFilterSetCache
    if level > 100:
        raise StandardError('expandFilterSet', 'infinite nesting of sets')
    if _expandedFilterSetCache.has_key(name):
        return _expandedFilterSetCache[name]
    set = getFilterSet(session, name, host, port)
    newSetMembers = []
    for member in set.members:
        if type(member.id) == type(9):
            nestedSet = expandFilterSet(session, member.name, level + 1)
            newSetMembers += nestedSet.members
        else:
            newSetMembers.append(member)
    set.members = newSetMembers
    set.expanded = 1
    _expandedFilterSetCache[name] = set
    return set

#----------------------------------------------------------------------
# Returns a dictionary containing all of the CDR filter sets, rolled
# out by the expandFilterSet() function above, indexed by the filter
# set names.
#----------------------------------------------------------------------
def expandFilterSets(session, host = DEFAULT_HOST, port = DEFAULT_PORT):
    sets = {}
    for set in getFilterSets(session):
        sets[set.name] = expandFilterSet(session, set.name, host = host,
                                         port = port)
    return sets

#----------------------------------------------------------------------
# Delete an existing CDR filter set.
#----------------------------------------------------------------------
def delFilterSet(session, name, host = DEFAULT_HOST, port = DEFAULT_PORT):
    cmd  = "<CdrDelFilterSet><FilterSetName>%s" \
           "</FilterSetName></CdrDelFilterSet>" % name
    resp = sendCommands(wrapCommand(cmd, session), host, port)
    extractResponseNode('delFilterSet', resp)

#----------------------------------------------------------------------
# Mark tracking documents generated by failed mailer jobs as deleted.
#----------------------------------------------------------------------
def mailerCleanup(session, host = DEFAULT_HOST, port = DEFAULT_PORT):
    resp = sendCommands(wrapCommand("<CdrMailerCleanup/>", session),
                        host, port)
    dom = xml.dom.minidom.parseString(resp)
    docs = []
    errs = []
    for elem in dom.getElementsByTagName('DeletedDoc'):
        digits = re.sub(r'[^\d]', '', getTextContent(elem))
        docs.append(int(digits))
    for elem in dom.getElementsByTagName('Err'):
        errs.append(getTextContent(elem))
    return (docs, errs, resp)

#----------------------------------------------------------------------
# Used by normalizeDoc; treats string as writable file object.
#----------------------------------------------------------------------
class StringSink:
    def __init__(self, s = ""):
        self.s = s
    def __repr__(self):
        return self.s
    def write(self, s):
        self.s += s

#----------------------------------------------------------------------
# Remove all lines from a multi-line string (e.g., an XML doc)
# that are empty or contain nothing but whitespace.
#----------------------------------------------------------------------
def stripBlankLines(s):
    # Make a sequence
    inSeq = s.split("\n")

    # Copy non blank lines to new sequence
    outSeq = []
    for line in inSeq:
        if len(string.lstrip(line)):
            outSeq.append(line)

    # Return them as a string with newlines at each line end
    return "\n".join(outSeq);

#----------------------------------------------------------------------
# Takes a utf-8 string for an XML document and creates a utf-8 string
# suitable for comparing two versions of XML documents by normalizing
# non-essential differences away.  Used by compareDocs() (below).
#----------------------------------------------------------------------
def normalizeDoc(utf8DocString):
    sFile = StringSink()
    dom = xml.dom.minidom.parseString(utf8DocString)
    dom.writexml(sFile)
    return sFile.s

#----------------------------------------------------------------------
# Extract the first CDATA section from a document.
# Simple version, only gets first CDATA, but we never use
#  more than one.
# If no CDATA, then returns None.
#----------------------------------------------------------------------
def getCDATA(utf8string):
    pat = re.compile(r"<!\[CDATA\[(.*?)]]>", re.DOTALL)
    data = pat.search(utf8string)
    if data:
        return data.group(1)
    return None

#----------------------------------------------------------------------
# Compares two XML documents by normalizing each.  Returns non-zero
# if documents are different; otherwise zero.  Expects each document
# to be passed as utf8-encoded documents.
#----------------------------------------------------------------------
def compareXmlDocs(utf8DocString1, utf8DocString2):
    return cmp(normalizeDoc(utf8DocString1), normalizeDoc(utf8DocString2))

#----------------------------------------------------------------------
# Compare two XML documents by normalizing each.
# Returns the output of a textual differencer as a sequence of lines.
# See Python difflib.Differ.compare() for diff format.
#   Pass:
#     2 utf8 strings to compare
#     chgOnly  - True=only show changed lines, else show all.
#     useCDATA - True=call getCDATA on each string before compare.
#----------------------------------------------------------------------
def diffXmlDocs(utf8DocString1, utf8DocString2, chgOnly=True, useCDATA=False):
    # Extract data if needed
    if useCDATA:
        d1 = getCDATA(utf8DocString1)
        d2 = getCDATA(utf8DocString2)
    else:
        d1 = utf8DocString1
        d2 = utf8DocString2

    # Normalize
    doc1 = stripBlankLines(xml.dom.minidom.parseString(d1).toprettyxml("  "))
    doc2 = stripBlankLines(xml.dom.minidom.parseString(d2).toprettyxml("  "))

    # Compare
    diffObj = difflib.Differ()
    diffSeq = diffObj.compare(doc1.splitlines(1),doc2.splitlines(1))

    # If caller only wants changed lines, drop all lines with leading space
    if chgOnly:
        chgSeq = []
        for line in diffSeq:
            if line[0] != ' ':
                chgSeq.append (line)
        # Return them as a (possibly empty) string
        return "".join(chgSeq)

    # Else return entire document as a string
    return "".join(diffSeq)

#----------------------------------------------------------------------
# Tell the caller if we are on the development host.
#----------------------------------------------------------------------
def isDevHost():
    localhost = socket.gethostname()
    return localhost.upper().startswith(DEV_NAME.upper())

#----------------------------------------------------------------------
# Tell the caller if we are on the development host.
#----------------------------------------------------------------------
def isProdHost():
    localhost = socket.gethostname()
    return localhost.upper().startswith(PROD_NAME.upper())

#----------------------------------------------------------------------
# Get the email address for the current session (if any).
#----------------------------------------------------------------------
def getEmail(session):
    if not session:
        return None
    conn = cdrdb.connect()
    cursor = conn.cursor()
    cursor.execute("""\
        SELECT u.email
          FROM usr u
          JOIN session s
            ON s.usr = u.id
         WHERE s.name = ?""", session)
    rows = cursor.fetchall()
    if not rows:
        return None
    return rows[0][0]

#----------------------------------------------------------------------
# Add a row to the external_map table.
#----------------------------------------------------------------------
def addExternalMapping(credentials, usage, value, id = None,
                       host = DEFAULT_HOST, port = DEFAULT_PORT):
    if type(usage) == type(u""):
        usage = usage.encode('utf-8')
    if type(value) == type(u""):
        value = value.encode("utf-8")
    id = id and ("<CdrId>%s</CdrId>" % normalize(id)) or ""
    cmd = ("<CdrAddExternalMapping><Usage>" + usage + "</Usage><Value>" +
           value + "</Value>" + id + "</CdrAddExternalMapping>")
    resp = sendCommands(wrapCommand(cmd, credentials), host, port)

    # This is how we should have been handling failures all along. :-<}
    errors = getErrors(resp, errorsExpected = False, asSequence = True)
    if errors:
        raise Exception(errors)

#----------------------------------------------------------------------
# Change the active_status column for a document.
#----------------------------------------------------------------------
def setDocStatus(credentials, docId, newStatus, host = DEFAULT_HOST,
                 port = DEFAULT_PORT):
    id   = "<DocId>%s</DocId>" % normalize(docId)
    stat = "<NewStatus>%s</NewStatus>" % newStatus
    cmd  = "<CdrSetDocStatus>%s%s</CdrSetDocStatus>" % (id, stat)
    resp = sendCommands(wrapCommand(cmd, credentials), host, port)
    errs = getErrors(resp, errorsExpected = False, asSequence = True)
    if errs:
        raise Exception(errs)

#----------------------------------------------------------------------
# Retrieve the active status for a document.
#----------------------------------------------------------------------
def getDocStatus(credentials, docId, host = DEFAULT_HOST):
    conn = cdrdb.connect('CdrGuest', dataSource = host)
    cursor = conn.cursor()
    idTuple = exNormalize(docId)
    id = idTuple[1]
    cursor.execute("SELECT active_status FROM all_docs WHERE id = ?", id)
    rows = cursor.fetchall()
    if not rows:
        raise Exception(['Invalid document ID %s' % docId])
    return rows[0][0]

#----------------------------------------------------------------------
# Convenience wrapper for unblocking a document.
#----------------------------------------------------------------------
def unblockDoc(credentials, docId, host = DEFAULT_HOST, port = DEFAULT_PORT):
    setDocStatus(credentials, docId, "A", host, port)
