#----------------------------------------------------------------------
#
# $Id: cdr.py,v 1.1 2001-03-26 00:32:57 bkline Exp $
#
# Module of common CDR routines.
#
# Usage:
#   import cdr
#
#----------------------------------------------------------------------

#----------------------------------------------------------------------
# Import required packages.
#----------------------------------------------------------------------
import socket, string, struct, sys, re

#----------------------------------------------------------------------
# Set some package constants
#----------------------------------------------------------------------
DEFAULT_HOST  = 'localhost'
DEFAULT_PORT  = 2019
LOGON_STRING  = """<CdrCommandSet><CdrCommand><CdrLogon>
                   <UserName>%s</UserName><Password>%s</Password>
                   </CdrLogon></CdrCommand>"""
LOGOFF_STRING = "<CdrCommand><CdrLogoff/></CdrCommand></CdrCommandSet>"

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
# Extract error elements from XML.
#----------------------------------------------------------------------
def getErrors(xml):

    # Comile the pattern for the regular expression.
    pattern = re.compile("<Errors[>\s].*</Errors>", re.DOTALL)

    # Search for the <Errors> element.
    errors  =  pattern.search(xml)
    if errors: return errors.group()
    else:      return "<Errors><Err>Internal failure</Err></Errors>"
    
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
# Log in to the CDR Server.  Returns session ID.
#----------------------------------------------------------------------
def login(userId, passWord, host = DEFAULT_HOST, port = DEFAULT_PORT):
    
    # Send the login request to the server.
    cmds = LOGON_STRING % (userId, passWord) + "</CdrCommandSet>"
    resp = sendCommands(cmds, host, port)

    # Extract the session ID.
    return extract("<SessionId[^>]*>(.+)</SessionId>", resp)

#----------------------------------------------------------------------
# Log out from the CDR.
#----------------------------------------------------------------------
def logout(session, host= DEFAULT_HOST, port = DEFAULT_PORT):

    sendCommands(wrapCommand("<CdrLogoff/>", session), host, port)

#----------------------------------------------------------------------
# Add a new document to the CDR Server.
#----------------------------------------------------------------------
def addDoc(credentials, file = None, doc = None, 
           checkIn = 'N', val = 'N', reason = '',
           host = DEFAULT_HOST, port = DEFAULT_PORT):

    # Load the document if necessary.
    if file: doc = open(file, "r").read()
    if not doc: 
        if file: return "<Errors><Err>%s not found</Err></Errors>" % (fileName)
        else:    return "<Errors><Err>Document missing.</Err></Errors>"

    # Create the command.
    checkIn = "<CheckIn>%s</CheckIn>" % (checkIn)
    val     = "<Validate>%s</Validate>" % (val)
    reason  = "<Reason>%s</Reason>" % (reason)
    cmd     = "<CdrAddDoc>%s%s%s%s</CdrAddDoc>" % (checkIn, val, reason, doc)

    # Submit the commands.
    resp = sendCommands(wrapCommand(cmd, credentials), host, port)

    # Extract the document ID.
    return extract("(<DocId[>\s].*</DocId>)", resp)

#----------------------------------------------------------------------
# Replace an existing document in the CDR Server.
#----------------------------------------------------------------------
def repDoc(credentials, file = None, doc = None, 
           checkIn = 'N', val = 'N', reason = '',
           host = DEFAULT_HOST, port = DEFAULT_PORT):

    # Load the document if necessary.
    if file: doc = open(file, "r").read()
    if not doc: 
        if file: return "<Errors><Err>%s not found</Err></Errors>" % (fileName)
        else:    return "<Errors><Err>Document missing.</Err></Errors>"

    # Create the command.
    checkIn = "<CheckIn>%s</CheckIn>" % (checkIn)
    val     = "<Validate>%s</Validate>" % (val)
    reason  = "<Reason>%s</Reason>" % (reason)
    cmd     = "<CdrRepDoc>%s%s%s%s</CdrRepDoc>" % (checkIn, val, reason, doc)

    # Submit the commands.
    resp = sendCommands(wrapCommand(cmd, credentials), host, port)

    # Extract the document ID.
    return extract("(<DocId[>\s].*</DocId>)", resp)

#----------------------------------------------------------------------
# Retrieve a specified document from the CDR Server.
#----------------------------------------------------------------------
def getDoc(credentials, docId, checkout = 'N', version = "Current",
           host = DEFAULT_HOST, port = DEFAULT_PORT):

    # Create the command.
    lck = "<Lock>%s</Lock>" % (checkout)
    ver = "<DocVersion>%s</DocVersion>" % (version)
    cmd = "<CdrGetDoc><DocId>%s</DocId>%s%s</CdrGetDoc>" % (docId, lck, ver)

    # Submit the commands.
    resp = sendCommands(wrapCommand(cmd, credentials), host, port)

    # Extract the document.
    return extract("(<CdrDoc[>\s].*</CdrDoc>)", resp)

#----------------------------------------------------------------------
# Retrieve a specified document from the CDR Server using a filter.
#----------------------------------------------------------------------
def filterDoc(credentials, docId, filterId,
              host = DEFAULT_HOST, port = DEFAULT_PORT):

    # Create the command.
    cmd = """<CdrFilter><Filter href='%s'/>
             <Document href='%s'/></CdrFilter>""" % (filterId, docId)

    # Submit the commands.
    resp = sendCommands(wrapCommand(cmd, credentials), host, port)

    # Extract the filtered document.
    return extract("<Document[>\s][^<]*"
                   "<!\[CDATA\[(.*)\]\]>\s*</Document>", resp)
