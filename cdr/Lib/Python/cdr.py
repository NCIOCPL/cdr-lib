#----------------------------------------------------------------------
#
# $Id: cdr.py,v 1.2 2001-04-08 16:31:53 bkline Exp $
#
# Module of common CDR routines.
#
# Usage:
#   import cdr
#
# $Log: not supported by cvs2svn $
#
#----------------------------------------------------------------------

#----------------------------------------------------------------------
# Import required packages.
#----------------------------------------------------------------------
import socket, string, struct, sys, re, cgi, xml.dom.minidom

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
    return extract("<DocId.*>(CDR\d+)</DocId>", resp)

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
    return extract("<DocId.*>(CDR\d+)</DocId>", resp)

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
def filterDoc(credentials, filterId, docId = None, doc = None,
              host = DEFAULT_HOST, port = DEFAULT_PORT):

    # Create the command.
    if docId: docElem = "<Document href='%s'/>" % docId
    elif doc: docElem = "<Document><![CDATA[%s]]></Document>" % doc
    else: return "<Errors><Err>Document not specified.</Err></Errors>"
    cmd = "<CdrFilter><Filter href='%s'/>%s</CdrFilter>" % (filterId, docElem)

    # Submit the commands.
    resp = sendCommands(wrapCommand(cmd, credentials), host, port)

    # Extract the filtered document.
    return extract("<Document[>\s][^<]*"
                   "<!\[CDATA\[(.*)\]\]>\s*</Document>", resp)

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

    # Extract the results.
    results = extract("<QueryResults>(.*)</QueryResults>", resp)
    if string.find(results, "<Err") != -1:
        return (None, results)
    qrElemsPattern  = re.compile("<QueryResult>(.*?)</QueryResult>", re.DOTALL)
    docIdPattern    = re.compile("<DocId>(.*)</DocId>", re.DOTALL)
    docTypePattern  = re.compile("<DocType>(.*)</DocType>", re.DOTALL)
    docTitlePattern = re.compile("<DocTitle>(.*)</DocTitle>", re.DOTALL)
    ret = []
    for qr in qrElemsPattern.findall(results):
        docId    = docIdPattern.search(qr).group(1)
        docType  = docTypePattern.search(qr).group(1)
        docTitle = docTitlePattern.search(qr).group(1)
        ret.append((docId, docType, docTitle))
    return (ret, None)

#----------------------------------------------------------------------
# Class to contain CDR document type information.
#----------------------------------------------------------------------
class dtinfo:
    def __init__(this,
                 type       = None,
                 format     = None,
                 versioning = None,
                 created    = None,
                 schema_mod = None,
                 dtd        = None,
                 schema     = None,
                 comment    = None,
                 error      = None):
        this.type           = type
        this.format         = format
        this.versioning     = versioning
        this.created        = created
        this.schema_mod     = schema_mod
        this.dtd            = dtd
        this.schema         = schema
        this.comment        = comment
        this.error          = error
    def __repr__(this):
        if this.error: return this.error
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
""" % (this.type or '',
       this.format or '',
       this.versioning or '',
       this.created or '',
       this.schema_mod or '',
       this.schema or '',
       this.dtd or '',
       this.comment or '')

#----------------------------------------------------------------------
# Retrieve document type information from the CDR.
#----------------------------------------------------------------------
def getDoctype(credentials, doctype, host = DEFAULT_HOST, port = DEFAULT_PORT):

    # Create the command
    cmd = "<CdrGetDocType Type='%s'/>" % doctype

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
    schemaExpr     = re.compile(r"<DocSchema>\s*<!\[CDATA\[(.*)\]\]>"
                                r"\s*</DocSchema>",
                                re.DOTALL)

    # Parse out the components.
    type       = typeExpr      .search(results)
    format     = formatExpr    .search(results)
    versioning = versioningExpr.search(results)
    created    = createdExpr   .search(results)
    schema_mod = schemaModExpr .search(results)
    dtd        = dtdExpr       .search(results)
    schema     = schemaExpr    .search(results)
    comment    = commentExpr   .search(results)
    
    # Return a dtinfo instance.
    return dtinfo(type       = type       and type      .group(1) or '',
                  format     = format     and format    .group(1) or '',
                  versioning = versioning and versioning.group(1) or '',
                  created    = created    and created   .group(1) or '',
                  schema_mod = schema_mod and schema_mod.group(1) or '',
                  dtd        = dtd        and dtd       .group(1) or '',
                  schema     = schema     and schema    .group(1) or '',
                  comment    = comment    and comment   .group(1) or '')

#----------------------------------------------------------------------
# Modify existing document type information in the CDR.
#----------------------------------------------------------------------
def modDoctype(credentials, info, host = DEFAULT_HOST, port = DEFAULT_PORT):

    # Create the command
    cmd = "<CdrModDocType Type='%s' Format='%s' Versioning='%s'>"\
          "<DocSchema><![CDATA[%s]]></DocSchema>"\
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

#----------------------------------------------------------------------
# Class representing recursive tree context for CDR terminology document.
#----------------------------------------------------------------------
class TermTree:
    def __init__(this, 
                 id         = None, 
                 name       = None, 
                 parents    = None, 
                 children   = None, 
                 error      = None):
        this.id             = id
        this.name           = name
        this.parents        = parents
        this.children       = children
        this.error          = error
    def parentRep(this, level = 0):
        if not this.parents: return ''
        rep = ''
        for p in this.parents:
            rep = rep + ' ' * level + "%s (%s)\n%s" % (p.name, 
                                                       p.id,
                                                       p.parentRep(level + 1))
        return rep
    def childrenRep(this, level = 0):
        if not this.children: return ''
        rep = ''
        for c in this.children:
            rep = rep + ' ' * level + "%s (%s)\n%s" % (c.name, 
                                                       c.id,
                                                       c.childrenRep(level + 1))
        return rep
    def __repr__(this):
        if this.error: return this.error
        rep = """\
[Term] %s (%s)
[Parents]
%s
[Children]
%s
""" % (this.name, 
       this.id, 
       this.parentRep(),
       this.childrenRep())
        return rep

#----------------------------------------------------------------------
# Extract the text content of a DOM element.
#----------------------------------------------------------------------
def getTextContent(node):
    text = ''
    for n in node.childNodes:
        if n.nodeType == xml.dom.minidom.Node.TEXT_NODE:
            text = text + n.nodeValue
    return text

#----------------------------------------------------------------------
# Recursively parse Term information from a DOM node.
#----------------------------------------------------------------------
def parseTermNode(node):
    id       = ''
    name     = ''
    parents  = []
    children = []
    for n in node.childNodes:
        if n.nodeType == xml.dom.minidom.Node.ELEMENT_NODE:
            if n.nodeName == 'DocId': id = getTextContent(n)
            elif n.nodeName == 'TermName': name = getTextContent(n)
            elif n.nodeName == 'Parents':
                for p in n.childNodes:
                    parents.append(parseTermNode(p))
            elif n.nodeName == 'Children':
                for c in n.childNodes:
                    children.append(parseTermNode(c))
    return TermTree(id, name, parents, children)

#----------------------------------------------------------------------
# Gets context information for term's position in terminology tree.
#----------------------------------------------------------------------
def getTree(credentials, docId, host = DEFAULT_HOST, port = DEFAULT_PORT):

    # Create the command
    cmd = "<CdrGetTree><DocId>%s</DocId></CdrGetTree>" % docId

    # Submit the request.
    resp = sendCommands(wrapCommand(cmd, credentials), host, port)
    if string.find(resp, "<Err>") != -1:
        expr = re.compile("<Err>(.*)</Err>", re.DOTALL)
        err = expr.search(resp)
        err = err and err.group(1) or "Unknown failure"
        return TermTree(error = err)

    # Parse the tree
    expr = re.compile("<CdrGetTreeResp>(.*)</CdrGetTreeResp>", re.DOTALL)
    docString = expr.search(resp)
    if not docString: return TermTree(error = "Response not found")
    try:
        dom = xml.dom.minidom.parseString(docString.group(1))
        if not dom: return TermTree(error = "Failure parsing response")
        return parseTermNode(dom.documentElement)
    except:
        return TermTree(error = "Failure parsing response")
