#----------------------------------------------------------------------
#
# $Id: cdr.py,v 1.5 2001-05-18 19:19:06 bkline Exp $
#
# Module of common CDR routines.
#
# Usage:
#   import cdr
#
# $Log: not supported by cvs2svn $
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
import socket, string, struct, sys, re, cgi

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
# Normalize a document id to form 'CDRnnnnnnnnnn'.
#----------------------------------------------------------------------
def normalize(id):
    if id is None: return None
    if type(id) == type(9):
        idNum = id
    else:
        digits = re.sub('[^\d]', '', id)
        idNum  = string.atoi(digits)
    return "CDR%010d" % idNum

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
        expr = re.compile("<Err>(.*)</Err>", re.DOTALL)
        err = expr.search(resp)
        err = err and err.group(1) or "Unknown failure"
        return err

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
    id  = normalize(docId)
    lck = "<Lock>%s</Lock>" % (checkout)
    ver = "<DocVersion>%s</DocVersion>" % (version)
    cmd = "<CdrGetDoc><DocId>%s</DocId>%s%s</CdrGetDoc>" % (id, lck, ver)

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
    if docId: docElem = "<Document href='%s'/>" % normalize(docId)
    elif doc: docElem = "<Document><![CDATA[%s]]></Document>" % doc
    else: return "<Errors><Err>Document not specified.</Err></Errors>"
    filt = normalize(filterId)
    cmd = "<CdrFilter><Filter href='%s'/>%s</CdrFilter>" % (filt, docElem)

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
                 vvLists    = None,
                 comment    = None,
                 error      = None):
        this.type           = type
        this.format         = format
        this.versioning     = versioning
        this.created        = created
        this.schema_mod     = schema_mod
        this.dtd            = dtd
        this.schema         = schema
        this.vvLists        = vvLists
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
    def __init__(this, id, name):
        this.id       = id
        this.name     = name
        this.parents  = []
        this.children = []
        
class TermSet:
    def __init__(this, error = None):
        this.terms = {}
        this.error = error

#----------------------------------------------------------------------
# Gets context information for term's position in terminology tree.
#----------------------------------------------------------------------
def getTree(credentials, docId, host = DEFAULT_HOST, port = DEFAULT_PORT):

    # Create the command
    cmd = "<CdrGetTree><DocId>%s</DocId></CdrGetTree>" % normalize(docId)

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
        (id, name) = term
        terms[id]  = Term(id = id, name = name)

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
    for a in re.findall("<Action>\s*<Name>(.*)</Name>\s*"
                        "<NeedDoctype>(.*)</NeedDoctype>\s*</Action>", resp):
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
    return re.findall("<UserName>(.*)</UserName>", resp)

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
    return re.findall("<GrpName>(.*)</GrpName>", resp)

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
    return re.findall("<DocType>(.*?)</DocType>", resp)

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
    name     = re.findall("<GrpName>(.*)</GrpName>", resp)[0]
    group    = Group(name)
    authExpr = re.compile("<Auth>(.*?)</Auth>", re.DOTALL)
    cmtExpr  = re.compile("<Comment>(.*)</Comment>", re.DOTALL)
    comment  = cmtExpr.findall(resp)
    for user in re.findall("<UserName>(.*?)</UserName>", resp):
        group.users.append(user)
    for auth in authExpr.findall(resp):
        action  = re.findall("<Action>(.*)</Action>", auth)
        docType = re.findall("<DocType>(.*)</DocType>", auth)
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
    name     = re.findall("<UserName>(.*)</UserName>", resp)[0]
    password = re.findall("<Password>(.*)</Password>", resp)[0]
    user     = User(name, password)
    fullname = re.findall("<FullName>(.*)</FullName>", resp)
    office   = re.findall("<Office>(.*)</Office>", resp)
    email    = re.findall("<Email>(.*)</Email>", resp)
    phone    = re.findall("<Phone>(.*)</Phone>", resp)
    groups   = re.findall("<GrpName>(.*?)</GrpName>", resp)
    cmtExpr  = re.compile("<Comment>(.*)</Comment>", re.DOTALL)
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
    return re.findall("<Name>(.*)</Name>", resp)

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
def putLinkType(credentials, name, linkType, host = DEFAULT_HOST, 
                                             port = DEFAULT_PORT):

    # Create the command
    if name:
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
    cmd += (name and "</CdrModLinkType>" or "</CdrAddLinkType>")
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
