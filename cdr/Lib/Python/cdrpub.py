#----------------------------------------------------------------------
#
# Script for command line and CGI publishing.
#
# $Id: cdrpub.py,v 1.7 2002-03-01 22:46:19 pzhang Exp $
# $Log: not supported by cvs2svn $
# Revision 1.6  2002/02/28 23:23:46  pzhang
# Don't care about destination directory if no_output != 'Y'.
#
# Revision 1.5  2002/02/22 19:01:14  pzhang
# Updated getSubsets to return flags for Params and UserSelect.
# Updated isPublishable to return version number or -1.
# Updated getAction to return the action name or ""
#
# Revision 1.4  2002/02/22 16:41:31  pzhang
# Fixed a bug in getParameters returning None instead of [].
#
# Revision 1.3  2002/02/20 22:31:47  pzhang
# First version of cdrpub.py merged with publish.py.
#
# Revision 1.10  2002/02/20 19:40:36  pzhang
# Fixed __userXXX naming convention. Fixed a couple of bugs in SQL of isPermitted.
#
# Revision 1.9  2002/02/20 15:23:19  pzhang
# Changed SCRIPTS to BASEDIR due to changes in cdr directory.
#
# Revision 1.8  2002/02/14 21:43:26  mruben
# Fixed log comment; changed no_output to self.no_output [bkline for mruben].
#
# Revision 1.7  2002/02/14 21:25:49  mruben
# Suppressed unused docTypes variable; added no_output support [commit by RMK].
#
# Revision 1.6  2002/02/07 14:46:17  mruben
# added no output option
#
# Revision 1.5  2002/01/31 18:20:49  mruben
# Fixed SQL for selecting publishing systems
#
# Revision 1.4  2001/12/03 23:14:15  Pzhang
# Added code for email notification.
# Disabled updateStatuses since pub_event is now a view.
#
# Revision 1.3  2001/10/05 18:50:49  Pzhang
# Changed Publish.SUCCESS to SUCCEED, Fail to Failure, Wait to Waiting.
#
# Revision 1.2  2001/10/05 15:08:01  Pzhang
# Added __invokePracessScript for Bob's Python Script.
# Imported traceback to handle exceptions.
#
# Revision 1.1  2001/10/01 15:07:21  Pzhang
# Initial revision
#
#----------------------------------------------------------------------

from win32com.client import Dispatch
import os, sys, shutil, re, cdr, xml.dom.minidom, copy
import pythoncom, string, time, cdrdb

DEBUG = 0

# This flag controls the print statement through the
#   package. It is critical to set NCGI to 0 in the
#   CGI script.
NCGI = 1

#-------------------------------------------------------------------
# class: Publish
#    This class encapsulate the publishing data and methods.
#    There is one publing method, publish(), for command line;
#       Other methods are helpers for CGI script.
#
# Inputs to the contructor:
#    strCtrlDocId:  a publishing system control document ID in STRING!
#    subsetName:    a publishing system subset name.
#    credential:    a Session ID (name in the session table).
#    docIds:        (optional) a list of selected CDR document ID
#                       and/or document version.
#    params:        (optional) a list of subset parameters.
#    jobId:         the process job id for a subset publishing.
# Issues:           Passing parameters or using class variables?
#                   Minimal error checking has been done.
#-------------------------------------------------------------------
class Publish:

    SUCCEED = "Success"
    FAIL = "Failure"
    WAIT = "Waiting user approval"
    RUN = "In process"
    INIT = "Initial"
    READY = "Ready"
    START = "Started"

    FILE = 4
    DOCTYPE = 5
    DOC = 6

    # Many options are not implemented.
    IGNORE = 7


    # class private variables
    __cdrConn = None
    __procId = 0    # This duplicates self.jobId.
                    # Keep it for code history or clarity.
    __specs = None
    __docIds = {}   # Dictionary to store non-duplicate docIds
    __userId = 0
    __userName = ""
    __cdr_email = "cdr@mmdb2.nci.nih.gov"

    # Do nothing but set local variables.
    def __init__(self, strCtrlDocId, subsetName, credential,
                docIds, params, email = None, no_output = 'N', jobId = 0):
        self.strCtrlDocId = strCtrlDocId
        self.subsetName = subsetName
        self.credential = credential
        self.docIds = docIds
        self.params = params
        self.email = email
        self.jobId = jobId
        self.no_output = no_output

    # This is a CGI helper function.
    def getPubSys(self):

        # Connect to CDR. Abort when failed. Cannot log status in this case.
        self.__getConn()

        # Initialized the list of tuples: (title, id, sysName, desc).
        pickList = []
        tuple = ["", "", "", ""]

        sql = "SELECT d.title, d.id, d.xml FROM document d " \
              "JOIN doc_type t ON d.doc_type = t.id " \
              "WHERE t.name = 'PublishingSystem' "
        rs = self.__execSQL(sql)

        while not rs.EOF:
            tuple[0] = rs.Fields("title").Value
            tuple[1] = rs.Fields("id").Value
            docElem = rs.Fields("xml").Value.encode('latin-1')

            docElem = xml.dom.minidom.parseString(docElem).documentElement
            for node in docElem.childNodes:
                if node.nodeType == xml.dom.minidom.Node.ELEMENT_NODE:
                    # SystemName comes first by schema. So tuple[2] will
                    #   be initialized once for all.
                    if node.nodeName == 'SystemName':
                        tuple[2] = ''
                        for n in node.childNodes:
                            if n.nodeType == xml.dom.minidom.Node.TEXT_NODE:
                                tuple[2] = tuple[2] + n.nodeValue
                    if node.nodeName == 'SystemDescription':
                        tuple[3] = ''
                        for n in node.childNodes:
                            if n.nodeType == xml.dom.minidom.Node.TEXT_NODE:
                                tuple[3] = tuple[3] + n.nodeValue

            deep = copy.deepcopy(tuple)
            pickList.append(deep)

            rs.MoveNext()

        rs.Close()
        rs = None
        self.__cdrConn = None

        return pickList

    # This is a CGI helper function.
    def getPubSubset(self):

        # Connect to CDR. Abort when failed. Cannot log status in this case.
        self.__getConn()

        # Initialized the list of tuples: 
        #   (subsetName, desc, sysName, param, userselect).
        pickList = []
        tuple = ["", "", "", "", ""]

        sql = "SELECT xml FROM document WHERE id = %s" % self.strCtrlDocId
        rs = self.__execSQL(sql)

        while not rs.EOF:
            docElem = rs.Fields("xml").Value.encode('latin-1')

            docElem = xml.dom.minidom.parseString(docElem).documentElement
            for node in docElem.childNodes:
                if node.nodeType == xml.dom.minidom.Node.ELEMENT_NODE:
                    # SystemName comes first by schema. So tuple[2] will
                    #   be initialized once for all.
                    # We may not need this if the next page
                    #   does not show the system name.
                    if node.nodeName == 'SystemName':
                        tuple[2] = ''
                        for n in node.childNodes:
                            if n.nodeType == xml.dom.minidom.Node.TEXT_NODE:
                                tuple[2] = tuple[2] + n.nodeValue

                    if node.nodeName == 'SystemSubset':
                        tuple[0] = ''
                        tuple[1] = ''
                        tuple[3] = ''
                        tuple[4] = ''
                        for n in node.childNodes:
                            if n.nodeName == 'SubsetName':
                                for m in n.childNodes:
                                    if m.nodeType == xml.dom.minidom.Node.TEXT_NODE:
                                        tuple[0] = tuple[0] + m.nodeValue
                            if n.nodeName == 'SubsetDescription':
                                for m in n.childNodes:
                                    if m.nodeType == xml.dom.minidom.Node.TEXT_NODE:
                                        tuple[1] = tuple[1] + m.nodeValue
                            if n.nodeName == 'SubsetParameters':
                                tuple[3] = 'Yes'
                            if n.nodeName == 'SubsetSpecifications':
                                for m in n.childNodes:
                                    if m.nodeName == 'SubsetSpecification':
                                        for k in m.childNodes:
                                            if k.nodeName == 'SubsetSelection':
                                                for j in k.childNodes:
                                                    if j.nodeName == 'UserSelect':
                                                        tuple[4] = 'Yes'
                                       
                        deep = copy.deepcopy(tuple)
                        pickList.append(deep)

            rs.MoveNext()

        rs.Close()
        rs = None
        self.__cdrConn = None

        return pickList

    # This is a CGI helper function.
    # Wanted to return the SQL statement as well, but not done yet.
    # Only returns the parameters so far.
    def getParamSQL(self):

        # Connect to CDR. Abort when failed. Cannot log status in this case.
        self.__getConn()

        # Initialized the list of tuples: (name, value).
        pickList = []
        tuple = ["", ""]

        sql = "SELECT xml FROM document WHERE id = %s" % self.strCtrlDocId
        rs = self.__execSQL(sql)

        while not rs.EOF:
            docElem = rs.Fields("xml").Value.encode('latin-1')
            rs.MoveNext()
        rs.Close()
        rs = None
        self.__cdrConn = None

        return self.__getParameters(self.__getSubSet(docElem))

    # This is a CGI helper function.
    # Return -1 if not publishable.
    # Return the version number if publishable.
    def isPublishable(self, docId, keepConnected = ""):

        # Connect to CDR. Abort when failed. Cannot log status in this case.
        if not self.__cdrConn:
            self.__getConn()

        # Get doc_id and doc_version.
        id = self.__getDocId(docId)
        version = self.__getVersion(docId)

        # Use the current publishable version, if exists.
        if version == -1:
            sql = "SELECT TOP 1 num FROM doc_version WHERE id = %s AND " \
                "publishable = 'Y' ORDER BY num DESC" % id

        # Query into doc_version table to verify this version.
        else:
            sql = "SELECT TOP 1 num FROM doc_version WHERE id = %s AND " \
                "num = %s AND publishable = 'Y' " % (id, version)
        rs = self.__execSQL(sql)

        ret = -1
        if not rs.EOF:
            ret = rs.Fields("num").Value
        rs.Close()
        rs = None

        if not keepConnected:
            self.__cdrConn = None

        return ret

    # This is a CGI helper function. It is the most important function
    #   for publishing CGI, which creates the publishing process.
    # This function returns an error message or a jobId. __procId is
    #   a useless LEGACY variable.
    def getJobId(self):

        # Connect to CDR. Abort when failed. Cannot log status in this case.
        self.__getConn()

        # Get user ID and Name from SessionName in CDR.
        self.__getUser()

        # At most one active publishing process can exist.
        self.__procId = self.__existProcess()
        if self.__procId:
            self.__cdrConn = None
            return "*Error: there is an active process with ID: %d." \
                    % self.__procId
        else:
            # A row in pub_proc table is created. Rows are also
            # created in pub_proc_parm and pub_proc_doc tables.
            # A job id is output for user to check status later.
            # The status is initially "Initial", and then "Ready".
            self.__procId = self.__createProcess()
            self.__cdrConn = None
        return self.__procId

    # This is a CGI helper function.
    def getStatus(self, jobId):

        # Connect to CDR. Abort when failed. Cannot log status in this case.
        self.__getConn()

        sql = """SELECT id, output_dir, CAST(started AS varchar(30)) as started,
            CAST(completed AS varchar(30)) as completed, status, messages, email
            FROM pub_proc
            WHERE id = %s""" % jobId
        rs = self.__execSQL(sql)

        row = ["id", "output_dir", "started", "completed",
            "status", "messages", "email"]
        while not rs.EOF:
            row[0] = rs.Fields("id").Value
            row[1] = rs.Fields("output_dir").Value
            row[2] = rs.Fields("started").Value
            row[3] = rs.Fields("completed").Value
            row[4] = rs.Fields("status").Value
            row[5] = rs.Fields("messages").Value
            row[6] = rs.Fields("email").Value
            rs.MoveNext()
        rs.Close()
        rs = None

        return row

    # This is the major public entry point to publishing.
    def publish(self):

            # Move the code into getJobId. getJobId sets __procId.
            if not self.jobId:
                newJob = self.getJobId()
                if type("") == type(newJob):
                    sys.exit(1)
            else:
                self.__procId = self.jobId

                # New design. Used jobId to reset all other parameters:
                # strCtrlDocId, subsetName, credential, docIds, and params
                self.__resetParamsByJobId()

            # Connect to CDR. Abort when failed. Cannot log status in this case.
            self.__getConn()

            # Get user ID and Name from SessionName in CDR.
            self.__getUser()

            # Get control document from the given sysName.
            docElem = self.__getCtrlDoc()
            if docElem is None:
                msg = "*Error: publishing control document not found."
                self.__updateStatus(Publish.FAIL, msg)
                sys.exit(1)

            # Get a node of the given SubSet.
            # Only one subset per publishing?
            subset = self.__getSubSet(docElem)

            # Handle process script.
            # Exit if there is a process script.
            self.__invokeProcessScript(subset)

            # Get the subset specifications node.
            self.__specs = self.__getSpecs(subset)

            # Get the action to check publishing permission.
            action = self.__getAction(subset)

            # Don't know the rule to check permission yet?
            permitted = self.__isPermitted(action)
            if not permitted:
                msg = "*Error: " + self.__userName + \
                      " is not permitted to publish."
                self.__updateStatus(Publish.FAIL, msg)
                sys.exit(1)

            # Get the name-value pairs of options.
            options = self.__getOptions(subset)

            # Get the name-value pairs of parameters.
            localParams = self.__getParameters(subset)

            # Get the destination directory.
            dest_base = self.__getDestination(options)
            dest_base += "." + self.__userName + "." + "%d" % time.time()
            dest = dest_base + "." + "InProcess"

            # Get the destination type.
            destType = self.__getDestinationType(options)
            if destType == Publish.FILE:
                file = self.__getDestinationFile(options)

            # For each spec, extract the associated docIds and filters.
            # Publish them based on various options.
            for spec in self.__specs.childNodes:

                # nodeName could be "#text" or others.
                if spec.nodeName != "SubsetSpecification":
                    continue

                # Replace default parameters.
                # A list of tuples: ((n1, v1), (n2, v2),)
                # localParams = self.__getParams(spec)

                # Append docIds from XQL or SQL.
                # A list of docIds: (1234, 5678,)
                # Need help from Mike to clarify???
                localDocIds = self.__getDocIds(spec, localParams)

                # Get the filters node in spec.
                filters = self.__getFilters(spec)

                # Collect document types.
                # A list of document type IDs or Names
                # A document type name is also a unique NCNAME.
                # Useful only when destType == DOCTYPE.
                # Not used.  MMR
                # docTypes = self.__getDocTypes(localDocIds)

                if destType == Publish.FILE:
                    self.__publishAll(localDocIds, filters,
                        localParams, dest, file, options)
                elif destType == Publish.DOCTYPE:
                    # Remove all files in the dest dir.
                    # No longer needed since it does not exist.
                    # os.path.isdir(dest) and shutil.rmtree(dest)

                    self.__publishType(localDocids, filters,
                        localParams, dest, options)
                elif destType == Publish.DOC:
                    # Remove all files in the dest dir.
                    # No longer needed since it does not exist.
                    # os.path.isdir(dest) and shutil.rmtree(dest)

                    self.__publishDoc(localDocIds, filters,
                        localParams, dest, options)

            # We need to check publishing status before finishing.
            if self.no_output != "Y":
                status = self.__getStatus()
                if status == Publish.SUCCEED:
                    if destType == Publish.FILE:
                        shutil.copy(dest + "/new/" + file, dest)
                    else: # Copy all files from subdir "new" to destination.
                        for file in os.listdir(dest + "/new"):
                            shutil.copy(dest + "/new/" + file, dest)
                    # Rename the destination dir to .SUCCEED
                    os.rename(dest, dest_base + ".SUCCEED")

                    # Update Publishing_Events and
                    #    Published_Documents tables
                    #     from Publishing_Process and
                    #    Publishing_Process_Documents tables,
                    #    respectively.
                    self.__updateStatuses()

                elif status == Publish.FAIL:
                    # Rename the destination dir to .FAIL
                    os.rename(dest, dest_base + ".FAIL")
                elif status == Publish.WAIT:
                    # Rename the destination dir to .WAIT
                    os.rename(dest, dest_base + ".WAIT")

            # Send email to notify user of job status.
            self.__sendMail()

            # Disconnected from CDR.
            if not self.__cdrConn is None:
                self.__cdrConn.Close()
                self.__cdrConn = None

    #------------------------------------------------------------------
    # Inform the user that the job has completed.
    #------------------------------------------------------------------
    def __sendMail(self):
        try:
            if self.email and self.email != "Do not notify":
                if NCGI: 
                    self.__logPub("Sending mail to %s, Job %d has completed" % \
                        (self.email, self.jobId))
                sender  = self.__cdr_email
                subject = "CDR Publishing Job Status"
                receivers = string.split(self.email, ",")
                message = """\
Job %d has completed.  You can view a status report for this job at:

    http://mmdb2.nci.nih.gov/cgi-bin/cdr/PubStatus.py?id=%d

Please do not reply to this message.
""" % (self.jobId, self.jobId)
                cdr.sendMail(sender, receivers, subject, message)
        except:
            if NCGI: self.__logPub("failure sending email to %s: %s" % \
                (self.email, cdr.exceptionInfo()))

    # This is the major helper function to reset input parameters:
    #    strCtrlDocId, subsetName, email, no_output, credential,
    #    docIds, and params
    def __resetParamsByJobId(self):

        # Connect to CDR. Abort when failed. Cannot log status in this case.
        self.__getConn()

        # reset strCtrlDocId, subsetName
        sql = """SELECT pub_system, pub_subset, usr, email, no_output
                FROM pub_proc
                WHERE id = %d
                    AND status = '%s'""" % (self.__procId, Publish.START)
        if NCGI: self.__logPub(sql)
        rs = self.__execSQL(sql)
        rows = 0
        while not rs.EOF:
            rows += 1
            self.strCtrlDocId = str(rs.Fields("pub_system").Value)
            self.subsetName = rs.Fields("pub_subset").Value
            self.__userId = rs.Fields("usr").Value
            self.email = rs.Fields("email").Value
            self.no_output = rs.Fields("no_output").Value
            rs.MoveNext()
        rs.Close()
        if rows == 0 or rows > 1:
            if NCGI: self.__logPub(
                "*Error: resetParamsByJobId failed in access to pub_proc:")
            if NCGI: self.__logPub("      Not a unique record returned.")
            sys.exit(1)

        # reset docIds. It could be an empty list.
        self.docIds = []
        sql = """SELECT doc_id, doc_version
                FROM pub_proc_doc
                WHERE pub_proc = %d """ % self.__procId
        rs = self.__execSQL(sql)
        while not rs.EOF:
            docId = rs.Fields("doc_id").Value
            version = rs.Fields("doc_version").Value
            self.docIds.append("%d/%d" % (docId, version))

            # Avoid duplicate docId in pub_proc_doc table.
            self.__docIds[str(docId)] = docId

            rs.MoveNext()
        rs.Close()

        # reset params. It could be an empty list.
        self.params = []
        sql = """SELECT parm_name, parm_value
                FROM pub_proc_parm
                WHERE pub_proc = %d """ % self.__procId
        rs = self.__execSQL(sql)
        while not rs.EOF:
            name = rs.Fields("parm_name").Value
            value = rs.Fields("parm_value").Value
            self.params.append("%s %s" % (name, value))
            rs.MoveNext()
        rs.Close()

        # reset credential.
        sql = """SELECT name, password
                FROM usr
                WHERE id = %d """ % self.__userId
        rs = self.__execSQL(sql)
        rows = 0
        while not rs.EOF:
            rows += 1
            self.__userName = rs.Fields("name").Value
            self.__passWord = rs.Fields("password").Value
            rs.MoveNext()
        rs.Close()
        if rows == 0 or rows > 1:
            if NCGI: 
                self.__logPub("*Error: resetParamsByJobId failed in access to usr:")
            if NCGI: self.__logPub("      Not a unique record returned.")
            sys.exit(1)
        self.credential = cdr.login(self.__userName, self.__passWord)

        # change status??
        # Ready to publish.
        sql = "UPDATE pub_proc SET status = '" + Publish.RUN + "' "
        sql += "WHERE id = %d" % self.__procId
        self.__execSQL(sql)

        rs = None
        self.__cdrConn = None

    #----------------------------------------------------------------
    # Set up a connection to CDR. Abort when failed.
    #----------------------------------------------------------------
    def __getConn(self):
        try:
            connStr = "DSN=cdr;UID=CdrPublishing;PWD=***REMOVED***"
            self.__cdrConn = Dispatch('ADODB.Connection')
            self.__cdrConn.ConnectionString = connStr
            self.__cdrConn.Open()
        except pythoncom.com_error, (hr, msg, exc, arg):
            self.__cdrConn = None
            reason = "*Error with connection to CDR."
            if exc is None:
                reason += "Code %d: %s" % (hr, msg)
            else:
                wcode, source, text, helpFile, helpId, scode = exc
                reason += " Src: " + source + ". Msg: " + text
            if NCGI: self.__logPub(reason)
            sys.exit(1)

    #----------------------------------------------------------------
    # Get user ID and Name using credential from CDR.
    #----------------------------------------------------------------
    def __getUser(self):
        if NCGI: self.__logPub("in __getUser\n")

        sql = "SELECT usr.id as uid, usr.name as uname "
        sql += "FROM session, usr "
        sql += "WHERE session.name = '" + self.credential + "' AND "
        sql += "session.usr = usr.id"
        rs = self.__execSQL(sql)

        while not rs.EOF:
            self.__userId = rs.Fields("uid").Value
            self.__userName = rs.Fields("uname").Value
            rs.MoveNext()
        rs.Close()
        rs = None

        if self.__userId == 0 or self.__userName == "":
            if NCGI: self.__logPub(
                "*Error: __getUser failed to get user id or user name.")
            sys.exit(1)

    #----------------------------------------------------------------
    # Execute the SQL statement using ADODB.
    #----------------------------------------------------------------
    def __execSQL(self, sql):
        if NCGI: self.__logPub("in __execSQL\n")

        try:
            (rs, err) = self.__cdrConn.Execute(sql)
        except pythoncom.com_error, (hr, msg, exc, arg):
            reason = "*Error with executing %s." % sql
            if exc is None:
                reason += " Code %d: %s" % (hr, msg)
            else:
                wcode, source, text, helpFile, helpId, scode = exc
                reason += " Src: " + source + ". Msg: " + text
            #if self.__procId != 0:
                #self.__updateStatus(Publish.FAIL, reason)
            if NCGI: self.__logPub(reason)
            self.__logPub(reason)
            rs = None
            self.__cdrConn.Close()
            self.__cdrConn = None
            sys.exit(1)
        return rs;

    #----------------------------------------------------------------
    # Return a document for publishingSystem by its name.
    # The document is either None or starting with <PublishingSystem>.
    #----------------------------------------------------------------
    def __getCtrlDoc(self):

        # Don't want to used title to select. New design!
        sql = "SELECT xml FROM document WHERE id = %s" % self.strCtrlDocId

        rs = self.__execSQL(sql)

        xml = None
        while not rs.EOF:
            xml = rs.Fields("xml").Value
            rs.MoveNext()
        rs.Close()
        rs = None
        if NCGI and DEBUG: self.__logPub(xml)
        if xml == None:
            return None
        return xml.encode('latin-1')

        #doc = cdr.getDoc(self.credential, "190931", 'N', 0)
        #return doc

    #----------------------------------------------------------------
    # Return a SubSet node based on subsetName.
    # Don't need to check nodeType since the schema is known
    #    and subsetName is unique.
    # Error checking: node not found.
    #----------------------------------------------------------------
    def __getSubSet(self, docElem):
        if NCGI: self.__logPub("in __getSubSet\n")
        pubSys = xml.dom.minidom.parseString(docElem).documentElement
        for node in pubSys.childNodes:
            if node.nodeName == "SystemSubset":
                for n in node.childNodes:
                    if n.nodeName == "SubsetName":
                        for m in n.childNodes:
                            if m.nodeValue == self.subsetName:
                                return node

        # not found
        if NCGI: self.__logPub(docElem)
        msg = "Failed in __getSubSet. SubsetName: %s." % self.subsetName
        self.__updateStatus(Publish.FAIL, msg)
        sys.exit(1)

    #----------------------------------------------------------------
    # Return a string for SubsetActionName.
    #----------------------------------------------------------------
    def __getAction(self, subset):
        if NCGI: self.__logPub("in __getAction\n")
        for node in subset.childNodes:
            if node.nodeName == "SubsetActionName":
                if NCGI: self.__logPub(node.childNodes[0].nodeValue)
                return node.childNodes[0].nodeValue

        return ""

    #----------------------------------------------------------------
    # Return id if there is a row in the publishing_process table
    #    with status 'active' for the given system and subset.
    #    Active = (In process).
    #----------------------------------------------------------------
    def __existProcess(self):
        if NCGI: self.__logPub("in __existProcess\n")
        sql = "SELECT id FROM pub_proc WHERE pub_system = "
        sql += self.strCtrlDocId + " AND pub_subset = '" + self.subsetName
        sql += """' AND status NOT IN ('%s', '%s', '%s')""" \
               % (Publish.SUCCEED, Publish.WAIT, Publish.FAIL)
        rs = self.__execSQL(sql)

        id = 0
        while not rs.EOF:
            id = rs.Fields("id").Value
            rs.MoveNext()
        rs.Close()
        rs = None

        return id

    #----------------------------------------------------------------
    # Create a row the pub_proc table for the given system and subset.
    # The status is from INIT to READY.
    # Also create rows in pub_proc_parm and pub_proc_doc tables.
    # Return id of the newly created row.
    # The id can be used to check status of this process event.
    #----------------------------------------------------------------
    def __createProcess(self):
        if NCGI: self.__logPub("in __createProcess\n")

        sql = """INSERT INTO pub_proc (pub_system, pub_subset, usr,
            output_dir, started, completed, status, messages, email,
            no_output)
            VALUES (%s, '%s', %d, 'temp', GETDATE(), null, '%s', '%s', '%s',
                    '%s')
            """ % (self.strCtrlDocId, self.subsetName, self.__userId,
                    Publish.INIT, 'This row has just been created.',
                    self.email, self.no_output)
        self.__execSQL(sql)

        sql = """SELECT id FROM pub_proc WHERE pub_system = %s AND
            pub_subset = '%s' AND status = '%s'""" % (self.strCtrlDocId,
            self.subsetName, Publish.INIT)
        rs = self.__execSQL(sql)

        id = 0
        while not rs.EOF:
            id = rs.Fields("id").Value
            rs.MoveNext()
        rs.Close()
        rs = None

        if id != 0:
            # For __insertDoc, we need __procId.
            self.__procId = id

            # Insert rows in pub_proc_parm table
            row = 1
            if self.params:
                for parm in self.params:
                    (name, value) = string.split(parm, ";")
                    sql = """INSERT INTO pub_proc_parm (id, pub_proc, parm_name,
                        parm_value) VALUES (%d, %d, '%s', '%s')
                        """ % (row, id, name, value)
                    self.__execSQL(sql)
                    row += 1

            # Insert rows in pub_proc_doc table
            if self.docIds:
                for doc in self.docIds:
                    docId = self.__getDocId(doc)
                    version = self.__getVersion(doc)
                    self.__insertDoc(docId, version)

        # Ready to publish.
        sql = """UPDATE pub_proc SET status = '%s'
                WHERE id = %d""" % (Publish.READY, id)
        self.__execSQL(sql)

        return id

    #----------------------------------------------------------------
    # Get a list of document IDs, possibly with versions, for publishing.
    # Return a list of docId/version.
    # If a version is not specified, the current publishable version
    #    will be used.
    # Be careful about < and >, &lt;, &gt;
    #----------------------------------------------------------------
    def __getDocIds(self, spec, localParams):
        if NCGI: self.__logPub("in __getDocIds\n")
        for node in spec.childNodes:
            if node.nodeName == "SubsetSelection":
                for m in node.childNodes:
                    if m.nodeName == "SubsetSQL":
                        sql = ""
                        for n in m.childNodes:
                            if n.nodeType == xml.dom.minidom.Node.TEXT_NODE:
                                if NCGI: self.__logPub(n.nodeValue)
                                sql += n.nodeValue
                        sql = self.__repParams(sql, localParams)
                        return self.__getIds(sql)
                    if m.nodeName == "SubsetXQL":
                        if NCGI: self.__logPub(m.childNodes[0].nodeValue)
                    elif m.nodeName == "UserSelect":
                        if NCGI: self.__logPub(m.childNodes[0].nodeValue)

    #----------------------------------------------------------------
    # Replace ?Name? with values in the parameter list.
    #----------------------------------------------------------------
    def __repParams(self, str, params):
        if NCGI: self.__logPub("in __repParams\n")
        ret = str
        for p in params:
            expr = re.compile("\?" + p[0] + "\?")
            ret = expr.sub(p[1], ret)

        if NCGI: self.__logPub(ret)
        return ret

    #----------------------------------------------------------------
    # Execute the SQL statement using ADODB.
    #----------------------------------------------------------------
    def __getIds(self, sql):
        if NCGI: self.__logPub("in __getIds\n")
        ids = self.docIds
        rs = self.__execSQL(sql)
        while not rs.EOF:
            id = "%s" % rs.Fields("id").Value
            ids.append(id)
            rs.MoveNext()
        rs.Close()
        rs = None

        if NCGI and DEBUG: self.__logPub(ids)
        return ids

    #----------------------------------------------------------------
    # Get a list of document type for publishing. This is used
    # when DestinationType is DocType
    #----------------------------------------------------------------
    # Not used.  MMR
    # def __getDocTypes(self, localDocIds):
    #     if NCGI: self.__logPub("in __getDocTypes\n")

    #----------------------------------------------------------------
    # Check to see if a user is allowed to publish this set
    #    of documents.
    # Return false if not allowed
    # The permission depends on user group
    # credential is used.
    #----------------------------------------------------------------
    def __isPermitted(self, action):

        if NCGI: self.__logPub("in __isPermitted\n")
        if action.strip() == "" : return 1        # no authorization required

        sql = "SELECT COUNT(*) " \
              "FROM action a " \
              "JOIN grp_action ga " \
              "ON a.id = ga.action " \
              "JOIN grp_usr gu " \
              "ON ga.grp = gu.grp " \
              "WHERE a.name = '" + action + "' " \
              "AND gu.usr = %d" % self.__userId
        rs = self.__execSQL(sql)
        rc = 0
        if not rs.EOF :
            rc = rs.Fields(0)

        rs.Close()

        return rc


    #----------------------------------------------------------------
    # Get a list of options from the subset.
    # The options specify what to do about publishing results or
    #     processing errors.
    # Minimal error checking is done on < and >.
    #----------------------------------------------------------------
    def __getOptions(self, subset):
        if NCGI: self.__logPub("in __getOptions\n")
        pairs = []
        pair = ["", ""]
        for node in subset.childNodes:
            if node.nodeName == "SubsetOptions":
                for n in node.childNodes:
                    if n.nodeName == "SubsetOption":
                        for m in n.childNodes:
                            if m.nodeName == "OptionName":
                                pair[0] = m.childNodes[0].nodeValue
                            elif m.nodeName == "OptionValue":
                                pair[1] = m.childNodes[0].nodeValue
                        deep = copy.deepcopy(pair)
                        pairs.append(deep)
                if NCGI: self.__logPub(pairs)
                return pairs

        if self.__specs is not None:
            msg = "*Error: no options for a subset specification."
            self.__updateStatus(Publish.FAIL, msg)
            sys.exit(1)

    #----------------------------------------------------------------
    # Get the filters node from the subset specification.
    # The filters are then sequentially applied to the documents
    #    in the subset.
    #----------------------------------------------------------------
    def __getFilters(self, spec):
        if NCGI: self.__logPub("in __getFilters\n")
        for node in spec.childNodes:
            if node.nodeName == "SubsetFilters":
                return node
        if NCGI: self.__logPub(
            "*Error: no filters for a subset specification.")

    #----------------------------------------------------------------
    # Get a list of subset parameters from the control document.
    # The list will be used by the same subset.
    # Replacing parameters from argv, params.
    #----------------------------------------------------------------
    def __getParameters(self, subset):
        if NCGI: self.__logPub("in __getParameters\n")
        pairs = []
        pair = ["", ""]
        for node in subset.childNodes:
            if node.nodeName == "SubsetParameters":
                for n in node.childNodes:
                    if n.nodeName == "SubsetParameter":
                        for m in n.childNodes:
                            if m.nodeName == "ParmName":
                                pair[0] = m.childNodes[0].nodeValue
                            elif m.nodeName == "ParmValue":
                                pair[1] = m.childNodes[0].nodeValue
                        deep = copy.deepcopy(pair)
                        pairs.append(deep)
                if NCGI: self.__logPub(pairs)
                return pairs

        return pairs

    #----------------------------------------------------------------
    # Get warnings or errors from response
    #----------------------------------------------------------------
    def __getWarningOrError(self, document, options):
        if NCGI: self.__logPub("in __getWarningOrEorror\n")
        return Publish.IGNORE

    #----------------------------------------------------------------
    # Ask user what to do with warnings and errors.
    #----------------------------------------------------------------
    def __getAnswer(self, errCode):
        if NCGI: self.__logPub("in __getAnswer\n")

    #----------------------------------------------------------------
    # Get the destination directory where the filtered documents will
    #     be stored.
    #----------------------------------------------------------------
    def __getDestination(self, options):
        if NCGI: self.__logPub("in __getDestination\n")
        for opt in options:
            if opt[0] == "Destination":
                dest = opt[1].encode('latin-1')

                # Update the pub_proc table for destination.
                sql = """UPDATE pub_proc SET output_dir = '%s'
                        WHERE id = %d""" % (dest, self.__procId)
                self.__execSQL(sql)

                return dest

        if self.__specs is not None:
            msg = "*Error: no Destination for the subset options."
            self.__updateStatus(Publish.FAIL, msg)
            sys.exit(1)

    #----------------------------------------------------------------
    # Get the destination type. The type determines how to store the
    #    results: a single file for all documents, a single file
    #     for each document type, or a single file for each document.
    # Minimal error checking is done.
    #----------------------------------------------------------------
    def __getDestinationType(self, options):
        if NCGI: self.__logPub("in __getDestinationType\n")
        for opt in options:
            if opt[0] == "DestinationType":
                if opt[1] == "File":
                    return Publish.FILE
                elif opt[1] == "Doc":
                    return Publish.DOC
                else:
                    return Publish.DOCTYPE

        if self.__specs is not None:
            msg = "*Error: no DestinationType for the subset options."
            self.__updateStatus(Publish.FAIL, msg)
            sys.exit(1)

    #----------------------------------------------------------------
    # Get the destination file. A fileName for all documents.
    # Minimal error checking is done.
    #----------------------------------------------------------------
    def __getDestinationFile(self, options):
        if NCGI: self.__logPub("in __getDestinationFile\n")
        for opt in options:
            if opt[0] == "DestinationFileName":
                return opt[1]

        if self.__specs is not None:
            msg = "*Error: no DestinationFile for the subset options."
            self.__updateStatus(Publish.FAIL, msg)
            sys.exit(1)

    #----------------------------------------------------------------
    # Get the subset specifications node.
    #----------------------------------------------------------------
    def __getSpecs(self, subset):
        if NCGI: self.__logPub("in __getSpecs\n")
        for node in subset.childNodes:
            if node.nodeName == "SubsetSpecifications":
                return node

        return None

    #----------------------------------------------------------------
    # Get the list of subset specifications filter Ids.
    #----------------------------------------------------------------
    def __getFilterId(self, filter):
        if NCGI: self.__logPub("in __getFilterId\n")
        for node in filter.childNodes:
            if node.nodeName == "SubsetFilterId":
                return node.childNodes[0].nodeValue
            elif node.nodeName == "SubsetFilterName":
                return node.childNodes[0].nodeValue

        if NCGI: self.__logPub("*Error: no filter Id or Name for a filter.")

    #----------------------------------------------------------------
    # Get the list of subset specifications filter parameters.
    #----------------------------------------------------------------
    def __getParams(self, filter):
        if NCGI: self.__logPub("in __getParams\n")
        pairs = []
        pair = ["", ""]
        for node in filter.childNodes:
            if node.nodeName == "SubsetFilterParm":
                for m in node.childNodes:
                    if m.nodeName == "ParmName":
                        pair[0] = m.childNodes[0].nodeValue
                    elif m.nodeName == "ParmValue":
                        pair[1] = m.childNodes[0].nodeValue
                deep = copy.deepcopy(pair)
                pairs.append(deep)
        if NCGI: self.__logPub(pairs)
        return pairs

    #----------------------------------------------------------------
    # Publish the whole subset in a single file. The file with name
    #     fileName is replaced.
    # Parameter "credential" is needed only if methods in cdr.py
    #    are used.
    #----------------------------------------------------------------
    def __publishAll(self, localDocIds, filters,
                localParams, dest, fileName, options):

            # Change to output documents as they are completed
            # rather than saving all output for end.  MMR

            pubDoc = ""
            if NCGI: self.__logPub(localDocIds)
            for doc in localDocIds:

                # doc = docId/version format?
                docId = self.__getDocId(doc)
                version = self.__getVersion(doc)

                # Insert a row into publishing_process_documents
                # table.
                self.__insertDoc(docId, version)

                # Get the document with the appropriate version.
                # This needs to be detailed! Lock?
                document = cdr.getDoc(self.credential, docId,
                        'N', version)

                # Apply filters sequentially to each document.
                # Mike said that this would be done by one call!
                for filter in filters.childNodes:

                    # There are nodes like "#text"
                    if filter.nodeName != "SubsetFilter":
                        continue

                    # Get ID/Name and Parameters.
                    filterId = self.__getFilterId(filter)
                    filterParam = self.__getParams(filter)

                    # How to pass filter parameters along?
                    # New API from CDR server?
                    if NCGI and DEBUG: self.__logPub(document)
                    document = cdr.filterDoc(self.credential, filterId,
                        docId)
                    # , document)
                    if NCGI: self.__logPub(docId)
                    if NCGI: self.__logPub(filterId)


                    # Abort On Error?
                    # Where to get the returned warnings or errors?
                    # From Response element in document?
                    errCode = self.__getWarningOrError(document,
                        options)

                    # If there are warnings or errors, do something
                    # about it.
                    if errCode == Publish.IGNORE: # Warning with No
                        self.__deleteDoc(docId, version)
                        continue
                    elif errCode == ASK:
                        self.__updateStatus(WAIT)
                        answer = self.__getAnswer(errCode, options)
                        self.__updateStatus(RUN)
                        if answer == NO:
                            self.__deleteDoc(docId, version)
                            continue
                    elif errCode == ABORT:
                        self.__deleteDoc(docId, version)
                        self.__updateStatus(FAIL)
                        sys.exit(1)

                # Merge all documents into one.
                # How to do this exactly? Just concatenate them?
                pubDoc += document[0]

            # Save the file in the "new" subdirectory.
            self.__saveDoc(pubDoc, dest + "/new", fileName)

    #----------------------------------------------------------------
    # Publish each type of documents in a single file with the
    #    document type name being the file name. All files in the
    #     destination directory are deleted before adding the
    #    new files.
    #----------------------------------------------------------------
    def __publishType(self, localDocIds, filters,
            localParams, dest, options):
        # Similar to publishAll(), but have to loop through all
        #     different docTypes.
        # No, don't loop through docTypes.  Should loop through the
        #     documents as in publishDoc(), but output to appropriate
        #     file for doctype.  MMR
        if NCGI: self.__logPub("in __publishType\n")

    #----------------------------------------------------------------
    # Publish each document in a file with the document ID being
    #    the file name. All files in the destination directory
    #     are deleted before adding the new files.
    #----------------------------------------------------------------
    def __publishDoc(self, localDocIds, filters,
            localParams, dest, options):
        if NCGI: self.__logPub("in __publishDoc\n")
        if NCGI: self.__logPub("no_output=%s" % self.no_output)

        msg = ""
        nDocsPublished = 0
        if NCGI: self.__logPub(localDocIds)
        for doc in localDocIds:

            # doc = docId/version format?
            version = self.isPublishable(doc, "keepConnected")
            if version == -1:
                msg += "Doc %s: not publishable. <BR>" % doc
                continue
            docId = self.__getDocId(doc)
            

            # Don't publish a document more than once.
            if NCGI: self.__logPub(docId)
            if NCGI: self.__logPub(version)
            if self.__docIds.has_key(docId):
                if NCGI: self.__logPub("Duplicate docId: %s" % docId)
                continue
            self.__docIds[docId] = docId

            # Insert a row into pub_proc_doc table.
            self.__insertDoc(docId, version)

            # Apply filters sequentially to each document.
            # Simply call cdr.filterDoc which accepts a list of filterIds.
            filterIds = []
            for filter in filters.childNodes:

                # There are nodes like "#text"
                if filter.nodeName != "SubsetFilter":
                    continue

                # Get ID/Name and Parameters.
                filterIds.append(self.__getFilterId(filter))
                filterParam = self.__getParams(filter)

            if NCGI: self.__logPub(filterIds)
            pubDoc = cdr.filterDoc(self.credential, filterIds, docId, 
                        docVer=version, no_output=self.no_output)

            # Detect error here!
            # updateStatus(WARNING, pubDoc[1])
            if pubDoc[1]:
                msg += "Doc %s: %s<BR>" % (doc, pubDoc[1]) 

            # Save the file in the "new" subdirectory.
            if self.no_output != "Y" :
                self.__saveDoc(pubDoc[0], dest + "/new", docId)
            if NCGI: self.__logPub(pubDoc[1])

        self.__updateStatus(Publish.SUCCEED, msg)

    #----------------------------------------------------------------
    # Get the document ID from ID/Version string 123456/2.5.
    # Wrong docId will be caught by cdr.getDoc.
    #----------------------------------------------------------------
    def __getDocId(self, doc):
        if NCGI: self.__logPub("in __getDocId\n")
        expr = re.compile("[\sCDR]*(\d+)", re.DOTALL)
        id = expr.search(doc)
        if id:
            return id.group(1)
        else:
            if NCGI: self.__logPub("*Error: bad docId format - " + doc)
            sys.exit(1)

    #----------------------------------------------------------------
    # Get the version from ID/Version string 123456/2.5.
    # Error in format has been caught by __getDocId.
    # Wrong version will be caught by cdr.getDoc.
    #----------------------------------------------------------------
    def __getVersion(self, doc):
        if NCGI: self.__logPub("in __getVersion\n")
        expr = re.compile("[\sCDR]*\d+/(.*)", re.DOTALL)
        id = expr.search(doc)
        if id and id.group(1) != "":
            return id.group(1)
        else:
            return 1

    #----------------------------------------------------------------
    # Update the publishing_process table.
    #----------------------------------------------------------------
    def __updateStatus(self, status, errMsg):
        if NCGI: self.__logPub("in __updateStatus\n")

        sql = "UPDATE pub_proc SET status = '" + status + "', messages = '"
        sql += errMsg + "' WHERE id = " + "%d" % self.__procId
        #self.__execSQL(sql)
        # What if update failed?
        self.__cdrConn.Execute(sql)

        if status == Publish.SUCCEED:
            sql = "UPDATE pub_proc SET completed = GETDATE() "
            sql += "WHERE id = " + "%d" % self.__procId
            #self.__execSQL(sql)
            # What if update failed?
            self.__cdrConn.Execute(sql)

        # sql += " DECLARE @ptrval varbinary(16) "
        # sql += " SELECT @ptrval = textptr(messages) FROM pub_proc "
        # sql += " WRITETEXT pub_proc.messages @ptrval '" + errMsg + "'"

    #----------------------------------------------------------------
    # Update the publishing_events, published_documents tables from
    #     publishing_process and publishing_process_documents tables.
    #----------------------------------------------------------------
    def __updateStatuses(self):
        if NCGI: self.__logPub("in __updateStatuses, Disabled\n")

        # No longer needed because pub_event is changed to a view of
        #   pub_proc.
        return

        # Copy a row with procId to insert into pub_event.
        sql = "INSERT INTO pub_event SELECT p.pub_system, p.pub_subset, "
        sql += "p.usr, p.started, p.completed FROM pub_proc p WHERE p.id "
        sql += " = %d" % self.__procId
        self.__execSQL(sql)

        # Get id from pub_event.
        sql = "SELECT e.id AS eid FROM pub_event e WHERE EXISTS (SELECT * "
        sql += "FROM pub_proc p WHERE p.pub_system = e.pub_system AND "
        sql += "p.pub_subset = e.pub_subset AND p.usr = e.usr AND "
        sql += "p.started = e.started AND "
        sql += "p.id = %d" % self.__procId + ")"
        rs = self.__execSQL(sql)

        id = 0
        while not rs.EOF:
            id = rs.Fields("eid").Value
            rs.MoveNext()
        rs.Close()
        rs = None

        if id == 0:
            msg = "*Error: fetching id from pub_event in __updateStatuses failed."
            self.__updateStatus(Publish.FAIL, msg)
            sys.exit(1)

        # We can now update published_doc table.
        sql = "INSERT INTO published_doc SELECT '" + "%d" % id + "', p.doc_id, p.doc_version "
        sql += "FROM pub_proc_doc p "
        sql += "WHERE p.pub_proc = %d" % self.__procId
        self.__execSQL(sql)

    #----------------------------------------------------------------
    # Return the status field from the publishing_process table.
    #----------------------------------------------------------------
    def __getStatus(self):
        if NCGI: self.__logPub("in __getStatus\n")
        sql = "SELECT status FROM pub_proc "
        sql += "WHERE id = " + "%d" % self.__procId
        rs = self.__execSQL(sql)

        status = None
        while not rs.EOF:
            status = rs.Fields("status").Value
            rs.MoveNext()
        rs.Close()
        rs = None

        if NCGI: self.__logPub(status)
        return status

    #----------------------------------------------------------------
    # Delete a row from publishing_process_documents table.
    #----------------------------------------------------------------
    def __deleteDoc(self, docId, version):
        if NCGI: self.__logPub("in __deleteDoc\n")
        sql = """DELETE FROM pub_proc_doc WHERE id = %d AND doc_id = %s
                AND doc_version = %s""" % (self.__procId, doc_id, version)
        self.__execSQL(sql)

    #----------------------------------------------------------------
    # Insert a row into pub_proc_doc table.
    #----------------------------------------------------------------
    def __insertDoc(self, docId, version):
        if NCGI: self.__logPub("in __insertDoc\n")
        sql = """INSERT INTO pub_proc_doc (pub_proc, doc_id, doc_version)
                VALUES (%d, %s, %s)""" % (self.__procId, docId, version)
        self.__execSQL(sql)

    #----------------------------------------------------------------
    # Save the document in the temporary subdirectory.
    #----------------------------------------------------------------
    def __saveDoc(self, document, dir, fileName, mode = "w"):
        if not os.path.isdir(dir):
            os.makedirs(dir)
        fileObj = open(dir + "/" + fileName, mode)
        fileObj.write(document)
        fileObj.close()

    #----------------------------------------------------------------
    # Handle process script. This is specific for Bob's Python script.
    # If this subset does not contain process script, simply return.
    # The cmd string should be determined by options in the control
    #   document, not hard-coded unless we agree that all the process
    #   script will only accept JobId as its only argument.
    #----------------------------------------------------------------
    def __invokeProcessScript(self, subset):
        if NCGI: self.__logPub("in __invokeProcessScript\n")
        scriptName = ""
        for node in subset.childNodes:
            # The 'choice' in schema requires one and only
            #   one element in this subset.
            # Second 'if' is not needed. Leave it there for safety
            #   or for future schema updates.
            if node.nodeName == "SubsetSpecifications":
                return
            if node.nodeName == "ProcessScript":
                for n in node.childNodes:
                    if n.nodeType == xml.dom.minidom.Node.TEXT_NODE:
                        scriptName += n.nodeValue
        # Is the location of the script always in cdr.SCRIPTS?
        # SCRIPTS is changed to BASEDIR.
        scriptName = cdr.BASEDIR + "/" + scriptName
        if not os.path.isfile(scriptName):
            if NCGI: self.__logPub(scriptName + " not found!")
            sys.exit(1)
        cmd = scriptName + " %d" % self.__procId
        if NCGI: self.__logPub("'" + cmd + "' is running!")
        os.system(cmd)
        sys.exit(0)

    #----------------------------------------------------------------------
    # Log debugging message to d:/cdr/log/publish.log
    #----------------------------------------------------------------------
    def __logPub(self, line):
        file = "d:/cdr/log/publish.log"
        open(file, "a").write("Job %d: %s\n" % (self.jobId, line))

#----------------------------------------------------------------------
# Create a new row in the pub_proc table, along with related rows in
#  pub_proc_doc and pub_proc_parm.  Return a job number and the output
#  directory if successful, an error string if not.
#----------------------------------------------------------------------
def initNewJob(ctrlDocId, subsetName, session, docIds = [], parms = [], 
               email = None):

    # Connect to the CDR database.
    try:
        conn = cdrdb.connect("CdrPublishing")
        cursor = conn.cursor()
    except cdrdb.Error, info:
        return 'Database connection failure: %s' % info[1][0]

    # Find the user id.
    try:
        cursor.execute("""\
          SELECT u.id, u.name
            FROM usr u
            JOIN session s
              ON s.usr = u.id
           WHERE s.name = ? 
             AND s.ended IS NULL
""", [session])
        row = cursor.fetchone()
        if not row:
            return 'Invalid or closed session: %s' % session
    except cdrdb.Error, info:
        return 'Database failure retrieving user ID: %s' % info[1][0]
    except Exception, eInfo:
        return 'Failure retrieving user ID: %s' % str(eInfo)
    (uid, uname) = row

    # Make sure the documents can be published.
    try:
        for docId in docIds:
            cursor.execute("""\
                SELECT d.active_status,
                       v.publishable
                  FROM document d
                  JOIN doc_version v
                    ON d.id  = v.id
                 WHERE d.id  = ?
                   AND v.num = ?
""", (docId[0], docId[1]))
            row = cursor.fetchone()
            if not row:
                return "Version %d of document CDR%010d not found" (docId[1],
                                                                    docId[0])
            #if row[0] != "A":
            #   return "Status of document CDR%010d is %s" % (docId[0], row[0])
            #if row[1] != "Y":
            #   return "Version %d of document CDR%010d not publishable" % (
            #       docId[1], docId[0])
    except cdrdb.Error, info:
        return 'Database failure checking doc statuses: %s' % info[1][0]
    except Exception, eInfo:
        return 'Failure checking doc statuses: %s' % str(eInfo)

    # Find the publishing system subset.
    try:
        cursor.execute("SELECT xml FROM document WHERE id = ?", [ctrlDocId])
        row = cursor.fetchone()
        if not row:
            return 'Control document CDR%010d not found' % ctrlDocId
    except cdrdb.Error, info:
        return 'Database failure retrieving control document: %s' % info[1][0]
    except Exception, eInfo:
        return 'Failure retrieving control document: %s' % str(eInfo)
    try:
        ctrlDoc = xml.dom.minidom.parseString(row[0].encode('utf-8'))
        subsetElems = ctrlDoc.getElementsByTagName("SystemSubset")
        if not subsetElems:
            return 'Subset %s not found' % subsetName
        subsetElem = None
        for elem in subsetElems:
            nameElems = elem.getElementsByTagName("SubsetName")
            if nameElems and cdr.getTextContent(nameElems[0]) == subsetName:
                subsetElem = elem
                break
    except Exception, eInfo:
        return 'Failure parsing control document: %s' % str(eInfo)
    if not subsetElem:
        return 'Subset %s not found' % subsetName

    # Make sure the user is authorized to use the publishing system.
    try:
        actionElems = subsetElem.getElementsByTagName("SubsetActionName")
        if actionElems:
            actionName = cdr.getTextContent(actionElems[0])
            cursor.execute("""\
                SELECT COUNT(*)
                  FROM grp_usr u
                  JOIN grp g
                    ON g.id = u.grp
                  JOIN grp_action ga
                    ON ga.grp = g.id
                  JOIN action a
                    ON a.id = ga.action
                 WHERE u.usr = ?
                   AND a.name = ?
""", (uid, actionName))
            row = cursor.fetchone()
            if not row or row[0] < 1:
                return 'User not authorized to invoke %s jobs' % actionName
    except cdrdb.Error, info:
        return 'Database failure checking permissions: %s' % info[1][0]
    except Exception, eInfo:
        return 'Failure checking permissions: %s' % str(eInfo)
    
    # Extract the output base directory for jobs of this type.
    try:
        options = subsetElem.getElementsByTagName("SubsetOption")
        if not options:
            return "Unable to find output base directory for %s jobs" % \
                    subsetName
        baseDir = None
        for option in options:
            optNames = option.getElementsByTagName("OptionName")
            if optNames and cdr.getTextContent(optNames[0]) == "Destination":
                optValues = option.getElementsByTagName("OptionValue")
                if optValues:
                    baseDir = cdr.getTextContent(optValues[0])
                    break
    except Exception, eInfo:
        return 'Failure extracting output base directory: %s' % str(eInfo)
    if not baseDir:
        return 'Unable to find output base directory for %s jobs' % subsetName
    outputDir = "%s.%s.%d" % (baseDir, uname, time.time())

    # Create the job row.
    try:
        cursor.execute("""\
            INSERT INTO pub_proc
            (
                        pub_system,
                        pub_subset,
                        usr,
                        output_dir,
                        started,
                        status,
                        email
            )
                 VALUES (?, ?, ?, ?, GETDATE(), 'Init', ?)
""", (ctrlDocId, subsetName, uid, outputDir, email))
        cursor.execute("SELECT @@IDENTITY")
        row = cursor.fetchone()
        if not row:
            return "Internal error retrieving job id"
        jobId = int(row[0])
        outputDir = "%sJob%d" % (baseDir, jobId)
        cursor.execute("""\
            UPDATE pub_proc
               SET output_dir = ?
             WHERE id = ?""", (outputDir, jobId))
    except cdrdb.Error, info:
        return 'Database failure creating new job: %s' % info[1][0]
    except Exception, eInfo:
        return "Failure creating new job: %s" % str(eInfo)

    # Create the directory.
    try:
        os.makedirs(outputDir)
    except Exception, eInfo:
        return 'Failure creating output directory %s: %s' % (outputDir,
                                                             str(eInfo))
                
    # Add the document IDs if provided.
    if docIds:
        try:
            for docId in docIds:
                cursor.execute("""\
                    INSERT INTO pub_proc_doc
                    (
                                pub_proc,
                                doc_id,
                                doc_version
                    )
                         VALUES (?, ?, ?)
""", (jobId, docId[0], docId[1]))
        except cdrdb.Error, info:
            return 'Database failure inserting document IDs: %s' % info[1][0]
        except Exception, eInfo:
            return "Failure inserting document IDs: %s" % str(eInfo)

    # Add the job's parameters.
    if parms:
        try:
            for i in range(len(parms)):
                cursor.execute("""\
                    INSERT INTO pub_proc_parm
                    (
                                id,
                                pub_proc,
                                parm_name,
                                parm_value
                    )
                         VALUES (?, ?, ?, ?)
""", (i + 1, jobId, parms[i][0], parms[i][1]))
        except cdrdb.Error, info:
            return 'Database failure inserting job parms: %s' % info[1][0]
        except Exception, eInfo:
            return "Failure inserting job parms: %s" % str(eInfo)

    # Wrap up the transaction and return the job ID
    try:
        cursor.execute("UPDATE pub_proc SET status = 'Ready' WHERE id = ?",
                       (jobId,))
        conn.commit()
        cursor.close()
        cursor = None
        conn = None
    except cdrdb.Error, info:
        return 'Database failure committing transaction: %s' % info[1][0]
    except Exception, eInfo:
        return "Failure committing transaction: %s" % str(eInfo)
    return (jobId, outputDir)
