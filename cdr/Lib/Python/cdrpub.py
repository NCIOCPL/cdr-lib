#----------------------------------------------------------------------
#
# $Id: cdrpub.py,v 1.24 2002-08-20 22:07:31 pzhang Exp $
#
# Module used by CDR Publishing daemon to process queued publishing jobs.
#
# $Log: not supported by cvs2svn $
# Revision 1.23  2002/08/16 21:36:36  pzhang
# Added __waitUserApproval function.
#
# Revision 1.22  2002/08/16 20:19:30  pzhang
# Added new implementation for Hotfix-Export.
#
# Revision 1.21  2002/08/15 17:39:16  pzhang
# Added checking failure details link
#
# Revision 1.20  2002/08/13 23:07:50  pzhang
# Added more filtering time messages.
# Added getLastCgJob to match CG's definition of lastJob.
#
# Revision 1.19  2002/08/12 16:49:18  pzhang
# Added updateMessage function to help trace publishing process.
#
# Revision 1.18  2002/08/08 22:43:38  pzhang
# Added AbortOnError as a parameter so that user can change this value.
# Made comparison of xml and file work for updating documents.
#
# Revision 1.17  2002/08/08 17:00:07  pzhang
# Encoded xml value out of pub_proc_cg_work to UTF-8
# before sending it to CG.
#
# Revision 1.16  2002/08/08 15:18:21  pzhang
# Don't push vendor documents that are not good.
#
# Revision 1.15  2002/08/07 19:47:38  pzhang
# Added code to handle Subdirectory of SubsetSpecification.
#
# Revision 1.14  2002/08/07 14:41:49  pzhang
# Added features to push documents to Cancer.gov. It is far from the
# final version and contains many bugs. Save this version before
# the changes are lost or out of control.
#
# Revision 1.13  2002/08/01 15:52:26  pzhang
# Used socket to get HOST instead of hard-coded mmdb2.
# Added validateDoc module public method.
#
# Revision 1.12  2002/04/09 13:12:32  bkline
# Plugged in support for XQL queries.
#
# Revision 1.11  2002/04/04 18:31:43  bkline
# Fixed status value in query for pub_proc row; fixed query placeholder typo.
#
# Revision 1.10  2002/04/04 15:31:38  bkline
# Cleaned up some of the obsolete log entries (see CVS logs for full history).
#
# Revision 1.9  2002/04/04 15:24:06  bkline
# Rewrote module to match Mike's design spec more closely.  Split out
# CGI support to a separate module.
#
#----------------------------------------------------------------------

import cdr, cdrdb, os, re, string, sys, xml.dom.minidom
import socket, cdr2cg, time
from xml.parsers.xmlproc import xmlval, xmlproc

#-----------------------------------------------------------------------
# Value for controlling debugging output.  None means no debugging
# output is generated.  An empty string means debugging output is 
# written to the standard error file.  Any other string is used as
# the pathname of the logfile to which to write debugging output.
#-----------------------------------------------------------------------
LOG = "d:/cdr/log/publish.log"

#-----------------------------------------------------------------------
# class: Publish
#    This class encapsulates the publishing data and methods.
#    There is one public method, publish().
#-----------------------------------------------------------------------
class Publish:

    # Used as optional argument to __publishDoc() for query-selected docs.
    STORE_ROW_IN_PUB_PROC_DOC_TABLE = 1

    # Used as optional argument to __addDocMessages().
    SET_FAILURE_FLAG = "Y"
    
    # Job status values.
    SUCCESS    = "Success"
    FAILURE    = "Failure"
    WAIT       = "Waiting user approval"
    RUN        = "In process"
    INIT       = "Init"
    READY      = "Ready"
    START      = "Started"

    # Output flavors.
    FILE       = 4
    DOCTYPE    = 5
    DOC        = 6

    # class private variables
    __cdrEmail = "cdr@%s.nci.nih.gov" % socket.gethostname()
    __pd2cg    = "Push_Documents_To_Cancer.Gov"
    __cdrHttp  = "http://%s.nci.nih.gov/cgi-bin/cdr" % socket.gethostname()
    __ignoreUserDocList = 0
    __interactiveMode = 0
    __checkRemovedDocs  = 1
    __vendorDocsOnly    = 0
    __includeLinkedDocs = 1
    __reportOnly        = 0
    __validateDocs      = 1
   
    #---------------------------------------------------------------
    # Load the job settings from the database.  User-specified
    # documents will already have been recorded in the pub_proc_doc
    # table, but other documents can be added through SQL or XQL 
    # queries.
    #---------------------------------------------------------------
    def __init__(self, jobId):
        
        # Initialize a few values used for error processing.
        self.__errorCount           = 0
        self.__errorsBeforeAborting = 0
        self.__warningCount         = 0
        self.__publishIfWarnings    = "No"

        # Keep a copy of the job ID.
        self.__jobId = jobId
        self.__debugLog("Publishing job processing commenced.")

        # Connect to the CDR database.  Exception is raised on failure.
        self.__getConn()
        cursor = self.__conn.cursor()

        # Retrieve the basic settings for the job from the database.
        sql = """\
            SELECT p.pub_system, 
                   p.pub_subset, 
                   p.usr, 
                   p.output_dir,
                   p.email, 
                   p.started,
                   p.no_output,
                   u.name,
                   u.password
              FROM pub_proc p
              JOIN usr u
                ON u.id     = p.usr
             WHERE p.id     = ?
               AND p.status = ?"""
        try:
            cursor.execute(sql, (self.__jobId, Publish.START))
            row = cursor.fetchone()
            if not row:
                msg = "Unable to retrieve information for job %d" % self.__jobId
                self.__debugLog(msg)
                raise StandardError(msg)
        except cdrdb.Error, info:
            msg = "Database failure retrieving information for job %d: %s" % \
                (self.__jobId, info[1][0])
            self.__debugLog(msg)
            raise StandardError(msg)

        self.__ctrlDocId   = row[0]
        self.__subsetName  = row[1]
        self.__userId      = row[2]
        self.__outputDir   = row[3]
        self.__email       = row[4]
        self.__jobTime     = row[5]
        self.__no_output   = row[6]
        self.__userName    = row[7]
        self.__passWord    = row[8]
        self.__credentials = cdr.login(self.__userName, self.__passWord)

        # Load user-supplied list of document IDs.
        self.__userDocList = []
        try:
            cursor.execute("""\
                SELECT pub_proc_doc.doc_id, 
                       pub_proc_doc.doc_version,
                       doc_type.name
                  FROM pub_proc_doc
                  JOIN doc_version
                    ON doc_version.id = pub_proc_doc.doc_id
                   AND doc_version.num = pub_proc_doc.doc_version
                  JOIN doc_type
                    ON doc_type.id = doc_version.doc_type
                 WHERE pub_proc_doc.pub_proc = ?""",  self.__jobId)

            row = cursor.fetchone()
            while row:
                self.__userDocList.append((row[0], row[1], row[2]))
                row = cursor.fetchone()
        except cdrdb.Error, info:
            msg = 'Failure retrieving documents for job %d: %s' % \
                  (self.__jobId, info[1][0])
            self.__updateStatus(Publish.FAILURE, msg)
            raise StandardError(msg)

        # Load the job parameters from the database.  The server
        # will have merged parameters explicitly set for this job
        # invocation with defaults for the publication system subset
        # for parameters not explicitly set for this job.
        self.__params = {}
        try:
            cursor.execute("""\
                SELECT parm_name, 
                       parm_value
                  FROM pub_proc_parm
                 WHERE pub_proc = ?""", self.__jobId)
            row = cursor.fetchone()
            while row:
                self.__params[row[0]] = row[1]
                self.__debugLog("Parameter %s='%s'." % (row[0], row[1]))
                row = cursor.fetchone()
        except cdrdb.Error, info:
            msg = 'Failure retrieving parameters for job %d: %s' % \
                  (self.__jobId, info[1][0])
            self.__updateStatus(Publish.FAILURE, msg)
            raise StandardError(msg)
   
        if self.__params.has_key("PushToCancerGov") and \
            self.__params["PushToCancerGov"] != "Yes":
            self.__vendorDocsOnly = 1
        if self.__params.has_key("IncludeLinkedDocs") and \
            self.__params["IncludeLinkedDocs"] != "Yes":
            self.__includeLinkedDocs = 0
        if self.__params.has_key("InteractiveMode") and \
            self.__params["InteractiveMode"] != "No":
            self.__interactiveMode = 1	
        if self.__params.has_key("CheckRemovedDocs") and \
            self.__params["CheckRemovedDocs"] != "Yes":
            self.__checkRemovedDocs = 0	
        if self.__params.has_key("ReportOnly") and \
            self.__params["ReportOnly"] != "No":
            self.__reportOnly = 1	
        if self.__params.has_key("ValidateDocs") and \
            self.__params["ValidateDocs"] != "Yes":
            self.__validateDocs = 0	

    #---------------------------------------------------------------
    # This is the major public entry point to publishing.
    #---------------------------------------------------------------
    def publish(self):
        
        try:

            # Record the fact that the job is in process.
            self.__updateStatus(Publish.RUN)
      
            # Load the publishing system's control document from the DB.
            docElem = self.__getCtrlDoc()

            # Extract the DOM node for this job's publishing system subset.
            # Set __sysName needed by Cancer.gov as a side-effect.
            subset = self.__getSubSet(docElem)

            # Invoke an external process script, if any.  Will not return 
            # if an external script is attached to this publishing system
            # subset.
            self.__invokeProcessScript(subset)

            # Get the destination directory.
            dest_base = self.__outputDir
            dest = dest_base + ".InProcess"

            # Get the subset specifications node.
            self.__specs = self.__getSpecs(subset)

            # Get the name-value pairs of options.  Error handling set here.
            options = self.__getOptions(subset)

            # Get the destination type.
            destType = self.__getDestinationType(options)
            if destType == Publish.FILE:
                self.__fileName = self.__getDestinationFile(options)

            # Two passes through the subset specification are required.
            # The first pass publishes documents specified by the user, and
            # builds the list of filters used for each specification.
            # The second pass publishes the documents selected by queries
            # (XML or XQL), skipping documents which have already been
            # published by the job (either because they were picked up by
            # another earlier query, or because they appeared in the
            # user-specified list).
            #
            # In the first pass a document on the user's list is published
            # only once, for the first specification which allows the user to
            # list documents of that document's type.
            self.__alreadyPublished = {}
            specFilters             = []
            specSubdirs             = []
            userListedDocsRemaining = len(self.__userDocList)
            self.__debugLog("Processing user-listed documents.")
            
            # Number of documents have been filtered and validated.
            numDocs = 0          
            self.__updateMessage(
                "Start filtering/validating at %s.<BR>" % time.ctime())

            for spec in self.__specs.childNodes:

                if spec.nodeName == "SubsetSpecification":

                    # Gather together filters (with parms) used for this SS.
                    filters = self.__getFilters(spec)
                    specFilters.append(filters)

                    # Get Subdirectory for this SS.
                    # Empty string is returned if not exist.
                    subdir = self.__getSubdir(spec)
                    specSubdirs.append(subdir)

                    # That's all we have to do in this pass if there are no
                    # user-listed documents remaining to be published.
                    if not userListedDocsRemaining:
                        continue
                   
                    # Hotfix-Export requirement and implementation have
                    # changed. User-selected docs will not be processed as 
                    # it was supposed to be. This trick is used only for
                    # Hotfix-Export where all linked documents including
                    # user-selected will be processed in their own typed SS. 
                    # Refer to the control document for this tricky 
                    # implementation.
                    if self.__sysName == "Primary" and \
                        self.__subsetName == "Hotfix-Export":
                        if self.__includeLinkedDocs:                                        
                            self.__addLinkedDocsToPPD()
                        self.__ignoreUserDocList = 1
                        userListedDocsRemaining = 0
                        continue

                    # Find out if this subset specification allows user 
                    # doc lists.
                    docTypesAllowed = self.__getAllowedDocTypes(spec)
                    if docTypesAllowed is None:
                        continue

                    # See which user-listed documents we can publish here.
                    for doc in self.__userDocList:
                        if doc[0] in self.__alreadyPublished:
                            continue
                        if docTypesAllowed and doc[2] not in docTypesAllowed:
                            continue  
                             
                        # Don't want to use subdir for user-listed docs
                        # due to complexity of specifying the multiple
                        # subdirs when calling cdr.publish(). This may
                        # change in release XX.                    
                        self.__publishDoc(doc, filters, destType, dest)
                        numDocs += 1
                        if numDocs % 1000 == 0:
                            self.__updateMessage(
                                "Filtered/validated %d docs at %s.<BR>" % (
                                            numDocs, time.ctime()))
                            numFailures = self.__getFailures()
                            self.__updateMessage("""%d docs failed so 
                                far.<BR>""" % numFailures)  
                            if self.__interactiveMode:
                                self.__waitUserApproval()

                        self.__alreadyPublished[doc[0]] = 1
                        userListedDocsRemaining -= 1
                        if not userListedDocsRemaining: break

            # Make sure all the user-listed documents are accounted for.
            if not self.__ignoreUserDocList:
                for doc in self.__userDocList:
                    if doc[0] not in self.__alreadyPublished:         
                        self.__checkProblems(doc, 
                                    "User-specified document CDR%010d "
                                    "has document type %s which is "
                                    "not allowed for this publication "
                                    "type" % (doc[0], doc[2]), "")

            # Now walk through the specifications again executing queries.
            self.__debugLog("Processing document-selection queries.")
            i = 0
            for spec in self.__specs.childNodes:

                if spec.nodeName == "SubsetSpecification":

                    for specChild in spec.childNodes:
                        if specChild.nodeName == "SubsetSelection":
                            docs = self.__selectQueryDocs(specChild)
                            for doc in docs:
                                self.__publishDoc(doc, specFilters[i], 
                                      destType, dest, 
                                      Publish.STORE_ROW_IN_PUB_PROC_DOC_TABLE,
                                      specSubdirs[i])
                                numDocs += 1
                                if numDocs % 1000 == 0:
                                    numFailures = self.__getFailures()
                                    self.__updateMessage("""Filtered/validated
                                        %d docs at %s, and %d docs failed so 
                                        far.<BR>""" % (numDocs, time.ctime(),
                                                       numFailures))                                
                    i += 1

            self.__updateMessage("""Finish filtering/validating all %d docs 
                                    at %s.<BR>""" % (numDocs, time.ctime())) 
            numFailures = self.__getFailures() 
            if numFailures > 0:
                msg = """Total of %d docs failed. 
                    <A href="%s/PubStatus.py?id=%d&type=FilterFailure">Check 
                    the failure details.</A><BR>""" % (numFailures, 
                    self.__cdrHttp, self.__jobId)  
                self.__updateMessage(msg)
            else:
                self.__updateMessage("Total of 0 docs failed.<BR>")                    

            if self.__publishIfWarnings == "Ask" and self.__warningCount:
                self.__updateStatus(Publish.WAIT, "Warnings encountered")

            # Rename the output directory from its working name if appropriate.
            else:                
                if self.__no_output != "Y":
                    try:
                        os.rename(dest, dest_base)
                    except:
                        pass

                    if self.__sysName != "Primary" or \
                        self.__vendorDocsOnly:
                        self.__updateStatus(Publish.SUCCESS)

                    # Filtered documents have to be in dest before sending 
                    # them to Cancer.gov. The following piece of code is
                    # quite confusing. Keep in mind that there are possibly
                    # two jobs involved.                 
                    else:                        

                        # Single job for sending filtered documents only,
                        # not producing the filtered documents again.
                        lenPd2Cg = len(self.__pd2cg)
                        if (self.__subsetName)[0:lenPd2Cg] == self.__pd2cg:

                            # Get the vendor_job and destination from
                            # the appropriate subset.
                            vendorInfo    = self.__findVendorData()
                            vendor_job    = vendorInfo[0]
                            vendor_dest   = vendorInfo[1]
                            vendor_subset = vendorInfo[2]
                         
                            if not vendor_job:
                                self.__updateStatus(Publish.FAILURE, 
                                    "No enough vendor info found.<BR>") 
                            else: 
                             
                                # Long job of many hours starts!                              
                                cgResp = self.__pushDocsToCG(vendor_job,
                                    vendor_dest, vendor_subset, self.__jobId)  
                                
                                # Update the status with message.                              
                                self.__updateStatus(cgResp[1], cgResp[2])  

                        # Two jobs involved. The second job will be created
                        # by __pushDocsToCG().
                        else: 
                        
                            # Send a first message indicating that the vendor
                            # data are ready and pushing these documents to
                            # CG is in progress.                          
                            self.__updateStatus(Publish.SUCCESS, 
                                """Pushing filtered documents to Cancer.gov
                                is in progress. You will receive a second
                                email when it is done.<BR>""")
                            self.__sendMail()

                            # Long job of many hours starts!
                            cgResp = self.__pushDocsToCG(self.__jobId, 
                                    dest_base, self.__subsetName)

                            # Update the message no matter what happened.                                                  
                            self.__updateMessage(cgResp[2])

                            # Update statuse of the cg_job and send a second
                            # email only when failed, because message with
                            # Success will be sent again with vendor_job.
                            if cgResp[0]:
                                self.__updateStatus(cgResp[1], cgResp[2],
                                    cgResp[0])
                                if cgResp[1] == Publish.FAILURE:
                                    self.__sendMail(cgResp[0]) 
                else: 
                    self.__updateStatus(Publish.SUCCESS)                   

        except StandardError, arg:
            self.__updateStatus(Publish.FAILURE, arg[0])
            if self.__no_output != "Y":
                try:
                    os.rename(dest, dest_base + ".FAILURE")
                except:
                    pass
        except:
            self.__updateStatus(Publish.FAILURE, "Unexpected failure")
            if self.__no_output != "Y":
                try:
                    os.rename(dest, dest_base + ".FAILURE")
                except:
                    pass

        # Send email to notify user of job status.
        if self.__reportOnly:
            self.__updateStatus(Publish.FAILURE, """The job statue is 
                set to Failure because it was running for pre-publishing 
                reports.<BR>""")
        self.__sendMail()

    #------------------------------------------------------------------
    # Find the vendor job and destination directory based on parameter 
    # value SubSetName belonging to this job.    
    #------------------------------------------------------------------
    def __findVendorData(self):
        if not self.__params.has_key('SubSetName'):
            return [None, None, None]
        else:
            subsetName = self.__params['SubSetName']
            try:
                cursor = self.__conn.cursor()
                cursor.execute("""\
                    SELECT TOP 1 id, output_dir
                      FROM pub_proc
                     WHERE status='%s' 
                       AND pub_subset='%s'
                       AND pub_system = %d
                  ORDER BY id DESC
                  """ % (Publish.SUCCESS, subsetName, self.__ctrlDocId))
                row = cursor.fetchone()
                if not row:
                    raise StandardError(
                        "Getting vendor id, output_dir failed.") 

                id   = row[0]
                dest = row[1]              
                
                prevId = self.__getLastJobId(subsetName) 
                if prevId > id:
                    raise StandardError("""This same job has been previously 
                        successfully done by job %d.""" % prevId)
                         
                return [id, dest, subsetName]

            except cdrdb.Error, info:
                raise StandardError("""Failure finding vendor job and vendor
                        destination for job %d: %s""" % (self.__jobId, 
                                                         info[1][0]))
              
    #------------------------------------------------------------------
    # Push documents of a specific vendor_job to Cancer.gov using cdr2cg 
    # module. Create a new cg_job to handle this task if a cg_job has not 
    # been created. When a value of cg_job is passed in, it must be the 
    # same as vendor job (i.e., the SubSet is Push_Documents_To_Cancer.Gov).
    # We handle different pubTypes in separate code for clarity.
    # Return a list [cg_job, status, message].   
    #------------------------------------------------------------------
    def __pushDocsToCG(self, vendor_job, vendor_dest, vendor_subsetName, 
                       cg_job=None):    
        
        msg = ""
        jobId = cg_job or 0

        # Create a new publishing job ID.
        if not cg_job:
            resp = cdr.publish(self.__credentials, "Primary",
                           "%s_%s" % (self.__pd2cg, vendor_subsetName),
                           email=self.__email,                           
                           noOutput=self.__no_output)
            cg_job = resp[0]
            if not cg_job:
                msg += "<B>Failed:</B> %s\n" % resp[1]
                msg += """<BR>Please run %s job separately or 
                    again later.<BR>""" % self.__pd2cg
                return [None, Publish.SUCCESS, msg]       
            jobId = int(cg_job)        
     
        # Get the value of pubType for this vendor_job.
        if self.__params.has_key('PubType'):
            pubType = self.__params['PubType']
            if not cdr2cg.PUBTYPES.has_key(pubType):              
                msg = """The value of parameter PubType, %s, is unsupported.
                       <BR>Please modify the control document or the source 
                       code.<BR>""" % pubType
                return [jobId, Publish.FAILURE, msg]
        else:
            msg = "There is no parameter PubType in the control document."
            return [jobId, Publish.FAILURE, msg] 
        
        try:
            cursor = self.__conn.cursor() 

            # If pubType is "Full Load", clean up pub_proc_cg table.
            if pubType == "Full Load":
                try: cursor.execute("DELETE pub_proc_cg") 
                except:
                    msg = "Deleting pub_proc_cg failed."
                    return [jobId, Publish.FAILURE, msg]
        
            # Create a working table pub_proc_cg_work to hold information
            # on transactions to Cancer.gov.  
            cgWorkLink = self.__cdrHttp + "/PubStatus.py?id=1&type=CgWork"
            link = "<A href='%s'>Check removed docs</A><BR>" % cgWorkLink         
            if pubType == "Full Load" or pubType == "Export":
                self.__createWorkPPC(vendor_job, vendor_dest, jobId) 
                pubTypeCG = pubType 
                if self.__checkRemovedDocs or self.__interactiveMode:                   
                    self.__updateMessage(link)
                    self.__waitUserApproval()
            elif pubType == "Hotfix (Remove)":
                self.__createWorkPPCHR(vendor_job, vendor_dest, jobId)
                pubTypeCG = "Hotfix"
            elif pubType == "Hotfix (Export)":
                self.__createWorkPPCHE(vendor_job, vendor_dest, jobId)  
                pubTypeCG = "Hotfix" 
                if self.__checkRemovedDocs or self.__interactiveMode:
                    self.__updateMessage(link)
                    self.__waitUserApproval()
            else:
                raise StandardError("pubType %s not supported." % pubType)   
            
            docNum  = 1
            numDocs = 0  
            cursor.execute ("""
                SELECT count(*)
                  FROM pub_proc_cg_work                
                            """)
            row = cursor.fetchone()            
            if row and row[0]:
                numDocs = row[0] 
            
            if numDocs == 0:
                msg = "No documents pushed to Cancer.gov."
                return [jobId, Publish.SUCCESS, msg]
            
            # Get last successful cg_jobId. GateKeeper does not
            # care which subset it belongs to.
            # Returns 0 if there is no previous success.
            # Raise an exception when failed.
            lastJobId = self.__getLastCgJob()   

            docType = "Deprecated"
   
            # See if the GateKeeper is awake.
            msg += "Initiating request with pubType=%s, \
                    docType=%s, lastJobId=%d ...<BR>" % (pubTypeCG, 
                    docType, lastJobId)
            response = cdr2cg.initiateRequest(pubTypeCG, docType, lastJobId)
            if response.type != "OK":
                msg += "%s: %s<BR>" % (response.type, response.message)
                if response.fault:
                    msg += "%s: %s<BR>" % (response.fault.faultcode,
                                           response.fault.faultstring)
                    return [jobId, Publish.FAILURE, msg]
                elif response.details:
                    lastJobId = response.details.lastJobId
                    msg += "Last job ID from server: %d<BR>" % lastJobId          

            # Prepare the server for a list of documents to send.
            msg += """Sending data prolog with jobId=%d, pubType=%s,
                    docType=%s, lastJobId=%d, numDocs=%d ...<BR>
                    """ % (jobId, pubTypeCG, docType, lastJobId, numDocs)          
            response = cdr2cg.sendDataProlog(jobId, pubTypeCG, docType,
                                                 lastJobId, numDocs)
            if response.type != "OK":
                msg += "%s: %s<BR>" % (response.type, response.message)
                return [jobId, Publish.FAILURE, msg]  
            
            # Send all new and updated documents.                    
            cursor.execute ("""
                SELECT id, doc_type, xml
                  FROM pub_proc_cg_work
                 WHERE NOT xml IS NULL
                            """)
            rows    = cursor.fetchall()  
            addCount = len(rows)
            msg += "Pushing documents starts at %s.<BR>" % time.ctime()
            self.__updateMessage(msg, jobId) 
            msg = ""    
            if addCount > 0:                 
                XmlDeclLine = re.compile("<\?xml.*?\?>\s*", re.DOTALL)
                DocTypeLine = re.compile("<!DOCTYPE.*?>\s*", re.DOTALL)                   
                for row in rows:                
                    id      = row[0]
                    docType = row[1]  
                    if docType == "InScopeProtocol":
                        docType = "Protocol"  
                    xml = row[2].encode('utf-8')            
                    xml = XmlDeclLine.sub("", xml)
                    xml = DocTypeLine.sub("", xml)

                    response = cdr2cg.sendDocument(jobId, docNum, 
                                "Export", docType, id, xml)
                    if response.type != "OK":
                        msg += "sent document: %d<BR>" % id
                        msg += "send failed. %s: %s<BR>" % \
                                (response.type, response.message)
                        return [jobId, Publish.FAILURE, msg] 
                    docNum  = docNum + 1 
                    if docNum % 1000 == 0:
                        msg += "Pushed %d documents at %s.<BR>" % (
                                    docNum, time.ctime())
                        self.__updateMessage(msg, jobId)
                        msg = ""  
                msg += "%d documents sent to Cancer.gov.<BR>" % addCount
                self.__updateMessage(msg, jobId)
                msg = "" 

            # Remove all the removed documents. 
            cursor.execute ("""
                SELECT id, doc_type
                  FROM pub_proc_cg_work
                 WHERE xml IS NULL
                            """)
            rows = cursor.fetchall()  
            delCount = len(rows)
            if delCount > 0: 
                for row in rows:               
                    id        = row[0]               
                    docType   = row[1] 
                    if docType == "InScopeProtocol":
                        docType = "Protocol"   
                    response = cdr2cg.sendDocument(jobId, docNum, "Remove", 
                                                   docType, id)                 
                    if response.type != "OK":
                        msg += "deleted document: %d<BR>" % id
                        msg += "deleting failed. %s: %s<BR>" % \
                                (response.type, response.message)
                        return [jobId, Publish.FAILURE, msg]  
                    docNum  = docNum + 1
                    if docNum % 1000 == 0:
                        msg += "Pushed %d documents at %s.<BR>" % (
                                    docNum, time.ctime())
                        self.__updateMessage(msg, jobId)
                        msg = ""
                msg += "%d documents removed from Cancer.gov.<BR>" % delCount    
                self.__updateMessage(msg, jobId)
                msg = ""                               
            
            # Before we claim success, we will have to update 
            # pub_proc_cg and pub_proc_doc from pub_proc_cg_work.
            # These transactions must succeed! Failure will cause
            # a mismatch between PPC/D and Cancer.gov database.
            if pubType == "Full Load" or pubType == "Export":
                self.__updateFromPPCW()
            elif pubType == "Hotfix (Remove)":
                self.__updateFromPPCWHR()
            elif pubType == "Hotfix (Export)":               
                self.__updateFromPPCWHE()   
            else:
                raise StandardError("pubType %s not supported." % pubType)  
                       
            msg += "Pushing done at %s.<BR>" % time.ctime() 
                   
        except StandardError, arg:
            msg += arg[0]
            return [jobId, Publish.FAILURE, msg]

        return [jobId, Publish.SUCCESS, msg]
    
    #------------------------------------------------------------------
    # Create rows in the working pub_proc_cg_work table before updating 
    # pub_proc_cg and pub_proc_doc tables. After successfully sending 
    # documents to CG, we can update PPC and PPD in a few instead of 
    # possible 80,000 transactions. We will also update PPD to record 
    # the history of deleted documents. cg_job column seems useless except
    # that it indicates which job has created rows in pub_proc_cg_work.
    # No rows in PPC or PPP will be created for cg_job except for those
    # for vendor part. Note that all docs in PPCW are partitioned into 3 
    # sets: updated, new, and removed, although we don't distiguish the
    # first two in the table (we could do it with an action column for 
    # clarity if needed).  
    #------------------------------------------------------------------
    def __createWorkPPC(self, vendor_job, vendor_dest, cg_job):  
    
        cursor = self.__conn.cursor()   

        # Wipe out all rows in pub_proc_cg_work. Only one job can be run 
        # for Cancer.gov transaction. This is garanteed by the uniqueness 
        # of the setset Name.
        try:                   
            cursor.execute("""
                DELETE pub_proc_cg_work
                  """      ) 
        except:
            raise StandardError("Deleting pub_proc_cg_work failed.")

        # Insert updated documents into pub_proc_cg_work. Updated documents
        # are those that are in both pub_proc_cg and pub_proc_doc belonging
        # to this vendor_job. This is slow. We compare the XML document 
        # content to see if it needs updating. If needed, we insert a row
        # into pub_proc_cg_work with xml set to the new document.        
        try: 
            qry = """
                SELECT ppc.id, t.name, ppc.xml, ppd2.subdir
                  FROM pub_proc_cg ppc, doc_type t, document d,
                       pub_proc_doc ppd2
                 WHERE d.id = ppc.id
                   AND d.doc_type = t.id 
                   AND ppd2.doc_id = d.id
                   AND ppd2.pub_proc = %d
                   AND EXISTS (
                           SELECT * 
                             FROM pub_proc_doc ppd
                            WHERE ppd.doc_id = ppc.id 
                              AND ppd.pub_proc = %d
                              AND ppd.failure IS NULL
                              )
                            """ % (vendor_job, vendor_job)        
            cursor.execute(qry)
            rows = cursor.fetchall()
            for row in rows:
                id     = row[0]
                type   = row[1]
                xml    = row[2]
                subdir = row[3]
                path   = "%s/%s/CDR%d.xml" % (vendor_dest, subdir, id)
                file   = open(path, "r").read()              
                xml    = xml.encode('utf-8')

                if xml != file:
                    cursor.execute("""
                        INSERT INTO pub_proc_cg_work (id, vendor_job, 
                                        cg_job, doc_type, xml)
                             VALUES (?, ?, ?, ?, ?)                             
                                   """, (id, vendor_job, cg_job, type, file)
                                  )
        except:
            raise StandardError("Setting U to pub_proc_cg_work failed.") 
    
        # Insert new documents into pub_proc_cg_work. New documents are 
        # those in pub_proc_doc belonging to vendor_job, but not in 
        # pub_proc_cg. 
        try:                
            cursor.execute ("""
                     SELECT ppd.doc_id, t.name, ppd.subdir
                       FROM pub_proc_doc ppd, doc_type t, document d
                      WHERE ppd.pub_proc = ?
                        AND d.id = ppd.doc_id
                        AND d.doc_type = t.id 
                        AND ppd.failure IS NULL
                        AND NOT EXISTS ( 
                                SELECT * 
                                  FROM pub_proc_cg ppc
                                 WHERE ppc.id = ppd.doc_id                                
                                       )
                            """, (vendor_job)
                           )      
            rows = cursor.fetchall()
            for row in rows:
                id     = row[0]
                type   = row[1]  
                subdir = row[2]            
                path   = "%s/%s/CDR%d.xml" % (vendor_dest, subdir, id)
                xml    = open(path, "r").read()              
                cursor.execute("""
                    INSERT INTO pub_proc_cg_work (id, vendor_job, cg_job,
                                                  doc_type, xml)
                         VALUES (?, ?, ?, ?, ?)                             
                               """, (id, vendor_job, cg_job, type, xml)
                              )
        except:
            raise StandardError("Setting A to pub_proc_cg_work failed.")     

        # Insert removed documents into pub_proc_cg_work.
        # Removed documents are those in pub_proc_cg, but not in
        # pub_proc_doc belonging to vendor_job. The document version number 
        # is obtained from the job in pub_proc_cg. If we want the most
        # recent version, we will either reconstruct the query or update 
        # pub_proc column in pub_proc_cg for each job. The version
        # number is only used for history recording.
        try:                  
            qry = """
                INSERT INTO pub_proc_cg_work (id, num, vendor_job, 
                                          cg_job, doc_type)
                     SELECT ppc.id, prevd.doc_version, %d, %d, t.name
                       FROM pub_proc_cg ppc, doc_type t, document d, 
                            pub_proc_doc prevd
                      WHERE d.id = ppc.id
                        AND d.doc_type = t.id 
                        AND prevd.doc_id = ppc.id
                        AND prevd.pub_proc = ppc.pub_proc
                        AND NOT EXISTS ( 
                                SELECT * 
                                  FROM pub_proc_doc ppd
                                 WHERE ppd.doc_id = ppc.id
                                   AND ppd.pub_proc = %d
                                       )
                  """ % (vendor_job, cg_job, vendor_job)                          
            cursor.execute(qry)
        except:
            raise StandardError("Setting D to pub_proc_cg_work failed.")
    
    #------------------------------------------------------------------
    # Different version of __createWorkPPC for Hotfix (Remove)
    #------------------------------------------------------------------
    def __createWorkPPCHR(self, vendor_job, vendor_dest, cg_job):  
    
        cursor = self.__conn.cursor()   

        # Wipe out all rows in pub_proc_cg_work. Only one job can be run 
        # for Cancer.gov transaction. This is garanteed by the uniqueness 
        # of the setset Name.
        try:                   
            cursor.execute("""
                DELETE pub_proc_cg_work
                  """      ) 
        except:
            raise StandardError("Deleting pub_proc_cg_work failed.")

        # Insert removed documents into pub_proc_cg_work.
        # Removed documents are those in vendor_job. We will later
        # set the removed column in PPD and remove the PPC rows if
        # exist.
        try:                  
            qry = """
                INSERT INTO pub_proc_cg_work (id, num, vendor_job, 
                                              cg_job, doc_type)
                     SELECT ppd.doc_id, ppd.doc_version, %d, %d, t.name
                       FROM pub_proc_doc ppd, doc_type t, document d                          
                      WHERE d.id = ppd.doc_id
                        AND d.doc_type = t.id                        
                        AND ppd.pub_proc = %d                        
                  """ % (vendor_job, cg_job, vendor_job)                          
            cursor.execute(qry)
        except:
            raise StandardError("Setting D to pub_proc_cg_work failed.")
    
    #------------------------------------------------------------------
    # Different version of __createWorkPPC for Hotfix (Export)
    #------------------------------------------------------------------
    def __createWorkPPCHE(self, vendor_job, vendor_dest, cg_job):  
    
        cursor = self.__conn.cursor()   

        # Wipe out all rows in pub_proc_cg_work. Only one job can be run 
        # for Cancer.gov transaction. This is garanteed by the uniqueness 
        # of the setset Name.
        try:                   
            cursor.execute("""
                DELETE pub_proc_cg_work
                  """      ) 
        except:
            raise StandardError("Deleting pub_proc_cg_work failed.")
       
        # Insert new or updated documents into pub_proc_cg_work. All 
        # documents are those in pub_proc_doc belonging to vendor_job
        # without ANY constraints.     
        try:                
            cursor.execute ("""
                     SELECT ppd.doc_id, t.name, ppd.subdir
                       FROM pub_proc_doc ppd, doc_type t, document d
                      WHERE ppd.pub_proc = ?
                        AND d.id = ppd.doc_id
                        AND d.doc_type = t.id 
                        AND ppd.failure IS NULL
                            """, (vendor_job)
                           )      
            rows = cursor.fetchall()
            for row in rows:
                id     = row[0]
                type   = row[1] 
                subdir = row[2]             
                path   = "%s/%s/CDR%d.xml" % (vendor_dest, subdir, id)
                xml    = open(path, "r").read()              
                cursor.execute("""
                    INSERT INTO pub_proc_cg_work (id, vendor_job, cg_job,
                                                  doc_type, xml)
                         VALUES (?, ?, ?, ?, ?)                             
                               """, (id, vendor_job, cg_job, type, xml)
                              )
        except:
            raise StandardError("Setting A to pub_proc_cg_work failed.") 
     
    #------------------------------------------------------------------
    # Update pub_proc_cg and pub_proc_doc from pub_proc_cg_work. 
    # These transactions have to be successful or we have to review
    # related tables to find out what is wrong.
    # Note that the order of execution for PPC is critical: delete, 
    # update, and insert.
    #------------------------------------------------------------------
    def __updateFromPPCW(self): 

        self.__conn.setAutoCommit(0)
        cursor = self.__conn.cursor()
    
        # Remove documents. The IN clause used should be OK.     
        try:         
            cursor.execute ("""
                DELETE pub_proc_cg                       
                 WHERE id IN ( 
                    SELECT ppcw.id
                      FROM pub_proc_cg_work ppcw  
                     WHERE ppcw.xml IS NULL
                             )
                            """) 
        except:
            raise StandardError("Deleting from pub_proc_cg_work failed.")   

        # Insert rows in PPD for removed documents.     
        try:            
            cursor.execute ("""
                INSERT INTO pub_proc_doc (doc_id, doc_version, pub_proc, 
                                          removed)
                     SELECT ppcw.id, ppcw.num, ppcw.vendor_job, 'Y'
                       FROM pub_proc_cg_work ppcw
                      WHERE ppcw.xml IS NULL
                            """)
        except:
            raise StandardError("Inserting D into pub_proc_doc failed.")  
            
        # Update a document, if its id is in both PPC and PPD.
        try:         
            cursor.execute ("""             
                SELECT ppcw.id, ppcw.xml
                  FROM pub_proc_cg_work ppcw  
                 WHERE EXISTS ( SELECT * 
                                  FROM pub_proc_cg ppc
                                 WHERE ppc.id = ppcw.id
                              )            
                            """)                                
            rows = cursor.fetchall()
            for row in rows:               
                cursor.execute("""
                        UPDATE pub_proc_cg
                           SET xml = ?
                         WHERE id  = ?                       
                               """, (row[1], row[0])
                              )
        except:
            raise StandardError("Updating xml to pub_proc_cg_work failed.")
            
        # Add new documents into PPC finally.
        try:            
            cursor.execute ("""
                INSERT INTO pub_proc_cg (id, pub_proc, xml)
                     SELECT ppcw.id, ppcw.vendor_job, ppcw.xml
                       FROM pub_proc_cg_work ppcw
                      WHERE NOT ppcw.xml IS NULL
                        AND NOT EXISTS ( SELECT * 
                                           FROM pub_proc_cg ppc
                                          WHERE ppc.id = ppcw.id
                                        )
                            """)
        except:
            raise StandardError("Inserting into pub_proc_cg failed.") 

        self.__conn.commit()
        self.__conn.setAutoCommit(1)

    #------------------------------------------------------------------
    # Different version of __updateFromPPCW for Hotfix (Remove)
    #------------------------------------------------------------------
    def __updateFromPPCWHR(self): 

        self.__conn.setAutoCommit(0)
        cursor = self.__conn.cursor()
    
        # Remove documents. The IN clause used should be OK.     
        try:         
            cursor.execute ("""
                DELETE pub_proc_cg                       
                 WHERE id IN ( 
                    SELECT ppcw.id
                      FROM pub_proc_cg_work ppcw 
                             )
                            """) 
        except:
            raise StandardError("Deleting from pub_proc_cg_work failed.")   

        # Alter rows in PPD for removed documents.     
        try:            
            cursor.execute ("""
                     UPDATE pub_proc_doc 
                        SET removed = 'Y'
                            """)
        except:
            raise StandardError("Updating D in pub_proc_doc failed.")  

        self.__conn.commit()
        self.__conn.setAutoCommit(1)

    #------------------------------------------------------------------
    # Different version of __updateFromPPCW for Hotfix (Export)
    #------------------------------------------------------------------
    def __updateFromPPCWHE(self): 

        self.__conn.setAutoCommit(0)
        cursor = self.__conn.cursor()
            
        # Update a document, if its id is in both PPC and PPD.
        try:         
            cursor.execute ("""             
                SELECT ppcw.id, ppcw.xml
                  FROM pub_proc_cg_work ppcw  
                 WHERE EXISTS ( SELECT * 
                                  FROM pub_proc_cg ppc
                                 WHERE ppc.id = ppcw.id
                              )            
                            """)                                
            rows = cursor.fetchall()
            for row in rows:               
                cursor.execute("""
                        UPDATE pub_proc_cg
                           SET xml = ?
                         WHERE id  = ?                       
                               """, (row[1], row[0])
                              )
        except:
            raise StandardError("Updating xml from PPCW to PPC failed.")
            
        # Add new documents into PPC finally.
        try:            
            cursor.execute ("""
                INSERT INTO pub_proc_cg (id, pub_proc, xml)
                     SELECT ppcw.id, ppcw.vendor_job, ppcw.xml
                       FROM pub_proc_cg_work ppcw
                      WHERE NOT ppcw.xml IS NULL
                        AND NOT EXISTS ( SELECT * 
                                           FROM pub_proc_cg ppc
                                          WHERE ppc.id = ppcw.id
                                        )
                            """)
        except:
            raise StandardError("Inserting into PPC from PPCW failed.") 

        self.__conn.commit()
        self.__conn.setAutoCommit(1)
        
    #------------------------------------------------------------------
    # Return the last successful cg_job for this vendor_job subset.   
    #------------------------------------------------------------------
    def __getLastJobId(self, subsetName):
        
        jobId = 0

        try:
            cursor = self.__conn.cursor()
            cursor.execute("""
                    SELECT MAX(pp.id)
                      FROM pub_proc pp, pub_proc_parm ppp
                     WHERE pp.status = ?                 
                       AND pp.pub_subset = ?
                       AND pp.pub_system = ?
                       AND ppp.pub_proc = pp.id
                       AND ppp.parm_name = 'SubSetName'
                       AND ppp.parm_value = ?                    
                           """, (Publish.SUCCESS, 
                                 "%s_%s" % (self.__pd2cg, subsetName),
                                 self.__ctrlDocId, subsetName)
                          )
            row = cursor.fetchone()

            if row and row[0]:
                return row[0] 
            else:
                msg = """Failure executing query to find last successful
                         jobId for this subset: %s""" % subsetName
                raise StandardError(msg)                 
           
        except cdrdb.Error, info:
            msg = """Failure executing query to find last successful
                     jobId for this subset: %s""" % subsetName
            raise StandardError(msg) 
             
        return jobId   
    
    #------------------------------------------------------------------
    # Return the last successful cg_job for any pushes.   
    #------------------------------------------------------------------
    def __getLastCgJob(self):
        try:
            cursor = self.__conn.cursor()
            cursor.execute("""
                    SELECT MAX(pp.id)
                      FROM pub_proc pp
                     WHERE pp.status = ?                 
                       AND pp.pub_subset LIKE ?
                       AND pp.pub_system = ?                         
                           """, (Publish.SUCCESS, 
                                 "%s%%" % self.__pd2cg,
                                 self.__ctrlDocId)
                          )
            row = cursor.fetchone()

            if row and row[0]:
                return row[0] 
            else:
                return 0
           
        except cdrdb.Error, info:
            msg = """Failure executing query to find last successful
                     cg_job.<BR>"""
            raise StandardError(msg)   
     
    #------------------------------------------------------------------
    # Update pub_proc_doc table with all linked documents.  
    #------------------------------------------------------------------
    def __addLinkedDocsToPPD(self):
        
        # Build the input hash.
        docPairList = {}      
        for doc in self.__userDocList:
            docPairList[doc[0]] = doc[1]
      
        # Find all the linked documents.
        resp = findLinkedDocs(docPairList)
        msg  = resp[0]
        hash = resp[1]
        self.__updateMessage(msg)

        # Insert all pairs into PPD. Because these linked documents are 
        # not in PPD yet, it should succeed with all insertions.     
        try:
            cursor = self.__conn.cursor()
            for id in hash.keys():
                cursor.execute ("""                
                    INSERT INTO pub_proc_doc
                            (pub_proc, doc_id, doc_version)                 
                    VALUES  (?, ?, ?) 
                                """, (self.__jobId, id, hash[id])
                               )
        except cdrdb.Error, info:
            msg = 'Failure adding linked docs to PPD for job %d: %s' % (
                    self.__jobId, info[1][0])
            raise StandardError(msg)

    #------------------------------------------------------------------
    # Publish one document.
    #
    #   doc         tuple containing doc ID, doc version, and doc type 
    #               string
    #   filters     list of filter sets, each set with its own parm list
    #   destType    FILE, DOCTYPE, or DOC
    #   destDir     directory in which to write output
    #   recordDoc   flag indicating whether to add row to pub_proc_doc
    #               table
    #   subDir      subdirectory to store a subset of vendor docs
    #------------------------------------------------------------------
    def __publishDoc(self, doc, filters, destType, destDir, 
                     recordDoc = 0, subDir = ''):

        self.__debugLog("Publishing CDR%010d." % doc[0])

        # Keep track of problems encountered during filtering.
        warnings = ""
        errors   = ""

        # Apply each filter set to the document.
        filteredDoc = None
        for filterSet in filters:

            # First filter set is run against document from database.
            if not filteredDoc:
                result = cdr.filterDoc(self.__credentials, filterSet[0],
                                       docId = doc[0], docVer = doc[1],
                                       parm = filterSet[1])

            # Subsequent filter sets are applied to previous results.
            else:
                result = cdr.filterDoc(self.__credentials, filterSet[0],
                                       doc = filteredDoc, parm = filterSet[1])
            if type(result) not in (type([]), type(())):
                errors = result or "Unspecified failure filtering document"
                filteredDoc = None
                break

            filteredDoc = result[0]
            if result[1]: warnings += result[1]

        # Validate the filteredDoc against Vendor DTD.       
        if self.__sysName == "Primary" and \
            self.__validateDocs and filteredDoc:
            errObj = validateDoc(filteredDoc, docId = doc[0])
            if len(errObj.Errors):
                errors = "Validating failed with errors."
            if len(errObj.Warnings):
                warnings = "Validating failed with warnings."
                
        # Save the output as instructed.
        if self.__no_output != 'Y' and filteredDoc:
            try:
                destDir = destDir + "/" + subDir
                if destType == Publish.FILE:
                    self.__saveDoc(filteredDoc, destDir, self.__fileName, "a")
                elif destType == Publish.DOCTYPE:
                    self.__saveDoc(filteredDoc, destDir, doc[2], "a")
                else:
                    self.__saveDoc(filteredDoc, destDir, "CDR%d.xml" % doc[0])
            except:
                errors = "Failure writing document CDR%010d" % doc[0]
        if recordDoc and not self.__ignoreUserDocList:
            self.__addPubProcDocRow(doc, subDir)

        # Handle errors and warnings.
        self.__checkProblems(doc, errors, warnings)

    #------------------------------------------------------------------
    # Handle errors and warnings.  Value of -1 for __errorsBeforeAborting
    # means never abort no matter how many errors are encountered.
    # If __publishIfWarnings has the value "Ask" we record the warnings
    # and keep going.
    #------------------------------------------------------------------
    def __checkProblems(self, doc, errors, warnings):
        if errors:
            self.__addDocMessages(doc, errors, Publish.SET_FAILURE_FLAG)
            self.__errorCount += 1
            if self.__errorsBeforeAborting != -1:
                if self.__errorCount > self.__errorsBeforeAborting:
                    if self.__errorsBeforeAborting:
                        msg = "Aborting on error detected in CDR%010d" % doc[0]
                    else:
                        msg = "Aborting: too many errors encountered"
                    self.__updateStatus(Publish.FAILURE, msg)
                    raise StandardError(msg)
        elif warnings:
            self.__addDocMessages(doc, warnings)
            self.__warningCount += 1
            if self.__publishIfWarnings == "No":
                msg = "Aborting on warning(s) detected in CDR%010d" % doc[0]
                self.__updateStatus(Publish.FAILURE, msg)
                raise StandardError(msg)

    #------------------------------------------------------------------
    # Record warning or error messages for the job.
    #------------------------------------------------------------------
    def __addJobMessages(self, messages):
        try:
            cursor = self.__conn.cursor()
            cursor.execute("""\
                SELECT messages
                  FROM pub_proc
                 WHERE id = ?""", self.__jobId)
            row = cursor.fetchone()
            if not row:
                raise StandardError("Failure reading messages for job %d" %
                                    self.__jobId)
            if row[0]:
                messages = row[0] + "|" + messages
            cursor.execute("""\
                UPDATE pub_proc
                   SET messages = ?
                WHERE id = ?""", (messages, self.__jobId))
        except cdrdb.Error, info:
            msg = 'Failure recording message for job %d: %s' % \
                  (self.__jobId, info[1][0])
            raise StandardError(msg)

    #------------------------------------------------------------------
    # Record warning or error messages for a document.
    #------------------------------------------------------------------
    def __addDocMessages(self, doc, messages, failure = None):
        try:
            cursor = self.__conn.cursor()
            cursor.execute("""\
                UPDATE pub_proc_doc
                   SET messages = ?,
                       failure  = ?
                 WHERE pub_proc = ?
                   AND doc_id   = ?""", (messages, failure, 
                                         self.__jobId, doc[0]))
        except cdrdb.Error, info:
            msg = 'Failure recording message for document %d: %s' % \
                  (doc[0], info[1][0])
            raise StandardError(msg)

    #------------------------------------------------------------------
    # Record the publication of the specified document.
    #------------------------------------------------------------------
    def __addPubProcDocRow(self, doc, subDir):
        try:
            cursor = self.__conn.cursor()
            cursor.execute("""\
                INSERT INTO pub_proc_doc
                (
                            pub_proc,
                            doc_id,
                            doc_version,
                            subdir
                )
                     VALUES
                (
                            ?,
                            ?,
                            ?,
                            ?
                )""", (self.__jobId, doc[0], doc[1], subDir))
        except cdrdb.Error, info:
            msg = 'Failure adding row for document %d: %s' % \
                  (self.__jobId, info[1][0])
            raise StandardError(msg)
        
    #------------------------------------------------------------------
    # Build a set of documents which match the queries for a subset
    # specification.  This version does not include any optimizations
    # which might be achieved using temporary tables to collapse 
    # multiple queries into one.  XXX XQL queries not yet supported.
    #------------------------------------------------------------------
    def __selectQueryDocs(self, specNode):

        # Start with an empty list.
        docs = []

        # Walk through the specification looking for queries to execute.
        for child in specNode.childNodes:

            # Gather documents selected by SQL query.
            if child.nodeName == "SubsetSQL":

                try:
                    cursor = self.__conn.cursor()
                    sql = self.__repParams(cdr.getTextContent(child))
                    cursor.execute(sql)

                    # XXX Dependency on result set column names is fragile
                    # and non-portable, relying on (among other things)
                    # DBMS treatment of case of object names.  Consider
                    # replacing this with a convention which uses the 
                    # column count and order in the result set.
                    idCol = -1
                    verCol = -1
                    if not cursor.description:
                        raise StandardError("Result set not returned for "
                                            "SQL query: %s" % sql)
                    i = 0
                    for field in cursor.description:
                        if i == 0: # field[0] == "id":
                            idCol = i
                        elif i == 1: # field[0] == "version":
                            verCol = i
                        i += 1
                    if idCol == -1:
                        raise StandardError("SQL query does not return an "
                                            "'id' column: %s" % sql)
                    row = cursor.fetchone()
                    while row:
                        id = row[idCol]
                        if id in self.__alreadyPublished: continue
                        ver = verCol != -1 and row[verCol] or None

                        try:
                            doc = self.__findPublishableVersion(id, ver)
                        except StandardError, arg:

                            # Can't record this in the pub_proc_doc table,
                            # because we don't really have a versioned document.
                            self.__errorCount += 1
                            threshold = self.__errorsBeforeAborting
                            if threshold != -1:
                                if self.__errorCount > threshold:
                                    raise
                            self.__addJobMessages(arg[0])
                        docs.append(doc)
                        self.__alreadyPublished[id] = 1
                        row = cursor.fetchone()

                except cdrdb.Error, info:
                    msg = 'Failure retrieving document IDs for job %d: %s' % \
                          (self.__jobId, info[1][0])
                    raise StandardError(msg)

            # Handle XQL queries.
            elif child.nodeName == "SubsetXQL":
                xql = self.__repParams(cdr.getTextContent(child))
                resp = cdr.search(self.__credentials, xql)
                if type(resp) in (type(""), type(u"")):
                    raise StandardError("XQL failure: %s" % resp)
                for queryResult in resp:
                    id     = queryResult.docId
                    type   = queryResult.docType
                    digits = re.sub('[^\d]', '', id)
                    id     = string.atoi(digits)
                    if id in self.__alreadyPublished: continue
                    try:
                        doc = self.__findPublishableVersion(id)
                    except StandardError, arg:
                        self.__errorCount += 1
                        threshold = self.__errorsBeforeAborting
                        if threshold != -1:
                            if self.__errorCount > threshold:
                                raise
                        self.__addJobMessages(arg[0])
                    docs.append(doc)
                    self.__alreadyPublished[id] = 1

        self.__debugLog("SubsetSpecification queries selected %d documents."
                        % len(docs))
        return docs

    #------------------------------------------------------------------
    # Find the requested publishable version of a specified document.
    #------------------------------------------------------------------
    def __findPublishableVersion(self, id, version = None):
        if version:
            sql = """\
                SELECT d.id,
                       d.num,
                       t.name
                  FROM doc_version d
                  JOIN doc_type    t
                    ON t.id          = d.doc_type
                  JOIN document    d2
                    ON d2.id         = d.id
                 WHERE d.id          = %d
                   AND d.num         = %d
                   AND d.publishable = 'Y'
                   AND d.val_status  = 'V'""" % (id, version)
        else:
            sql = """\
                SELECT d.id,
                       MAX(d.num),
                       t.name
                  FROM doc_version d
                  JOIN doc_type    t
                    ON t.id          = d.doc_type
                  JOIN document    d2
                    ON d2.id         = d.id
                 WHERE d.id          = %d
                   AND d.publishable = 'Y'
                   AND d.val_status  = 'V'
                   AND d.dt         <= '%s'
              GROUP BY d.id,
                       t.name""" % (id, self.__jobTime)
        try:
            cursor = self.__conn.cursor()
            cursor.execute(sql)
            row = cursor.fetchone()
            if not row:
                if version:
                    raise StandardError("Version %d for document CDR%010d "
                                        "is not publishable or does not "
                                        "exist" % (id, version))
                else:
                    raise StandardError("Unable to find publishable version "
                                        "for document CDR%010d" % id)
        except cdrdb.Error, info:
            msg = "Failure executing query to find publishable version " \
                  "for CDR%010d: %s" % (self.__jobId, info[1][0])
            raise StandardError(msg)
        return tuple(row)

    #------------------------------------------------------------------
    # Inform the user that the job has completed.
    # XXX Add code to notify list of standard users for publishing
    # job notification.
    #------------------------------------------------------------------
    def __sendMail(self, newJobId=None):
        
        jobId = newJobId or self.__jobId
        try:
            if self.__email and self.__email != "Do not notify":
                self.__debugLog("Sending mail to %s." % self.__email)
                sender    = self.__cdrEmail
                subject   = "CDR Publishing Job Status"
                receivers = string.split(self.__email, ",")
                message   = """\
Job %d has completed.  You can view a status report for this job at:

    %s/PubStatus.py?id=%d

Please do not reply to this message.
""" % (jobId, self.__cdrHttp, jobId)
                cdr.sendMail(sender, receivers, subject, message)
        except:
            msg = "failure sending email to %s: %s" % \
                (self.__email, cdr.exceptionInfo())
            self.__debugLog(msg)
            raise StandardError(msg)

    #----------------------------------------------------------------
    # Set up a connection to CDR.  Processing of the publishing job
    # is likely to take long enough that we can't afford to keep 
    # locks on the publishing tables during the whole job, so we
    # avoid wrapping the whole job in a single transaction by turning
    # on auto commit mode.
    #----------------------------------------------------------------
    def __getConn(self):
        try:
            self.__conn = cdrdb.connect("CdrPublishing")
            self.__conn.setAutoCommit()
        except cdrdb.Error, info:
            self.__conn = None
            msg = 'Database connection failure: %s' % info[1][0]
            self.__debugLog(msg)
            raise StandardError(msg)

    #----------------------------------------------------------------
    # Return the document for the publishing control system from 
    # the database.  Be sure to retrieve the version corresponding
    # to the date/time of the publication job.
    #----------------------------------------------------------------
    def __getCtrlDoc(self):

        try:
            cursor = self.__conn.cursor()

            # Do this in two queries to work around an ADODB bug.
            cursor.execute("""\
                    SELECT MAX(num)
                      FROM doc_version
                     WHERE id  = ?
                       AND dt <= ?""", (self.__ctrlDocId, self.__jobTime))
            row = cursor.fetchone()
            if not row:
                raise StandardError("Unable to find version of document "
                                    "CDR%010d created on or before %s" %
                                    (self.__ctrlDocId, self.__jobTime))
            cursor.execute("""\
                    SELECT xml
                      FROM doc_version
                     WHERE id  = ?
                       AND num = ?""", (self.__ctrlDocId, row[0]))
            row = cursor.fetchone()
            if not row or not row[0]:
                raise StandardError("Failure retrieving xml for control "
                                    "document CDR%010d" % self.__ctrlDocId)
        except cdrdb.Error, info:
            raise StandardError("Failure retrieving version of control "
                                "document CDR%010d on or before %s: %s" % 
                                (self.__ctrlDocId, self.__jobTime, info[1][0]))

        xml = row[0]

        # XXX Latin 1 may not be adequate for all documents!
        return xml.encode('latin-1')

    #----------------------------------------------------------------
    # Return a SubSet node based on __subsetName.
    # Set __sysName needed by Cancer.gov as a side-effect.
    # Don't need to check nodeType since the schema is known
    #    and __subsetName is unique.
    # Error checking: node not found.
    #----------------------------------------------------------------
    def __getSubSet(self, docElem):
        pubSys = xml.dom.minidom.parseString(docElem).documentElement
        for node in pubSys.childNodes:
            if node.nodeName == "SystemName":
                self.__sysName = cdr.getTextContent(node)
            if node.nodeName == "SystemSubset":
                for n in node.childNodes:
                    if n.nodeName == "SubsetName":
                        for m in n.childNodes:
                            if m.nodeValue == self.__subsetName:
                                return node

        # not found
        msg = "Failed in __getSubSet. SubsetName: %s." % self.__subsetName
        raise StandardError(msg)

    #----------------------------------------------------------------
    # Replace ?Name? with values in the parameter list.
    #----------------------------------------------------------------
    def __repParams(self, str):
        ret = str
        for name in self.__params.keys():
            ret = re.sub(r"\?%s\?" % name, self.__params[name], ret)
        ret = re.sub(r"\?$JobDateTime\?", self.__jobTime, ret)

        return ret

    #----------------------------------------------------------------
    # Get a list of options from the subset.
    # The options specify what to do about publishing results or
    #     processing errors.
    #----------------------------------------------------------------
    def __getOptions(self, subset):
        options = {}
        abortOnError = "Yes"
        for node in subset.childNodes:
            if node.nodeName == "SubsetOptions":
                for n in node.childNodes:
                    if n.nodeName == "SubsetOption":
                        name = None
                        value = ""
                        for m in n.childNodes:
                            if m.nodeName == "OptionName":
                                name = cdr.getTextContent(m)
                            elif m.nodeName == "OptionValue":
                                value = cdr.getTextContent(m)
                        if not name:
                            raise StandardError("SubsetOption missing "
                                                "required OptionName element")
                        if name in options and options[name] != value:
                            raise StandardError("Duplicate option '%s'" % name)
                        options[name] = value
                        self.__debugLog("Option %s='%s'." % (name, value))
                        if name == "AbortOnError":
                            abortOnError = value
                            if self.__params.has_key('AbortOnError'):
                                abortOnError = self.__params['AbortOnError']                               
                        elif name == "PublishIfWarnings":
                            if value not in ["Yes", "No", "Ask"]:
                                raise StandardError("Invalid value for "
                                                    "PublishIfWarnings: %s" %
                                                    value)
                            self.__publishIfWarnings = value
                if abortOnError:
                    if abortOnError == "Yes": self.__errorsBeforeAborting = 0
                    elif abortOnError == "No": self.__errorsBeforeAborting = -1
                    else: 
                        try:
                            self.__errorsBeforeAborting = int(abortOnError)
                        except:
                            raise StandardError("Invalid value for "
                                                "AbortOnError: %s" % 
                                                abortOnError)
                break
        
        return options

    #----------------------------------------------------------------
    # Get the list of filter sets for this subset specification.
    # There must be at least one filter set, and each filter set
    # and each filter set must have at least one filter.  Each
    # filter set has a possibly empty list of parameters.
    #----------------------------------------------------------------
    def __getFilters(self, spec):
        filterSets = []
        for node in spec.childNodes:
            if node.nodeName == "SubsetFilters":
                filterSets.append(self.__getFilterSet(node))
        if filterSets:
            return filterSets
        raise StandardError("Subset specification has no filters")

    #----------------------------------------------------------------
    # Extract a set of filters and associated parameters.
    #----------------------------------------------------------------
    def __getFilterSet(self, node):
        filters = []
        parms   = []
        for child in node.childNodes:
            if child.nodeName == "SubsetFilter":
                filters.append(self.__getFilter(child))
            elif child.nodeName == "SubsetFilterParm":
                parms.append(self.__getFilterParm(child))
        if not filters:
            raise StandardError("SubsetFilters element must have at least " \
                                "one SubsetFilter child element")
        return (filters, parms)
   
    #----------------------------------------------------------------
    # Extract the document ID or title for a filter.
    #----------------------------------------------------------------
    def __getFilter(self, node):
        for child in node.childNodes:
            if child.nodeName == "SubsetFilterName":
                return "name:%s" % cdr.getTextContent(child)
            elif child.nodeName == "SubsetFilterId":
                return cdr.getTextContent(child)
        raise StandardError("SubsetFilter must contain SubsetFilterName " \
                            "or SubsetFilterId")

    #----------------------------------------------------------------
    # Extract the name/value pair for a filter parameter.  Substitute
    # any job parameters for ?name? placeholders as appropriate.
    #----------------------------------------------------------------
    def __getFilterParm(self, node):
        parmName  = None
        parmValue = ""
        for child in node.childNodes:
            if child.nodeName == "ParmName":
                parmName = cdr.getTextContent(child)
            elif child.nodeName == "ParmValue":
                parmValue = cdr.getTextContent(child)
                parmValue = self.__repParams(parmValue)
        if not parmName:
            raise StandardError("Missing ParmName in SubsetFilterParm")
        return (parmName, parmValue)

    #----------------------------------------------------------------
    # Extract the Subdirectory value. Return "" if not found.    
    #----------------------------------------------------------------
    def __getSubdir(self, spec):       
        for node in spec.childNodes:
            if node.nodeName == "Subdirectory":
                return cdr.getTextContent(node)
                     
        return ""

    #----------------------------------------------------------------
    # Find out which document types the user can list individual
    # documents for.  Return None if this subset doesn't allow
    # listing of individual documents.  Return an empty list if
    # no document type restrictions are imposed on user-supplied
    # document ID lists for this subset.  Otherwise, return a 
    # list of document type names.
    #----------------------------------------------------------------
    def __getAllowedDocTypes(self, node):
        for child in node.childNodes:
            if child.nodeName == "SubsetSelection":
                for grandchild in child.childNodes:
                    if grandchild.nodeName == "UserSelect":
                        docTypes = []
                        for dt_name in grandchild.childNodes:
                            if dt_name.nodeName == "UserSelectDoctype":
                                docTypes.append(cdr.getTextContent(dt_name))
                        return docTypes
        return None

    #----------------------------------------------------------------
    # Get the destination type. The type determines how to store the
    #    results: a single file for all documents, a single file
    #    for each document type, or a single file for each document.
    #----------------------------------------------------------------
    def __getDestinationType(self, options):
        if "DestinationType" in options:
            value = options["DestinationType"]
            if value   == "File"   : return Publish.FILE
            elif value == "DocType": return Publish.DOCTYPE
        return Publish.DOC

    #----------------------------------------------------------------
    # Get the destination file. A fileName for all documents.
    #----------------------------------------------------------------
    def __getDestinationFile(self, options):
        if "DestinationFileName" in options:
            return options["DestinationFileName"]
        else:
            return "PublicationOutput.xml"

    #----------------------------------------------------------------
    # Get the subset specifications node.
    #----------------------------------------------------------------
    def __getSpecs(self, subset):
        for node in subset.childNodes:
            if node.nodeName == "SubsetSpecifications":
                return node
        return None

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
    # Handle process script, if one is specified, in which case 
    # control is not returned to the caller.
    #----------------------------------------------------------------
    def __invokeProcessScript(self, subset):
        scriptName = ""
        for node in subset.childNodes:
            if node.nodeName == "ProcessScript":
                scriptName = cdr.getTextContent(node)
        if scriptName:
            if not os.path.isabs(scriptName):
                scriptName = cdr.BASEDIR + "/" + scriptName
            if not os.path.isfile(scriptName):
                msg = "Processing script '%s' not found" % scriptName
                raise StandardError(msg)
            cmd = scriptName + " %d" % self.__jobId
            self.__debugLog("Publishing command '%s' invoked." % cmd)
            os.system(cmd)
            sys.exit(0)

    #----------------------------------------------------------------------
    # Set job status (with optional message) in pub_proc table.
    #----------------------------------------------------------------------
    def __updateStatus(self, status, message = None, newJobId=None):
        self.__debugLog("Updating job status to %s." % status)
        if message: self.__debugLog(message)
        id = newJobId or self.__jobId
        date = "NULL"
        if status in (Publish.SUCCESS, Publish.FAILURE):
            date = "GETDATE()"
        try:
            cursor = self.__conn.cursor()
            cursor.execute("""
                SELECT messages
                  FROM pub_proc
                 WHERE id = %d 
                           """ % id
                          )
            row     = cursor.fetchone()            
            message = (row and row[0] or '') + (message or '')
           
            cursor.execute("""
                UPDATE pub_proc
                   SET status    = ?,
                       messages  = ?,
                       completed = %s
                 WHERE id        = ?""" % date, (status, message, id))
        except cdrdb.Error, info:
            msg = 'Failure setting status for job %d: %s' % (id, info[1][0])
            self.__debugLog(msg)
            raise StandardError(msg)

    #----------------------------------------------------------------------
    # Update message in pub_proc table.
    #----------------------------------------------------------------------
    def __updateMessage(self, message, newJobId=None):        
        id = newJobId or self.__jobId      
        try:
            cursor = self.__conn.cursor()
            cursor.execute("""
                SELECT messages
                  FROM pub_proc
                 WHERE id = %d 
                           """ % id
                          )
            row     = cursor.fetchone()            
            message = (row and row[0] or '') + message
           
            cursor.execute("""
                UPDATE pub_proc
                   SET messages  = ?                     
                 WHERE id        = ?""", (message, id))
        except cdrdb.Error, info:
            msg = 'Failure setting status for job %d: %s' % (id, info[1][0])
            self.__debugLog(msg)
            raise StandardError(msg)

    #----------------------------------------------------------------------
    # Get the number of failed documents in pub_proc_doc table.
    #----------------------------------------------------------------------
    def __getFailures(self):
        id = self.__jobId
        try:
            cursor = self.__conn.cursor()
            cursor.execute("""
                SELECT count(*)
                  FROM pub_proc_doc
                 WHERE failure = 'Y'
                   AND pub_proc = %d 
                           """ % id
                          )
            row = cursor.fetchone() 
            if row and row[0]:           
                return row[0]
            else:         
                return 0
                     
        except cdrdb.Error, info:
            msg = 'Failure getting failed docs for job %d: %s' % (id, 
                                                            info[1][0])
            self.__debugLog(msg)
            raise StandardError(msg)

    #----------------------------------------------------------------------
    # Wait for user's approval to proceed.
    #----------------------------------------------------------------------
    def __waitUserApproval(self, newJobId=None):
            
        # Depend on which part this is called from.
        id     = newJobId or self.__jobId

        # Put a stop there.
        status = Publish.WAIT
        msg = "Job is waiting for user's approval at %s.<BR>" % time.ctime()
        msg += "Change the publishing job status using Manage Publishing " \
               "Job Status link under Publishing. <BR>"
        self.__updateStatus(status, msg, id)
        self.__sendMail(id)

        # Wait until user does something.
        while 1:
            try:
                cursor = self.__conn.cursor()
                cursor.execute("""
                    SELECT status
                      FROM pub_proc
                     WHERE id = %d 
                               """ % id
                              )
                row = cursor.fetchone() 
                if row and row[0]:           
                    status = row[0]
                else:
                    msg = 'No status for job %d: %s' % (id, info[1][0])               
                    raise StandardError(msg)
                     
            except cdrdb.Error, info:
                msg = 'Failure getting status for job %d: %s' % (
                            id, info[1][0])              
                raise StandardError(msg)

            # Wait another 10 seconds.
            now = time.ctime()
            if status == Publish.WAIT:
                time.sleep(10)
            elif status == Publish.RUN:               
                self.__updateMessage(
                    "Job is resumed by user at %s.<BR>" % now, id)
                return
            elif status == Publish.FAILURE:
                raise StandardError(
                    "Job is killed by user at %s.<BR>" % now)
            else:
                msg = "Unexpected status: %s for job %d.<BR>" % (status, id)
                raise StandardError(msg)                

    #----------------------------------------------------------------------
    # Log debugging message to d:/cdr/log/publish.log
    #----------------------------------------------------------------------
    def __debugLog(self, line):
        if LOG is not None:
            msg = "Job %d: %s\n" % (self.__jobId, line)
            if LOG == "":
                sys.stderr.write(msg)
            else:
                open(LOG, "a").write(msg)

#-----------------------------------------------------------------------
# class: ErrObject
#    This class encapsulates the DTD validating errors. 
#-----------------------------------------------------------------------
class ErrObject:
    def __init__(self, Warnings=None, Errors=None):        
        self.Warnings  = Warnings or []
        self.Errors    = Errors or []

#-----------------------------------------------------------------------
# class: ErrHandler
#    This class encapsulates the error handler for XML parser.
#-----------------------------------------------------------------------
class ErrHandler:
    def __init__(self, loc):        self.locator = loc        
    def set_locator(self, loc):     self.fulminator = loc
    def get_locator(self):          return self.locator
    def set_sysid(self, sysid):     self.__sysid = sysid
    def set_errobj(self, errObj):   self.__errObj = errObj
    def warning(self, msg):         self.__output("W:", msg)
    def error(self, msg):           self.__output("E:", msg)
    def fatal(self, msg):           self.__output("F:", msg)   
    def __output(self, prefix, msg):
        where = self.locator.get_current_sysid()
        if where == 'Unknown': where = self.__sysid
        xmlString = self.locator.get_raw_construct()
        if prefix == "W:":          
            self.__errObj.Warnings.append("%s:%d:%d: %s (%s)\n" % (where,
                                         self.locator.get_line(),
                                         self.locator.get_column(),
                                         msg,
                                         xmlString))
        else:           
            self.__errObj.Errors.append("%s:%d:%d: %s (%s)\n" % (where,
                                         self.locator.get_line(),
                                         self.locator.get_column(),
                                         msg,
                                         xmlString))

#----------------------------------------------------------------------
# Set a parser instance to validate filtered documents.
#----------------------------------------------------------------------
__parser     = xmlval.XMLValidator()
__app        = xmlproc.Application()
__errHandler = ErrHandler(__parser)
__parser.set_application(__app)
__parser.set_error_handler(__errHandler)
      
#----------------------------------------------------------------------
# Validate a given document against its DTD.
#----------------------------------------------------------------------
def validateDoc(filteredDoc, docId = 0):

    errObj      = ErrObject()
    docTypeExpr = re.compile(r"<!DOCTYPE\s+(.*?)\s+.*?>", re.DOTALL)   
    docType     = """<!DOCTYPE %s SYSTEM "%s">
                  """ 

    match = docTypeExpr.search(filteredDoc)
    if match:
        topElement = match.group(1)
        docType    = docType % (topElement, cdr2cg.PDQDTD)
        doc        = docTypeExpr.sub(docType, filteredDoc)
    else:
        errObj.Errors.append(
            "%d.xml:0:0:DOCTYPE declaration is missing." % docId)  
        return errObj      

    __errHandler.set_sysid("%d.xml" % docId)    
    __errHandler.set_errobj(errObj)
    __parser.feed(doc)
    __parser.reset()
  
    return errObj

#----------------------------------------------------------------------
# Find all the linked documents for a given hash of docId/docVer pairs.
# Return a message and a hash of docId/docVer pairs excluding the input.
# Make this public in case that other programs can easily call it.
#----------------------------------------------------------------------
def findLinkedDocs(docPairList):

    try:
        msg = "Starting building link_net hash at %s.<BR>" % time.ctime()
        conn = cdrdb.connect('CdrGuest')
        cursor = conn.cursor()
        cursor.execute("""
                SELECT DISTINCT ln.source_doc,
                                ln.target_doc,
                                MAX(v.num)                    
                           FROM link_net ln
                           JOIN doc_version v
                             ON v.id = ln.target_doc
                           JOIN document d
                             ON d.id = v.id
                           JOIN doc_type t
                             ON t.id = v.doc_type
                          WHERE t.name <> 'Citation'
                            AND v.val_status = 'V'
                            AND v.publishable = 'Y'
                       GROUP BY ln.target_doc,
                                ln.source_doc
                       """)

        # Build a hash in memory that reflects link_net.
        row = cursor.fetchone()
        links = {}
        nRows = 0
        while row:
            if not links.has_key(row[0]):
                links[row[0]] = []
            links[row[0]].append(row[1:])
            row = cursor.fetchone()
            nRows += 1
        msg += "Finishing link_net hash for %d linking docs at %s.<BR>" % (
                        len(links), time.ctime())

        # Seed the hash with documents passed in.
        linkedDocHash = {}
        for key in docPairList.keys():
            linkedDocHash[key] = docPairList[key]
 
        # Find all linked docs recursively.
        done = 0
        passNum = 0
        while not done:
            done = 1 
            passNum += 1
            docIds = linkedDocHash.keys()
            for id in docIds:
                if links.has_key(id):
                    for targetPair in links[id]:
                        if not linkedDocHash.has_key(targetPair[0]):
                            linkedDocHash[targetPair[0]] = targetPair[1]
                            done = 0

        # Delete all input hash.
        for key in docPairList.keys():
            del linkedDocHash[key]

        msg += "Found all %d linked documents in %d passes at %s.<BR>" % (
                        len(linkedDocHash), passNum, time.ctime())

        # Return what we have got.
        return [msg, linkedDocHash]
                                           
    except:
        raise StandardError("Failure finding linked docs.<BR>")        

    
#----------------------------------------------------------------------
# Test driver.
#----------------------------------------------------------------------
if __name__ == "__main__":
    if len(sys.argv) < 2:
        sys.stderr.write("usage: cdrpub.py job-id\n")
        sys.exit(1)
    LOG = ""
    p = Publish(int(sys.argv[1]))
    p.publish()
