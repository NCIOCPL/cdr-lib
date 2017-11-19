#------------------------------------------------------------
# Common routines and classes for global change scripts.
#------------------------------------------------------------

import xml.dom.minidom, sys, os, string, time, cdr, cdrdb, cdrbatch, cdrcgi

#------------------------------------------------------------
# Constants
#------------------------------------------------------------

# A useful HTML constant
BOXCHKD = " checked=1"

# Strings representing different types of change, also used as prompts
PERSON_CHG  = "Person"
ORG_CHG     = "Organization"
STATUS_CHG  = "OrgProtStatus"
TERM_CHG    = "Term"
INS_ORG_CHG = "InsertOrg"
DEL_ORG_CHG = "DeleteOrg"

# Protocol document types for terminology change
TYPE_CDR_DOC = "typeCDR"
TYPE_CTG_DOC = "typeCTG"

# What a function might return
RET_ERROR  = -1         # Function failed, see message
RET_HTML   = 1          # Look for html in FuncReturn object
RET_NONE   = 2          # Function didn't return anything

# Max number of elements in a pick list
MAX_PICK_LIST_SIZE = 100

# Terminology change constants used in forming and display variables
# These determine the role of a path/term combo in selecting or modifying docs
TERMREQ = "Req"
TERMOPT = "Opt"
TERMNOT = "Not"
TERMADD = "Add"
TERMDEL = "Del"
TERMTYP = "Typ"

TERM_USES = (TERMREQ, TERMOPT, TERMNOT, TERMDEL, TERMADD, TERMTYP)
TERM_MSGS = { \
    TERMREQ:"Required",
    TERMOPT:"Any of",
    TERMNOT:"Not",
    TERMDEL:"Delete",
    TERMADD:"Add",
    TERMTYP:"Associated"}

TERM_PROMPTS = ( \
"<span class='termblock'>Only if they include ALL of these terms</span>",
"<span class='termblock'>And at least ONE of these terms</span>",
"<span class='termblock'>And NOT ANY of these terms</span>",
"<span class='termblock'>Delete these terms from the documents</span>",
"<span class='termblock'>Add these terms to the documents</span>",
"""<span class='termblock'>Qualifying terms</span><br>
 &nbsp; &nbsp; Specify <em>StudyCategory</em> if changing InterventionType or
 NameLink<br>
 &nbsp; &nbsp; Also specify <em>InterventionType</em> if changing InterventionNameLink<br>""")

# Field name option display constants
TERM_FLD_DIAG = "Eligibility/Diagnosis"
TERM_FLD_EXC  = "ExclusionCriteria"
TERM_FLD_INTV = "InterventionType"
TERM_FLD_INTN = "InterventionNameLink"
TERM_FLD_GENE = "Gene"
TERM_FLD_COND = "Condition"
TERM_FLD_SCAT = "StudyCategory"

# Some hardwired naming for qualifier info (ugh)
TERM_SCAT_FLD = "trmTypField0"
TERM_SCAT_VAL = "trmTypVal0"
TERM_SCAT_ID  = "trmTypId0"
TERM_INTV_FLD = "trmTypField1"
TERM_INTV_VAL = "trmTypVal1"
TERM_INTV_ID  = "trmTypId1"

# Terminology field names, in the order they should appear in the option list
TERM_FIELD_ORDER = (\
    TERM_FLD_DIAG, TERM_FLD_EXC, TERM_FLD_INTV,
    TERM_FLD_INTN, TERM_FLD_GENE, TERM_FLD_COND)

# Map of field display names to full XML paths to these fields
CDR_TERM_FIELDS = {\
 TERM_FLD_DIAG: "/InScopeProtocol/Eligibility/Diagnosis/@cdr:ref",
 TERM_FLD_EXC:  "/InScopeProtocol/Eligibility/ExclusionCriteria/@cdr:ref",
 TERM_FLD_INTV: "/InScopeProtocol/ProtocolDetail/StudyCategory/Intervention" +\
                "/InterventionType/@cdr:ref",
 TERM_FLD_INTN: "/InScopeProtocol/ProtocolDetail/StudyCategory/Intervention" +\
                "/InterventionNameLink/@cdr:ref",
 TERM_FLD_GENE: "/InScopeProtocol/ProtocolDetail/Gene/@cdr:ref",
 TERM_FLD_COND: "/InScopeProtocol/ProtocolDetail/Condition/@cdr:ref"}

# Map of field display names for CTGovProtocols
CTG_TERM_FIELDS = {\
 TERM_FLD_DIAG: "/CTGovProtocol/PDQIndexing/Eligibility/Diagnosis/@cdr:ref",
 TERM_FLD_EXC:  "/CTGovProtocol/PDQIndexing/Eligibility/ExclusionCriteria/@cdr:ref",
 TERM_FLD_INTV: "/CTGovProtocol/PDQIndexing/StudyCategory/Intervention" +\
                "/InterventionType/@cdr:ref",
 TERM_FLD_INTN: "/CTGovProtocol/PDQIndexing/StudyCategory/Intervention" +\
                "/InterventionNameLink/@cdr:ref",
 TERM_FLD_GENE: "/CTGovProtocol/PDQIndexing/Gene/@cdr:ref",
 TERM_FLD_COND: "/CTGovProtocol/PDQIndexing/Condition/@cdr:ref"}

# Map of field display names to actual field names
TERM_ELEMENT = {
 TERM_FLD_DIAG: "Diagnosis",
 TERM_FLD_EXC:  TERM_FLD_EXC,
 TERM_FLD_INTV: TERM_FLD_INTV,
 TERM_FLD_INTN: TERM_FLD_INTN,
 TERM_FLD_GENE: TERM_FLD_GENE,
 TERM_FLD_COND: TERM_FLD_COND}

# Term status values session variable key
TERM_STATVAL = "trmStatusName"

# First 3 prompts/msgs/uses are for searching, next 2 for modifying
#  last is for associated InterventionType for InterventionNameLink
TERM_SEARCH_USES = 3
TERM_MODIFY_USES = 5
TERM_ASSOC_USES  = 6

# Max allowed terminology criteria of one type
TERM_MAX_CRITERIA = 5

# Max allowed add or delete terms
TERM_MAX_CHANGES = 2

# Max qualifiers we may require
TERM_MAX_QUALS = 2

# Temporary table names for intermediate selection results
TTBL_RES0 = "#gcRes0"
TTBL_RES1 = "#gcRes1"
TTBL_RES2 = "#gcRes2"
TTBL_RES3 = "#gcRes3"

# Kinds of filtering, used as indexes into TermChg.doneChgs
FLTR_CWD = 0    # Filtering current working document
FLTR_PUB = 1    # Filtering last publishable version

# Logfile
LF = cdr.DEFAULT_LOGDIR + "/GlobalChange.log"

# Today's date (YYYY-MM-DD) passed to filters to update DateLastModified
tDate = time.localtime()
TODAY = "%4d-%02d-%02d" % (tDate[0], tDate[1], tDate[2])

#------------------------------------------------------------
# Factory for creating global change objects
#------------------------------------------------------------
def createChg (ssVars):
    """
    Returns a global change object of the correct type for the
    type of change desired.

    Pass:
        ssVars - Dictionary of session variables - preserved by the
                      browser client between calls to GlobalChange.py.
    Return:
        GlblChg object of proper type.
    """
    chgType = ssVars["chgType"]
    chg = None
    if chgType == PERSON_CHG:
        chg = PersonChg()
        ssVars['docType'] = 'Person'
    if chgType == ORG_CHG:
        chg = OrgChg()
        ssVars['docType'] = 'Organization'
    if chgType == STATUS_CHG:
        chg = OrgStatusChg()
        ssVars['docType'] = 'Organization'
    if chgType == TERM_CHG:
        chg = TermChg()
        ssVars['docType'] = 'Term'
    if chgType == INS_ORG_CHG:
        chg = InsertOrgChg()
        ssVars['docType'] = 'Organization'
    if chgType == DEL_ORG_CHG:
        chg = DeleteOrgChg()
        ssVars['docType'] = 'Organization'

    # Can't happen unless there's a bug
    if not chg:
        raise cdrbatch.BatchException("No change type selected, can't happen!")

    # Save session variables here for shared access with GlobalChange.py
    chg.ssVars = ssVars

    # Give caller our new object
    return chg


#------------------------------------------------------------
# Execute a query, returning rows
#------------------------------------------------------------
def _execQry (qry, args=None, cursor=None):
    """
    Called by specific subclass objects to execute their particular
    queries.

    Pass:
        qry    - Query string
        args   - Optional single arg or tuple of args for replacements.
        cursor - Optional cursor.  Else create one.
    Return:
        Sequence of all matching database rows, each containing a
        sequence of:
            document id
            document title
    """

    try:
        # If no cursor passed, create one
        callerCursor = 1
        if not cursor:
            # Use read-only credentials
            conn   = cdrdb.connect ('CdrGuest')
            cursor = conn.cursor()
            callerCursor = 0
        cursor.execute (qry, args, timeout=300)
        rows = cursor.fetchall()

        # Only free cursor if it's created here
        if not callerCursor:
            cursor.close()

        return rows
    except cdrdb.Error, info:
        raise cdrbatch.BatchException (\
            "Database error selecting docs for change %s<br>In query:<br>%s" \
            % (info[1][0], qry))

#------------------------------------------------------------
# Execute an update query, returning a count of updated items
#------------------------------------------------------------
def _execUpdate (qry, args=None, cursor=None):
    """
    Called by specific subclass objects to execute their particular
    queries.

    Pass:
        qry    - Query string
        args   - Optional single arg or tuple of args for replacements.
        cursor - Optional cursor.  Else create one.
    Return:
        Rowcount resulting from query, may be 0.
        No data is returned.
    """
    try:
        # If no cursor passed, create one
        callerCursor = 1
        if not cursor:
            # This is an update query, can't use guest credentials
            conn   = cdrdb.connect ('cdr')
            cursor = conn.cursor()
            callerCursor = 0
        cursor.execute (qry, args, timeout=300)
        rowCnt = cursor.rowcount

        cdr.logwrite ("Excuting:\n%s\n  Row count=%d" % (qry, rowCnt), LF)

        # Only free cursor if it's created here
        if not callerCursor:
            cursor.close()

        return rowCnt
    except cdrdb.Error, info:
        raise cdrbatch.BatchException (\
            "Database error executing update for change %s<br>In query:<br>%s"\
            % (info[1][0], qry))

#------------------------------------------------------------
# Get a list of study categories in an HTML selection string
#------------------------------------------------------------
def getStudyCategories (varName, defaultVal=None):
    """
    Create an HTML option list of legal StudyCategoryName values.

    Pass:
        varName    - Name of variable to put in HTML select form.
        defaultVal - Optional selected value.

    Return:
        String of HTML for inclusion in the input form.
    """
    # Get valid names - they're the same for all protocol doc types
    vals = cdr.getVVList("guest", "InScopeProtocol", "StudyCategoryName")

    # Create html hard wired for the form
    html = "<select name=%s>\n" % varName

    # Insure there is no default value
    html += "  <option></option>\n"

    # Add in the real ones
    for val in vals:
        if val == defaultVal:
            html += "  <option selected='selected'>%s</option>\n" % val
        else:
            html += "  <option>%s</option>\n" % val
    html += "</select>\n"

    return html

#------------------------------------------------------------
# Get a list of docId + title pairs from a list of docIds
#------------------------------------------------------------
def getIdTitles (docIdList):
    """
    Create a list of tuples of docId + title for documents.

    Used by GlobalChangeBatch.

    Pass:
        docIdList - Sequence of document id strings, e.g.,
                    ("12345", "43276", "798812")

    Return:
        Pairs of integer id + string title, e.g.,
            (
             (12345, "Title of this document"),
             (43276, "Title of another document"),
             (...)
            )
    Raises:
        BatchException from _execQry may occur if database error.
        BatchException if doc not found - should never happen.
    """
    idTitleList = []
    for idStr in docIdList:
        idNum  = int(idStr)
        titles = _execQry ("SELECT title FROM document WHERE id=%d" % idNum)

        # Should never happen
        if not titles or len(titles) == 0:
            raise cdrbatch.BatchException (\
                "Title not found for doc ID: %d: shouldn't happen" % idNum)

        # Also should never happen
        if len(titles) > 1:
            raise cdrbatch.BatchException (\
                "Multiple titles found for doc ID: %d: can't happen" % idNum)

        idTitleList.append ((idNum, titles[0][0]))

    return idTitleList

#------------------------------------------------------------
# Create a directory to receive global change documents when
# running in test mode.
#------------------------------------------------------------
def createOutputDir():
    """
    Create a subdirectory in /cdr/GlobalChange.
    The name is the date and time concatenated together as:
        YYYY-MM-DD_HH-MM-SS

    Return:
        Name of created directory.
    """
    t = time.localtime()
    dirName = "%s/GlobalChange/%4d-%02d-%02d_%02d-%02d-%02d" % \
               (cdr.BASEDIR, t[0], t[1], t[2], t[3], t[4], t[5])

    try:
        os.makedirs (dirName)
    except:
        msg = "Unable to create directory '%s'" % dirName
        cdr.logwrite (msg, LF)
        raise Exception (msg)

    cdr.logwrite ("Created directory '%s'" % dirName, LF)
    return dirName

#------------------------------------------------------------
# Write a pair of document files plus a diff file to the file
# system.
#------------------------------------------------------------
def writeDocs (dirName, docId, oldDoc, newDoc, verType, valErrs=None):
    """
    Writes documents before and after global change.
    Used in test mode to output everything to the file
    system when we want to see the output, but don't want
    to change the database.

    Pass:
        dirName - Directory to output to, from createOutputDir().
        docId   - Used in constructing filenames.
        oldDoc  - XML string for old document, in utf-8.
                   Has to be the real XML, not CdrDoc with embedded CDATA.
        newDoc  - XML string for new document, in utf-8.
                   Has to be the real XML, not CdrDoc with embedded CDATA.
        verType - "cwd" for current working doc, "pub" for last
                   publishable version.
        valErrs - Sequence of validation errors, or None.
                   If present & non-empty write them to a NEW_ERRORS.txt file.
    """
    # Construct filenames.  exNormalize() can raise Exception
    idStr = cdr.exNormalize(docId)[0]
    oldName = "%s/%s.%sold.xml" % (dirName, idStr, verType)
    newName = "%s/%s.%snew.xml" % (dirName, idStr, verType)
    diffName = "%s/%s.%s.diff" % (dirName, idStr, verType)
    errsName = "%s/%s.%s.NEW_ERRORS.txt" % (dirName, idStr, verType)

    try:
        # Write two docs as XML, not CDATA
        fp = open(oldName, "w")
        fp.write(oldDoc)
        fp.close()

        fp = open(newName, "w")
        fp.write(newDoc)
        fp.close()

        # Generate a diff
        diff = cdr.diffXmlDocs(oldDoc, newDoc)

        # Might not be any differences, if not, make it more user-friendly
        if not diff:
            diff = "-- No differences --"

        # And write it
        fp = open(diffName, "w")
        fp.write(diff)
        fp.close()

        # If there are errors to report, write them out
        if valErrs:
            fp = open(errsName, "w")
            for err in valErrs:
                fp.write("%s\n" % err)
            fp.close()

    except Exception, e:
        raise Exception("Error writing global change output files: %s" %
                        str(e))


#------------------------------------------------------------
# Check correspondence between a filter and a schema
#------------------------------------------------------------
def checkSchema(docType, parentElem, filterName, filterElems):
    """
    We have damaged data in the past when new elements were included in
    a schema but a filter that processed the documents of that type didn't
    know about those elements and failed them.  This can happen when a
    filter copies a known list of elements, inserts a new one, then copies
    another known list - in order to get the new element in the right place.
    Adding an element to the document that is unknown to the filter causes
    it to be lost after copying.

    This function checks the expected list against the schema.  It requires
    that code be inserted in cdrglblchg to perform the check.  If the check
    fails:
        The program will raise an exception.
        A programmer must fix this Python program and the XSLT filter.

    Pass:
        docType     - Document type name to find the schema.
        parentElem  - Name of parent element whose children will be checked.
        filterName  - Name of the filter - only used to report errors.
        filterElems - A sequence of element names expected by the filter.

    Return:
        Void.

    Raises:
        Exception - if a mismatch is detected.
        Also writes to the debug log.
    """
    # Get doctype object encapsulating the schema
    dti = cdr.getDoctype('guest', docType)

    # Get list of children of the node we want.  Node must have unique name.
    schemaElems = dti.getChildren(parentElem)

    # Compare, irrespective of sequence types
    if list(filterElems) != list(schemaElems):

        msg = """\
Schema/XSLT filter mismatch error!<br />
The filter %s expects a schema for doc type "%s"
to have children of the parent element "%s":<br />
%s.<br />
But the schema reports that the children are:<br />
%s<br />
Please contact a programmer to modify the filter before re-running this program.
""" % (filterName, docType, parentElem, filterElems, schemaElems)
        cdr.logwrite(msg)
        raise Exception(msg)

    # Else all okay


#------------------------------------------------------------
# Function return object contains information returned by a
# function called to execute a stage of processing.
#------------------------------------------------------------
class FuncReturn:
    """
    This provides a more orderly way to return multiple values than
    using a tuple - which is not easily expandable without breaking
    things.

    The FuncReturn object is just a dictionary containing things
    a called function needs to communicate back to its caller.

    Initial version is for returning data for sending out in an
    HTML page, but more elements might be added later.
    """
    def __init__(self, retType, errMsg=None):

        # Tells the caller what he might expect here
        # Values are constants understood by convention
        self.retType = retType

        # If a serious error occurred
        self.errMsg = errMsg

        # For construction of an html page
        self.pageTitle   = None
        self.pageHtml    = None
        self.pageButtons = None

    def setRetType(self, retType):   self.retType = retType
    def getRetType(self):            return self.retType

    def setErrMsg(self, msg):        self.errMsg = msg
    def getErrMsg(self):             return self.errMsg

    def setPageTitle(self, title):   self.pageTitle = title
    def getPageTitle(self):          return (self.pageTitle)

    def setPageHtml(self, html):     self.pageHtml = html
    def getPageHtml(self):           return self.pageHtml

    def setPageButtons(self, bttns): self.pageButtons = bttns
    def getPageButtons(self):        return self.pageButtons


#------------------------------------------------------------
# Stage object, holds what we have to do for a single round of
# processing.
#------------------------------------------------------------
class Stage:
    """
    Information used to identify all elements in a single stage
    of processing.

    A GlblChg object contains a sequence of these.  They are
    executed in order, often with round trips to the user in between.

    This is really a part of the GlblChg subclasses, included by
    composition.

      condition
        Evaluate this expression to determine whether to execute the stage.
        Expression is a string containing any boolean expression for eval().
        Default = None - do it unconditionally.
        Example:
          # Only do this if we don't have a fragment id already
          "string.find (self.ssVars['fromId'], '#') == -1"
      subr
        Reference to subroutine to execute.

      parms
        Tuple of arguments to pass to subroutine
        If an argument has the form "eval:blah blah blah"
          then first convert the argument by evaluating "blah blah blah"

      excpMsg
        String to prefix in an exception message, example:
          "Error getting id of person"

    """

    def __init__(self, condition, subr, parms=None, excpMsg=None):
        """
        Construct object containing everything we need to execute it.
        """
        self.condition = condition
        self.subr      = subr
        self.parms     = parms
        self.excpMsg   = excpMsg

    def getExcpMsg(self):
        return self.excpMsg


#------------------------------------------------------------
# Base class for all global changes.  Has some common stuff.
#------------------------------------------------------------
class GlblChg:

    # Holds filter to find specific locations with Person or Organization
    # Defined in subclasses
    locFilter = None

    def __init__(self):
        # Holds variables for this session, assigned in factory method
        self.ssVars = {}
        self.stages = ()

        # Holds count of passes through getFilterInfo()
        # Re-initialized for each doc in GlobalChangeBatch.py
        self.filtered = [0,0]

        # This should be overridden by subclasses
        self.description = None

    def getStages(self):
        return self.stages

    def execStage(self, stage):
        """
        Execute one stage of processing.

        Pass:
            Reference to Stage object to execute.
            All info comes from the stage itself, or from
            information that it derives by evaluating arguments.

        Return:
            FuncReturn object.
            FuncReturn.retType tells what's inside.

        Errors:
            If a serious error arises, we return the error to caller
            who must eventually inform the user.
        """
        # First check to see if the condition is okay
        if stage.condition and not eval (stage.condition):
            return FuncReturn (RET_NONE)

        # Perform any required parameter conversions
        parms = []
        for parm in stage.parms:
            if type(parm) == type("") and parm[:5] == "eval:":
                try:
                    parm = eval(parm[5:])
                except:
                    return FuncReturn (RET_ERROR,
                      "%s: Internal error converting parameter \"%s\"" %\
                      (stage.excpMsg, parm[5:]) +" - inform support staff")

            parms.append (parm)

        # Try to execute the subroutine for this stage
        try:
            # result must be a FuncReturn object
            result = stage.subr (parms)
        except Exception, e:
            msg = "execStage Exception: %s: %s" % (stage.excpMsg, str(e))
            cdr.logwrite (msg, LF, tback=1)
            return FuncReturn (RET_ERROR, msg)
        except:
            msg = "execStage Exception: %s: Unknown error" % stage.excpMsg
            cdr.logwrite (msg, LF, tback=1)
            return FuncReturn (RET_ERROR, msg)

        # Caller will decide what to do with results
        return result


    # Connection and cursor objects for actions with this object
    def reportWillChange (self):
        """
        Create an HTML list of documents that will be changed.

        Pass:
          Uses the Stage.subr interface, all parameters in a sequence:
            But no parameters needed - empty sequence.

        Return:
            FuncReturn with table of docs in HTML format.
            Return type = RET_NONE if no docs will change.
        """
        # Select docs
        cdr.logwrite ("Selecting docs for willChange report", LF)
        try:
            rows = self.selDocs()
        except cdrbatch.BatchException, be:
            msg = "Error selecting docs: %s" % str(be)
            cdr.logwrite (msg, LF, tback=1)
            return FuncReturn (RET_ERROR, msg)

        # If there aren't any
        if not rows:
            return FuncReturn(RET_NONE)

        # Remember the count
        self.ssVars['chgCount'] = str(len(rows))
        cdr.logwrite ("Got %d rows" % len(rows), LF)

        # Session for host query
        session = self.ssVars[cdrcgi.SESSION]
        cdr.logwrite ("Got session %s" % session, LF)

        # Setup some html and javascript for managing the list of docs
        # Select All, Clear All buttons
        selButtons = """
<center>
<input type='button' value='Select All' onclick='javascript:setChecks(1)' />
&nbsp;
<input type='button' value='Clear All' onclick='javascript:setChecks(0)' />
</center>
 """
        # This is what the buttons invoke, passing 1 to set, 0 to clear
        javaScript = """
<script type='text/javascript' language='javascript'>
 function setChecks(val) {
   frm = document.glblChgForm;
   len = frm.elements.length;
   var i;
   for (i=0; i<len; i++) {
     if (frm.elements[i].name=='glblDocId') {
         frm.elements[i].checked=val;
     }
   }
 }
</script>
"""
        # This is the actual checkbox html. Doc id will be inserted in loop
        clickBox = r"""
<input type='checkbox' name='glblDocId' value='%d' checked='1' />
"""

        # Table header for docs that will change
        newRows = [['<b>Chg</b>','<b>DocID</b>', '<b>P</b>', '<b>Title</b>']]

        # Find out if there's a publishable version of each
        for row in rows:
            (docIdStr, docIdNum, fragDummy) = cdr.exNormalize (row[0])
            chkBox   = clickBox % docIdNum
            vers = cdr.lastVersions (session, docIdStr)
            if type(vers) == type("") or type(vers) == type(u""):
                pVer = vers
            else:
                pubVer = vers[1]
                if pubVer < 0:
                    pVer = 'N'
                else:
                    pVer = 'Y'
            newRows.append ([chkBox, row[0], pVer, row[1]])

        # Create the table
        cdr.logwrite ("Ready to create table", LF)
        html = javaScript + selButtons + cdr.tabularize (newRows, " border='1'")
        cdr.logwrite ("Table created", LF)

        # Hand it all back
        result = FuncReturn (RET_HTML)
        result.setPageHtml (html)
        cdr.logwrite ("Established result page", LF)
        result.setPageTitle ("The following documents will change")
        cdr.logwrite ("Ready to return will change report", LF)
        return result


    def verifyId(self, parms):
        """
        Verify that a document exists and has the expected document type.
        Pass:
          Uses the Stage.subr interface, all parameters in a sequence:
            Document id as string or integer.
            Document type as string.
            Name of session variable to receive results.
        Return:
            FuncReturn object with no content (retType=RET_NONE).
            RET_ERROR if error.
        """
        # Get parameters
        if (len(parms) != 3):
            raise Exception ("verifyId expected 3 parms, got " +
                             str(len(parms)))
        docId     = parms[0]
        docType   = parms[1]
        titleName = parms[2]

        cdr.logwrite("Verifying docId=%s, docType=%s, titleName=%s" %\
                     (docId, docType, titleName), LF)

        # There are lots of possible errors, create a default error return
        result = FuncReturn (RET_ERROR)

        # Normalize any type of valid id
        try:
            oneId = cdr.exNormalize(docId)[1]
        except Exception, e:
            result.setErrMsg ("Internal error: %s" % str(e))
            return result

        qry = """
    SELECT d.title, t.name
      FROM document d
      JOIN doc_type t
        ON d.doc_type = t.id
     WHERE d.id = %d
    """ % oneId

        try:
            conn   = cdrdb.connect ('CdrGuest')
            cursor = conn.cursor()
            cursor.execute (qry)
            row    = cursor.fetchone()
            cursor.close()
        except cdrdb.Error, info:
            result.setErrMsg ("Database error checking document id: %s" % \
                              info[1][0])
            return result

        if not row:
            msg = "Could not find doc with id=%d" % oneId
            cdr.logwrite (msg, LF)
            result.setErrMsg (msg)
            return result

        title = row[0]
        dType = row[1]
        if dType != docType:
            msg = "Document %d is of type %s, not %s" % (oneId, dType, docType)
            cdr.logwrite (msg, LF)
            result.setErrMsg (msg)
            return result

        # Success, set session variable
        cdr.logwrite ("Verified title='%s'" % title, LF)
        self.ssVars[titleName] = title

        # Caller doesn't need to do anything, no return value
        result.setRetType (RET_NONE)
        return result


    def getPickList (self, parms):
        """
        Construct a pick list for a web page.

        Pass:
          Uses the Stage.subr interface, all parameters in a sequence:
            docType      - Name of document type, used in database query
                           and in prompt to user.
            searchString - String to search for in this doc type.  May be
                           full title or leading substring.
            title        - Put this title on the picklist.
            varName      - Construct hidden variable with this name to
                           receive user's pick.

        Return:
            FuncReturn with html form contents, suitable for display.
            Number of elements is limited to MAX_PICK_LIST_SIZE
        """
        # Get parameters
        if (len(parms) != 4):
            raise Exception ("getPickList expected 4 parms, got " +
                             str(len(parms)))
        docType      = parms[0]
        searchString = parms[1]
        title        = parms[2]
        varName      = parms[3]

        # Prepare query
        qry = """
    SELECT TOP %d d.id, d.title
      FROM document d, doc_type t
     WHERE d.title LIKE ?
       AND d.doc_type = t.id
       AND t.name = ?
     ORDER BY d.title
    """ % MAX_PICK_LIST_SIZE

        rows = _execQry (qry, (searchString + '%', docType))

        # Result object
        result = FuncReturn (RET_HTML)

        # Nothing there with that name?
        html = self.showSoFarHtml()
        if not rows:
            # Message is no hits, only button to push is Cancel
            html += '<h3>No hits beginning with "%s"</h3>' % searchString
            result.setPageButtons ((('cancel', 'Done'),))

        else:
            html += "<h3>%s</h3>\n" % title +\
                   "<select name='%s' size='15'>\n" % varName

            # Get list, up to max (caller may have limited too)
            for row in rows:
                html += " <option value='%s'>%s</option>\n" % (row[0], row[1])

            html += "</select>\n"

        result.setPageHtml (html)
        result.setPageTitle (title)
        return result


    def genValidValPickList (self, parms):
        """
        Construct an html picklist of valid values for a particular
        schema complex type.
        The name of the picklist is the same as the name of the
        schema type, prepended by action, e.g.,
            genValidValPickList ('InScopeProtocol', 'StatusName', 'from')
            returns "<select name='fromStatusName' ..."
        Bob's getDoctype does the work.

        Pass:
          Uses the Stage.subr interface, all parameters in a sequence:
            docType   - Doctype name.
            vvType    - Name of complex type in schema containing valid values.
            vvVarName - Name of form variable to receive pick, e.g., 'fromFoo'.
            defaultVal- Exact string matching default value, or None.
            optional  - True = add a "None" choice to list.
        Returns:
            HTML format selectable picklist.
        """
        # Parameters
        if (len(parms) != 5):
            raise Exception ("genValidValPickList expected 5 parms, got " +
                             str(len(parms)))
        docType    = parms[0]
        vvType     = parms[1]
        vvVarName  = parms[2]
        defaultVal = parms[3]
        optional   = parms[4]

        # Default value goes first in list
        putFirst = defaultVal

        # If no default value and list is optional, use blank as default
        if not defaultVal and optional:
            putFirst = ""

        # Get the valid value list from the schema
        vals = cdr.getVVList("guest", docType, vvType, first=putFirst)

        if type(vals)==type("") or type(vals)==type(u""):
            cdr.logwrite ("Error getting valid values: %s" % vals, LF)
            return FuncReturn (RET_ERROR,"Error getting valid values: %s"%vals)

        # Finally, if we had a default value but none is an option
        #   put it at the end
        if defaultVal and optional:
            vals.append("")

        # Construct an html picklist
        html = "<select name='%s'>\n" % vvVarName

        # Add all the values in the created order
        for val in vals:
            html += " <option value='%s'>%s</option>\n" % (val, val)
        html += "</select>\n"

        result = FuncReturn (RET_HTML)
        result.setPageHtml (html)
        result.setPageTitle ("Choose %s" % vvType)
        return result


    def showSoFarHtml (self):
        """
        Generate HTML to show what we've selected so far.

        This one returns straight HTML for inclusion in larger pages.
        It does not return a FuncReturn object.

        Return:
            HTML with 0 or more lines of info about what has happened so far.
        """
        haveSoFar = 0
        html = "<hr><table border='0' cellspacing='6'>"

        # Document type checkboxes handled a bit differently
        if str(self.__class__).endswith('TermChg'):
            html += "<tr><td align='right'>Changing doc types: </td>\n"
            docTypeStr = ""
            if self.ssVars.has_key(TYPE_CDR_DOC) and \
               self.ssVars[TYPE_CDR_DOC]=='Y':
                docTypeStr = "InScopeProtocol "
            if self.ssVars.has_key(TYPE_CTG_DOC) and \
               self.ssVars[TYPE_CTG_DOC]=='Y':
                docTypeStr += "CTGovProtocol"
            if len(docTypeStr) == 0:
                docTypeStr = "None selected"
            html += "<td> %s </td></tr>\n" % docTypeStr

        # Doc ids and restrictions of various types
        if self.ssVars.has_key ('fromTitle'):
            html += "<tr><td align='right'>Changing links from: </td>\n"
            html += "<td> %s (%s)</td></tr>\n" % \
                 (self.ssVars['fromId'], self.ssVars['fromTitle'])
            haveSoFar = 1
        if self.ssVars.has_key ('toTitle'):
            html += "<tr><td align='right'>Changing links to: </td>\n"
            html += "<td> %s (%s)</td></tr>\n" % \
                (self.ssVars['toId'], self.ssVars['toTitle'])
            haveSoFar = 1
        if self.ssVars.has_key ('fromStatusName'):
            html += \
              "<tr><td align='right'>Changing protocol status from: </td>\n"
            html += "<td> %s</td></tr>\n" % self.ssVars['fromStatusName']
            haveSoFar = 1
        if self.ssVars.has_key ('chgOrgTitle'):
            html += \
              "<tr><td align='right'>Inserting organization site: </td>\n"
            html += "<td> %s</td></tr>\n" % self.ssVars['chgOrgTitle']
            haveSoFar = 1
        if self.ssVars.has_key ('delOrgTitle'):
            html += \
              "<tr><td align='right'>Deleting organization site: </td>\n"
            html += "<td> %s</td></tr>\n" % self.ssVars['delOrgTitle']
            haveSoFar = 1
        if self.ssVars.has_key ('coopTitle'):
            html += \
              "<tr><td align='right'>with CoopMember attribute: </td>\n"
            html += "<td> %s</td></tr>\n" % self.ssVars['coopTitle']
            haveSoFar = 1
        if self.ssVars.has_key ('insertPersTitle'):
            html += \
              "<tr><td align='right'>Site contact person: </td>\n"
            html += "<td> %s</td></tr>\n" % self.ssVars['insertPersTitle']
            haveSoFar = 1
        if self.ssVars.has_key ('specificRole'):
            html += \
              "<tr><td align='right'>with role: </td>\n"
            html += "<td> %s</td></tr>\n" % self.ssVars['specificRole']
            haveSoFar = 1
        if self.ssVars.has_key ('specificPhone'):
            html += \
              "<tr><td align='right'>with specific phone number: </td>\n"
            html += "<td> %s</td></tr>\n" % self.ssVars['specificPhone']
            haveSoFar = 1
        if self.ssVars.has_key ('toStatusName'):
            html += \
              "<tr><td align='right'>Changing protocol status to: </td>\n"
            html += "<td> %s</td></tr>\n" % self.ssVars['toStatusName']
            haveSoFar = 1
        if self.ssVars.has_key ('restrTitle'):
            html += \
              "<tr><td align='right'>" +\
              "Restricting to protocols with lead org: </td>\n"
            html += "<td> %s (%s)</td></tr>\n" % \
                 (self.ssVars['restrId'], self.ssVars['restrTitle'])
            haveSoFar = 1
        if self.ssVars.has_key ('restrPiTitle'):
            html += \
              "<tr><td align='right'>" +\
              "Restricting to protocols with org site PI: </td>\n"
            html += "<td> %s (%s)</td></tr>\n" % \
                 (self.ssVars['restrPiId'], self.ssVars['restrPiTitle'])
            haveSoFar = 1

        # When multiply selected status values are possible
        # Terms and delete orgsites
        statusKey = None
        if self.ssVars.has_key (TERM_STATVAL):
            statusKey = TERM_STATVAL
        elif self.ssVars.has_key ('statusName'):
            statusKey = 'statusName'
        if statusKey:
            pattern = \
              "<tr><td align='right'>Protocol status: </td><td> %s</td></tr>\n"
            stvals = self.ssVars[statusKey]

            # May be one or more of these
            if type(stvals) == type([]) or type(stvals)==type(()):
                for st in stvals:
                    html += pattern % st
            else:
                html += pattern % stvals

        # Terms are complicated, get them from a subroutine
        termHtml = self.showTermsSoFar()
        if len (termHtml) > 0:
            html += termHtml
            haveSoFar = 1

        # Count of docs to change
        if self.ssVars.has_key ('chgCount'):
            html += \
              "<tr><td align='right'>Count of documents to change: </td>\n"
            html += "<td> %s</td></tr>\n" % self.ssVars['chgCount']
            haveSoFar = 1

        # Don't show anything if there's nothing to show
        if haveSoFar:
            html += "</table></font><hr>\n"
        else:
            html = "<hr>\n"

        return html


    def showTermsSoFar (self):
        """
        Subroutine of showSoFarHtml() to generate rows in the the caller's
        html table for each term particpating in a terminology change.

        Assumptions:
            Program has already resolved all term values to form IDs.

        Pass:
            Void (other than self).
        Return:
            String of html containing table rows and columns for
              insertion in an already existing table.
            "" if there are no terms to show.
        """
        html = ""

        # If we're not the right type of change, there's nothing to do
        if self.ssVars["chgType"] != TERM_CHG:
            return html

        # Search for every type of saved term criterion
        for trmUse in TERM_USES:
            for i in range (TERM_MAX_CRITERIA):
                # Only compose a row if we have all info for it
                keyId    = "trm%sId%d" % (trmUse, i)
                keyVal   = "trm%sVal%d" % (trmUse, i)
                keyField = "trm%sField%d" % (trmUse, i)
                if self.ssVars.has_key(keyId) and self.ssVars.has_key(keyVal):
                    html += \
                 "<tr><td align='right'>%s %s: </td><td> %s (%s)</td></tr>\n"%\
                     (TERM_MSGS[trmUse], self.ssVars[keyField],
                      self.ssVars[keyVal], self.ssVars[keyId])
                # Special case for StudyCategory - no trmTypId0 for it
                if keyField == TERM_SCAT_FLD and self.ssVars.has_key(keyVal):
                    html += \
                 "<tr><td align='right'>%s %s: </td><td> %s</td></tr>\n"%\
                     (TERM_MSGS[trmUse], self.ssVars[keyField],
                      self.ssVars[keyVal])


        return html


    def genInputHtml (self, parms):
        """
        Generate a 'from' or 'to' or whatever document id/name input
        screen for a user to enter the document identifer to change or
        use in a restriction.

        Pass:
          Uses the Stage.subr interface, all parameters in a sequence:
            docType   - Name of document type for presentation to user.
            inTitle   - Title to put on input box.
            varPrefix - Results go in this session variable + "Id" or "Name"
            optional  - True=input is optional, skipping is allowed
        Return:
            FuncReturn with HTML form contents for presentation.
        """

        # Get parameters
        if (len(parms) != 4):
            raise Exception ("genInputHtml expected 4 parms, got " +
                             str(len(parms)))
        docType    = parms[0]
        inputTitle = parms[1]
        varPrefix  = parms[2]
        optional   = parms[3]

        # If input is optional, see if we've already done this
        # Don't need to do it again
        if optional:
            inputChkVar = "%sChk" % varPrefix
            if self.ssVars.has_key (inputChkVar):
                return FuncReturn (RET_NONE)

            # Set the state variable to show that we have already checked
            #   for a fragment
            # This allows us to make the form optional, without constantly
            #   requiring the user to see it again
            self.ssVars[inputChkVar] = "1"

        # Construct input form, prefaced by what we've done so far
        html = self.showSoFarHtml() + """
<table border='0'>
<tr><td colspan='2'><h3>%s:</h3></td></tr>
<tr><td align='right'>%s DocId: </td>
    <td><input type='text' size='30' name='%sId'></td></tr>
<tr><td colspan='2'>&nbsp;&nbsp;&nbsp;&nbsp;Or</td></tr>
<tr><td align='right'>%s Name: </td>
    <td><input type='text' size='30' name='%sName'></td></tr>
</table>
    """ % (inputTitle, docType, varPrefix, docType, varPrefix)

        result = FuncReturn (RET_HTML)
        result.setPageHtml (html)
        result.setPageTitle (inputTitle)
        return result


    def genFragPickListHtml (self, parms):
        """
        Create an HTML form with a table containing a picklist of
        address fragments for a person or organization.

        Should only be called if the current id doesn't have a fragment
        already associated with it.

        Pass:
            docId     - ID of document without fragment that needs one.
            varPrefix - Prefix to session variable name.
            optional  - True=Entering an address fragment is optional.

        Returns:
            FuncReturn object with HTML for display
        """
        # Get parameters
        if (len(parms) != 3):
            raise Exception ("genFragPickList expected 3 parms, got " +
                             str(len(parms)))
        docId     = parms[0]
        varPrefix = parms[1]
        optional  = parms[2]
        cdr.logwrite ("Getting frag ID for docId=%s, varPrefix=%s, optional=%d"\
                      % (docId, varPrefix, optional), LF)

        # We're getting a 'fromId' and 'fromTitle', or whatever
        idType    = varPrefix + 'Id'
        titleType = varPrefix + 'Title'

        # Filter this document with the picklist filter to select
        #   fragments <Link> and corresponding addresses <Data>
        filtResp = cdr.filterDoc (self.ssVars[cdrcgi.SESSION],
                                  filter=self.locFilter,
                                  parm=[['docId', docId]],
                                  docId=docId, docVer=None)

        if type(filtResp) != type(()):
            return FuncReturn (RET_ERROR,
              "Error filtering addresses:<br>\n%s" % cdr.getErrors (filtResp))

        # Parse results to get full id, fragment id, address tuples
        idAddrs = []
        docElem = xml.dom.minidom.parseString (filtResp[0]).documentElement
        for node in docElem.childNodes:
            if node.nodeName == 'ReportRow':
                fullId=cdr.getTextContent(node.getElementsByTagName("Link")[0])
                addr  =cdr.getTextContent(node.getElementsByTagName("Data")[0])
                fragId=cdr.exNormalize (fullId)[2]
                idAddrs.append ((fullId, fragId, addr))

        # Create HTML table to display it
        html = self.showSoFarHtml() + """
<p>Address fragments for: %s (%s)</p>
<p>Select a fragment identifier for the address desired.</p>
<table border='1'>
""" % (docId, self.ssVars[titleType])

        # First frag item is checked, then set unchecked for next
        checked=' checked'

        # Caller may say that it is legal to select no fragment
        if optional:
            # See if we've already done this
            # Stage condition can test it, or just let this code handle it
            fragChkVar = "%sFragChk" % varPrefix
            if self.ssVars.has_key (fragChkVar):
                return FuncReturn (RET_NONE)

            # Set the state variable to show that we have already checked
            #   for a fragment.
            self.ssVars[fragChkVar] = "1"

            # Seed the pick list with an entry for no/all fragments
            # value attribute = doc id with no fragment added to it
            html += """
 <tr><td><input type='radio' name='%s' value='%s' %s>&nbsp;</input></td>
     <td>No specific address fragment - all occurrences match regardless of
     presence or absence of fragment id</td></tr>
""" % (idType, docId, checked)
            # We've checked our default
            checked = ''

        # Populate table with radio buttoned cdr ids and addresses
        # value attribute = doc id with fragment appended
        for addrInfo in idAddrs:
            html += """
 <tr><td><input type='radio' name='%s' value='%s'%s>%s</input></td>
     <td>%s</td></tr>
""" % (idType, addrInfo[0], checked, addrInfo[1], addrInfo[2])
            # Only first button is checked
            checked = ''

        html += "</table>\n"

        # The from or to id is now in the radio buttons
        # Remove it from the session vars so it only appears once
        #   in the CGI form
        del (self.ssVars[idType])

        result = FuncReturn (RET_HTML)
        result.setPageTitle ("Choose address fragment")
        result.setPageHtml (html)
        return result


    def genTermInputHtml (self, parms):
        """
        Create a screen to get all terminology search and change
        criteria.

        This is more advanced than other input screens in that it
        gets everything we need on one screen.

        Pass:
            No parms required.  Uses self.ssVars.

        Returns:
            HTML string.
        """
        # Silence pychecker warning about unused parms
        if parms != []:
           return FuncReturn (RET_ERROR, "Too many parms to genTermInputHtml")

        # Header information
        html = self.showSoFarHtml() + """
<h2>Select protocols for terminology change</h2>
"""

        # Tells whether we've been here before
        firstTime = 1

        # This particular key is always present if we have been here before
        if self.ssVars.has_key("trmReqField0"):
            firstTime = 0

        # Get protocol document types
        typeCDR = ""
        typeCTG = ""

        if firstTime or self.ssVars.has_key(TYPE_CDR_DOC):
            typeCDR = BOXCHKD
            if not firstTime:
                del (self.ssVars[TYPE_CDR_DOC])
        if firstTime or self.ssVars.has_key(TYPE_CTG_DOC):
            typeCTG = BOXCHKD
            if not firstTime:
                del (self.ssVars[TYPE_CTG_DOC])
        html += """
<span class='termblock'>Choose at least one protocol document type</span>
<br /><br />
<input name="%s" type="checkbox" value="Y"%s>InScopeProtocol documents</input>
<input name="%s" type="checkbox" value="Y"%s>CTGovProtocol documents</input>
<hr />
""" % (TYPE_CDR_DOC, typeCDR, TYPE_CTG_DOC, typeCTG)

        # Protocol status selections - an array of selected items, if any
        html += """
<span class='termblock'>Choose one or more protocol status values</span>
<br /><br />
<select name="%s" size="6" multiple="multiple">
""" % TERM_STATVAL
        selectedStatus = None
        if self.ssVars.has_key (TERM_STATVAL):
            selectedStatus = self.ssVars[TERM_STATVAL]
            del (self.ssVars[TERM_STATVAL])

        # Possible status values come from protocol schema valid values list
        # Status values are the same for both protocol doc types
        session = self.ssVars[cdrcgi.SESSION]
        for status in cdr.getVVList (session, 'InScopeProtocol', "StatusName"):

            # Protocol status with any previous selections
            alreadySelected=""
            if selectedStatus:
                # Status may be scalar or array of multiple selections
                if type(selectedStatus) == type([]):
                    if status in selectedStatus:
                        alreadySelected=" selected='selected'"
                else:
                    if status == selectedStatus:
                        alreadySelected=" selected='selected'"
            html += "  <option%s>%s</option>\n" % (alreadySelected, status)
        html += "</select>\n"

        # Input search criteria and change criteria
        for i in range (len(TERM_PROMPTS)):

            html += "<hr />\n%s\n" % TERM_PROMPTS[i]

            if i < TERM_SEARCH_USES:
                rowCount = TERM_MAX_CRITERIA
            elif i < TERM_MODIFY_USES:
                rowCount = TERM_MAX_CHANGES
            else:
                rowCount = TERM_MAX_QUALS

            # Table headers
            html += """
<table cellpadding="2" cellspacing="2" border="0">
 <tr>
   <td><strong>Element</strong></td>
   <td><strong>String Value</strong></td>
   <td><strong>Or Term DocID</strong></td>
 </tr>
 <tr>
"""
            for row in range (rowCount):
                # Row of element, term string value, term docId
                html += " <tr>\n"

                # Name of the XML element containing the term
                name = "trm%sField%d" % (TERM_USES[i], row)
                html += "  <td><select name='%s' size='0'>\n" % name

                # Selection list for element name
                if name == TERM_SCAT_FLD:
                    # StudyCategory is special case, hard wire it in
                    html += "   <option selected='selected'>%s</option>\n" %\
                                TERM_FLD_SCAT
                    # Copy value back into the form for editing
                    if self.ssVars.has_key (name) and \
                                self.ssVars[name] == TERM_FLD_SCAT:
                        # Don't duplicate in hidden vars. Delete in session
                        del (self.ssVars[name])

                elif name == TERM_INTV_FLD:
                    # InterventionType - same treatment
                    html += "   <option selected='selected'>%s</option>\n" %\
                                TERM_FLD_INTV
                    if self.ssVars.has_key (name) and \
                                self.ssVars[name] == TERM_FLD_INTV:
                        del (self.ssVars[name])

                else:
                    # All other field selection options
                    for field in TERM_FIELD_ORDER:
                        alreadySelected=""
                        if self.ssVars.has_key (name) and \
                                    self.ssVars[name] == field:
                            alreadySelected=" selected='selected'"
                            del (self.ssVars[name])
                        html += \
                             "   <option %s size='0'>%s</option>\n"%\
                              (alreadySelected, field)
                html += "  </select></td>"

                # Place for user to enter string value of term
                name = "trm%sVal%d" % (TERM_USES[i], row)
                alreadySelected=""
                if self.ssVars.has_key (name):
                    alreadySelected = self.ssVars[name]
                    del (self.ssVars[name])

                # Again, study category is a special case
                #   String values come from a valid values table, they aren't
                #   titles of term documents XXXX
                if name == TERM_SCAT_VAL:
                    html += "<td>" + getStudyCategories(name, alreadySelected)\
                                   + "</td>"
                else:
                    html += """
  <td><input type="text" name="%s" size="50" maxlength="300" value="%s" /></td>"""%\
                    (name, alreadySelected)

                # Place for user to enter document ID of term
                name = "trm%sId%d" % (TERM_USES[i], row)

                # Is it possible that this is the last StudyCategory kludge?
                if name == TERM_SCAT_ID:
                    html += "<td>&nbsp;</td>\n"
                else:
                    alreadySelected=""
                    if self.ssVars.has_key (name):
                        alreadySelected = self.ssVars[name]
                        del (self.ssVars[name])
                    html += """
  <td><input type="text" name="%s" size="10" maxlength="10" value="%s" /></td>\n"""%\
                    (name, alreadySelected)

                # End of this input row
                html += " </tr>\n"

            # End of table
            html += "</table>\n"

        # Prepend error message if needed
        if not firstTime:
            html = """
<h4><font color='red'>Must select at least:
<ul>
  <li>one protocol document type</li>
  <li>one protocol status</li>
  <li>one required term (ALL) or one additional term (ANY)</li>
  <li>one Add or Delete term</li>
  <li>qualifying StudyCategory if changing InterventionType or NameLink</li>
  <li>qualifying InterventionType if changing InterventionNameLink</li>
</ul>
</font></h4>\n""" + html

        # Return info to stage executor
        result = FuncReturn (RET_HTML)
        result.setPageHtml (html)
        result.setPageTitle ("CDR Global Change")
        return result

    def getFromToStatus (self, parms):
        """
        Create a screen to get from and to OrgSiteStatus values.

        Pass:
          Uses the Stage.subr interface, all parameters in a sequence:
            Empty sequence - no parameters.

        Return:
            FuncReturn object.
            FuncReturn.retType tells what's inside.
        """
        # Silence pychecker warning about unused parms
        if parms != []:
           return FuncReturn (RET_ERROR,
                "Too many parms to getFromToStatus")

        # Get the actual picklists
        fromListResult = self.genValidValPickList (('InScopeProtocol',
                         'StatusName','fromStatusName', None, 0))
        toListResult   = self.genValidValPickList (('InScopeProtocol',
                         'StatusName','toStatusName', None, 0))

        # Check
        if fromListResult.getRetType() != RET_HTML:
           return FuncReturn (RET_ERROR,
                "Unexpected result from fromStatus genValidValPickList: " +\
                str(fromListResult.getRetType()))
        if toListResult.getRetType() != RET_HTML:
           return FuncReturn (RET_ERROR,
                "Unexpected result to toStatus genValidValPickList: " +\
                str(fromListResult.getRetType()))

        # Extract html
        fromList = fromListResult.getPageHtml()
        toList   = toListResult.getPageHtml()

        # Construct full screen
        html = self.showSoFarHtml() + """
<table border='0'>
 <tr>
  <td align='right'>Select status to change from</td>
  <td>%s</td>
 </tr><tr>
  <td align='right'>Select status to change to</td>
  <td>%s</td>
 </tr>
</table>""" % (fromList, toList)

        result = FuncReturn (RET_HTML)
        result.setPageTitle ("Pick status to change from/to")
        result.setPageHtml (html)
        return result


    def getPersonSpecifics (self, parms):
        """
        Create html for gathering specific information about a person.

        Used only in INS_ORG_CHG.

        Pass:
          Uses the Stage.subr interface, all parameters in a sequence:
            But no parameters needed - empty sequence.

        Return:
            FuncReturn with HTML format input form to get specifics.
        """
        # Silence pychecker warning about unused parms
        if parms != []:
           return FuncReturn (RET_ERROR,
                "Too many parms to getPersonSpecifics")

        # Get a picklist of roles to plug into the html
        roleResult = self.genValidValPickList (('InScopeProtocol',
                     'Role', 'specificRole',
                     'Principal investigator', 0))
        # Check
        if roleResult.getRetType() != RET_HTML:
           return FuncReturn (RET_ERROR,
                "Unexpected result from role genValidValPickList: " +\
                str(roleResult.getRetType()))

        # Construct the form
        html = """
<table border='0'>
<tr><td colspan='2'><h3>Accompanying information for person:</h3></td></tr>
<tr><td align='right'>Specific phone (optional): </td>
    <td><input type='text' size='30' name='specificPhone'></td></tr>
<tr><td align='right'>Role: </td>
    <td>%s</td></tr>
</table>
    """ % roleResult.getPageHtml()

        # Package for return
        result = FuncReturn (RET_HTML)
        result.setPageTitle ("Accompanying person information")
        result.setPageHtml (html)
        return result

    def getStatusVals (self, parms):
        """
        Create an HTML form block to get one or more status values.

        Pass:
          Uses the Stage.subr interface, all parameters in a sequence:
            Title for the HTML, may include HTML tags, or maybe None
            Name of the form select box, e.g., "statusName"
          May also be called as a separate function, not as a stage.

        Return:
            FuncReturn object.
            FuncReturn.retType tells what's inside.
        """
        # Parms
        blockTitle = parms[0]
        blockName  = parms[1]

        # Create HTML option selection list with requested name
        html = """
<span class='termblock'>Choose one or more protocol status values</span>
<br /><br />
<select name="%s" size="6" multiple="multiple">""" % blockName

        # If coming back here from elsewhere, find already selected values
        #   and delete them from the session, they'll be replaced with
        #   whatever is selected here
        selectedStatus = None
        if self.ssVars.has_key (blockName):
            selectedStatus = self.ssVars[blockName]
            del (self.ssVars[blockName])

        # Possible status values come from protocol schema valid values list
        # Status values are the same for both protocol doc types
        session = self.ssVars[cdrcgi.SESSION]
        for status in cdr.getVVList (session, 'InScopeProtocol', "StatusName"):

            # Protocol status with any previous selections
            alreadySelected=""
            if selectedStatus:
                # Status may be scalar or array of multiple selections
                if type(selectedStatus) == type([]):
                    if status in selectedStatus:
                        alreadySelected=" selected='selected'"
                else:
                    if status == selectedStatus:
                        alreadySelected=" selected='selected'"
            html += "  <option%s>%s</option>\n" % (alreadySelected, status)
        html += "</select>\n"

        # Prepare results to return
        result = FuncReturn (RET_HTML)

        # Title for list
        if blockTitle:
            result.setPageTitle (blockTitle)

        # Selection form widget
        result.setPageHtml (html)

        return result

    def getCoopAttribute (self, parms):
        """
        Create html for getting optional cooperative group attribute
        information in INS_ORG_CHG.

        Pass:
          Uses the Stage.subr interface, all parameters in a sequence:
            But no parameters needed - empty sequence.

        Return:
            FuncReturn with HTML format input form to get specifics.
        """
        # Silence pychecker warning about unused parms
        if parms != []:
           return FuncReturn (RET_ERROR,
                "Too many parms to getCoopAttribute")

        # Get a picklist of coop groups
        cdr.logwrite ("About to get picklist", LF)
        coopResult = self.genValidValPickList (('InScopeProtocol',
                     'OrgSite@CoopMember', 'coopType', None, 1))
        cdr.logwrite ("Got picklist", LF)

        # Check
        if coopResult.getRetType() != RET_HTML:
           return FuncReturn (RET_ERROR,
                "Unexpected result from coop genValidValPickList: " +\
                str(coopResult.getRetType()))
        cdr.logwrite ("Checked return value", LF)

        # Construct the form
        html = self.showSoFarHtml() + """
<table border='0'>
<tr><td colspan='2'><h3>Cooperative Group Affiliation Type:</h3></td></tr>
<tr><td align='right'>Affiliation type (optional): </td>
    <td>%s</td></tr>
</table>
    """ % coopResult.getPageHtml()
        cdr.logwrite ("Created html", LF)

        # Since this attribute is optional, set a variable to show that
        #   we asked for it even if there isn't one
        self.ssVars['coopChk'] = 1

        # Package for return
        result = FuncReturn (RET_HTML)
        result.setPageTitle ("Cooperative group affiliation")
        result.setPageHtml (html)
        return result



    def selDocs (self):
        """ Select documents - implemented only in subclasses """
        pass

    def chkFiltered (self, filterVer):
        """
        Increment the count of filter passes of a given type.
            FLTR_CWD
            FLTR_PUB
        Return the count prior to the increment.

        Used by getFilterInfo to determine whether filtering has
        been done or not.
        """
        self.filtered[filterVer] += 1
        return self.filtered[filterVer] - 1

    def getFilterInfo (self, filterVer):
        """
        Determine the correct filter, description, and filter parameters
        for the global change.  If there is more than one filter to
        apply, it is the subclass' responsibility to keep track of what
        it has already returned and return the next filter, or None if
        there are no more.

        self.description is set here for convenience.

        Implemented only in subclasses.

        Pass:
            filterVer - Tells whether we're processing the current
                        working document or a publishable version, one of:
                           cdrglblchg.FLTR_CWD
                           cdrglblchg.FLTR_PUB
        Return:
            If there is another filter to apply then:
                Tuple of:
                    Filter identifier.
                    Tuple of tuples of parameters:
                        Each parameter is a tuple of:
                            parameter name
                            parameter value
                    Indicate whether to save or discard filter output:
                        True  = Keep output.
                        False = Discard, filter is only used for validation.
            Else
                None
        """
        # Fatal error if this hasn't been overridden
        cdr.logwrite (\
            "FATAL ERROR: No override for getFilterInfo(%s)\nABORTING!" % \
            filterVer, LF)
        sys.exit(1)

    def dumpSsVars (self):
        """
        Write the contents of the ssVars (session variables) array
        to the log file for debugging.
        """
        cdr.logwrite ("All current session variables:", LF)
        svars = self.ssVars.keys()
        svars.sort()
        for var in svars:
            cdr.logwrite ("   '%s'='%s'" % (var, self.ssVars[var]), LF)


#------------------------------------------------------------
# Person specific global change object
#------------------------------------------------------------
class PersonChg (GlblChg):

    # Names of filter for Person Link global changes
    # cdr.filterDoc() requires a list, with leading 'name:' for named
    #   filters
    locFilter = ['name:Person Locations Picklist']

    def __init__(self):
        GlblChg.__init__(self)

        # Define the stages of processing for person changes
        self.stages = (\
          Stage (
            # Generate and send a form to user to get "from" person
            condition = 'not (self.ssVars.has_key("fromId") or ' +
                        'self.ssVars.has_key("fromName"))',
            subr      = self.genInputHtml,
            parms     = ('Person', 'Change Person links from', 'from', 0),
            excpMsg   = 'Generating form for getting the "from" person'),
          Stage (
            # If name entered rather than ID, resolve it
            condition = 'self.ssVars.has_key("fromName") and ' +
                        'not self.ssVars.has_key("fromId")',
            subr      = self.getPickList,
            parms     = ('Person', 'eval:self.ssVars["fromName"]',
                         'Choose Person to be changed', 'fromId'),
            excpMsg   = 'Generating picklist for name'),
          Stage (
            # Verify that this is a Person document, not something else
            condition = 'self.ssVars.has_key("fromId") and ' +
                        'not self.ssVars.has_key("fromTitle")',
            subr      = self.verifyId,
            parms     = ('eval:self.ssVars["fromId"]', 'Person', 'fromTitle'),
            excpMsg   = 'Verifying document type for "from" person"'),
          Stage (
            # If no fragment id entered, get one
            condition = 'self.ssVars.has_key("fromId") and ' +
                        'string.find (self.ssVars["fromId"], "#") == -1',
            subr      = self.genFragPickListHtml,
            parms     = ('eval:self.ssVars["fromId"]', 'from', 0),
            excpMsg   = 'Generating fragment picklist for "from" person'),
          Stage (
            # Generate form for "to" person
            condition = 'not (self.ssVars.has_key("toId") or ' +
                        'self.ssVars.has_key("toName"))',
            subr      = self.genInputHtml,
            parms     = ('Person', 'Change Person links to', 'to', 0),
            excpMsg   = 'Generating form for getting the "to" person'),
          Stage (
            # If name entered rather than ID, resolve it
            condition = 'self.ssVars.has_key("toName") and ' +
                        'not self.ssVars.has_key("toId")',
            subr      = self.getPickList,
            parms     = ('Person', 'eval:self.ssVars["toName"]',
                         'Choose Person to be changed to', 'toId'),
            excpMsg   = 'Generating picklist for to name'),
          Stage (
            # Verify that 'to' is a Person document, not something else
            condition = 'self.ssVars.has_key("toId") and ' +
                        'not self.ssVars.has_key("toTitle")',
            subr      = self.verifyId,
            parms     = ('eval:self.ssVars["toId"]', 'Person', 'toTitle'),
            excpMsg   = 'Verifying document type for "to" person"'),
          Stage (
            # If no fragment id entered, get one
            condition = 'self.ssVars.has_key("toId") and ' +
                        'string.find (self.ssVars["toId"], "#") == -1',
            subr      = self.genFragPickListHtml,
            parms     = ('eval:self.ssVars["toId"]', 'to', 0),
            excpMsg   = 'Generating fragment picklist for "to" person'),
          Stage (
            # Optional restriction by lead organization
            # genInputHtml() knows how to ask for optional info only once
            condition = None,
            subr      = self.genInputHtml,
            parms     = ('Organization',
                         'Restrict change to protocols with particular ' +\
                         'lead org, or leave blank', 'restr', 1),
            excpMsg   = 'Generating form for getting the restriction org'),
          Stage (
            # If name entered rather than ID, resolve it
            condition = 'self.ssVars.has_key("restrName") and ' +
                        'not self.ssVars.has_key("restrId")',
            subr      = self.getPickList,
            parms     = ('Organization', 'eval:self.ssVars["restrName"]',
                         'Choose lead organization to restrict change to',
                         'restrId'),
            excpMsg   = 'Generating picklist for restricting org name'),
          Stage (
            # Verify that restricting doc is an Organization
            condition = 'self.ssVars.has_key("restrId") and '
                        'not self.ssVars.has_key("restrTitle")',
            subr      = self.verifyId,
            parms     = ('eval:self.ssVars["restrId"]', 'Organization',
                         'restrTitle'),
            excpMsg   = 'Verifying document type for restricting org')

          # Returns to standard processing here
        )


    def selDocs (self):
        """
        Select ids and titles from the database matching the correct query
        for the user's selection of documents to be changed.

        Selects different based on whether there are restrictions in effect
        or not.

        Return:
            Sequence of all matching database rows, each containing a
            sequence of:
                document id
                document title
        """

        # If no restrictions
        if not self.ssVars.has_key ('restrId'):
            qry = """
SELECT DISTINCT doc.id, doc.title FROM document doc
  JOIN query_term protpers
    ON protpers.doc_id = doc.id
  JOIN query_term protstat
    ON protstat.doc_id = doc.id
 WHERE (
        protpers.path =
   '/InScopeProtocol/ProtocolAdminInfo/ProtocolLeadOrg/LeadOrgPersonnel/Person/@cdr:ref'
      OR protpers.path =
   '/InScopeProtocol/ProtocolAdminInfo/ProtocolLeadOrg/ProtocolSites/PrivatePracticeSite/PrivatePracticeSiteID/@cdr:ref'
      OR protpers.path =
   '/InScopeProtocol/ProtocolAdminInfo/ProtocolLeadOrg/ProtocolSites/OrgSite/OrgSiteContact/SpecificPerson/Person/@cdr:ref'
       )
   AND protpers.value = '%s'
   AND protstat.path = '/InScopeProtocol/ProtocolAdminInfo/ProtocolLeadOrg/LeadOrgProtocolStatuses/CurrentOrgStatus/StatusName'
   AND (protstat.value = 'Active' OR
        protstat.value = 'Approved-not yet active' OR
        protstat.value = 'Temporarily closed')
 ORDER BY doc.title
""" % self.ssVars['fromId']

            # Return rows of id + title
            return _execQry (qry)

        # Else restrict them by a particular lead organization
        else:
            qry = """
SELECT DISTINCT doc.id, doc.title FROM document doc
  JOIN query_term protpers
    ON protpers.doc_id = doc.id
  JOIN query_term protstat
    ON protstat.doc_id = doc.id
  JOIN query_term leadorg
    ON leadorg.doc_id = doc.id
 WHERE (
        protpers.path =
   '/InScopeProtocol/ProtocolAdminInfo/ProtocolLeadOrg/LeadOrgPersonnel/Person/@cdr:ref'
      OR protpers.path =
   '/InScopeProtocol/ProtocolAdminInfo/ProtocolLeadOrg/ProtocolSites/PrivatePracticeSite/PrivatePracticeSiteID/@cdr:ref'
      OR protpers.path =
   '/InScopeProtocol/ProtocolAdminInfo/ProtocolLeadOrg/ProtocolSites/OrgSite/OrgSiteContact/SpecificPerson/Person/@cdr:ref'
       )
   AND protpers.value = ?
   AND protstat.path = '/InScopeProtocol/ProtocolAdminInfo/ProtocolLeadOrg/LeadOrgProtocolStatuses/CurrentOrgStatus/StatusName'
   AND (protstat.value = 'Active' OR
        protstat.value = 'Approved-not yet active' OR
        protstat.value = 'Temporarily closed')
   AND leadorg.path = '/InScopeProtocol/ProtocolAdminInfo/ProtocolLeadOrg/LeadOrganizationID/@cdr:ref'
   AND leadorg.value = ?
   AND LEFT(leadorg.node_loc, 8) = LEFT(protpers.node_loc, 8)
 ORDER BY doc.title
"""
            # Return rows of id + title
            return _execQry (qry, (self.ssVars['fromId'],
                                   self.ssVars['restrId']))

    def getFilterInfo (self, filterVer):
        """
        See class GlblChg.getFilterInfo for description.
        """
        # Set description if not yet done
        if not self.description:
            self.description = "global change of person link %s to %s on %s" %\
                               (self.ssVars['fromId'],
                                self.ssVars['toId'],
                                time.ctime (time.time()))
        # Only one pass
        if not self.chkFiltered (filterVer):
            filterName    = ['name:Global Change: Person Link']
            parms         = []
            parms.append (['changeFrom', self.ssVars['fromId']])
            parms.append (['changeTo', self.ssVars['toId']])

            return (filterName, parms, True)

        return None


#------------------------------------------------------------
# Organization specific global change object
#------------------------------------------------------------
class OrgChg (GlblChg):

    # Name of filter for Organization link picklist
    locFilter = ['name:Organization Locations Picklist']

    def __init__(self):
        GlblChg.__init__(self)

        # Stages of processing for Organization changes
        self.stages = (\
          Stage (
            # Generate and send a form to user to get "from" org
            condition = 'not (self.ssVars.has_key("fromId") or ' +
                        'self.ssVars.has_key("fromName"))',
            subr      = self.genInputHtml,
            parms     = ('Organization', 'Change Organization links from',
                         'from', 0),
            excpMsg   = 'Generating form for getting the "from" organization'),
          Stage (
            # If name entered rather than ID, resolve it
            condition = 'self.ssVars.has_key("fromName") and ' +
                        'not self.ssVars.has_key("fromId")',
            subr      = self.getPickList,
            parms     = ('Organization', 'eval:self.ssVars["fromName"]',
                         'Choose Organization to be changed', 'fromId'),
            excpMsg   = 'Generating picklist for name'),
          Stage (
            # Verify that this is an org document, not something else
            condition = 'self.ssVars.has_key("fromId") and ' +
                        'not self.ssVars.has_key("fromTitle")',
            subr      = self.verifyId,
            parms     = ('eval:self.ssVars["fromId"]', 'Organization',
                         'fromTitle'),
            excpMsg   = 'Verifying document type for "from" organization"'),
          Stage (
            # If no fragment id entered, get one
            condition = 'self.ssVars.has_key("fromId") and ' +
                        'string.find (self.ssVars["fromId"], "#") == -1',
            subr      = self.genFragPickListHtml,
            parms     = ('eval:self.ssVars["fromId"]', 'from', 1),
            excpMsg   = 'Generating fragment picklist for "from" organization'),
          Stage (
            # Generate form for "to" org
            condition = 'not (self.ssVars.has_key("toId") or ' +
                        'self.ssVars.has_key("toName"))',
            subr      = self.genInputHtml,
            parms     = ('Organization', 'Change Organization links to',
                         'to', 0),
            excpMsg   = 'Generating form for getting the "to" organization'),
          Stage (
            # If name entered rather than ID, resolve it
            condition = 'self.ssVars.has_key("toName") and ' +
                        'not self.ssVars.has_key("toId")',
            subr      = self.getPickList,
            parms     = ('Organization', 'eval:self.ssVars["toName"]',
                         'Choose Organization to be changed to', 'toId'),
            excpMsg   = 'Generating picklist for to name'),
          Stage (
            # Verify that 'to' is an org document, not something else
            condition = 'self.ssVars.has_key("toId") and ' +
                        'not self.ssVars.has_key("toTitle")',
            subr      = self.verifyId,
            parms     = ('eval:self.ssVars["toId"]', 'Organization', 'toTitle'),
            excpMsg   = 'Verifying document type for "to" organization"'),
          Stage (
            # If no fragment id entered, get one
            condition = 'self.ssVars.has_key("toId") and ' +
                        'string.find (self.ssVars["toId"], "#") == -1',
            subr      = self.genFragPickListHtml,
            parms     = ('eval:self.ssVars["toId"]', 'to', 1),
            excpMsg   = 'Generating fragment picklist for "to" organization'),
          Stage (
            # Optional restriction by lead organization
            # genInputHtml() knows how to ask for optional info only once
            condition = None,
            subr      = self.genInputHtml,
            parms     = ('Organization',
                         'Restrict change to protocols with particular ' +\
                         'lead org, or leave blank',
                         'restr', 1),
            excpMsg   = 'Generating form for getting the restriction org'),
          Stage (
            # If name entered rather than ID, resolve it
            condition = 'self.ssVars.has_key("restrName") and ' +
                        'not self.ssVars.has_key("restrId")',
            subr      = self.getPickList,
            parms     = ('Organization', 'eval:self.ssVars["restrName"]',
                         'Choose lead organization to restrict change to',
                         'restrId'),
            excpMsg   = 'Generating picklist for restricting org name'),
          Stage (
            # Verify that restricting doc is an Organization
            condition = 'self.ssVars.has_key("restrId") and '
                        'not self.ssVars.has_key("restrTitle")',
            subr      = self.verifyId,
            parms     = ('eval:self.ssVars["restrId"]', 'Organization',
                         'restrTitle'),
            excpMsg   = 'Verifying document type for restricting org')

          # Returns to standard processing here
        )

    def selDocs (self):
        """
        See PersonChg.selDocs()
        """

        # If searching for an org with a fragment id, we need an exact match
        # But if no fragment, we need to pick up all fragments, or matches
        #   with no fragment at all
        fromId = self.ssVars['fromId']
        if fromId.find ('#') >= 0:
            protOrgMatchStr = "protorg.value = '%s'" % fromId
        else:
            protOrgMatchStr = "protorg.int_val=%d" % cdr.exNormalize(fromId)[1]

        # If no restrictions
        if not self.ssVars.has_key ('restrId'):
            qry = """
SELECT DISTINCT doc.id, doc.title FROM document doc
  JOIN query_term protorg
    ON protorg.doc_id = doc.id
  JOIN query_term protstat
    ON protstat.doc_id = doc.id
 WHERE protorg.path =
   '/InScopeProtocol/ProtocolAdminInfo/ProtocolLeadOrg/ProtocolSites/OrgSite/OrgSiteID/@cdr:ref'
   AND %s
   AND protstat.path = '/InScopeProtocol/ProtocolAdminInfo/ProtocolLeadOrg/LeadOrgProtocolStatuses/CurrentOrgStatus/StatusName'
   AND (protstat.value = 'Active' OR
        protstat.value = 'Approved-not yet active' OR
        protstat.value = 'Temporarily closed')
 ORDER BY doc.title
""" % protOrgMatchStr

        # Else restrict them by a particular lead organization
        else:
            qry = """
SELECT DISTINCT doc.id, doc.title FROM document doc
  JOIN query_term protorg
    ON protorg.doc_id = doc.id
  JOIN query_term protstat
    ON protstat.doc_id = doc.id
  JOIN query_term leadorg
    ON leadorg.doc_id = doc.id
 WHERE protorg.path =
   '/InScopeProtocol/ProtocolAdminInfo/ProtocolLeadOrg/ProtocolSites/OrgSite/OrgSiteID/@cdr:ref'
   AND %s
   AND protstat.path = '/InScopeProtocol/ProtocolAdminInfo/ProtocolLeadOrg/LeadOrgProtocolStatuses/CurrentOrgStatus/StatusName'
   AND (protstat.value = 'Active' OR
        protstat.value = 'Approved-not yet active' OR
        protstat.value = 'Temporarily closed')
   AND leadorg.path = '/InScopeProtocol/ProtocolAdminInfo/ProtocolLeadOrg/LeadOrganizationID/@cdr:ref'
   AND leadorg.value = '%s'
   AND LEFT(leadorg.node_loc, 8) = LEFT(protorg.node_loc, 8)
 ORDER BY doc.title
""" % (protOrgMatchStr, self.ssVars['restrId'])

        # Call a common routine to get the rows corresponding to the query
        return _execQry (qry)

    def getFilterInfo (self, filterVer):
        """
        See class GlblChg.getFilterInfo for description.
        """
        if not self.description:
            self.description = "global change of org link %s to %s on %s" % \
                               (self.ssVars['fromId'],
                                self.ssVars['toId'],
                                time.ctime (time.time()))
        # Only one pass
        if not self.chkFiltered (filterVer):
            filterName    = ['name:Global Change: Organization Link']
            parms         = []
            parms.append (['changeFrom', self.ssVars['fromId']])
            parms.append (['changeTo', self.ssVars['toId']])

            return (filterName, parms, True)

        return None


#------------------------------------------------------------
# Organization status global change object
#------------------------------------------------------------
class OrgStatusChg (GlblChg):

    # There is no location picklist.  All locations are affected.

    def __init__(self):
        GlblChg.__init__(self)

        # Stages for organization status changes
        self.stages = (\
          Stage (
            # Get org id for org whose status will change
            condition = 'not (self.ssVars.has_key("fromId") or ' +
                        'self.ssVars.has_key("fromName"))',
            subr      = self.genInputHtml,
            parms     = ('Organization', 'Change status for which Organization',
                         'from', 0),
            excpMsg   = 'Generating form for getting the "from" organization'),
          Stage (
            # If name entered rather than ID, resolve it
            condition = 'self.ssVars.has_key("fromName") and ' +
                        'not self.ssVars.has_key("fromId")',
            subr      = self.getPickList,
            parms     = ('Organization', 'eval:self.ssVars["fromName"]',
                         'Choose Organization to be changed', 'fromId'),
            excpMsg   = 'Generating picklist for name'),
          Stage (
            # Verify that this is an org document, not something else
            condition = 'self.ssVars.has_key("fromId") and ' +
                        'not self.ssVars.has_key("fromTitle")',
            subr      = self.verifyId,
            parms     = ('eval:self.ssVars["fromId"]', 'Organization',
                         'fromTitle'),
            excpMsg   = 'Verifying document type for "from" organization"'),
          Stage (
            # Get statuses to change to and from
            condition = 'not (self.ssVars.has_key("fromStatusName") or ' +
                        'self.ssVars.has_key("toStatusName"))',
            subr      = self.getFromToStatus,
            parms     = [],
            excpMsg   = 'Generating form for from/to status'),
          Stage (
            # Optional restriction by lead organization
            condition = None,
            subr      = self.genInputHtml,
            parms     = ('Organization',
                         'Restrict change to protocols with particular ' +\
                         'lead org, or leave blank',
                         'restr', 1),
            excpMsg   = 'Generating form for getting the restriction org'),
          Stage (
            # If name entered rather than ID, resolve it
            condition = 'self.ssVars.has_key("restrName") and ' +
                        'not self.ssVars.has_key("restrId")',
            subr      = self.getPickList,
            parms     = ('Organization', 'eval:self.ssVars["restrName"]',
                         'Choose lead organization to restrict change to',
                         'restrId'),
            excpMsg   = 'Generating picklist for restricting org name'),
          Stage (
            # Verify that restricting doc is an Organization
            condition = 'self.ssVars.has_key("restrId") and '
                        'not self.ssVars.has_key("restrTitle")',
            subr      = self.verifyId,
            parms     = ('eval:self.ssVars["restrId"]', 'Organization',
                         'restrTitle'),
            excpMsg   = 'Verifying document type for restricting org'),
          Stage (
            # If restricting org entered, ask for optional principal
            #   investigator (PI)
            condition = 'self.ssVars.has_key("restrId") and ' +
                        'not self.ssVars.has_key("restrPiId")',
            subr      = self.genInputHtml,
            parms     = ('Person',
          'Restrict changes to protocols with org site Principal Investigator',
                         'restrPi', 1),
            excpMsg   = 'Generating form for getting the "from" person'),
          Stage (
            # If name entered rather than ID, resolve it
            condition = 'self.ssVars.has_key("restrPiName") and ' +
                        'not self.ssVars.has_key("restrPiId")',
            subr      = self.getPickList,
            parms     = ('Person', 'eval:self.ssVars["restrPiName"]',
                         'Choose Principal Investigator', 'restrPiId'),
            excpMsg   = 'Generating picklist for restrPi name'),
          Stage (
            # Verify that this is a Person document, not something else
            condition = 'self.ssVars.has_key("restrPiId") and ' +
                        'not self.ssVars.has_key("restrPiTitle")',
            subr      = self.verifyId,
            parms     = ('eval:self.ssVars["restrPiId"]', 'Person',
                         'restrPiTitle'),
            excpMsg   = 'Verifying document type for "restrPi" person"'),

          # Returns to standard processing here
        )

    # The selection queries are virtually identical to those in OrgChg
    # The only difference is that we ignore the fragment id at the end
    #   of the OrgSiteID/@cdr:ref
    # Unfortunately that requires a restatement of the queries
    def selDocs (self):
        """
        See PersonChg.selDocs()
        """

        # fromId looks like "CDR0000001234". OrgSiteID is "CDR0000001234#F1".
        # We only need the CDR id.  We don't care about the fragment.
        # The query for organizations ran 46 times faster (!) when I
        #   searched for:
        #        protorg.int_val = 1234
        #   than for:
        #        protorg.value LIKE 'CDR0000001234%'
        # So we get the integer value here
        fromIdNum = cdr.exNormalize (self.ssVars['fromId'])[1]

        # If no restrictions
        if not self.ssVars.has_key ('restrId'):
            qry = """
SELECT DISTINCT doc.id, doc.title FROM document doc
  JOIN query_term protorg
    ON protorg.doc_id = doc.id
  JOIN query_term protstat
    ON protstat.doc_id = doc.id
  JOIN query_term orgstat
    ON orgstat.doc_id = doc.id
 WHERE protorg.path =
   '/InScopeProtocol/ProtocolAdminInfo/ProtocolLeadOrg/ProtocolSites/OrgSite/OrgSiteID/@cdr:ref'
   AND protorg.int_val = %d
   AND protstat.path = '/InScopeProtocol/ProtocolAdminInfo/ProtocolLeadOrg/LeadOrgProtocolStatuses/CurrentOrgStatus/StatusName'
   AND (protstat.value = 'Active' OR
        protstat.value = 'Approved-not yet active' OR
        protstat.value = 'Temporarily closed')
   AND orgstat.path =
   '/InScopeProtocol/ProtocolAdminInfo/ProtocolLeadOrg/ProtocolSites/OrgSite/OrgSiteStatus'
   AND orgstat.value = '%s'
   AND LEFT (orgstat.node_loc, 16) = LEFT (protorg.node_loc, 16)
 ORDER BY doc.title
""" % (fromIdNum,
       self.ssVars['fromStatusName'])

        # Else restrict them by a particular lead organization
        elif not self.ssVars.has_key ('restrPiId'):
            qry = """
SELECT DISTINCT doc.id, doc.title FROM document doc
  JOIN query_term protorg
    ON protorg.doc_id = doc.id
  JOIN query_term protstat
    ON protstat.doc_id = doc.id
  JOIN query_term orgstat
    ON orgstat.doc_id = doc.id
  JOIN query_term leadorg
    ON leadorg.doc_id = doc.id
 WHERE protorg.path =
   '/InScopeProtocol/ProtocolAdminInfo/ProtocolLeadOrg/ProtocolSites/OrgSite/OrgSiteID/@cdr:ref'
   AND protorg.int_val = %d
   AND protstat.path = '/InScopeProtocol/ProtocolAdminInfo/ProtocolLeadOrg/LeadOrgProtocolStatuses/CurrentOrgStatus/StatusName'
   AND (protstat.value = 'Active' OR
        protstat.value = 'Approved-not yet active' OR
        protstat.value = 'Temporarily closed')
   AND leadorg.path = '/InScopeProtocol/ProtocolAdminInfo/ProtocolLeadOrg/LeadOrganizationID/@cdr:ref'
   AND orgstat.path =
   '/InScopeProtocol/ProtocolAdminInfo/ProtocolLeadOrg/ProtocolSites/OrgSite/OrgSiteStatus'
   AND orgstat.value = '%s'
   AND LEFT (orgstat.node_loc, 16) = LEFT (protorg.node_loc, 16)
   AND leadorg.value = '%s'
 ORDER BY doc.title
""" % (fromIdNum,
       self.ssVars['fromStatusName'],
       self.ssVars['restrId'])

        # Else restrict them by a lead organization AND specific person
        #   at that protocol site
        else:
            qry = """
SELECT DISTINCT doc.id, doc.title FROM document doc
  JOIN query_term protorg
    ON protorg.doc_id = doc.id
  JOIN query_term protstat
    ON protstat.doc_id = doc.id
  JOIN query_term orgstat
    ON orgstat.doc_id = doc.id
  JOIN query_term leadorg
    ON leadorg.doc_id = doc.id
  JOIN query_term protpers
    ON protpers.doc_id = doc.id
 WHERE protorg.path =
   '/InScopeProtocol/ProtocolAdminInfo/ProtocolLeadOrg/ProtocolSites/OrgSite/OrgSiteID/@cdr:ref'
   AND protorg.int_val = %d
   AND protstat.path = '/InScopeProtocol/ProtocolAdminInfo/ProtocolLeadOrg/LeadOrgProtocolStatuses/CurrentOrgStatus/StatusName'
   AND (protstat.value = 'Active' OR
        protstat.value = 'Approved-not yet active' OR
        protstat.value = 'Temporarily closed')
   AND leadorg.path = '/InScopeProtocol/ProtocolAdminInfo/ProtocolLeadOrg/LeadOrganizationID/@cdr:ref'
   AND leadorg.value = '%s'
   AND orgstat.path =
   '/InScopeProtocol/ProtocolAdminInfo/ProtocolLeadOrg/ProtocolSites/OrgSite/OrgSiteStatus'
   AND orgstat.value = '%s'
   AND LEFT (orgstat.node_loc, 16) = LEFT (protorg.node_loc, 16)
   AND LEFT (orgstat.node_loc, 8) = LEFT (leadorg.node_loc, 8)
   AND protpers.path =
   '/InScopeProtocol/ProtocolAdminInfo/ProtocolLeadOrg/ProtocolSites/OrgSite/OrgSiteContact/SpecificPerson/Person/@cdr:ref'
   AND protpers.int_val = %s
   AND LEFT (orgstat.node_loc, 16) = LEFT (protpers.node_loc, 16)
 ORDER BY doc.title
""" % (fromIdNum,
       self.ssVars['restrId'],
       self.ssVars['fromStatusName'],
       cdr.exNormalize(self.ssVars['restrPiId'])[1])

        # Call a common routine to get the rows corresponding to the query
        return _execQry (qry)


    def getFilterInfo (self, filterVer):
        """
        See class GlblChg.getFilterInfo for description.
        """
        if not self.description:
            self.description = "global status change from %s to %s on %s" % \
                              (self.ssVars['fromStatusName'],
                               self.ssVars['toStatusName'],
                                time.ctime (time.time()))

        if self.filtered[filterVer] == 0:
            self.filtered[filterVer] += 1;

            # First pass performs the global change
            cdr.logwrite ("will return filter parms", LF)
            filterName    = ['name:Global Change: Org Status']
            parms         = []
            parms.append (['orgId', self.ssVars['fromId']])
            parms.append (['oldStatus', self.ssVars['fromStatusName']])
            parms.append (['newStatus', self.ssVars['toStatusName']])
            if self.ssVars.has_key ('restrId'):
                parms.append (['leadOrgId', self.ssVars['restrId']])
                if self.ssVars.has_key ('restrPiId'):
                    parms.append (['personId', self.ssVars['restrPiId']])

            return (filterName, parms, True)

        elif self.filtered[filterVer] == 1:
            self.filtered[filterVer] += 1;

            # Next pass checks for and updates DateLastModified
            return (\
              ['name:Global Change: Manage InScopeProtocol DateLastModified'],
              [['today', TODAY],], True)

        return None


#------------------------------------------------------------
# Insert a new site into protocols with a particular lead org
#------------------------------------------------------------
class InsertOrgChg (GlblChg):

    # Picklist filter
    locFilter = ['name:Person Locations Picklist']

    def __init__(self):
        GlblChg.__init__(self)

        # Stages for inserting an organization
        self.stages = (\
          Stage (
            # Get org id for org to be inserted
            condition = 'not (self.ssVars.has_key("chgOrgId") or ' +
                        'self.ssVars.has_key("chgOrgName"))',
            subr      = self.genInputHtml,
            parms     = ('Organization', 'Insert Organization Link',
                         'chgOrg', 0),
            excpMsg   = 'Generating form for getting "insert" organization'),
          Stage (
            # If name entered rather than ID, resolve it
            condition = 'self.ssVars.has_key("chgOrgName") and ' +
                        'not self.ssVars.has_key("chgOrgId")',
            subr      = self.getPickList,
            parms     = ('Organization', 'eval:self.ssVars["chgOrgName"]',
                         'Choose Organization to be inserted', 'chgOrgId'),
            excpMsg   = 'Generating picklist for name'),
          Stage (
            # Verify that this is an org document, not something else
            condition = 'self.ssVars.has_key("chgOrgId") and ' +
                        'not self.ssVars.has_key("chgOrgTitle")',
            subr      = self.verifyId,
            parms     = ('eval:self.ssVars["chgOrgId"]', 'Organization',
                         'chgOrgTitle'),
            excpMsg   = 'Verifying document type for inserted organization"'),
          Stage (
            # Get coop group affiliation for this org, if not tried yet
            condition = 'not (self.ssVars.has_key("coopType") or ' +
                        'self.ssVars.has_key("coopChk"))',
            subr      = self.getCoopAttribute,
            parms     = [],
            excpMsg   = 'Getting coop affiliation'),
          Stage (
            # Generate form for contact person to insert with org
            condition = 'not (self.ssVars.has_key("insertPersId") or ' +
                        'self.ssVars.has_key("insertPersName"))',
            subr      = self.genInputHtml,
            parms     = ('Person', 'Insert Person Link', 'insertPers', 0),
            excpMsg   = 'Generating form for getting the "insertPers" person'),
          Stage (
            # If name entered rather than ID, resolve it
            condition = 'self.ssVars.has_key("insertPersName") and ' +
                        'not self.ssVars.has_key("insertPersId")',
            subr      = self.getPickList,
            parms     = ('Person', 'eval:self.ssVars["insertPersName"]',
                         'Choose Person to be changed', 'insertPersId'),
            excpMsg   = 'Generating picklist for name'),
          Stage (
            # Verify that this is a Person document, not something else
            condition = 'self.ssVars.has_key("insertPersId") and ' +
                        'not self.ssVars.has_key("insertPersTitle")',
            subr      = self.verifyId,
            parms     = ('eval:self.ssVars["insertPersId"]', 'Person',
                         'insertPersTitle'),
            excpMsg   = 'Verifying document type for "insertPers" person"'),
          Stage (
            # If no fragment id entered, must have one
            condition = 'self.ssVars.has_key("insertPersId") and ' +
                        'string.find (self.ssVars["insertPersId"], "#") == -1',
            subr      = self.genFragPickListHtml,
            parms     = ('eval:self.ssVars["insertPersId"]', 'insertPers', 0),
            excpMsg   = 'Generating fragment picklist for "insertPers" person'),
          Stage (
            # Get specific phone and role, at least role is required
            condition = 'not self.ssVars.has_key("specificRole")',
            subr      = self.getPersonSpecifics,
            parms     = [],
            excpMsg   = "Requesting specific phone and role"),
          Stage (
            # Required restriction by lead organization
            condition = None,
            subr      = self.genInputHtml,
            parms     = ('Organization',
                         'Restrict change to protocols with this ' +\
                         'lead org (required)',
                         'restr', 1),
            excpMsg   = 'Generating form for insert restriction org'),
          Stage (
            # If name entered rather than ID, resolve it
            condition = 'self.ssVars.has_key("restrName") and ' +
                        'not self.ssVars.has_key("restrId")',
            subr      = self.getPickList,
            parms     = ('Organization', 'eval:self.ssVars["restrName"]',
                         'Choose lead organization to restrict change to',
                         'restrId'),
            excpMsg   = 'Generating picklist for restricting org name'),
          Stage (
            # Verify that restricting doc is an Organization
            condition = 'self.ssVars.has_key("restrId") and '
                        'not self.ssVars.has_key("restrTitle")',
            subr      = self.verifyId,
            parms     = ('eval:self.ssVars["restrId"]', 'Organization',
                         'restrTitle'),
            excpMsg   = 'Verifying document type for restricting org'),

          # Returns to standard processing here
        )

    # The selection query looks for all orgs with one of the statuses
    #   that participate in global change, and that has the requested
    #   lead org.
    def selDocs (self):
        """
        See PersonChg.selDocs()
        """

        qry = """
SELECT DISTINCT doc.id, doc.title FROM document doc
  JOIN query_term protstat
    ON protstat.doc_id = doc.id
  JOIN query_term leadorg
    ON leadorg.doc_id = doc.id
 WHERE protstat.path = '/InScopeProtocol/ProtocolAdminInfo/ProtocolLeadOrg/LeadOrgProtocolStatuses/CurrentOrgStatus/StatusName'
   AND (protstat.value = 'Active' OR
        protstat.value = 'Approved-not yet active' OR
        protstat.value = 'Temporarily closed')
   AND leadorg.path = '/InScopeProtocol/ProtocolAdminInfo/ProtocolLeadOrg/LeadOrganizationID/@cdr:ref'
   AND leadorg.int_val = '%d'
 ORDER BY doc.title
""" % (cdr.exNormalize(self.ssVars['restrId'])[1])

        # Call a common routine to get the rows corresponding to the query
        return _execQry (qry)


    def getFilterInfo (self, filterVer):
        """
        See class GlblChg.getFilterInfo for description.
        """
        if not self.description:
            self.description = "global insert of orgsite %s on %s" % \
                              (self.ssVars['chgOrgId'],
                                time.ctime (time.time()))

        if self.filtered[filterVer] == 0:
            self.filtered[filterVer] += 1;

            # First pass inserts the new org
            filterName    = ['name:Global Change: Insert Participating Org']
            parms         = []
            parms.append (['leadOrgID', self.ssVars['restrId']])
            parms.append (['newOrgSiteID', self.ssVars['chgOrgId']])
            parms.append (['newPersonID', self.ssVars['insertPersId']])
            if self.ssVars.has_key ('coopType'):
                parms.append (['coop', self.ssVars['coopType']])
            if self.ssVars.has_key ('specificRole'):
                parms.append (['setRole', self.ssVars['specificRole']])
            if self.ssVars.has_key ('specificPhone'):
                parms.append (['setSpecificPhone',self.ssVars['specificPhone']])

            return (filterName, parms, True)

        elif self.filtered[filterVer] == 1:
            self.filtered[filterVer] += 1;

            # Next pass checks for and updates DateLastModified
            return (\
              ['name:Global Change: Manage InScopeProtocol DateLastModified'],
              [['today', TODAY],], True)

        return None


#------------------------------------------------------------
# Delete an organization site from protocols
#------------------------------------------------------------
class DeleteOrgChg (GlblChg):

    # Picklist filter not needed?

    def __init__(self):
        GlblChg.__init__(self)

        # Stages for deleting an organization site
        self.stages = (\
          Stage (
            # Get legal status values
            condition = 'not self.ssVars.has_key("statusName")',
            subr      = self.getStatusVals,
            parms     = ('Select status values for org sites to delete',
                         'statusName'),
            excpMsg   = 'Generating form to get status values for protocols'),
          Stage (
            # Get org id for org to be deleted
            condition = 'not (self.ssVars.has_key("delOrgId") or ' +
                        'self.ssVars.has_key("delOrgName"))',
            subr      = self.genInputHtml,
            parms     = ('Organization', 'Delete participating site',
                         'delOrg', 0),
            excpMsg   = 'Generating form for getting "delete" organization'),
          Stage (
            # If name entered rather than ID, resolve it
            condition = 'self.ssVars.has_key("delOrgName") and ' +
                        'not self.ssVars.has_key("delOrgId")',
            subr      = self.getPickList,
            parms     = ('Organization', 'eval:self.ssVars["delOrgName"]',
                         'Choose Organization to be deleted', 'delOrgId'),
            excpMsg   = 'Generating picklist for name'),
          Stage (
            # Verify that this is an org document, not something else
            condition = 'self.ssVars.has_key("delOrgId") and ' +
                        'not self.ssVars.has_key("delOrgTitle")',
            subr      = self.verifyId,
            parms     = ('eval:self.ssVars["delOrgId"]', 'Organization',
                         'delOrgTitle'),
            excpMsg   = 'Verifying document type for deleted organization"'),
          Stage (
            # Ask for optional principal investigator (PI)
            condition = 'not self.ssVars.has_key("restrPiId") and ' +
                        'not self.ssVars.has_key("restrPiName")',
            subr      = self.genInputHtml,
            parms     = ('Person',
                         'Restrict change to sites with specific PI,' +
                         ' or leave blank', 'restrPi', 1),
            excpMsg   = 'Generating form for getting the PI person'),
          Stage (
            # If name entered rather than ID, resolve it
            condition = 'self.ssVars.has_key("restrPiName") and ' +
                        'not self.ssVars.has_key("restrPiId")',
            subr      = self.getPickList,
            parms     = ('Person', 'eval:self.ssVars["restrPiName"]',
                         'Choose Principal Investigator', 'restrPiId'),
            excpMsg   = 'Generating picklist for deletion restrPi name'),
          Stage (
            # Verify that this is a Person document, not something else
            condition = 'self.ssVars.has_key("restrPiId") and ' +
                        'not self.ssVars.has_key("restrPiTitle")',
            subr      = self.verifyId,
            parms     = ('eval:self.ssVars["restrPiId"]', 'Person',
                         'restrPiTitle'),
            excpMsg   = 'Verifying doctype for deletion "restrPi" person"'),
          Stage (
            # Optional restriction by lead organization
            # genInputHtml() knows how to ask for optional info only once
            condition = None,
            subr      = self.genInputHtml,
            parms     = ('Organization',
                         'Restrict deletions to protocols with particular ' +\
                         'lead org, or leave blank', 'restr', 1),
            excpMsg   = 'Generating form for getting the del restriction org'),
          Stage (
            # If name entered rather than ID, resolve it
            condition = 'self.ssVars.has_key("restrName") and ' +
                        'not self.ssVars.has_key("restrId")',
            subr      = self.getPickList,
            parms     = ('Organization', 'eval:self.ssVars["restrName"]',
                         'Restrict change to protocols with this lead org,' +
                         ' or leave blank', 'restrId'),
            excpMsg   = 'Generating picklist for del restricting org name'),
          Stage (
            # Verify that restricting doc is an Organization
            condition = 'self.ssVars.has_key("restrId") and '
                        'not self.ssVars.has_key("restrTitle")',
            subr      = self.verifyId,
            parms     = ('eval:self.ssVars["restrId"]', 'Organization',
                         'restrTitle'),
            excpMsg   = 'Verifying document type for del restricting org'),
          # Returns to standard processing here
        )

    def selDocs (self):
        """
        See PersonChg.selDocs()
        """
        # There many options and a potentially very complex selection
        #   strategy
        # To simplify and speed selections, we do them in stages, using
        #   temporary tables to hold intermediate results

        # First stage finds all protocols with the organization site we
        #   want to delete

        # Construct base query, we'll add clauses as needed
        selQry = """
SELECT DISTINCT doc.id, doc.title FROM document doc
  JOIN query_term protorg
    ON protorg.doc_id = doc.id
"""
        # Will add to select and where clauses separately
        whereQry = """
 WHERE protorg.path = '/InScopeProtocol/ProtocolAdminInfo/ProtocolLeadOrg/ProtocolSites/OrgSite/OrgSiteID/@cdr:ref'
   AND protorg.int_val = %s""" % cdr.exNormalize(self.ssVars['delOrgId'])[1]

        # If restricting by status
        if self.ssVars.has_key('statusName'):

            # List of one or more statuses
            statVals = str(self.ssVars['statusName'])
            if (statVals[0] == '['):
                statVals = statVals[1:-1]
            else:
                statVals = "'%s'" % statVals
            cdr.logwrite("Type of statusNames=%s" % str(type(statVals)), LF)
            cdr.logwrite('Status values="%s"' % statVals, LF)

            # Query
            selQry += """
  JOIN query_term protstat
    ON protstat.doc_id = doc.id"""
            whereQry += """
   AND protstat.path='/InScopeProtocol/ProtocolAdminInfo/CurrentProtocolStatus'
   AND protstat.value IN (%s)""" % statVals

        # Restriction by lead org XXX Check this ssVar is right
        if self.ssVars.has_key('restrId'):
            selQry += """
  JOIN query_term leadorg
    ON leadorg.doc_id = doc.id"""
            whereQry += """
   AND leadorg.path = '/InScopeProtocol/ProtocolAdminInfo/ProtocolLeadOrg/LeadOrganizationID/@cdr:ref'
   AND leadorg.value = '%s'
   AND LEFT(leadorg.node_loc, 8) = LEFT(protorg.node_loc, 8)""" % \
            self.ssVars['restrId']

        # Restriction by principal investigator
        if self.ssVars.has_key('restrPiId'):
            selQry += """
  JOIN query_term protpi
    ON protpi.doc_id = doc.id
  JOIN query_term pirole
    ON pirole.doc_id = doc.id"""
            whereQry += """
   AND protpi.path = '/InScopeProtocol/ProtocolAdminInfo/ProtocolLeadOrg/ProtocolSites/OrgSite/OrgSiteContact/SpecificPerson/Person/@cdr:ref'
   AND protpi.int_val = %d
   AND LEFT(protorg.node_loc, 16) = LEFT(protpi.node_loc, 16)
   AND pirole.path = '/InScopeProtocol/ProtocolAdminInfo/ProtocolLeadOrg/ProtocolSites/OrgSite/OrgSiteContact/SpecificPerson/Role'
   AND pirole.value = 'Principal investigator'
   AND LEFT(protpi.node_loc, 16) = LEFT(pirole.node_loc, 16)""" % \
            cdr.exNormalize(self.ssVars['restrPiId'])[1]

        qry = selQry + whereQry

        # XXX DEBUG
        cdr.logwrite ("DelOrgQry====%s====" % qry, LF)

        # Execute
        return _execQry (qry)

    def getFilterInfo (self, filterVer):
        """
        See class GlblChg.getFilterInfo for description.
        """
        if not self.description:
            self.description = "global change deleting OrgSite %s at %s" % \
                               (self.ssVars['delOrgId'],
                                time.ctime (time.time()))

        if self.filtered[filterVer] == 0:
            self.filtered[filterVer] += 1;

            # First pass deletes org
            filterName    = ['name:Global Change: Delete Participating Org']
            parms         = []

            # Setup parameters
            parms.append (['orgSiteId', self.ssVars['delOrgId']])
            if self.ssVars.has_key('restrId'):
                parms.append (['leadOrgId', self.ssVars['restrId']])
            if self.ssVars.has_key('restrPiId'):
                parms.append (['piId', self.ssVars['restrPiId']])

            return (filterName, parms, True)

        elif self.filtered[filterVer] == 1:
            self.filtered[filterVer] += 1;

            # Next pass checks for and updates DateLastModified
            return (\
              ['name:Global Change: Manage InScopeProtocol DateLastModified'],
              [['today', TODAY],], True)

        return None

#------------------------------------------------------------
# Modify terminology in a protocol
#------------------------------------------------------------
class TermChg (GlblChg):

    # For testing Python logic
    chgFilter = ['name:Passthrough Filter']

    def __init__(self):
        GlblChg.__init__(self)

        # Let's stop right at the beginning if the Add Term filter is
        # out of sync with the protocol schemas.
        # This will blow up before the batch job starts
        # Note: The filter includes some elements for both doc types, when
        #       only one of them has that element.  However that does no
        #       because the only danger we need to guard against is an
        #       element in the document that is not in filter and will
        #       fail to be copied from input to output.
        self.addFilter = \
                    ["name:Global Change: Add Terminology Link to Protocol"]

        # This is what the schemas looked like on 2011-07-05
        schemaAssumptions = (
         ('InScopeProtocol', 'Eligibility',
            ['HealthyVolunteers', 'LowAge', 'HighAge', 'AgeText', 'Gender',
            'Diagnosis', 'ExclusionCriteria', 'Comment']),
         ('CTGovProtocol', 'Eligibility',
            ['HealthyVolunteers', 'LowAge', 'HighAge', 'AgeText',
            'Diagnosis', 'ExclusionCriteria', 'Comment']),

         ('InScopeProtocol', 'StudyCategory',
            ['StudyCategoryType', 'StudyCategoryName', 'StudyFocus',
            'Intervention', 'Comment']),
         ('CTGovProtocol', 'StudyCategory',
            ['StudyCategoryType', 'StudyCategoryName', 'Intervention']),

         ('InScopeProtocol', 'Intervention',
            ['InterventionType', 'InterventionNameLink', 'ArmOrGroupLink',
            'InterventionDescription']),
         ('CTGovProtocol', 'Intervention',
            ['InterventionType', 'InterventionNameLink']),

         ('InScopeProtocol', 'ProtocolDetail',
            ['StudyType', 'StudyCategory', 'Condition',
            'Gene', 'EnteredBy', 'EntryDate']),
         ('CTGovProtocol', 'PDQIndexing',
            ['StudyType', 'StudyCategory', 'ProtocolDesign', 'Condition',
            'Gene', 'Eligibility', 'EnteredBy', 'EntryDate']),
        )

        # Check all of the assumptions against the filter
        msgs = []
        for data in schemaAssumptions:
            docType = data[0]
            parent  = data[1]
            want    = data[2]
            try:
                checkSchema(docType, parent, self.addFilter, want)
            except Exception as e:
                # Cumulate any errors
                msgs += "\n<hr />%s<hr />\n" % e

        if msgs:
            raise Exception(msgs)

        # Need two dictionaries of all filters already executed
        # One dictionary used for processing CWD, one for PUB version
        # Simple self.filtered boolean isn't always enough because
        #   there may be multiple filters for one global change
        self.doneChgs = [{},{}]

        # Stages for organization status changes
        self.stages = (\
          Stage (
            # Generate the form to get info
            condition = 'not self.haveEnoughTermInfo()',
            subr      = self.genTermInputHtml,
            parms     = [],
            excpMsg   = 'Generating form for getting Terminology'),
          Stage (
            # Convert strings to ids, and verify ids
            condition = 'not self.ssVars.has_key ("termsVerified")',
            subr      = self.verifyTermIds,
            parms     = [],
            excpMsg   = 'Generating form for getting Terminology'),
          # Returns to standard processing here
        )

    def selDocs (self):
        """
        See PersonChg.selDocs()
        """
        ### DEBUG, one CTGov and one InScope doc ###
        # return([648354, 'Test title 1'],
        #        [584262, 'Test title 2'])

        # Pairs of doc id + doc title
        idTitles = []

        # If handling InScopeProtocols, get their ids and titles
        if self.ssVars.has_key(TYPE_CDR_DOC):
            results = self.selOneDocType (TYPE_CDR_DOC)
            if results:
                idTitles += results
        cdr.logwrite("Title count from first selection=%d" % len(idTitles),LF)

        # Same for CTGovProtocols, appending them to InScopeProtocols, if any
        if self.ssVars.has_key(TYPE_CTG_DOC):
            results = self.selOneDocType (TYPE_CTG_DOC)
            if results:
                idTitles += results
        cdr.logwrite("Title count after 2nd selection=%d" % len(idTitles),LF)

        # Return ids and titles, or None if there aren't any
        if len(idTitles):
            return idTitles
        return None

    def selOneDocType (self, docType):
        """
        We select documents for global change up to two times, once
        for each protocol document type.  The caller, selDocs() tells
        us which type to fetch.

        The logic is the same for each selection, but the fully qualified
        names of the fields are different.

        Pass:
            docType - One of the document type constants:
                TYPE_CDR_DOC
                TYPE_CTG_DOC

        Return:
            Sequence of all matching database rows, each containing a
            sequence of:
                document id
                document title
        """
        # Constants for temporary table names
        # These tables exist only for the life of this routine and
        #   disappear when the cursor on which they are created is closed
        TTBL_REQ  = "#gcTermReq"
        TTBL_OPT  = "#gcTermOpt"
        TTBL_NOT  = "#gcTermNot"
        TTBL_DEL  = "#gcTermDel"
        TTBL_STAT = "#gcTermStat"
        TTBL_SCAT = "#gcTermScat"

        # All temporary tables have to use a single connection/cursor
        #  so they are visible to each other
        conn   = None
        cursor = None
        try:
            conn = cdrdb.connect ("cdr")
            cursor = conn.cursor()
            conn.setAutoCommit(1)
        except cdrdb.Error, info:
            raise cdrbatch.BatchException (\
                "Database error creating update cursor: %s" % info[1][0])

        # This flag gets set if we know there are no hits and don't
        #   have to do any more searching
        done = 0

        # Create a temporary table of docs with the status values we want
        statCnt = self.makeTempStatTable (cursor, docType, TTBL_STAT,
                                          self.ssVars[TERM_STATVAL])
        # If none, we're done
        if statCnt == 0:
            done = 1

        # Create a temporary table of docs having all required terms.
        if not done:
            reqCnt = 0
            if self.countSelTypes (TERMREQ, TERM_MAX_CRITERIA) > 0:
                reqCnt = self.makeTempSelTable (cursor, docType, TTBL_REQ,
                                            TERMREQ, TERM_MAX_CRITERIA, "AND")
                # If no docs with required terms, we're done
                if reqCnt == 0:
                    done = 1

        if not done:
            optCnt = 0
            if self.countSelTypes (TERMOPT, TERM_MAX_CRITERIA) > 0:
                optCnt = self.makeTempSelTable (cursor, docType, TTBL_OPT,
                                            TERMOPT, TERM_MAX_CRITERIA, "OR")
                # If optional terms specified, at least one must be in a doc
                if optCnt == 0:
                    done = 1

        # If only one table retrieved, take that one table as our output
        if not done:
            if reqCnt == 0:
                resultTbl = TTBL_OPT
            elif optCnt == 0:
                resultTbl = TTBL_REQ

            # Else something in both, get the intersection of the two tables
            else:
                resCnt0 = _execUpdate (\
                 """SELECT doc_id INTO %s FROM %s
                     WHERE doc_id IN (SELECT doc_id FROM %s)"""% \
                  (TTBL_RES0, TTBL_REQ, TTBL_OPT), args=None, cursor=cursor)

                # Might not be any
                if resCnt0 == 0:
                    done = 1

                # The intersection is now our temp results table
                resultTbl = TTBL_RES0

        # We have some protocols with our status and some with our terms
        # Find intersection, if any
        if not done:
            resCnt1 = _execUpdate (\
             """SELECT doc_id INTO %s FROM %s
                 WHERE doc_id IN (SELECT doc_id FROM %s)"""% \
              (TTBL_RES1, TTBL_STAT, resultTbl), args=None, cursor=cursor)

            # Might not be any
            if resCnt1 == 0:
                done = 1

            # The intersection is now our temp results table
            resultTbl = TTBL_RES1

        # Check for terms that must NOT be in the documents
        if not done:
            notCnt = 0
            if self.countSelTypes (TERMNOT, TERM_MAX_CRITERIA) > 0:
                 notCnt = self.makeTempSelTable (cursor, docType, TTBL_NOT,
                                            TERMNOT, TERM_MAX_CRITERIA, "OR")

            # If any found, discard intersection of these and previous results
            if notCnt > 0:
                resCnt2 = _execUpdate (\
              """SELECT doc_id INTO %s FROM %s
                  WHERE doc_id NOT IN (SELECT doc_id FROM %s)""" % \
                   (TTBL_RES2, resultTbl, TTBL_NOT), args=None, cursor=cursor)

                # Might not be any
                if resCnt2 == 0:
                    done = 1

                # The intersection is now our temp results table
                resultTbl = TTBL_RES2

        # Restrict to docs with a particular StudyCategoryName, if specified
        if not done:
            if self.ssVars.has_key (TERM_SCAT_VAL):

                # Searching existing results for docs with StudyCategoryName
                path = "/InScopeProtocol/ProtocolDetail"
                if docType == TYPE_CTG_DOC:
                    path = '/CTGovProtocol/PDQIndexing'
                sCatCnt = _execUpdate ("""
SELECT doc_id INTO %s
 FROM query_term
WHERE path='%s/StudyCategory/StudyCategoryName'
  AND value = '%s'
  AND doc_id IN (SELECT doc_id FROM %s)
  """ % (TTBL_SCAT, path, self.ssVars[TERM_SCAT_VAL], resultTbl), cursor=cursor)

                # If none, we're done
                if sCatCnt == 0:
                    done = 1

                # Point to new results
                resultTbl = TTBL_SCAT

        # One last selection
        # If we're deleting terms and not adding them, the selected set
        #   must have at least one of the deleted terms to be worth
        #   processing.
        # If we are adding some, then we might add even if the term(s) to
        #   delete are not present.
        if not done:
            if self.countSelTypes (TERMDEL, TERM_MAX_CHANGES) > 0:
                if self.countSelTypes (TERMADD, TERM_MAX_CHANGES) == 0:
                    delCnt = self.makeTempSelTable (cursor, docType, TTBL_DEL,
                                            TERMDEL, TERM_MAX_CHANGES, "OR")
                    if delCnt == 0:
                        return None

                    resCnt3 = _execUpdate (\
                        """SELECT doc_id INTO %s FROM %s
                            WHERE doc_id IN (SELECT doc_id FROM %s)""" % \
                       (TTBL_RES3, resultTbl, TTBL_DEL), args=None,
                       cursor=cursor)

                    if resCnt3 == 0:
                        done = 1

                    resultTbl = TTBL_RES3

            # XXXX I could do the same thing in reverse for add terms.
            #  If there are no delete terms, then could make sure that a
            #   doc does not already contain all the add terms.
            #  Could even check to be sure that an update will be possible
            #   even if both add and delete are specified.
            # Or maybe forget all this.  It reduces the appearance of unchanged
            #  docs in the report, but it's basically an optimization.  User
            #  can achieve the same thing by formulating different queries. XXXX


        # If we got this far, there are some docs that match our
        #   (possibly complex) search criteria
        # Return a query that retrieves the doc ids and titles
        if not done:
            rows = _execQry ("""
                SELECT distinct id, title FROM document
                 WHERE id IN (SELECT doc_id FROM %s)
                 ORDER BY title""" % resultTbl, args=None, cursor=cursor)

        # Release the cursor
        try:
            cursor.close()
        except cdrdb.Error, info:
            raise cdrbatch.BatchException (\
                "Database error closing doc selection cursor: %s" % info[1][0])

        # Return data or empty sequence
        if not done:
            return rows
        return []

    def getFilterInfo (self, filterVer):
        """
        See class GlblChg.getFilterInfo for description.

        This is the one that has to do some real work.
        Concept is to keep a dictionary of changes we've already done.
        For each possible change:
            If we haven't done it yet:
                Add it to the dictionary
                Do it (i.e., return the info.)
        Note: If I were redesigning this, I _might_ create a list
              of lists of filter id + parm lists, then done
              all of the iteration in the calling program.
        """
        # Create a description
        if not self.description:
            # Can't easily describe this because lots of changes could
            #   be made by one change.
            self.description = "Global terminology change"
            for termNum in range (TERM_MAX_CHANGES):
                if self.ssVars.has_key ("trmAddId%d" % termNum):
                    self.description += " adding %s=%s" % \
                        (self.ssVars['trmAddField%d' % termNum],
                         self.ssVars['trmAddId%d' % termNum])
            for termNum in range (TERM_MAX_CHANGES):
                if self.ssVars.has_key ("trmDelId%d" % termNum):
                    self.description += " deleting %s=%s" % \
                        (self.ssVars['trmDelField%d' % termNum],
                         self.ssVars['trmDelId%d' % termNum])

            # Add date
            self.description += " on %s" % time.ctime(time.time())

        # Initial values of data to return
        filterName  = None
        parms       = []

        # Search for terms to add
        for termNum in range (TERM_MAX_CHANGES):
            addId = "trmAddId%d" % termNum
            if self.ssVars.has_key (addId):
                keyId = "add" + addId
                if not self.doneChgs[filterVer].has_key (keyId):
                    # Mark this one as done
                    self.doneChgs[filterVer][keyId] = 1

                    # Setup the filter info for adding
                    filterName = self.addFilter
                    trmOption  = self.ssVars['trmAddField%d' % termNum]
                    trmField   = TERM_ELEMENT[trmOption]
                    trmId      = cdr.exNormalize(self.ssVars[addId])[0]
                    parms.append (['addElement', trmField])
                    parms.append (['addTermID', trmId])

                    # Done for now.  Return filter info with any required
                    #   qualifier parms (StudyCategory, InterventionType)
                    return (filterName, self.addQualifierParms(parms), True)

        # If no adds, or done them all, try deletes
        # Initial logic is the same, but filter setup is different
        for termNum in range (TERM_MAX_CHANGES):
            delId = "trmDelId%d" % termNum
            if self.ssVars.has_key (delId):
                keyId = "del" + delId
                if not self.doneChgs[filterVer].has_key (keyId):
                    # Mark this one as done
                    self.doneChgs[filterVer][keyId] = 1

                    # Setup the filter info for deleting
                    filterName  = \
                  ["name:Global Change: Delete Terminology Link from Protocol"]
                    trmOption= self.ssVars['trmDelField%d' % termNum]
                    trmField = TERM_ELEMENT[trmOption]
                    trmId    = cdr.exNormalize(self.ssVars[delId])[0]
                    parms.append (['deleteElement', trmField])
                    parms.append (['deleteTermID', trmId])

                    # Done for now.  Return filter info with any required
                    #   qualifier parms (StudyCategory, InterventionType)
                    return (filterName, self.addQualifierParms(parms), True)

        if not self.doneChgs[filterVer].has_key ("DateLastModified"):
            self.doneChgs[filterVer]["DateLastModified"] = 1

            # Update DateLastModified
            return (\
              ['name:Global Change: Manage InScopeProtocol DateLastModified'],
              [['today', TODAY],], True)

        # Finally, run the filter that checks for missing
        #   InterventionNameLink elements
        if not self.doneChgs[filterVer].has_key ("INLCheck"):
            # Mark this one as done
            self.doneChgs[filterVer]["INLCheck"] = 1
            filterName = \
                ["name:Global Change: Check for Missing Terminology Elements"]
            # Output of this one should not be stored (3rd arg indicates this)
            return (filterName, parms, False)

        return None

    def addQualifierParms (self, parms):
        """
        Add StudyCategory and/or InterventionType to filter parameter list.
        Subroutine of getFilterInfo().

        Pass
            Filter parameter list.
        Return
            Filter parameter list, possibly with more parameters added.
        """
        # Only add qualifier parameters if they're present
        if self.ssVars.has_key (TERM_SCAT_VAL):
            parms.append (['studyCategory', self.ssVars[TERM_SCAT_VAL]])
        if self.ssVars.has_key (TERM_INTV_ID):
            parms.append (['interventionType',
                 cdr.exNormalize(self.ssVars[TERM_INTV_ID])[0]])

        return parms

    def countSelTypes (self, termUse, maxCnt):
        """
        Count the number of active terms entered by the user for a given
        purpose.

        Pass:
            termUse - One of the TERM_USE constants - Req, Opt, etc.
            maxCnt  - Max allowed of this type TERM_MAX_CRITERIA, or
                      TERM_MAX_CHANGES.
        Return:
            Count of term ids entered for this usage.  May be 0.
        """
        count = 0
        for i in range (maxCnt):
            if self.ssVars.has_key ("trm%sId%d" % (termUse, i)):
                count += 1
        return count

    def makeTempStatTable (self, cursor, docType, tblName, status):
        """
        Create a temporary table of doc ids of protocols having any
        of the requested current status values.

        Pass:
            cursor  - Database cursor, needed because caller may need
                      multiple tables to be visible to each other.  Using
                      a single cursor for multiple actions can achieve that.
            docType - One of the document type constants:
                        TYPE_CDR_DOC
                        TYPE_CTG_DOC
            tblName - Output goes to this table name.
                      Caller should put '#' or '##' on front, as desired
                        for SQL Server temporary table naming convention.
            status  - Sequence of one or more status values.
        """
        # Path to status field is different for our two document types
        path = '/InScopeProtocol/ProtocolAdminInfo/CurrentProtocolStatus'
        if docType == TYPE_CTG_DOC:
            path = '/CTGovProtocol/OverallStatus'

        # Base query
        qry = \
"""SELECT DISTINCT d.id as doc_id INTO %s FROM document d
     JOIN query_term q ON d.id = q.doc_id
    WHERE q.path = '%s'
      AND (""" % (tblName, path)

        # Debug
        cdr.logwrite ("Type status=%s, str(status)=%s" % (type(status),
                      str(status)), LF)

        # Add all requested status values - may be only one
        if type(status)==type("") or type(status)==type(u""):
            qry += "q.value='%s'" % status

        # Or may be multiples
        else:
            stcounter = 0
            for stat in status:
                qry += "q.value='%s'" % stat
                if stcounter < len(status) - 1:
                    qry += " OR "
                stcounter += 1
        qry += ")"

        # Search the database, returning count of rows created
        return _execUpdate (qry, args=None, cursor=cursor)

    def makeTempSelTable (self, cursor, docType, tblName,
                          termUse, maxCnt, opCode):
        """
        Create a temporary table of doc ids of protcols with a particular
        type of usage - required, optional, or negated.

        Pass:
            cursor  - Database cursor, needed because caller may need
                      multiple tables to be visible to each other.  Using
                      a single cursor for multiple actions can achieve that.
            docType - Which kind of protocol document.  One of:
                        TYPE_CDR_DOC
                        TYPE_CTG_DOC
            tblName - Output goes to this table name.
                      Caller should put '#' or '##' on front, as desired
                        for SQL Server temporary table naming convention.
            termUse - Terminology change constant (Req, Opt, Not, etc.)
            maxCnt  - Max count of terms of this usage.
            opCode  - SQL expression connector ("AND" or "OR").
                      Note: For NOT, we pass OR to make the temp table
                            Then we subtract these from from the other ids
                            as a post process.

        Return:
            Count of records in table, may be 0
        """
        # Lists of names to put in table names and where clauses
        tables = []
        wheres = []
        idNums = []

        # Point to the right set of term XML elements based on doctype
        fieldList = CDR_TERM_FIELDS
        if docType == TYPE_CTG_DOC:
            fieldList = CTG_TERM_FIELDS

        # Fill them in only for term ids actually supplied by the user
        for i in range(maxCnt):
            docId = "trm%sId%d" % (termUse, i)
            if self.ssVars.has_key (docId):
                fieldAbbrev = self.ssVars["trm%sField%d" % (termUse, i)]
                wheres.append (fieldList[fieldAbbrev])
                tables.append ("qt%s%i" % (termUse, i))
                idNums.append (cdr.exNormalize(self.ssVars[docId])[1])

        # Were there any?
        termCount = len (tables)
        if termCount == 0:
            # User didn't select any of these
            return 0

        # Construct the query
        # Selecting from query_term table with each self join
        #   dynamically named in the tables list
        # Select goes into a temporary table
        qry = "SELECT DISTINCT %s.doc_id INTO %s FROM query_term %s\n" % \
              (tables[0], tblName, tables[0])

        # Boolean AND requires self join to any remaining query_terms
        i = 1
        while i<termCount and opCode == 'AND':
            qry += " JOIN query_term %s ON %s.doc_id = %s.doc_id\n" % \
                    (tables[i], tables[0], tables[i])
            i += 1

        # Add qualifcations to make path and value for terms match
        qry += "WHERE "
        for i in range(termCount):
            # Add AND or OR if we're between two independent clauses
            if i > 0 and i < termCount:
                qry += " %s " % opCode

            # If we're joining query_term rows, name the tables differently
            # Else always use the same name (e.g., "qtOpt0")
            if opCode == 'AND':
                tableIdx = i
            else:
                tableIdx = 0

            # Term id must be found in particular XML element
            qry += "\n (%s.path = '%s'\n AND %s.int_val = %d)" % \
                    (tables[tableIdx], wheres[i], tables[tableIdx], idNums[i])
            qry += "\n"

        # Search the database, returning count of rows created
        return _execUpdate (qry, args=None, cursor=cursor)

    def haveEnoughTermInfo (self):
        """
        Check if the user has entered enough criteria to do a
        terminology change.

        Return:
            1 = true  = Yes, he has.
            0 = false = More data entry required.
        """
        # We need at least one status value
        if not self.ssVars.has_key (TERM_STATVAL):
            cdr.logwrite ("No status selected", LF)
            return 0

        # If user adds or deletes an InterventionNameLink, he must
        #   identify the associated InterventionType
        # If changing Intervention, Gene or Condition, must identify
        #   a StudyCategory
        for i in range(TERM_MAX_CHANGES):
            for use in (TERMADD, TERMDEL):
                keyId = "trm%sId%d" % (use, i)
                if self.ssVars.has_key(keyId):
                    keyField = "trm%sField%d" % (use, i)
                    fieldName = self.ssVars[keyField]
                    if fieldName in (TERM_FLD_INTV, TERM_FLD_INTN):
                        if not self.ssVars.has_key(TERM_SCAT_VAL):
                            cdr.logwrite ("No StudyCategory selected", LF)
                            return 0
                    if fieldName == TERM_FLD_INTN:
                        if not self.ssVars.has_key(TERM_INTV_VAL) and \
                           not self.ssVars.has_key(TERM_INTV_ID):
                            cdr.logwrite ("No InterventionType selected", LF)
                            return 0

        # User must have entered at least one search criterion, a term
        #   that must be in the documents to change, and at least one
        #   change criterion, a term that is to be added or deleted.
        if self.haveTermCriterion (TERMREQ) or \
                    self.haveTermCriterion (TERMOPT):
            if self.haveTermCriterion (TERMADD) or \
                        self.haveTermCriterion (TERMDEL):
                return 1
            cdr.logwrite ("No add/delete terms entered", LF)
        cdr.logwrite ("No search criteria entered", LF)
        return 0

    def haveTermCriterion (self, criterion):
        """
        Check whether the session fields contain a variable of the
        requested criterion type.

        Pass:
            criterion - TERMREQ, TERMOPT, etc.

        Return:
            1 = true  = User has entered this term criterion.
            0 = false = User has not.
        """
        # This looks for more than it has to, but so what
        for i in range (TERM_MAX_CRITERIA):
            # Look for ID or string value
            if self.ssVars.has_key ("trm%sId%d" % (criterion, i)):
                return 1
            if self.ssVars.has_key ("trm%sVal%d" % (criterion, i)):
                return 1

        return 0

    def verifyTermIds (self, parms):
        """
        Uses the Stage.subr interface.

        Check every term/id pair.  If one is not valid, get user to fix it
        and resubmit.  Each error or disambiguation causes a return to the
        user.  Only when they are all valid do we go on to the next stage.

        Pass:
            No parms needed.
        """
        cdr.logwrite ("Verifying term IDs", LF)
        # No bugs, no args, no pychecker warning
        if parms != []:
           return FuncReturn (RET_ERROR,
                "Too many parms to getFromToStatus")

        # This is always "Term", but no reason not to know that in just one
        #   place
        docType = self.ssVars["docType"]

        # For each docId and/or value
        for termUse in TERM_USES:
            for termRow in range (TERM_MAX_CRITERIA):
                keyId  = "trm%sId%d" % (termUse, termRow)
                keyVal = "trm%sVal%d" % (termUse, termRow)

                # If the ID exists, validate it
                if self.ssVars.has_key (keyId):
                    termId = self.ssVars[keyId]
                    result = self.verifyId ((termId, docType, keyVal))

                    # If verify failed, result will be an error msg
                    # Bounce back to stage interpreter to give this to user
                    if result.getRetType == RET_ERROR:
                        cdr.logwrite ("Error in verifying ids", LF)
                        return result

                # Else no ID, if there's a value string, disambiguate it
                # Constructs picklist and returns it to user
                # Yet another StudyCategory kludge
                elif self.ssVars.has_key (keyVal) and keyVal != TERM_SCAT_VAL:
                    cdr.logwrite ("About to get picklist", LF)
                    return self.getPickList ((docType, self.ssVars[keyVal],
                                              "Select term string", keyId))

        # If we got this far without returning, all IDs are okay
        # Signal it in the session context so we don't do this again
        self.ssVars["termsVerified"] = 1
        cdr.logwrite ("Finished verification", LF)

        # Return code with no errors or html means continue on
        return FuncReturn (RET_NONE)


#------------------------------------------------------------
# Pychecker warns about string module not used, but it is used
# in an eval expression.  I got tired of seeing the warning and
# so wrote this dummy function that is never called from anywhere.
#------------------------------------------------------------
def silencePycheckerImportStringWarning(s):
    return string.strip(s)