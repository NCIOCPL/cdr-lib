# $Id: cdrglblchg.py,v 1.12 2003-03-27 18:39:30 ameyer Exp $
#
# Common routines and classes for global change scripts.
#
# $Log: not supported by cvs2svn $
# Revision 1.11  2002/11/20 00:46:32  ameyer
# Removed some copious debug logging.
#
# Revision 1.10  2002/11/20 00:42:53  ameyer
# Added ability to select a particular Principal Investigator when a
# particular Lead Org has been selected.
#
# Revision 1.9  2002/10/17 23:13:47  ameyer
# Fixed bug introduced recently that attempted to pass replacement parameters
# where none existed.
#
# Revision 1.8  2002/10/03 19:38:32  ameyer
# Fixed sessionVar stored as integer that should have been string.
# Fixed ugly error message.
#
# Revision 1.7  2002/09/24 23:38:05  ameyer
# Fix problem discovered by Bob in using %s when I needed variable interpolation.
#
# Revision 1.6  2002/08/27 22:45:17  ameyer
# Now allowing user to enter organization ids with or without address
# fragments.
#
# Revision 1.5  2002/08/16 03:16:52  ameyer
# Replaced 'rmk' userid with CdrGuest.
# Added publishable version check and report in the will change report.
# Changed some cosmetics.
#
# Revision 1.4  2002/08/13 21:16:12  ameyer
# Finished the third type of global change.
# Ready for production unless further testing reveals some problem.
#
# Revision 1.3  2002/08/09 03:47:46  ameyer
# Changes for organization status protocol global change.
#
# Revision 1.2  2002/08/06 22:52:20  ameyer
# Fixed SQL select statement for organizations.
#
# Revision 1.1  2002/08/02 03:38:56  ameyer
# Global change common routines for batch and interactive use.
# First working version.
#
#------------------------------------------------------------

import xml.dom.minidom, string, cdr, cdrdb, cdrbatch, cdrcgi

#------------------------------------------------------------
# Constants
#------------------------------------------------------------

# Strings representing different types of change, also used as prompts
PERSON_CHG  = "Person"
ORG_CHG     = "Organization"
STATUS_CHG  = "OrgProtStatus"
INS_ORG_CHG = "InsertOrg"

# What a function might return
RET_ERROR  = -1         # Function failed, see message
RET_HTML   = 1          # Look for html in FuncReturn object
RET_NONE   = 2          # Function didn't return anything

# Max number of elements in a pick list
MAX_PICK_LIST_SIZE = 100

# Logfile
LF = cdr.DEFAULT_LOGDIR + "/GlobalChange.log"

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
    if chgType == INS_ORG_CHG:
        chg = InsertOrgChg()
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
def _execQry (qry, args=None):
    """
    Called by specific subclass objects to execute their particular
    queries.

    Pass:
        qry - Query string
        args - Optional single arg or tuple of args for replacements.
    Return:
        Sequence of all matching database rows, each containing a
        sequence of:
            document id
            document title
    """

    try:
        conn   = cdrdb.connect ('CdrGuest')
        cursor = conn.cursor()
        cursor.execute (qry, args)
        rows   = cursor.fetchall()
        cursor.close()
        return rows
    except cdrdb.Error, info:
        raise cdrbatch.BatchException (\
            "Database error selecting docs for change %s<br>In query:<br>%s" \
            % (info[1][0], qry))

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
          # Only do this if we don't have an id already
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
        except StandardError, e:
            msg = "execStage StandardError: %s: %s" % (stage.excpMsg, str(e))
            cdr.logwrite (msg, LF)
            return FuncReturn (RET_ERROR, msg)
        except:
            msg = "execStage Exception: %s: Unknown error" % stage.excpMsg
            cdr.logwrite (msg, LF)
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
        rows = self.selDocs()

        # If there aren't any
        if not rows:
            return FuncReturn(RET_NONE)

        # Remember the count
        self.ssVars['chgCount'] = str(len(rows))

        # Session for host query
        session = self.ssVars[cdrcgi.SESSION]

        # Find out if there's a publishable version of each
        newRows = [['<b>DocID</b>', '<b>P</b>', '<b>Title</b>']]
        for row in rows:
            docIdStr = cdr.exNormalize (row[0])[0]
            vers = cdr.lastVersions (session, docIdStr)
            if type(vers) == type("") or type(vers) == type(u""):
                pVer = vers
            else:
                pubVer = vers[1]
                if pubVer < 0:
                    pVer = 'N'
                else:
                    pVer = 'Y'
            newRows.append ([row[0], pVer, row[1]])

        # Create the table
        html = cdr.tabularize (newRows, " border='1'")

        # Hand it all back
        result = FuncReturn (RET_HTML)
        result.setPageHtml (html)
        result.setPageTitle ("The following documents will change")
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
            raise StandardError ("verifyId expected 3 parms, got " +
                                 str(len(parms)))
        docId     = parms[0]
        docType   = parms[1]
        titleName = parms[2]

        # There are lots of possible errors, create a default error return
        result = FuncReturn (RET_ERROR)

        # Normalize any type of valid id
        try:
            id = cdr.exNormalize(docId)[1]
        except StandardError, e:
            result.setErrMsg ("Internal error: %s" % str(e))
            return result

        qry = """
    SELECT d.title, t.name
      FROM document d
      JOIN doc_type t
        ON d.doc_type = t.id
     WHERE d.id = %d
    """ % id

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
            result.setErrMsg ("Could not find doc with id=%d" % id)
            return result

        title = row[0]
        type  = row[1]
        if type != docType:
            result.setErrMsg (\
                   "Document %d is of type %s, not %s" % (id, type, docType))
            return result

        # Success, set session variable
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
            raise StandardError ("getPickList expected 4 parms, got " +
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
            html += "<h2>No hits</h2>"
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
            raise StandardError ("genValidValPickList expected 5 parms, got " +
                                 str(len(parms)))
        docType    = parms[0]
        vvType     = parms[1]
        vvVarName  = parms[2]
        defaultVal = parms[3]
        optional   = parms[4]

        # Get the valid value list from the schema
        dt = cdr.getDoctype (('CdrGuest', 'never.0n-$undaY'),'InScopeProtocol')
        if type(dt)==type("") or type(dt)==type(u""):
            cdr.logwrite ("Error getting valid values: %s" % dt, LF)
            return FuncReturn (RET_ERROR,"Error getting valid values: %s" % dt)

        # Find the valid value list in the returned squence
        vals = []
        for vvList in dt.vvLists:
            if vvList[0] == vvType:
                vals = vvList[1]
                break

        # Should never happen
        if vals == []:
            return FuncReturn (RET_ERROR, "No valid value list found " + \
                               "for %s in doctype %s" % (vvType, docType))

        # Create a new list, in proper order
        orderedVals = []

        # If there is a default, put it first
        if defaultVal:
            orderedVals.append (defaultVal)

        # If no default and list is optional, make empty value the default
        elif optional:
            orderedVals.append("")

        # Add the rest, all except the default, which we've already added
        for val in vals:
            if val != defaultVal:
                orderedVals.append (val)

        # Finally, if we had a default value but none is an option
        #   put it at the end
        if defaultVal and optional:
            orderedVals.append("")

        # Construct an html picklist
        html = "<select name='%s'>\n" % vvVarName

        # Add all the values in the created order
        for val in orderedVals:
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
        if self.ssVars.has_key ('insertOrgTitle'):
            html += \
              "<tr><td align='right'>Inserting organization site: </td>\n"
            html += "<td> %s</td></tr>\n" % self.ssVars['insertOrgTitle']
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
              "Restricting to protocols with lead org PI: </td>\n"
            html += "<td> %s (%s)</td></tr>\n" % \
                 (self.ssVars['restrPiId'], self.ssVars['restrPiTitle'])
            haveSoFar = 1
        if self.ssVars.has_key ('chgCount'):
            html += \
              "<tr><td align='right'>Count of documents to change: </td>\n"
            html += "<td> %s</td></tr>\n" % self.ssVars['chgCount']
            haveSoFar = 1

        if haveSoFar:
            html += "</table></font><hr>\n"
        else:
            html = "<hr>\n"

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
            raise StandardError ("genInputHtml expected 4 parms, got " +
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
            raise StandardError ("genFragPickList expected 3 parms, got " +
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
 <tr><td><input type='radio' name='%s' value='%s'>&nbsp;</input></td>
     <td>No specific address fragment - all occurrences match regardless of
     presence or absence of fragment id</td></tr>
""" % (idType, docId)

        # Populate table with radio buttoned cdr ids and addresses
        # value attribute = doc id with fragment appended
        checked=' checked'
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


#------------------------------------------------------------
# Person specific global change object
#------------------------------------------------------------
class PersonChg (GlblChg):

    # Names of filter for Person Link global changes
    # cdr.filterDoc() requires a list, with leading 'name:' for named
    #   filters
    chgFilter = ['name:Global Change: Person Link']
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
          Stage(
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
          Stage(
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
 ORDER BY doc.title
"""
            # Return rows of id + title
            return _execQry (qry, (self.ssVars['fromId'],
                                   self.ssVars['restrId']))

#------------------------------------------------------------
# Organization specific global change object
#------------------------------------------------------------
class OrgChg (GlblChg):

    # Name of filter for Organization Link global changes
    chgFilter = ['name:Global Change: Organization Link']
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
          Stage(
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
          Stage(
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
 ORDER BY doc.title
""" % (protOrgMatchStr, self.ssVars['restrId'])

        # Call a common routine to get the rows corresponding to the query
        return _execQry (qry)


#------------------------------------------------------------
# Organization status global change object
#------------------------------------------------------------
class OrgStatusChg (GlblChg):

    # Name of filter for OrgStatus global changes
    # There is no location picklist.  All locations are affected.
    chgFilter = ['name:Global Change: Org Status']

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
          Stage(
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
                'Restrict changes to protocols with Principal Investigator',
                         'restrPi', 1),
            excpMsg   = 'Generating form for getting the "from" person'),
          Stage(
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
   AND orgstat.path =
   '/InScopeProtocol/ProtocolAdminInfo/ProtocolLeadOrg/ProtocolSites/OrgSite/OrgSiteStatus'
   AND orgstat.value = '%s'
   AND LEFT (orgstat.node_loc, 16) = LEFT (protorg.node_loc, 16)
   AND leadorg.value = '%s'
   AND protpers.path =
   '/InScopeProtocol/ProtocolAdminInfo/ProtocolLeadOrg/ProtocolSites/OrgSite/OrgSiteContact/SpecificPerson/Person/@cdr:ref'
   AND protpers.int_val = %s
   AND LEFT (orgstat.node_loc, 16) = LEFT (protpers.node_loc, 16)
 ORDER BY doc.title
""" % (fromIdNum,
       self.ssVars['fromStatusName'],
       self.ssVars['restrId'],
       cdr.exNormalize(self.ssVars['restrPiId'])[1])

        # Call a common routine to get the rows corresponding to the query
        return _execQry (qry)


#------------------------------------------------------------
# Insert a new site into protocols with a particular lead org
#------------------------------------------------------------
class InsertOrgChg (GlblChg):

    # Names of filters for global changes and for contact info
    chgFilter = ['name:Global Change: Insert Participating Org']
    locFilter = ['name:Person Locations Picklist']

    def __init__(self):
        GlblChg.__init__(self)

        # Stages for organization status changes
        self.stages = (\
          Stage (
            # Get org id for org to be inserted
            condition = 'not (self.ssVars.has_key("insertOrgId") or ' +
                        'self.ssVars.has_key("insertOrgName"))',
            subr      = self.genInputHtml,
            parms     = ('Organization', 'Insert Organization Link',
                         'insertOrg', 0),
            excpMsg   = 'Generating form for getting "insert" organization'),
          Stage(
            # If name entered rather than ID, resolve it
            condition = 'self.ssVars.has_key("insertOrgName") and ' +
                        'not self.ssVars.has_key("insertOrgId")',
            subr      = self.getPickList,
            parms     = ('Organization', 'eval:self.ssVars["insertOrgName"]',
                         'Choose Organization to be inserted', 'insertOrgId'),
            excpMsg   = 'Generating picklist for name'),
          Stage (
            # Verify that this is an org document, not something else
            condition = 'self.ssVars.has_key("insertOrgId") and ' +
                        'not self.ssVars.has_key("insertOrgTitle")',
            subr      = self.verifyId,
            parms     = ('eval:self.ssVars["insertOrgId"]', 'Organization',
                         'insertOrgTitle'),
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
          Stage(
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
