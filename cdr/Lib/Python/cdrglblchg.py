#------------------------------------------------------------
# $Id: cdrglblchg.py,v 1.1 2002-08-02 03:38:56 ameyer Exp $
#
# Common routines and classes for global change scripts.
#
# $Log: not supported by cvs2svn $
#------------------------------------------------------------

import xml.dom.minidom, cdr, cdrdb, cdrbatch, cdrcgi

#------------------------------------------------------------
# Constants
#------------------------------------------------------------

# Strings representing different types of change, also used as prompts
PERSON_CHG = "Person"
ORG_CHG    = "Organization"
STATUS_CHG = "ProtStatus"

# Max number of elements in a pick list
MAX_PICK_LIST_SIZE = 100

# Logfile
LF = cdr.DEFAULT_LOGDIR + "/GlobalChange.log"

#------------------------------------------------------------
# Factory for creating global change objects
#------------------------------------------------------------
def createChg (sessionVars):
    """
    Returns a global change object of the correct type for the
    type of change desired.

    Pass:
        sessionVars - Dictionary of session variables - preserved by the
                      browser client between calls to GlobalChange.py.
    Return:
        GlblChg object of proper type.
    """
    chgType = sessionVars["chgType"]
    chg = None
    if chgType == PERSON_CHG:
        chg = PersonChg()
        sessionVars['docType'] = 'Person'
    if chgType == ORG_CHG:
        chg = OrgChg()
        sessionVars['docType'] = 'Organization'
    if chgType == STATUS_CHG:
        chg = StatusChg()
        sessionVars['docType'] = 'Organization'

    # Can't happen unless there's a bug
    if not chg:
        raise cdrbatch.BatchException("No change type selected, can't happen!")

    # Save session variables here for shared access with GlobalChange.py
    chg.sessionVars = sessionVars
    cdr.logwrite("Added sessionVars to chg object", LF)

    # Give caller our new object
    return chg


#------------------------------------------------------------
# Verify existence and correct document type for an id.
#------------------------------------------------------------
def verifyId(docId, docType):
    """
    Verify that a document exists and has the expected document type.
    Pass:
        docId as string or integer.
        docType as string
    Return:
        Title string.
        If error, raises BatchException.
    """
    try:
        id = cdr.exNormalize(docId)[1]
    except StandardError, e:
        raise cdrbatch.BatchException (str(e))

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
        raise cdrbatch.BatchException (\
            "Database error checking document id: %s" % info[1][0])

    if not row:
        raise cdrbatch.BatchException ("Could not find doc with id=%d", id)

    title = row[0]
    type  = row[1]
    if type != docType:
        raise cdrbatch.BatchException (\
                   "Document %d is of type %s, not %s" % (id, type, docType))

    return title

#------------------------------------------------------------
# Generate a picklist of document titles
#------------------------------------------------------------
def _getPickList (docType, searchString, action):

    """
    Construct a pick list for a web page.
    Pass:
        docType      - Name of document type, used in database query
                       and in prompt to user.
        searchString - String to search for in this doc type.  May be
                       full title or leading substring.
        action       - One of:
                        'from'   - Change from ...
                        'to'     - Change to ...
                        'restr'  - Restrict to protocols with ...
    Return:
        html form contents, suitable for display.
        Number of elements is limited to MAX_PICK_LIST_SIZE
    """
    # Prepare query
    qry = """
SELECT TOP %d d.id, d.title
  FROM document d, doc_type t
 WHERE d.title LIKE '%s%%'
   AND d.doc_type = t.id
   AND t.name = '%s'
 ORDER BY d.title
""" % (MAX_PICK_LIST_SIZE, searchString, docType)

    rows = _execQry (qry)

    # Nothing there with that name?
    if not rows:
        html = "<h2>No hits</h2>"

    else:
        # Selection presentation
        if action == 'from':
            html = """
<h3>Choose %s to be changed</h3>
""" % docType
        elif action == 'to':
            html = """
<h3>Choose %s to replace the existing %s from list:</h3>
""" % (docType, docType)

        elif action == 'restr':
            html = """
<h3>Choose Lead Organization to restrict global change:</h3>
"""

        html += "<select name='%sId' size='15'>\n" % action

        # Get list, up to max (caller may have limited too)
        for row in rows:
            html += "<option value='%s'>%s</option>" % (row[0], row[1])

        html += "</select>"

    return html



#------------------------------------------------------------
# Execute a query, returning rows
#------------------------------------------------------------
def _execQry (qry):
    """
    Called by specific subclass objects to execute their particular
    queries.

    Pass:
        qry - Query string
    Return:
        Sequence of all matching database rows, each containing a
        sequence of:
            document id
            document title
    """

    try:
        conn   = cdrdb.connect ('CdrGuest')
        cursor = conn.cursor()
        cursor.execute (qry)
        rows   = cursor.fetchall()
        cursor.close()
        return rows
    except cdrdb.Error, info:
        raise cdrbatch.BatchException (\
            "Database error selecting docs for change %s<br>In query:<br>%s" \
            % (info[1][0], qry))


#------------------------------------------------------------
# Base class for all global changes.  Has some common stuff.
#------------------------------------------------------------
class GlblChg:

    # Connection and cursor objects for actions with this object
    def reportWillChange (self):
        """
        Create an HTML list of documents that will be changed.

        Return:
            Table of docs in HTML format.
            None if no docs will change.
        """
        # Select docs
        cdr.logwrite ("Selecting docs for willChange report", LF)
        rows = self.selDocs()
        cdr.logwrite ("Done selecting docs for willChange report", LF)

        # If there aren't any
        if not rows:
            return None

        # Put in a session variable to identify this screen
        html = "<input type='hidden' name='okToRun' value='Y'>\n"

        # Create the table
        html += cdr.tabularize (rows, " border='1'")

        # Hand it all back
        return html

    def showSoFarHtml (self):
        """
        Generate HTML to show what we've selected so far.

        Return:
            HTML with 0, 1, or 2 lines of selections of from and to docs.
        """
        html = ""
        if self.sessionVars.has_key ('fromTitle'):
            html += "<hr><p>Changing links from %s (%s)</p>\n" %\
                 (self.sessionVars['fromId'], self.sessionVars['fromTitle'])
        if self.sessionVars.has_key ('toTitle'):
            html += \
                "<p>&nbsp;&nbsp;&nbsp;&nbsp;Changing links to %s (%s)</p>\n" %\
                (self.sessionVars['toId'], self.sessionVars['toTitle'])
        if len(html) > 0:
            html += "<hr>\n"

        return html

    def genInputHtml (self, action):
        """
        Generate a 'from' or 'to' document id/name input screen for
        a user to enter the document identifer to change from or to.

        Pass:
            docType - Name of document type for presentation to user.
            action  - 'from' or 'to'
        Return:
            HTML form contents for presentation.
        """
        # Type of document to change
        docType = self.sessionVars['docType']

        # Some unfortunate manipulations
        actMsg = "Change %s links %s" % (docType, action)
        if action == 'restr':
            actMsg = \
        'Restrict change to protocols with particular lead org, or leave blank'
            docType = 'Organization'

        # Construct input form, prefaced by what we've done so far
        html = self.showSoFarHtml() + """
<table border='0'>
<tr><td colspan='2'>%s:</td></tr>
<tr><td align='right'>%s DocId: </td>
    <td><input type='text' width='50' name='%sId'></td></tr>
<tr><td colspan='2'>&nbsp;&nbsp;&nbsp;&nbsp;Or</td></tr>
<tr><td align='right'>%s Name: </td>
    <td><input type='text' width='50' name='%sName'></td></tr>
</table>
    """ % (actMsg, docType, action, docType, action)

        return html

    def genLeadOrgHtml (self):
        """
        Generate an html page for user input of a lead organization.
        Also returns a hidden field that tells us not to do this again.
        """
        html = "<h3>Optional restrictions by Lead Org</h3>\n" + \
               self.showSoFarHtml() + """
<input type='hidden' name='restrByLeadOrgChk' value='N' />
<p>Restrict change to protocols with particular lead org, or leave blank</p>
<table border='0'>
<tr><td align='right'>Organization DocId: </td>
    <td><input type='text' width='50' name='restrId'></td></tr>
<tr><td colspan='2'>&nbsp;&nbsp;&nbsp;&nbsp;Or</td></tr>
<tr><td align='right'>Organization Name: </td>
    <td><input type='text' width='50' name='restrName'></td></tr>
</table>
"""
        return html

    def getFromId (self):
        """
        Construct html form contents for getting user input for person
        to change from.

        Asks for ID or name, which must be saved in variables named:
            fromId, or
            fromName

        Returns tuple of strings to pass to GlobalChange.sendGlblChgPage:
            header string
            content string
            No custom buttons (None)
        """
        # return ("Change person link:", _genInputHtml ("Person", "from"), None)
        return ("Enter id or name of 'from' document",
                self.genInputHtml ("from"), None)

    def genFragPickListHtml (self, action):
        """
        Create an HTML form with a table containing a picklist of
        address fragments for a person or organization.

        Should only be called if the current id doesn't have a fragment
        already associated with it.

        Pass:
            action - 'from' or 'to'

        Returns tuple of strings to pass to GlobalChange.sendGlblChgPage:
            header string
            content string
            No custom buttons (None)
        """
        # We're getting a 'fromId' or 'toId'
        idType    = action + 'Id'
        titleType = action + 'Title'

        # Get the document id to process, of form "CDR0000..."
        docId = self.sessionVars[idType]

        # Filter this document with the picklist filter to select
        #   fragments <Link> and corresponding addresses <Data>
        filtResp = cdr.filterDoc (self.sessionVars[cdrcgi.SESSION],
                                  filter=self.locFilter,
                                  parm=[['docId', docId]],
                                  docId=docId, docVer=None)

        if type(filtResp) != type(()):
            raise cdrbatch.BatchException (\
                         "Error filtering addresses:<br>\n%s" %\
                         cdr.getErrors (filtResp))

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
        html = """
<p>Address fragments for: %s (%s)</p>
<p>Select a fragment identifier for the address desired.</p>
<table border='1'>
""" % (docId, self.sessionVars[titleType])

        # Populate table with radio buttoned cdr ids and addresses
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
        del (self.sessionVars[idType])

        return ("Choose address fragment", html, None)


    def getFromPick (self, name):
        docType = self.sessionVars['docType']
        return ("Choose %s for change" % docType,
                _getPickList (docType, name, 'from'), None)

    def getToId (self):
        """ Like getFromId """
        docType = self.sessionVars['docType']
        return ("Change %s link:" % docType, self.genInputHtml ("to"), None)

    def getToPick (self, name):
        """ Like getFromPick """
        docType = self.sessionVars['docType']
        return ("Choose %s for change" % docType,
                _getPickList (docType, name, 'to'), None)

    def getRestrId (self):
        """ Create the restrictions screen """
        return ("Restrict changes to protocols with Lead Org",
                self.genLeadOrgHtml(), None)

    def getRestrPick (self, name):
        """ Just like getFromPick """
        return ("Choose protocol Lead Org to restrict changes to",
                _getPickList ('Organization', name, 'restr'), None)



#------------------------------------------------------------
# Person specific global change object
#------------------------------------------------------------
class PersonChg (GlblChg):

    # Names of filter for Person Link global changes
    # cdr.filterDoc() requires a list, with leading 'name:' for named
    #   filters
    chgFilter = ['name:Global Change: Person Link']
    locFilter = ['name:Person Locations Picklist']

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
        if not self.sessionVars.has_key ('restrId'):
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
""" % self.sessionVars['fromId']

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
   AND protpers.value = '%s'
   AND protstat.path = '/InScopeProtocol/ProtocolAdminInfo/ProtocolLeadOrg/LeadOrgProtocolStatuses/CurrentOrgStatus/StatusName'
   AND (protstat.value = 'Active' OR
        protstat.value = 'Approved-not yet active' OR
        protstat.value = 'Temporarily closed')
   AND leadorg.path = '/InScopeProtocol/ProtocolAdminInfo/ProtocolLeadOrg/LeadOrganizationID/@cdr:ref'
   AND leadorg.value = '%s'
 ORDER BY doc.title
""" % (self.sessionVars['fromId'], self.sessionVars['restrId'])

        # Call a common routine to get the rows corresponding to the query
        return _execQry (qry)

#------------------------------------------------------------
# Organization specific global change object
#------------------------------------------------------------
class OrgChg (GlblChg):

    # Name of filter for Organization Link global changes
    chgFilter = ['name:Global Change: Organization Link']
    locFilter = ['name:Organization Locations Picklist']

    def selDocs (self):
        """
        See PersonChg.selDocs()
        """

        # If no restrictions
        if not self.sessionVars.has_key ('restrId'):
            qry = """
SELECT DISTINCT doc.id, doc.title FROM document doc
  JOIN query_term protorg
    ON protorg.doc_id = doc.id
  JOIN query_term protstat
    ON protstat.doc_id = doc.id
 WHERE protorg.path =
   '/InScopeProtocol/ProtocolAdminInfo/ProtocolLeadOrg/LeadOrgPersonnel/ProtocolSites/OrgSite/@cdr:ref'
   AND protorg.value = '%s'
   AND protstat.path = '/InScopeProtocol/ProtocolAdminInfo/ProtocolLeadOrg/LeadOrgProtocolStatuses/CurrentOrgStatus/StatusName'
   AND (protstat.value = 'Active' OR
        protstat.value = 'Approved-not yet active' OR
        protstat.value = 'Temporarily closed')
 ORDER BY doc.title
""" % self.sessionVars['fromId']

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
   '/InScopeProtocol/ProtocolAdminInfo/ProtocolLeadOrg/LeadOrgPersonnel/ProtocolSites/OrgSite/@cdr:ref'
   AND protorg.value = '%s'
   AND protstat.path = '/InScopeProtocol/ProtocolAdminInfo/ProtocolLeadOrg/LeadOrgProtocolStatuses/CurrentOrgStatus/StatusName'
   AND (protstat.value = 'Active' OR
        protstat.value = 'Approved-not yet active' OR
        protstat.value = 'Temporarily closed')
   AND leadorg.path = '/InScopeProtocol/ProtocolAdminInfo/ProtocolLeadOrg/LeadOrganizationID/@cdr:ref'
   AND leadorg.value = '%s'
 ORDER BY doc.title
""" % (self.sessionVars['fromId'], self.sessionVars['restrId'])

        # Call a common routine to get the rows corresponding to the query
        return _execQry (qry)
