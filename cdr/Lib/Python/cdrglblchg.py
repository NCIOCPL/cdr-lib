# $Id: cdrglblchg.py,v 1.8 2002-10-03 19:38:32 ameyer Exp $
#
# Common routines and classes for global change scripts.
#
# $Log: not supported by cvs2svn $
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

import xml.dom.minidom, cdr, cdrdb, cdrbatch, cdrcgi

#------------------------------------------------------------
# Constants
#------------------------------------------------------------

# Strings representing different types of change, also used as prompts
PERSON_CHG = "Person"
ORG_CHG    = "Organization"
STATUS_CHG = "OrgProtStatus"

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
        chg = OrgStatusChg()
        sessionVars['docType'] = 'Organization'

    # Can't happen unless there's a bug
    if not chg:
        raise cdrbatch.BatchException("No change type selected, can't happen!")

    # Save session variables here for shared access with GlobalChange.py
    chg.sessionVars = sessionVars

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
        raise cdrbatch.BatchException ("Could not find doc with id=%d" % id)

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
 WHERE d.title LIKE ?
   AND d.doc_type = t.id
   AND t.name = ?
 ORDER BY d.title
""" % MAX_PICK_LIST_SIZE

    rows = _execQry (qry, (searchString + '%', docType))

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
            html += " <option value='%s'>%s</option>\n" % (row[0], row[1])

        html += "</select>\n"

    return html


#------------------------------------------------------------
# Generate a picklist of valid values from a schema
#------------------------------------------------------------
def _genValidValPickList (docType, vvType, action):
    """
    Construct an html picklist of valid values for a particular
    schema complex type.
    The name of the picklist is the same as the name of the
    schema type, prepended by action, e.g.,
        genValidValPickList ('InScopeProtocol', 'StatusName', 'from')
        returns "<select name='fromStatusName' ..."
    Bob's getDoctype does the work.

    Pass:
        docType - Doctype name.
        vvType  - Name of complex type in schema containing valid values.
        action  - 'from' or 'to'
    Returns:
        HTML format selectable picklist.
    """
    # Get the valid value list from the schema
    dt = cdr.getDoctype (('CdrGuest', 'never.0n-$undaY'), 'InScopeProtocol')
    if type(dt)==type("") or type(dt)==type(u""):
        cdr.logwrite ("Error getting valid values: %s" % dt, LF)
        raise dt

    vals = []
    for vvList in dt.vvLists:
        if vvList[0] == vvType:
            vals = vvList[1]
            break

    # Should never happen
    if vals == []:
        raise cdrbatch.BatchException ("No valid value list found " \
                          "for %s in doctype %s" % vvType, docType)

    # Construct an html picklist
    html = "<select name='%s%s'>\n" % (action, vvType)
    for val in vals:
        html += " <option value='%s'>%s</option>\n" % (val, val)
    html += "</select>\n"

    return html


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
# Base class for all global changes.  Has some common stuff.
#------------------------------------------------------------
class GlblChg:

    # Holds filter to find specific locations with Person or Organization
    # Defined in subclasses
    locFilter = None

    def __init__(self):
        # Holds variables for this session, assigned in factory method
        self.sessionVars = []

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

        # Remember the count
        self.sessionVars['chgCount'] = str(len(rows))

        # Session for host query
        session = self.sessionVars[cdrcgi.SESSION]

        # Find out if there's a publishable version of each
        newRows = [['<b>DocID</b>', '<b>P</b>', '<b>Title</b>']]
        for row in rows:
            docIdStr = cdr.exNormalize (row[0])[0]
            vers = cdr.lastVersions (session, docIdStr)
            if type(vers) == type("") or type(vers) == type(u""):
                pVer = vers
            else:
                pubVer = vers[1]
                cdr.logwrite ("Version info: %d, %d, %s" % \
                              (vers[0], vers[1], vers[2]), LF)
                if pubVer < 0:
                    pVer = 'N'
                else:
                    pVer = 'Y'
            newRows.append ([row[0], pVer, row[1]])

        # Create the table
        html += cdr.tabularize (newRows, " border='1'")

        # Hand it all back
        return html

    def showSoFarHtml (self):
        """
        Generate HTML to show what we've selected so far.

        Return:
            HTML with 0, 1, or 2 lines of selections of from and to docs.
        """
        haveSoFar = 0
        html = "<hr><table border='0' cellspacing='6'>"
        if self.sessionVars.has_key ('fromTitle'):
            html += "<tr><td align='right'>Changing links from: </td>\n"
            html += "<td> %s (%s)</td></tr>\n" % \
                 (self.sessionVars['fromId'], self.sessionVars['fromTitle'])
            haveSoFar = 1
        if self.sessionVars.has_key ('toTitle'):
            html += "<tr><td align='right'>Changing links to: </td>\n"
            html += "<td> %s (%s)</td></tr>\n" % \
                (self.sessionVars['toId'], self.sessionVars['toTitle'])
            haveSoFar = 1
        if self.sessionVars.has_key ('fromStatusName'):
            html += \
              "<tr><td align='right'>Changing protocol status from: </td>\n"
            html += "<td> %s</td></tr>\n" % self.sessionVars['fromStatusName']
            haveSoFar = 1
        if self.sessionVars.has_key ('toStatusName'):
            html += \
              "<tr><td align='right'>Changing protocol status to: </td>\n"
            html += "<td> %s</td></tr>\n" % self.sessionVars['toStatusName']
            haveSoFar = 1
        if self.sessionVars.has_key ('chgCount'):
            html += \
              "<tr><td align='right'>Count of documents to change: </td>\n"
            html += "<td> %s</td></tr>\n" % self.sessionVars['chgCount']
            haveSoFar = 1

        if haveSoFar:
            html += "</table></font><hr>\n"
        else:
            html = "<hr>\n"

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
        chgType = self.sessionVars['chgType']

        # Some unfortunate manipulations
        if chgType == STATUS_CHG:
            actMsg = "Change status for organization"
        else:
            actMsg = "Change %s links %s:" % (docType, action)
        if action == 'restr':
            actMsg = \
        'Restrict change to protocols with particular lead org, or leave blank'
            docType = 'Organization'
        actMsg = "<h3>" + actMsg + "</h3>"

        # Construct input form, prefaced by what we've done so far
        html = self.showSoFarHtml() + """
<table border='0'>
<tr><td colspan='2'>%s</td></tr>
<tr><td align='right'>%s DocId: </td>
    <td><input type='text' size='30' name='%sId'></td></tr>
<tr><td colspan='2'>&nbsp;&nbsp;&nbsp;&nbsp;Or</td></tr>
<tr><td align='right'>%s Name: </td>
    <td><input type='text' size='30' name='%sName'></td></tr>
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
    <td><input type='text' size='30' name='restrId'></td></tr>
<tr><td colspan='2'>&nbsp;&nbsp;&nbsp;&nbsp;Or</td></tr>
<tr><td align='right'>Organization Name: </td>
    <td><input type='text' size='30' name='restrName'></td></tr>
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

        # For organizations, it is legal to select no fragment
        # Seed the pick list with an entry for no/all fragments
        # value attribute = doc id with no fragment added to it
        if self.sessionVars['chgType'] == ORG_CHG:
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

    def getFromToStatus (self):
        """ Create a screen to get from and to OrgSiteStatus values
            Returns tuple ready for sendGlblChgPage """
        html = self.showSoFarHtml() + """
<table border='0'>
 <tr>
  <td align='right'>Select status to change from</td>
  <td>%s</td>
 </tr><tr>
  <td align='right'>Select status to change to</td>
  <td>%s</td>
 </tr>
</table>""" % (_genValidValPickList ('InScopeProtocol', 'StatusName', 'from'),
               _genValidValPickList ('InScopeProtocol', 'StatusName', 'to'))

        return ("Pick status to change from/to", html, None)

    def getRestrId (self):
        """ Create the restrictions screen """
        return ("Restrict changes to protocols with Lead Org",
                self.genLeadOrgHtml(), None)

    def getRestrPick (self, name):
        """ Just like getFromPick """
        return ("Choose protocol Lead Org to restrict changes to",
                _getPickList ('Organization', name, 'restr'), None)

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
   AND protpers.value = ?
   AND protstat.path = '/InScopeProtocol/ProtocolAdminInfo/ProtocolLeadOrg/LeadOrgProtocolStatuses/CurrentOrgStatus/StatusName'
   AND (protstat.value = 'Active' OR
        protstat.value = 'Approved-not yet active' OR
        protstat.value = 'Temporarily closed')
   AND leadorg.path = '/InScopeProtocol/ProtocolAdminInfo/ProtocolLeadOrg/LeadOrganizationID/@cdr:ref'
   AND leadorg.value = ?
 ORDER BY doc.title
"""

        # Call a common routine to get the rows corresponding to the query
        return _execQry (qry, (self.sessionVars['fromId'],
                               self.sessionVars['restrId']))

#------------------------------------------------------------
# Organization specific global change object
#------------------------------------------------------------
class OrgChg (GlblChg):

    # Name of filter for Organization Link global changes
    chgFilter = ['name:Global Change: Organization Link']
    locFilter = ['name:Organization Locations Picklist']

    def __init__(self):
        GlblChg.__init__(self)

    def selDocs (self):
        """
        See PersonChg.selDocs()
        """

        # If searching for an org with a fragment id, we need an exact match
        # But if no fragment, we need to pick up all fragments, or mathes
        #   with no fragment at all
        fromId = self.sessionVars['fromId']
        if fromId.find ('#') >= 0:
            protOrgMatchStr = "protorg.value = '%s'" % fromId
        else:
            protOrgMatchStr = "protorg.int_val=%d" % cdr.exNormalize(fromId)[1]

        # If no restrictions
        if not self.sessionVars.has_key ('restrId'):
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
""" % (protOrgMatchStr, self.sessionVars['restrId'])

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
        fromIdNum = cdr.exNormalize (self.sessionVars['fromId'])[1]

        # If no restrictions
        if not self.sessionVars.has_key ('restrId'):
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
       self.sessionVars['fromStatusName'])

        # Else restrict them by a particular lead organization
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
       self.sessionVars['fromStatusName'],
       self.sessionVars['restrId'])

        # Call a common routine to get the rows corresponding to the query
        return _execQry (qry)

