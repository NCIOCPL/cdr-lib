#----------------------------------------------------------------------
#
# $Id: cdrcgi.py,v 1.41 2004-01-09 16:48:52 venglisc Exp $
#
# Common routines for creating CDR web forms.
#
# $Log: not supported by cvs2svn $
# Revision 1.40  2003/12/30 16:50:32  bkline
# Extraced out common functionality into generateHtmlPicklist().
#
# Revision 1.39  2003/12/17 01:20:14  bkline
# Fixed bug in CTGovProtocol advanced search report.
#
# Revision 1.38  2003/12/17 01:09:05  bkline
# Added advanced search support for CTGovProtocol documents.
#
# Revision 1.37  2003/12/16 16:16:07  bkline
# Main menu rewritten at Lakshmi's request (#1000).
#
# Revision 1.36  2003/11/04 16:56:22  bkline
# Added CTGov to main admin menu.
#
# Revision 1.35  2003/09/12 12:36:38  bkline
# Modified the function which constructs an advanced search query,
# adding support for finding documents based on values in linked
# documents.  Also added significantly expanded documentation to
# the function.
#
# Revision 1.34  2003/08/12 19:59:23  ameyer
# Added optional logfile parameter to bail().  If given, the messages written
# back to the browser are also written to the named logfile.
#
# Revision 1.33  2003/07/29 13:04:16  bkline
# Took out call to convert HTML to latin 1 in sendPage().
#
# Revision 1.32  2003/03/04 22:55:05  bkline
# Added support for display format selection on advanced Protocol search
# page.
#
# Revision 1.31  2003/01/29 21:00:19  bkline
# Added another parameter to function for creating the top of an
# advanced search page (for errors).
#
# Revision 1.30  2002/11/13 16:57:28  bkline
# Added extra args to bail() and header().
#
# Revision 1.29  2002/11/07 13:08:23  bkline
# Added slot for stylesheet in standard header.
#
# Revision 1.28  2002/09/05 16:31:14  pzhang
# Added port parameter to get session.
#
# Revision 1.27  2002/08/15 21:18:07  bkline
# Added command for sending broadcast email.
#
# Revision 1.26  2002/08/07 16:35:35  bkline
# Added unicodeToLatin1() to sendPage().
#
# Revision 1.25  2002/07/25 18:22:14  ameyer
# Added View Batch Job Status option.
#
# Revision 1.24  2002/07/17 18:52:28  bkline
# New Admin menu item for CDR filter maintenance.
#
# Revision 1.23  2002/07/15 20:19:21  bkline
# New argument (textType) for sendPage() function.  Mods for Summary
# advanced search display.
#
# Revision 1.22  2002/07/11 17:20:10  bkline
# Fixed problems with miscellaneous document advanced search.
#
# Revision 1.21  2002/07/05 18:10:52  bkline
# Modified previous fix to use more efficient approach.
#
# Revision 1.20  2002/07/05 18:04:05  bkline
# Fixed bug in advanced search page.
#
# Revision 1.19  2002/07/02 14:18:04  ameyer
# Added global change entry to main menu.
#
# Revision 1.18  2002/07/02 13:47:16  bkline
# Plugged in cdrcgi.DAY_ONE; added getFullUserName().
#
# Revision 1.17  2002/06/28 20:13:57  bkline
# Plugged in QcReport.py for Organization advanced search.
#
# Revision 1.16  2002/06/28 03:17:39  bkline
# Tweaked WEBSERVER using gethostbyaddr.
#
# Revision 1.15  2002/06/27 20:22:54  ameyer
# Added tabularize.
#
# Revision 1.14  2002/06/26 20:05:25  bkline
# Modified advanced Person search forms to use QcReport.py for doc display.
#
# Revision 1.13  2002/06/04 20:17:26  bkline
# Added option for choosing between POST and GET requests.
#
# Revision 1.12  2002/05/30 17:01:06  bkline
# Added extra blank line for Protocol advanced search results page.
#
# Revision 1.11  2002/05/24 18:03:22  bkline
# Fixed encoding bug in Advanced Search screens.
#
# Revision 1.10  2002/04/12 19:56:21  bkline
# Added import statement for cdrdb module.
#
# Revision 1.9  2002/03/21 20:01:59  bkline
# Added function for converting integers to Roman numerals.
#
# Revision 1.8  2002/03/02 13:50:54  bkline
# Added unicodeToLatin1().
#
# Revision 1.7  2002/02/21 15:21:08  bkline
# Added navigateTo() function.
#
# Revision 1.6  2002/02/14 19:33:21  bkline
# Adjusted code for advanced search pulldown lists to match schema changes.
#
# Revision 1.5  2001/12/01 17:55:45  bkline
# Added support for advanced search.
#
# Revision 1.4  2001/06/13 22:33:10  bkline
# Added logout and mainMenu functions.
#
# Revision 1.3  2001/04/08 22:52:42  bkline
# Added code for mapping to/from UTF-8.
#
# Revision 1.2  2001/03/27 21:15:27  bkline
# Paramaterized body background for HTML; added RCS Log keyword.
#
#----------------------------------------------------------------------

#----------------------------------------------------------------------
# Import external modules needed.
#----------------------------------------------------------------------
import cgi, cdr, cdrdb, sys, codecs, re, socket

#----------------------------------------------------------------------
# Create some useful constants.
#----------------------------------------------------------------------
USERNAME = "UserName"
PASSWORD = "Password"
PORT     = "Port"
SESSION  = "Session"
REQUEST  = "Request"
DOCID    = "DocId"
FILTER   = "Filter"
FORMBG   = '/images/back.jpg'
BASE     = '/cgi-bin/cdr'
MAINMENU = 'Admin Menu'
WEBSERVER= socket.gethostbyaddr(socket.gethostname())[0] #'mmdb2.nci.nih.gov'
DAY_ONE  = '2002-06-24'
HEADER   = """\
<!DOCTYPE HTML PUBLIC '-//IETF//DTD HTML//EN'>
<HTML>
 <HEAD>
  <TITLE>%s</TITLE>
  <BASEFONT FACE='Arial, Helvetica, sans-serif'>
  <LINK REL='STYLESHEET' HREF='/stylesheets/dataform.css'>
 %s</HEAD>
 <BODY BGCOLOR='EEEEEE'>
  <FORM ACTION='/cgi-bin/cdr/%s' METHOD='%s'%s>
   <TABLE WIDTH='100%%' CELLSPACING='0' CELLPADDING='0' BORDER='0'>
    <TR>
     <TH NOWRAP BGCOLOR='silver' ALIGN='left' BACKGROUND='/images/nav1.jpg'>
      <FONT SIZE='6' COLOR='white'>&nbsp;%s</FONT>
     </TH>
"""
B_CELL = """\
     <TD BGCOLOR='silver'
         VALIGN='middle'
         ALIGN='right'
         WIDTH='100%'
         BACKGROUND='/images/nav1.jpg'>
"""
BUTTON = """\
      <INPUT TYPE='submit' NAME='%s' VALUE='%s'>&nbsp;
"""
SUBBANNER = """\
    </TR>
    <TR>
     <TD BGCOLOR='#FFFFCC' COLSPAN='3'>
      <FONT SIZE='-1' COLOR='navy'>&nbsp;%s<BR></FONT>
     </TD>
    </TR>
   </TABLE>
"""

#----------------------------------------------------------------------
# Display the header for a CDR web form.
#----------------------------------------------------------------------
def header(title, banner, subBanner, script = '', buttons = None, bkgd = '',
           numBreaks = 2, method = 'POST', stylesheet='', formExtra = ''):
    html = HEADER % (title, stylesheet, script, method, formExtra, banner)
    if buttons:
        html = html + B_CELL
        for button in buttons:
            if button == "Load":
                html = html + "      <INPUT NAME='DocId' SIZE='14'>&nbsp;\n"
            html = html + BUTTON % (REQUEST, button)
        html = html + "     </TD>\n"
    html = html + SUBBANNER % subBanner
    return html + numBreaks * "   <BR>\n"

#----------------------------------------------------------------------
# Get a session ID based on current form field values.
#----------------------------------------------------------------------
def getSession(fields):

    # If we already have a Session field value, use it.
    if fields.has_key(SESSION):
        session = fields[SESSION].value
        if len(session) > 0:
            return session

    # Check for missing fields.
    if not fields.has_key(USERNAME) or not fields.has_key(PASSWORD):
        return None
    userId = fields[USERNAME].value
    password = fields[PASSWORD].value
    if len(userId) == 0 or len(password) == 0:
        return None

    # Log on to the CDR Server.
    if fields.has_key(PORT):
        session = cdr.login(userId, password, port = cdr.getPubPort())
    else:
        session = cdr.login(userId, password)
    if session.find("<Err") >= 0: return None
    else:                         return session

#----------------------------------------------------------------------
# Get the name of the submitted request.
#----------------------------------------------------------------------
def getRequest(fields):

    # Make sure the request field exists.
    if not fields.has_key(REQUEST): return None
    else:                           return fields[REQUEST].value

#----------------------------------------------------------------------
# Send an HTML page back to the client.
#----------------------------------------------------------------------
def sendPage(page, textType = 'html'):
    print """\
Content-type: text/%s

%s""" % (textType, unicodeToLatin1(page))
    sys.exit(0)

#----------------------------------------------------------------------
# Emit an HTML page containing an error message and exit.
#----------------------------------------------------------------------
def bail(message, banner = "CDR Web Interface", extra = None, logfile = None):
    """
    Send an error page to the browser with a specific banner and title.
    Pass:
        message - Display this.
        banner  - Optional changed line for page banner.
        extra   - Optional sequence of extra lines to append.
        logfile - Optional name of logfile to write to.
    Return:
        No return. Exits here.
    """
    if extra:
        for arg in extra:
            message += "<br>%s" % cgi.escape(arg)
    if logfile:
        cdr.logwrite ("cdrcgi bailout:\n %s" % message, logfile)

    page = header("CDR Error", banner, "An error has occured", "", [])
    page = page + "<B>%s</B></FORM></BODY></HTML>" % message
    sendPage(page)
    sys.exit(0)

#----------------------------------------------------------------------
# Encode XML for transfer to the CDR Server using utf-8.
#----------------------------------------------------------------------
def encode(xml): return unicode(xml, 'latin-1').encode('utf-8')

#----------------------------------------------------------------------
# Convert CDR Server's XML from utf-8 to latin-1 encoding.
#----------------------------------------------------------------------
decodePattern = re.compile(u"([\u0080-\uffff])")
def decode(xml):
    return re.sub(decodePattern,
                  lambda match: u"&#x%X;" % ord(match.group(0)[0]),
                  unicode(xml, 'utf-8')).encode('latin-1')

def unicodeToLatin1(s):
    return re.sub(decodePattern,
                  lambda match: u"&#x%X;" % ord(match.group(0)[0]),
                  s).encode('latin-1')

#----------------------------------------------------------------------
# Log out of the CDR session and put up a new login screen.
#----------------------------------------------------------------------
def logout(session):

    # Make sure we have a session to log out of.
    if not session: bail('No session found.')

    # Create the page header.
    title   = "CDR Administration"
    section = "Login Screen"
    buttons = ["Log In"]
    hdr     = header(title, title, section, "Admin.py", buttons)

    # Perform the logout.
    error = cdr.logout(session)
    message = error or "Session Logged Out Successfully"

    # Put up the login screen.
    form = """\
        <H3>%s</H3>
           <TABLE CELLSPACING='0'
                  CELLPADDING='0'
                  BORDER='0'>
            <TR>
             <TD ALIGN='right'>
              <B>CDR User ID:&nbsp;</B>
             </TD>
             <TD><INPUT NAME='UserName'></TD>
            </TR>
            <TR>
             <TD ALIGN='right'>
              <B>CDR Password:&nbsp;</B>
             </TD>
             <TD><INPUT NAME='Password'
                        TYPE='password'>
             </TD>
            </TR>
           </TABLE>
          </FORM>
         </BODY>
        </HTML>\n""" % message

    sendPage(hdr + form)

#----------------------------------------------------------------------
# Display the CDR Administation Main Menu.
#----------------------------------------------------------------------
def mainMenu(session, news = None):

    userPair = cdr.idSessionUser(session, session)
    session  = "?%s=%s" % (SESSION, session)
    title    = "CDR Administration"
    section  = "Main Menu"
    buttons  = []
    hdr      = header(title, title, section, "", buttons)

    extra    = news and ("<H2>%s</H2>\n" % news) or ""
    menu     = """\
    <ol>
"""
    if userPair[0].lower() == 'venglisc':
        menu += """\
     <li>
      <a href='%s/EditFilters.py%s'>Manage Filters (Just for you, Volker!)</a>
     </li>
""" % (BASE, session)
    for item in (
        ('BoardManagers.py', 'CIPS Board Managers'             ),
        ('CiatCipsStaff.py', 'CIAT/CIPS Staff'                 ),
        ('DevSA.py',         'Developers/System Administrators'),
        ('Logout.py',        'Log Out'                         )
        ):
        menu += """\
     <li><a href='%s/%s%s'>%s</a></li>
""" % (BASE, item[0], session, item[1])
    sendPage(hdr + extra + menu + """\
    </ol>
  </form>
 </body>
</html>""")

#----------------------------------------------------------------------
# Navigate to menu location.
#----------------------------------------------------------------------
def navigateTo(where, session):
    print "Location:http://%s%s/%s?%s=%s\n" % (WEBSERVER,
                                               BASE,
                                               where,
                                               SESSION,
                                               session)
    sys.exit(0)

#----------------------------------------------------------------------
# Determine whether query contains unescaped wildcards.
#----------------------------------------------------------------------
def getQueryOp(query):
    escaped = 0
    for char in query:
        if char == '\\':
            escaped = not escaped
        elif not escaped and char in "_%": return "LIKE"
    return "="

#----------------------------------------------------------------------
# Escape single quotes in string.
#----------------------------------------------------------------------
def getQueryVal(val):
    return val.replace("'", "''")

#----------------------------------------------------------------------
# Query components.
#----------------------------------------------------------------------
class SearchField:
    def __init__(self, var, selectors):
        self.var       = var
        self.selectors = selectors

#----------------------------------------------------------------------
# Generate picklist for miscellaneous document types.
#----------------------------------------------------------------------
def miscTypesList(conn, fName):
    path = '/MiscellaneousDocument/MiscellaneousDocumentMetadata' \
           '/MiscellaneousDocumentType'
    try:
        cursor = conn.cursor()
        query  = """\
SELECT DISTINCT value
           FROM query_term
          WHERE path = '%s'
       ORDER BY value""" % path
        cursor.execute(query)
        rows = cursor.fetchall()
        cursor.close()
        cursor = None
    except cdrdb.Error, info:
        bail('Failure retrieving misc type list from CDR: %s' % info[1][0])
    html = """\
      <SELECT NAME='%s'>
       <OPTION VALUE='' SELECTED>&nbsp;</OPTION>
""" % fName
    for row in rows:
        html += """\
       <OPTION VALUE='%s'>%s &nbsp;</OPTION>
""" % (row[0], row[0])
    html += """\
      </SELECT>
"""
    return html

#----------------------------------------------------------------------
# Generate picklist for document publication status valid values.
#----------------------------------------------------------------------
def pubStatusList(conn, fName):
    return """\
      <SELECT NAME='%s'>
       <OPTION VALUE='' SELECTED>&nbsp;</OPTION>
       <OPTION VALUE='A'>Ready For Publication &nbsp;</OPTION>
       <OPTION VALUE='I'>Not Ready For Publication &nbsp;</OPTION>
      </SELECT>
""" % fName

#----------------------------------------------------------------------
# Generate picklist for countries.
#----------------------------------------------------------------------
def countryList(conn, fName):
    query  = """\
  SELECT d.id, d.title
    FROM document d
    JOIN doc_type t
      ON t.id = d.doc_type
   WHERE t.name = 'Country'
ORDER BY d.title
"""
    pattern = "<option value='CDR%010d'>%s &nbsp;</option>"
    return generateHtmlPicklist(conn, fName, query, pattern)

#----------------------------------------------------------------------
# Generate picklist for states.
#----------------------------------------------------------------------
def stateList(conn, fName):
    query  = """\
SELECT DISTINCT s.id,
                s.title,
                c.title
           FROM document s
           JOIN query_term clink
             ON clink.doc_id = s.id
           JOIN document c
             ON clink.int_val = c.id
          WHERE clink.path = '/PoliticalSubUnit/Country/@cdr:ref'
       ORDER BY s.title, c.title"""
    pattern = "<option value='CDR%010d'>%s [%s]&nbsp;</option>"
    return generateHtmlPicklist(conn, fName, query, pattern)

#----------------------------------------------------------------------
# Generate picklist for GlossaryAudience.
#----------------------------------------------------------------------
def glossaryAudienceList(conn, fName):
    query  = """\
SELECT DISTINCT value, value
           FROM query_term
          WHERE path = '/GlossaryTerm/TermDefinition/Audience'
       ORDER BY value"""
    pattern = "<option value='%s'>%s&nbsp;</option>"
    return generateHtmlPicklist(conn, fName, query, pattern)

#----------------------------------------------------------------------
# Generic HTML picklist generator.
#
# Note: this only works if the query generates exactly as many
# columns for each row as are needed by the % conversion placeholders
# in the pattern argument, and in the correct order.
#
# For example invocations, see stateList and countryList above.
#----------------------------------------------------------------------
def generateHtmlPicklist(conn, fieldName, query, pattern):
    try:
        cursor = conn.cursor()
        cursor.execute(query)
        rows = cursor.fetchall()
        cursor.close()
        cursor = None
    except cdrdb.Error, info:
        bail('Failure retrieving %s list from CDR: %s' % (fieldName,
                                                          info[1][0]))
    html = """\
      <select name='%s'>
       <option value='' selected>&nbsp;</option>
""" % fieldName
    for row in rows:
        option = pattern % tuple(row)
        html += "%s\n" % option
    html += """\
      </select>
"""
    return html
    
#----------------------------------------------------------------------
# Generate the top portion of an advanced search form.
#----------------------------------------------------------------------
def startAdvancedSearchPage(session, title, script, fields, buttons, subtitle,
                            conn, errors = "", extraField = None):

    html = """\
<!DOCTYPE HTML PUBLIC '-//IETF//DTD HTML//EN'>
<HTML>
 <HEAD>
  <TITLE>%s</TITLE>
  <META         HTTP-EQUIV  = "Content-Type"
                CONTENT     = "text/html; charset=iso-8859-1">
  <STYLE        TYPE        = "text/css">
   <!--
    .Page { font-family: Arial, Helvietica, sans-serif; color: #000066 }
   -->
  </STYLE>
 </HEAD>
 <BODY          BGCOLOR     = "#CCCCFF">
  <FORM         METHOD      = "GET"
                ACTION      = "%s/%s">
   <INPUT       TYPE        = "hidden"
                NAME        = "%s"
                VALUE       = "%s">
   <TABLE       WIDTH       = "100%%"
                BORDER      = "0"
                CELLSPACING = "0">
    <TR         BGCOLOR     = "#6699FF">
     <TD        NOWRAP
                HEIGHT      = "26"
                COLSPAN     = "2">
      <FONT     SIZE        = "+2"
                CLASS       = "Page">CDR Advanced Search</FONT>
     </TD>
    </TR>
    <TR         BGCOLOR     = "#FFFFCC">
     <TD        NOWRAP
                COLSPAN     = "2">
      <FONT     SIZE        = "+1"
                CLASS       = "Page">%s</FONT>
     </TD>
    <TR>
    <TR>
     <TD        NOWRAP
                COLSPAN     = "2">&nbsp;</TD>
    </TR>
""" % (title, BASE, script, SESSION, session, subtitle)

    if errors:
        html += """\
    <TR>
     <TD ALIGN="left" COLSPAN="2">
      %s
     </TD>
    </TR>
""" % errors

    for field in fields:
        if len(field) == 2:
            html += """\
    <TR>
     <TD        NOWRAP
                ALIGN       = "right"
                CLASS       = "Page">%s &nbsp; </TD>
     <TD        WIDTH       = "55%%"
                ALIGN       = "left">
      <INPUT    TYPE        = "text"
                NAME        = "%s"
                SIZE        = "60">
     </TD>
    </TR>
""" % field
        else:
            html += """\
    <TR>
     <TD        NOWRAP
                ALIGN       = "right"
                CLASS       = "Page">%s &nbsp; </TD>
     <TD        WIDTH       = "55%%"
                ALIGN       = "left">
%s
     </TD>
    </TR>
""" % (field[0], field[2](conn, field[1]))

    if len(fields) > 1:
        html += """\
    <TR>
     <TD        NOWRAP
                WIDTH       = "15%"
                CLASS       = "Page"
                VALIGN      = "top"
                ALIGN       = "right">Search Connector &nbsp; </TD>
     <TD        WIDTH       = "30%"
                ALIGN       = "left">
      <SELECT   NAME        = "Boolean"
                SIZE        = "1">
       <OPTION  SELECTED>AND</OPTION>
       <OPTION>OR</OPTION>
      </SELECT>
     </TD>
    </TR>"""

    if extraField:
        html += """\
    <TR>
     <TD        NOWRAP
                CLASS       = "Page"
                VALIGN      = "top"
                ALIGN       = "right">%s &nbsp; </TD>
     <TD        WIDTH       = "55%%">
%s
     </TD>
    </TR>
""" % (extraField[0], extraField[1])

    html += """\
    <TR>
     <TD        WIDTH       = "15%">&nbsp;</TD>
     <TD        WIDTH       = "55%">&nbsp;</TD>
    </TR>
   </TABLE>
   <TABLE       WIDTH       = "100%"
                BORDER      = "0">
    <TR>
     <TD        COLSPAN     = "2">&nbsp; </TD>
"""

    for button in buttons:
        html += """\
     <TD        WIDTH       = "13%%"
                ALIGN       = "center">
      <INPUT    TYPE        = "%s"
                NAME        = "%s"
                VALUE       = "%s">
     </TD>
""" % button

    html += """\
     <TD        WIDTH       = "33%">&nbsp;</TD>
    </TR>
   </TABLE>
   <BR>
"""

    return html

#----------------------------------------------------------------------
# Construct query for advanced search page.
#
# The caller passes the following arguments:
#
#   searchFields
#     a list of one or more objects with two attributes:
#       * var:       a string containing the content of one of the
#                    fields in the HTML search form
#       * selectors: this can be either a single string or a list;
#                    the normal case is the list, each member of which
#                    is a string identifying the path(s) to be matched
#                    in the query_term table; if a string contains an
#                    ampersand, then the WHERE clause to find the
#                    paths will use the SQL 'LIKE' keyword; otherwise
#                    that clause with use '=' for an exact match.
#                    If the selector is a single string, rather than
#                    a list, it represents a column in the document
#                    table which is to be compared with the value of
#                    the var member of the SearchField object (for an
#                    example, see SummarySearch.py, which has a
#                    SearchField object for checking a value in the
#                    active_status column of the document table
#
#   boolOp
#     the string value 'AND' or 'OR' - used as a boolean connector
#     between the WHERE clauses for each of the search fields passed
#     in by the first argument.
#
#   docType
#     either a string identifying which document type the result
#     set must come from, or a list of strings identifying a choice
#     of document types from the which the result set can be drawn.
#
# Modified 2003-09-11 RMK as follows:
#   If the `selectors' attribute of a SearchField object is a list,
#   then for any string in that list, if the string ends in the
#   substring "/@cdr:ref[int_val]" then instead of matching the
#   contents of the `var' attribute against the `value' column of
#   the query_term table, an lookup will be added to the SQL query
#   to find occurrences of the target string (in the var attribute)
#   in the title column of the document table, joined to the int_val
#   column of the query_term table.  This allow the users to perform
#   advanced searches which include matches against titles of linked
#   documents.
#----------------------------------------------------------------------
exampleQuery = """\
SELECT d.id
  FROM document d
  JOIN doc_type t
    ON t.id = d.doc_type
   AND t.name = 'Citation'
 WHERE d.id IN (SELECT doc_id
                  FROM query_term
                 WHERE value LIKE '%immunohistochemical%'
                   AND path = '/Citation/PubmedArticle/%/Article/%Title'
                    OR value LIKE '%immunohistochemical%'
                   AND path = '/Citation/PDQCitation/CitationTitle')
   AND d.id IN (SELECT doc_id
                  FROM query_term
                 WHERE value = 'Histopathology'
                   AND path = '/Citation/PubmedArticle/MedlineCitation/MedlineJournalInfo/MedlineTA'
                    OR path = '/Citation/PDQCitation/PublicationDetails/PublishedIn/@cdr:ref'
                   AND int_val IN (SELECT id
                                     FROM document
                                    WHERE title = 'Histopathology'))"""

def constructAdvancedSearchQuery(searchFields, boolOp, docType):
    where      = ""
    strings    = ""
    boolOp     = boolOp == "AND" and " AND " or " OR "

    for searchField in searchFields:

        #--------------------------------------------------------------
        # Skip empty fields.
        #--------------------------------------------------------------
        if searchField.var:

            queryOp  = getQueryOp(searchField.var)  # '=' or 'LIKE'
            queryVal = getQueryVal(searchField.var) # escape single quotes

            #----------------------------------------------------------
            # Remember the fields' values in a single string so we can
            # show it to the user later, reminding him what he searched
            # for.
            #----------------------------------------------------------
            if strings: strings += ' '
            strings += queryVal.strip()

            #----------------------------------------------------------
            # Start another portion of the WHERE clause.
            #----------------------------------------------------------
            if not where:
                where = " WHERE "
            else:
                where += boolOp

            #----------------------------------------------------------
            # Handle special case of match against a column in the
            # document table.
            #----------------------------------------------------------
            if type(searchField.selectors) == type(""):
                where += "(d.%s %s '%s')" % (searchField.selectors,
                                             queryOp,
                                             queryVal)
                continue

            #----------------------------------------------------------
            # Build up a portion of the WHERE clause to represent this
            # search field.
            #----------------------------------------------------------
            where += "d.id IN (SELECT doc_id FROM query_term"
            prefix = " WHERE "

            #----------------------------------------------------------
            # Build up a sub-select which checks to see if the value
            # desired for this field can be found in any of the
            # paths identified by the SearchField object's selectors
            # attribute.
            #----------------------------------------------------------
            for selector in searchField.selectors:
                pathOp = selector.find("%") == -1 and "=" or "LIKE"

                #------------------------------------------------------
                # Handle the normal case: a string stored in the doc.
                #------------------------------------------------------
                if not selector.endswith("/@cdr:ref[int_val]"):
                    where += ("%spath %s '%s' AND value %s '%s'"
                              % (prefix, pathOp, selector, queryOp, queryVal))
                else:

                    #--------------------------------------------------
                    # Special code to handle searches for linked
                    # documents.  We need a sub-subquery to look at
                    # the title fields of the linked docs.
                    #--------------------------------------------------
                    where += ("%spath %s '%s' AND int_val IN "
                              "(SELECT id FROM document WHERE title %s '%s')"
                              % (prefix, pathOp, selector[:-9],
                                 queryOp, queryVal))
                prefix = " OR "
            where += ")"

    #------------------------------------------------------------------
    # If the user didn't fill in any fields, we can't make a query.
    #------------------------------------------------------------------
    if not where:
        return (None, None)

    #------------------------------------------------------------------
    # Join the document and doc_type tables.  We could be looking for
    # a single document type or more than one document type.  If we're
    # looking for more than one document type, the query has to get
    # the document type in the result set for each of the documents
    # found.
    #------------------------------------------------------------------
    if type(docType) == type(""):
        query = ("SELECT DISTINCT d.id, d.title FROM document d "
                 "JOIN doc_type t ON t.id = d.doc_type AND t.name = '%s'"
                 % docType)
    else:
        query = ("SELECT DISTINCT d.id, d.title, t.name FROM document d "
                 "JOIN doc_type t ON t.id = d.doc_type AND t.name IN (")
        sep = ""
        for dt in docType:
            query += "%s'%s'" % (sep, dt)
            sep = ","
        query += ")"

    query += where + " ORDER BY d.title"
    return (query, strings)

#----------------------------------------------------------------------
# Construct top of HTML page for advanced search results.
#----------------------------------------------------------------------
def advancedSearchResultsPageTop(docType, nRows, strings):
    return """\
<!DOCTYPE HTML PUBLIC '-//IETF//DTD HTML//EN'>
<HTML>
 <HEAD>
  <TITLE>CDR %s Search Results</TITLE>
  <META   HTTP-EQUIV = "Content-Type"
             CONTENT = "text/html; charset=iso-8859-1">
  <STYLE        TYPE = "text/css">
   <!--
    .Page { font-family: Arial, Helvetica, sans-serif; color: #000066 }
    :link { color: navy }
    :link:visited { color: navy }
   -->
  </STYLE>
 </HEAD>
 <BODY       BGCOLOR = "#CCCCFF">
  <TABLE       WIDTH = "100%%"
              BORDER = "0"
         CELLSPACING = "0"
               CLASS = "Page">
   <TR       BGCOLOR = "#6699FF">
    <TD       NOWRAP
              HEIGHT = "26"
             COLSPAN = "4">
     <FONT      SIZE = "+2"
               CLASS = "Page">CDR Advanced Search Results</FONT>
    </TD>
   </TR>
   <TR       BGCOLOR = "#FFFFCC">
    <TD       NOWRAP
             COLSPAN = "4">
     <SPAN     CLASS = "Page">
      <FONT     SIZE = "+1">%s</FONT>
     </SPAN>
    </TD>
   </TR>
   <TR>
    <TD       NOWRAP
             COLSPAN = "4"
              HEIGHT = "20">&nbsp;</TD>
   </TR>
   <TR>
    <TD       NOWRAP
             COLSPAN = "4"
               CLASS = "Page">
     <FONT     COLOR = "#000000">%d documents match '%s'</FONT>
    </TD>
   </TR>
   <TR>
    <TD       NOWRAP
             COLSPAN = "4"
               CLASS = "Page">&nbsp;</TD>
   </TR>
""" % (docType, docType, nRows, strings)

#----------------------------------------------------------------------
# Construct HTML page for advanced search results.
#----------------------------------------------------------------------
def advancedSearchResultsPage(docType, rows, strings, filter, session = None):
    html = advancedSearchResultsPageTop(docType, len(rows), strings)

    session = session and ("&%s=%s" % (SESSION, session)) or ""
    for i in range(len(rows)):
        docId = "CDR%010d" % rows[i][0]
        title = rows[i][1]
        dtcol = "<TD>&nbsp;</TD>"
        filt  = filter
        if len(rows[i]) > 2:
            dt = rows[i][2]
            if filter:
                filt = filter[dt]
            dtcol = """\
    <TD       VALIGN = "top">%s</TD>
""" % dt

        # XXX Consider using QcReport.py for all advanced search results pages.
        if docType in ("Person", "Organization"):
            href = "%s/QcReport.py?DocId=%s%s" % (BASE, docId, session)
        elif len(rows[i]) > 2 and dt == "CTGovProtocol":
            href = "%s/QcReport.py?DocId=%s%s" % (BASE, docId, session)
        elif docType == "Summary":
            href = "%s/QcReport.py?DocId=%s&ReportType=nm%s" % (BASE, docId,
                                                                session)
        else:
            href = "%s/Filter.py?DocId=%s&Filter=%s%s" % (BASE, docId, filt,
                                                          session)
        html += """\
   <TR>
    <TD       NOWRAP
               WIDTH = "10"
              VALIGN = "top">
     <DIV      ALIGN = "right">%d.</DIV>
    </TD>
    <TD        WIDTH = "75%%">%s</TD>
%s
    <TD        WIDTH = "20"
              VALIGN = "top">
     <A         HREF = "%s">%s</A>
    </TD>
   </TR>
""" % (i + 1, cgi.escape(title), dtcol, href, docId)

        # Requested by LG, Issue #193.
        if docType == "Protocol":
            html += "<TR><TD COLWIDTH='3'>&nbsp;</TD></TR>\n"

    return html + "  </TABLE>\n </BODY>\n</HTML>\n"

#----------------------------------------------------------------------
# Create an HTML table from a passed data
#----------------------------------------------------------------------
def tabularize (rows, tblAttrs=None):
    """
    Create an HTML table string from passed data.

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
# Get the full user name for a given session.
#----------------------------------------------------------------------
def getFullUserName(session, conn):
    try:
        cursor = conn.cursor()
        cursor.execute("""\
                SELECT fullname
                  FROM usr
                  JOIN session
                    ON session.usr = usr.id
                 WHERE session.name = ?""", session)
        name = cursor.fetchone()[0]
    except:
        bail("Unable to find current user name")
    return name


#----------------------------------------------------------------------
# Borrowed from ActiveState's online Python cookbook.
#----------------------------------------------------------------------
def int_to_roman(input):
   """
   Convert an integer to Roman numerals.

   Examples:
   >>> int_to_roman(0)
   Traceback (most recent call last):
   ValueError: Argument must be between 1 and 3999

   >>> int_to_roman(-1)
   Traceback (most recent call last):
   ValueError: Argument must be between 1 and 3999

   >>> int_to_roman(1.5)
   Traceback (most recent call last):
   TypeError: expected integer, got <type 'float'>

   >>> for i in range(1, 21): print int_to_roman(i)
   ...
   I
   II
   III
   IV
   V
   VI
   VII
   VIII
   IX
   X
   XI
   XII
   XIII
   XIV
   XV
   XVI
   XVII
   XVIII
   XIX
   XX
   >>> print int_to_roman(2000)
   MM
   >>> print int_to_roman(1999)
   MCMXCIX
   """
   if type(input) != type(1):
      raise TypeError, "expected integer, got %s" % type(input)
   if not 0 < input < 4000:
      raise ValueError, "Argument must be between 1 and 3999"
   ints = (1000, 900,  500, 400, 100,  90, 50,  40, 10,  9,   5,  4,   1)
   nums = ('M',  'CM', 'D', 'CD','C', 'XC','L','XL','X','IX','V','IV','I')
   result = ""
   for i in range(len(ints)):
      count = int(input / ints[i])
      result += nums[i] * count
      input -= ints[i] * count
   return result

