#----------------------------------------------------------------------
# $Id: GlobalChangeCTGovMappingBatch.py,v 1.4 2007-10-10 04:05:30 ameyer Exp $
#
# Examine CTGovProtocol documents and map any unmapped Facility/Name
# and LeadSponsor/Name fields for which mappings exist in the
# external_map table.
#
# This is the background half of a process begun interactively with
# CTGovMapFacilities.py
#
# The program is run in batch because it can take a long time to run,
# depending on the number of CTGovProtocols changed.
#
# Results are emailed to the user.
#
# Command line:
#   Last argument = job id of the mapping job to run.
#                   Identifies a row in batch_job table.
#
# $Log: not supported by cvs2svn $
# Revision 1.3  2007/10/05 04:38:23  ameyer
# Fixed bug - writing naked ID number instead of correct CDR000... number.
#
# Revision 1.1  2007/09/19 04:43:57  ameyer
# Initial version.
#
#----------------------------------------------------------------------
import sys, socket, cdr, cdrcgi, cdrdb, cdrbatch, ModifyDocs

_logf = cdr.Log("GlobalChange.log")

#----------------------------------------------------------------------
# Filter class, an instance of which is passed to ModifyDocs object
# to enable it to retrieve documents.
#----------------------------------------------------------------------
class Filter:

    def __init__(self, startDT, endDT):
        """
        Constructor.

        Pass:
            startDT - Update datetime, as an ISO string, for CTGovProtocol
                      versions that are to be examined for mappings.
            endDT   - Upper (recent) limit on versions to be examined.
        """
        self.__startDT = startDT
        self.__endDT   = endDT

    def getDocIds(self):
        """
        Select docs to be processed.
        """
        global _logf

        _logf.write("Getting docs from '%s' to '%s' for CTGovMapping" %
                    (self.__startDT, self.__endDT))

        conn = cdrdb.connect('CdrGuest')
        cursor = conn.cursor()
        cursor.execute("""
            SELECT distinct a.document
              FROM audit_trail a
              JOIN action act
                ON a.action = act.id
              JOIN document d
                ON a.document = d.id
              JOIN doc_type t
                ON d.doc_type = t.id
             WHERE t.name = 'CTGovProtocol'
               AND act.name IN ('ADD DOCUMENT', 'MODIFY DOCUMENT')
               AND d.active_status = 'A'
               AND a.dt >= '%s'
               AND a.dt <= '%s'""" % (self.__startDT, self.__endDT))

        # Return the IDs
        return [row[0] for row in cursor.fetchall()]

#----------------------------------------------------------------------
# An instance of the Transform class is passed to the ModifyDocs
# object to provide a transformation method, in this case using XSLT.
#----------------------------------------------------------------------
class Transform:
    def run(self, docObj):
        """
        Execute a transformation on the CTGovProtocol selected by
        getDocIds() above.

        Pass:
            docObj - cdr.Doc object created by cdr.getDoc().

        Return:
            Transformed XML, with or without changes.
        """
        global _logf

        xsl = """<?xml version='1.0' encoding='UTF-8'?>

<xsl:transform                version = '1.0'
                            xmlns:xsl = 'http://www.w3.org/1999/XSL/Transform'
                            xmlns:cdr = 'cips.nci.nih.gov/cdr'>

 <xsl:output                   method = 'xml'/>

 <!-- ======================================================================
 SQL query for looking up values in the external_map table to find
 CDR IDs of documents to which these values map.

 Pass:
    Value to lookup.
 Return:
    Content:
        CDR ID to which this string maps, or none.
 ======================================================================= -->
 <xsl:variable name='lookupQry'>
 SELECT m.doc_id
   FROM external_map m
   JOIN external_map_usage u
     ON m.usage = u.id
  WHERE m.value = ?
    AND u.name = ?
    AND bogus &lt;&gt; 'Y'
    AND mappable &lt;&gt; 'N'
    AND doc_id IS NOT NULL
 </xsl:variable>

 <!-- ======================================================================
 Use this to put a CDR00... prefix on id integers.
 ======================================================================= -->

 <xsl:variable name='idPrefix'>CDR0000000000</xsl:variable>
 <!-- ======================================================================
 Copy almost everything straight through.
 ======================================================================= -->
 <xsl:template match='@*|node()|comment()|processing-instruction()'>
  <xsl:copy>
   <xsl:apply-templates select='@*|node()|comment()|text()|
                                processing-instruction()'/>
  </xsl:copy>
 </xsl:template>

 <!-- ======================================================================
 Facilities without cdr:ref attributes
 ======================================================================= -->
 <xsl:template match="/CTGovProtocol/Location/Facility/Name[not(@cdr:ref)]">

   <!-- For each one, invoke the lookupQry to find any CDR ID matching
        the element content for the Name.
        ~param = current element content -->

   <!-- This is fragile, but is there any other way to do it? -->
   <xsl:variable name="countryName">
     <xsl:choose>
       <!-- XXX Not sure USA is the only country needing remap, bit it's the
            most common one.  Otherwise need to do one or more additional
            database lookups. -->
       <xsl:when test="../PostalAddress/Country='U.S.A.'"
          >United States</xsl:when>
       <xsl:otherwise
          ><xsl:value-of select="../PostalAddress/Country"/></xsl:otherwise>
      </xsl:choose>
   </xsl:variable>

   <!-- More fragility -->
   <xsl:variable name="stringVal"
                 select="concat(.,'|',
                 normalize-space(../PostalAddress/City), '|',
                 normalize-space(../PostalAddress/PoliticalSubUnit_State), '|',
                 normalize-space(../PostalAddress/PostalCode_ZIP), '|',
                 normalize-space($countryName))"/>

   <!-- Search the external_map table -->
   <xsl:variable name="resultSet"
               select="document(concat('cdrutil:/sql-query/', $lookupQry,
                        '~', $stringVal, '~', 'CT.gov Facilities'))"/>
   <xsl:variable name="plainId"
               select="$resultSet/SqlResult/row/col[@name='doc_id']"/>
   <xsl:variable name='cdrId' select=
     "concat(substring($idPrefix, 1, 13-string-length($plainId)), $plainId)"/>

<!-- DEBUG
<xsl:element name="resultSet"><xsl:value-of select="$resultSet"/></xsl:element>
<xsl:element name="stringVal"><xsl:value-of select="$stringVal"/></xsl:element>
<xsl:element name="plain"><xsl:value-of select="$plainId"/></xsl:element>
<xsl:element name="cdrId"><xsl:value-of select="$cdrId"/></xsl:element>
     DEBUG -->

   <xsl:choose>
     <xsl:when test="$plainId">
       <!-- CDR ID found for string, copy element and add attribute -->
       <xsl:message terminate="no">Adding attribute</xsl:message>
       <xsl:element name="Name">
         <xsl:attribute name="cdr:ref">
           <xsl:value-of select="$cdrId"/>
         </xsl:attribute>
         <xsl:value-of select="."/>
       </xsl:element>
     </xsl:when>
     <xsl:otherwise>
       <!-- No CDR ID found.  Just copy what's there -->
       <xsl:message terminate="no">Leaving alone</xsl:message>
       <xsl:copy>
         <xsl:apply-templates select="@*|node()|comment()|text()|
                                      processing-instruction()"/>
       </xsl:copy>
     </xsl:otherwise>
   </xsl:choose>

 </xsl:template>

 <!-- ======================================================================
 LeadSponsors without cdr:ref attributes.  Simpler query.
 ======================================================================= -->
 <xsl:template match="/CTGovProtocol/Sponsors/LeadSponsor[not(@cdr:ref)]">

   <!-- For each one, invoke the lookupQry to find any CDR ID matching
        the element content for the Name.
        ~param = current element content -->

   <xsl:variable name="stringVal" select="."/>
   <xsl:variable name="resultSet"
               select="document(concat('cdrutil:/sql-query/', $lookupQry,
                        '~', $stringVal, '~', 'CT.gov Agencies'))"/>
   <xsl:variable name="plainId"
               select="$resultSet/SqlResult/row/col[@name='doc_id']"/>
   <xsl:variable name='cdrId' select=
     "concat(substring($idPrefix, 1, 13-string-length($plainId)), $plainId)"/>

   <xsl:choose>
     <xsl:when test="$plainId">
       <!-- CDR ID found for string, copy element and add attribute -->
       <xsl:element name="LeadSponsor">
         <xsl:attribute name="cdr:ref">
           <xsl:value-of select="$cdrId"/>
         </xsl:attribute>
<!-- DEBUG
<xsl:attribute name="cdrId">
  <xsl:value-of select="$cdrId"/>
</xsl:attribute>
     DEBUG -->
         <xsl:value-of select="."/>
       </xsl:element>
     </xsl:when>
     <xsl:otherwise>
       <!-- No CDR ID found.  Just copy what's there -->
       <xsl:copy>
         <xsl:apply-templates select="@*|node()|comment()|text()|
                                      processing-instruction()"/>
       </xsl:copy>
     </xsl:otherwise>
   </xsl:choose>

 </xsl:template>
</xsl:transform>"""

        # Perform the filtering
        response = cdr.filterDoc('guest', xsl, doc = docObj.xml, inline = 1)
        if type(response) in (type(""), type(u"")):
            msg = "Failure in global change filter: %s" % response
            _logf.write(msg)
            raise Exception(msg)
        return response[0]

#----------------------------------------------------------------------
# Fatal error processing
#----------------------------------------------------------------------
def fatal(msg):
    """
    Complete processing.  Send final report.  Set status.  Abort.

    Pass:
        msg - Error message.
    """
    global _logf

    html = """
<html>
<head>
 <title>CTGov Mapping Global Change Report - Fatal Error</title>
</head>
<body>
 <h2>Global Change of CTGov Mapping Values - Fatal Error</h2>

<p>An error occurred while attempting to execute the batch portion
of the CTGov Mapping Global Change.  The content of the error message
is:</p>

<p><em>%s</em>
</body>
</html>
""" % msg
    report(html)

    _logf.write("Fatal error: %s" % msg)

    # Unsuccessful completion
    batchJob.setStatus(cdrbatch.ST_ABORTED)


#----------------------------------------------------------------------
# Final report
#----------------------------------------------------------------------
def report(html):
    """
    Send a final report as email to all recipients.

    Pass:
        html - Content of email, in HTML.
    """
    global _logf

    # Make html safe for email
    if type(html)==type(u""):
        safeHtml = cdrcgi.unicodeToLatin1 (html)
    else:
        safeHtml = html

    # Send it by email
    emailList = batchJob.getEmailList()
    _logf.write("Sending email to: %s" % emailList)
    resp = cdr.sendMail ("cdr@%s.nci.nih.gov" % socket.gethostname(),
                         emailList,
                         subject="Final report on global change",
                         body=safeHtml,
                         html=1)
    if resp:
        # Returns None if no error
        _logf.write("Email of final report failed: %s" % resp)
    else:
        _logf.write("Completed CTGov Mapping Global Change")

#----------------------------------------------------------------------
# Main
#----------------------------------------------------------------------
if __name__ == "__main__":

    # Job ID on command line
    if len(sys.argv) != 2:
        fatal("Expecting single job ID on command line\n"
                    "  Got: %s" % str(sys.argv))

    jobId = 0
    try:
        jobId = int(sys.argv[1])
    except ValueError:
        fatal("Expecting job ID on command line, got: '%s'" % sys.argv[1])

    try:
        batchJob = cdrbatch.CdrBatch(jobId=jobId)
    except Exception, info:
        fatal("Error loading batch job: '%s'" % str(info))

    # Get parameters
    try:
        session  = batchJob.getParm(cdrcgi.SESSION)
        runMode = batchJob.getParm("runMode")
        startDt  = batchJob.getParm("startDt")
        endDt    = batchJob.getParm("endDt")
    except Exception, info:
        fatal("Error fetching parms: '%s'" % str(info))

    # Create modification job
    (userid, pw) = cdr.idSessionUser(session, session)
    if runMode == "run":
        testMode = False
    else:
        testMode = True
    modifyJob = ModifyDocs.Job(userid, pw, Filter(startDt, endDt), Transform(),
         "Global change CTGovProtocol unmapped strings (request #3451).",
         testMode=testMode)

    # Perform the changes
    modifyJob.run()

    # Construct tables of results - docs failing checkout
    results = modifyJob.getNotCheckedOut(markup=True)
    if results:
        notCheckedHtml = "<h3>Documents that could NOT be checked out</h3>\n" \
                         + results
    else:
        notCheckedHtml = \
            "<h3>All selected docs successfully checked out</h3>\n"

    # Docs modified
    results = modifyJob.getProcessed(markup=True)
    if results:
        processedHtml="<h3>Documents successfully processed</h3>\n" + results
    else:
        processedHtml="<h3>No selected docs successfully processed</h3>\n"

    # Construct report
    html = """
<html>
<head>
 <title>CTGov Mapping Global Change Report</title>
</head>
<body>
 <h2>Final report on Global Change of CTGov Mapping Values</h2>

<p>The batch job started to attempt to map unmapped Facility/Name
and LeadSponsor strings in CTGovProtocols is complete.  Documents
were examined that have been modfied between %s and %s.<p>

<p>The global change was run in %s mode.</p>

<p>Results are as follows:</p>

<table border="1" align="center" cellpadding="6">
 <tr>
  <td>Documents selected for examination</td>
  <td>%d</td>
 </tr>
 <tr>
  <td>Documents processed</td>
  <td>%d</td>
 </tr>
 <tr>
  <td>Distinct documents saved</td>
  <td>%d</td>
 </tr>
 <tr>
  <td>Distinct versions saved</td>
  <td>%d</td>
 </tr>
</table>
<hr />
%s
<hr />
%s
<hr />
""" % (startDt, endDt, runMode, modifyJob.getCountDocsSelected(),
       modifyJob.getCountDocsProcessed(), modifyJob.getCountDocsSaved(),
       modifyJob.getCountVersionsSaved(), notCheckedHtml, processedHtml)

    if runMode == "test":
        html += """
<p>See
<a href="http://%s.nci.nih.gov/cgi-bin/cdr/ShowGlobalChangeTestResults.py">
Global Change Test Results</a> for change/diff information for test
results.</p>
""" % socket.gethostname()
    html += " </body>\n</html>\n"

    # Report by email
    report(html)

    # Completion
    batchJob.setStatus(cdrbatch.ST_COMPLETED)
