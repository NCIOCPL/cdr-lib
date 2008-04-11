import sys, re, time, cdr, cdrdb

#----------------------------------------------------------------------
# class GroupNums
#
# This class should be instantiated at the end of a publishing job to
# assign group numbers to each document that is published in that
# job.
#
# Group numbers group documents in such a way that, if one document
# fails to load in cancer.gov, the cancer.gov load software can tell
# what documents should also fail because they depend on the first
# document loading successfully.
#
# This class makes no updates to the database and does not interact
# with cancer.gov.  It simply produces some internal-to-itself data
# structures and uses them to answer questions about documents.
#
# Assumptions/Limitations:
#
#   1. Only documents being pushed from the last publishing job can
#      be processed.
#
#      The program uses pub_proc_doc and pub_proc_cg to determine what
#      documents are newly published to cancer.gov, and pub_proc_cg
#      to find the last published vendor xml.  pub_proc_cg is only
#      accurate for the last run.
#
#   2. GroupNums must be instantiated AFTER pub_proc_cg_work
#      has been populated with the results of a run, i.e.,
#      after the call to Publish.__createWorkPPC() completes.
#
# These assumptions mean that the class must be instantiated in the
# push job that calls __createWorkPPC().
#
# $Id: AssignGroupNums.py,v 1.10 2008-04-11 02:55:08 ameyer Exp $
#
# $Log: not supported by cvs2svn $
# Revision 1.9  2007/11/20 16:08:53  ameyer
# Added logic to check SummaryURL attributes, determine whether they have
# changed since the last publication and, if so, force the Summary with
# the changed attribute to become the head of a group.  If it fails, all
# Summaries or other docs that link to it will also fail so that their
# modified SummaryRef references by URL will not be loaded (and vice
# versa if one of them fails.)
#
# Revision 1.8  2007/05/11 03:54:36  ameyer
# Updated a few comments.
#
# Revision 1.7  2007/05/07 01:34:54  bkline
# Modified logic to handle links between new documents correctly.  Used
# cdr.Exception class.  Replaced some sequences with sets.
#
# Revision 1.6  2007/05/02 21:10:26  bkline
# Added use of cg_new column to detect documents which aren't available
# on Cancer.gov even though they're in the pub_proc_cg table.
#
# Revision 1.5  2007/04/25 03:51:44  ameyer
# Numerous bug fixes.  This is the first tested version.
#
# Revision 1.4  2007/04/25 00:54:47  ameyer
# Added some debugging.
# Fixed what I think was a bug.
# Version is still not fully tested.
#
# Revision 1.3  2007/04/24 01:46:20  ameyer
# Fixed some bugs.
# Added diagnostics to __main__ for stand-alone runs.
#
# Revision 1.2  2007/04/20 17:42:47  bkline
# Corrected db connection code ('connect()' for 'cursor()').
#
# Revision 1.1  2007/04/11 02:03:03  ameyer
# Initial version, not yet tested.
#
#----------------------------------------------------------------------

# Global (for this module) logger appends to publish.log
gLog = cdr.Log("publish.log", banner=None)

class GroupNums:

    def __init__(self, jobNum):
        """
        Construct a GroupNums instance for the passed job number.
        The job number should be for the last job for which documents
        are to be pushed to cancer.gov.

        The constructor does all the hard work for the class.  It
        can take a significant time to execute since it has to select
        data from the database and read and process each and every
        document in the job.

        Assumptions:


        Pass
            jobNum - Job number to process.
                     It is the caller's responsibility to always
                     pass the right job number.  It must be the
                     id of the current push job.  Otherwise inaccurate
                     groupings will result.

        Raises:
            cdr.Exception if failure.
        """
        gLog.write("AGN = Instantiating AssignGroupNums object")
        self.__jobNum = jobNum

        # Read only access to the database
        self.__conn   = cdrdb.connect('CdrGuest')
        self.__cursor = self.__conn.cursor()

        # Set of document IDs for all new documents (i.e., not on Cancer.gov)
        self.__newDocs = None

        # Dictionary of docId -> groupNum
        # Dictionary for all the docs, not just new ones
        self.__docs = {}

        # Dictionary of group -> set of doc ids in that group
        self.__groups = {}

        # Next group number to assign
        self.__nextGroupNum = 1

        # Regex pattern for finding ref and href attributes
        # Can't get them from query_term_pub because we need refs and
        #   hrefs from the final vendor filtered version
        self.__refPat = re.compile("ref=['\"](CDR\\d{10})")

        # Regex pattern for finding SummaryURL attribute from
        #   vendor filtered doc
        self.__sumURLpat = re.compile("<SummaryURL xref=['\"]([^'\"]*)['\"]")

        gLog.write("AGN: Searching for newly published documents")
        # Find all newly published documents
        # jobNum must match pub_proc id
        qry = """
SELECT id
  FROM pub_proc_cg_work
 WHERE cg_job=%d
   AND id NOT IN (
            SELECT id FROM pub_proc_cg WHERE cg_new = 'N'
            -- Alternative slower and maybe less accurate
            -- SELECT doc_id FROM published_doc
        )
""" % jobNum
        try:
            self.__cursor.execute(qry)
            self.__newDocs = set([row[0] for row in self.__cursor.fetchall()])
        except cdrdb.Error, info:
            raise cdr.Exception(
                "GroupNums: Database error fetching list of newly published "
                "docs in job %d: %s" % (jobNum, str(info)))

        # Add to the list any Summaries for which the SummaryURL changed
        self.__addChangedUrlIds()

        # Get a list of all docs in the run that aren't removals
        qry = """
SELECT id
  FROM pub_proc_cg_work
 WHERE xml IS NOT NULL
   AND cg_job = %d
""" % jobNum
        try:
            self.__cursor.execute(qry)
            docList = [row[0] for row in self.__cursor.fetchall()]
        except cdrdb.Error, info:
            raise cdr.Exception(
             "GroupNums: Database error fetching list of docs in job %d: %s" %
                (jobNum, str(info)))

        # Remember counts
        self.__newDocCount = len(self.__newDocs)
        self.__docCount    = len(docList)
        gLog.write("AGN: Found %d docs, including %d new docs" %
                   (self.__docCount, self.__newDocCount))

        # Process every doc in the job
        for docId in docList:

            # The document might already be in a group if it's new.
            groupId = self.__docs.get(docId)
            group   = groupId and self.__groups[groupId] or None

            # Get list of doc ids referenced in this document
            xml  = self.__getXml(docId)
            refs = self.__getRefs(xml)

            # Check to see if any of them are newly published docs
            linkedNewDocs = refs.intersection(self.__newDocs)

            # Ensure that we're in the same group as all of these new docs.
            for newDocId in linkedNewDocs:

              try:

                # Find out if the new document is already in a group
                newDocGroupId = self.__docs.get(newDocId)
                if newDocGroupId:

                    # If we don't already have a group, use this one
                    if not groupId:
                        groupId = newDocGroupId
                        group   = self.__groups[groupId]
                        group.add(docId)
                        self.__docs[docId] = groupId

                    # If we're already in a different group, merge the groups.
                    elif newDocGroupId != groupId:
                        self.__mergeGroups(groupId, newDocGroupId)

                # New document doesn't have a group; bring him into ours
                # if we have one.
                elif groupId:
                    group.add(newDocId)
                    self.__docs[newDocId] = groupId

                # Otherwise, we need to create a new group.
                else:
                    groupId = self.genNewUniqueNum()
                    group   = self.__groups[groupId] = set()
                    group.add(docId)
                    group.add(newDocId)
                    self.__docs[docId] = groupId
                    self.__docs[newDocId] = groupId

              except Exception, e:
                # Report to log and bubble it up
                gLog.write("AGN: Caught exception=%s value='%s' on docId=%d" %
                           (str(type(e)), str(e), docId), tback=True)
                raise


            # If we still don't have a group for this document, make one.
            if not groupId:
                groupId = self.genNewUniqueNum()
                group   = self.__groups[groupId] = set()
                group.add(docId)
                self.__docs[docId] = groupId

        gLog.write("AGN: Completed AssignGroupNums processing")

    def __getXml(self, docId):
        """
        Retrieve the vendor filtered XML text for a document by ID.
        This is NOT the XML in the all_docs table.

        Pass:
            ID of document.
        """
        qry = "SELECT xml FROM pub_proc_cg_work WHERE cg_job=%d and id=%d" %\
               (self.__jobNum, docId)
        try:
            self.__cursor.execute(qry)
            rows = self.__cursor.fetchall()
            if len(rows) != 1:
                raise cdr.Exception(
                  "GroupNums: Unable to fetch xml for doc %d, rowcount=%d" %
                   (docId, len(rows)))
            return rows[0][0]
        except cdrdb.Error, info:
            raise cdr.Exception(
             "GroupNums: Database error fetching list of docs in job %d: %s" %
                (jobNum, str(info)))


    def __getRefs(self, xml):
        """
        Get a set of all of the CDR IDs referenced by this vendor
        format XML.  Finds values of "ref" and "href" attributes,
        stripped of any '#' fragment extensions.

        Uses regular expression to find the docs.  A test comparing that
        to SAX parsing appeared 2 orders of magnitude faster and found
        all the same CDR IDs.  This should be safe because the vendor
        filter does not currently include comments in the exported XML
        documents.  If that ever changes, the worst that can happen
        will be that the groupings will be more conservative than they
        need to be.

        Pass:
            xml - Vendor format text in utf-8 ( XXX - UNICODE?)

        Return:
            Set of CDR IDs, as integers.
        """
        refs = set()
        for match in self.__refPat.finditer(xml):
            cdrId = match.group(1)
            refs.add(cdr.exNormalize(cdrId)[1])

        return refs

    def __getSummaryURL(self, xml):
        """
        Get the SummaryURL value from a Summary.

        Uses regular expression for same reason as getRefs.

        Pass:
            xml - Vendor format text in utf-8 ( XXX - UNICODE?)

        Return:
            SummaryURL as a string.
            None if not found.
        """
        match = self.__sumURLpat.search(xml, re.MULTILINE)
        if match:
            return match.group(1)
        return None

    def __addChangedUrlIds(self):
        """
        If any Summary has a changed SummaryURL, add it's ID to the
        list of docs that function as heads of groups.
        """
        # Find any Summary docs published this run
        qry = """
SELECT ppcw.id
  FROM pub_proc_cg_work ppcw
  JOIN document d
    ON ppcw.id = d.id
  JOIN doc_type t
    ON d.doc_type = t.id
 WHERE t.name = 'Summary'
 """
        try:
            self.__cursor.execute(qry)
            docList = [row[0] for row in self.__cursor.fetchall()]
        except cdrdb.Error, info:
            raise cdr.Exception(
             "GroupNums: Database error fetching list of Summary doc IDs: %s"
                % str(info))

        # If no Summaries published, we're done
        if len(docList) == 0:
            return

        # Else have to check each one to see if the doc published
        #   before this has a different value
        for docId in docList:
            try:
                self.__cursor.execute(
                 "SELECT xml FROM pub_proc_cg WHERE id = %d" % docId)
                # Extract unicode string from row and convert to utf-8
                xml = self.__cursor.fetchone()[0].encode('utf-8')
            except cdrdb.Error, info:
                raise cdr.Exception(
                 "GroupNums: DB error fetching pub_proc_cg.xml for ID=%d: %s"
                    % (docId, str(info)))

            # If not found, it's a new Summary, never before published
            # We can skip it because it will already be in the __newDocs
            #   list
            if not xml:
                continue

            # Get it's SummaryURL attribute value
            oldUrl = self.__getSummaryURL(xml)

            # Get new XML and extract value
            try:
                self.__cursor.execute(
                 "SELECT xml FROM pub_proc_cg_work WHERE id = %d" % docId)
                # Extract unicode string from row and convert to utf-8
                xml = self.__cursor.fetchone()[0].encode('utf-8')
            except cdrdb.Error, info:
                raise cdr.Exception(
             "GroupNums: DB error fetching pub_proc_cg_work.xml for ID=%d: %s"
                    % (docId, str(info)))
            newUrl = self.__getSummaryURL(xml)

            # If they aren't the same, or one didn't exist, make the
            #   doc head of a group
            if newUrl != oldUrl:
                self.__newDocs.add(docId)

        return

    def __mergeGroups(self, dstGrpId, srcGrpId):
        """
        Merge all docs in srcGrpId's group into group for dstGrpId and
        eliminate srcGrpId's group.

        Pass:
            dstGrpId - A group number, all docs wind up here.
            srcGrpId - Another group number, no docs wind up here and the
                       group itself is deleted.
        """

        # Get direct references to the sets.
        srcGrp = self.__groups[srcGrpId]
        dstGrp = self.__groups[dstGrpId]

        # Plug the documents from the source group into the destination group.
        # We must update the old group, not create a new one, because there
        #   are docs pointing to the destination group.
        dstGrp.update(srcGrp)
        for docId in srcGrp:
            self.__docs[docId] = dstGrpId

        # No longer need the source group: drop it from the dictionary.
        del self.__groups[srcGrpId]

    #--------------------------------------------------------------
    # Public methods
    #--------------------------------------------------------------
    def getNewDocs(self):
        """
        Return a list of docIds for newly published documents in the job.
        """
        gLog.write("AGN: getNewDocs()")
        newDocs = list(self.__newDocs)
        newDocs.sort()
        return newDocs

    def getAllDocs(self):
        """
        Return a list of all docIds, newly published or not.
        """
        gLog.write("AGN: getAllDocs()")
        allDocs = self.__docs.keys()
        allDocs.sort()
        return allDocs

    def getDocGroupNum(self, docId):
        """
        Return the group number for any particular document.
        """
        return self.__docs[docId]

    def genNewUniqueNum(self):
        """
        Return a new number that won't conflict with any existing number
        now or later.
        """
        newNum = self.__nextGroupNum
        self.__nextGroupNum += 1
        return newNum

    def isDocNew(self, docId):
        """
        Tell the caller if this doc id is newly published in this job.
        """
        return docId in self.__newDocs

    def getNewDocCount(self):
        """
        Report how many docs were newly published.
        """
        gLog.write("AGN: getNewDocCount()")
        return self.__newDocCount

    def getDocCount(self):
        """
        Report how many docs were processed (docs that didn't fail)
        """
        gLog.write("AGN: getDocCount()")
        return self.__docCount

    def getGroupIds(self):
        """
        Return sequence of unique group IDs assigned.
        """
        gLog.write("AGN: getGroupIds()")
        return tuple(self.__groups.keys())

# Main routine for test purposes only
if __name__ == "__main__":

    if len(sys.argv) != 2:
        sys.stderr.write(
            "usage: AssignGroupNums.py job_number_of_last_push_job\n")
        sys.exit(1)
    jobNum = sys.argv[1]

    # Time this
    startTime = time.time()
    gn = GroupNums(int(jobNum))
    endTime = time.time()

    # Processed
    print("Completed assignment in %d seconds" % (endTime - startTime))
    print("New docs = %s" % gn.getNewDocs())
    print("Doc groups:")
    for docId in gn.getAllDocs():
        print("ID: %7d  isNew=%s groupNum=%d" % (docId, gn.isDocNew(docId),
                                                 gn.getDocGroupNum(docId)))
    print "Group IDs: %s" % str(gn.getGroupIds())

