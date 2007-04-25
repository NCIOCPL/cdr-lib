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
#   1. Only documents from the last Export job can be processed.
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
# $Id: AssignGroupNums.py,v 1.5 2007-04-25 03:51:44 ameyer Exp $
#
# $Log: not supported by cvs2svn $
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
                     most recent export job.  Otherwise inaccurate
                     groupings will result.

        Raises:
            StandardError if failure.
        """
        self.__jobNum = jobNum

        # Read only access to the database
        self.__conn   = cdrdb.connect('CdrGuest')
        self.__cursor = self.__conn.cursor()

        # Dictionary of docId -> groupNum
        # Only newly published docs go in here
        self.__newDoc2Group = {}

        # Dictionary for all the docs, not just new ones
        self.__doc2Group = {}

        # Dictionary of group -> list of doc ids in that group
        # Inverse of __doc2group
        self.__group2Doc = {}

        # Next group number to assign
        self.__nextGroupNum = 1

        # Regex pattern for finding ref and href attributes
        # Can't get them from query_term_pub because we need refs and
        #   hrefs from the final vendor filtered version
        self.__refPat = re.compile(r"""ref=['"](CDR[0-9]{10})""", re.MULTILINE)

        # Find all newly published documents
        # jobNum must match pub_proc id
        qry = """
SELECT id
  FROM pub_proc_cg_work
 WHERE cg_job=%d
   AND id NOT IN (
            SELECT id FROM pub_proc_cg
            -- Alternative slower and maybe less accurate
            -- SELECT doc_id FROM published_doc
        )
""" % jobNum
        try:
            self.__cursor.execute(qry)
            newDocs = [row[0] for row in self.__cursor.fetchall()]
        except cdrdb.Error, info:
            raise StandardError( \
                "GroupNums: Database error fetching list of newly published "+\
                "docs in job %d: %s" % (jobNum, str(info)))

        # All doc ids in the job
        docList = []

        # Constant meaning doc not assigned to a group yet
        self.__NOT_ASSIGNED = -1

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
            raise StandardError( \
             "GroupNums: Database error fetching list of docs in job %d: %s" %\
                (jobNum, str(info)))

        # Remember counts
        self.__newDocCount = len(newDocs)
        self.__docCount    = len(docList)

        # Create base groups for the new docs, one group per doc
        for docId in newDocs:
            self.__newDoc2Group[docId] = self.__nextGroupNum
            self.__group2Doc[self.__nextGroupNum] = [docId,]
            self.__nextGroupNum += 1

        # Process every doc in the job
        for docId in docList:

            # If this is a newly published doc, we've already handled it
            if self.__newDoc2Group.has_key(docId):
                continue

            # Get list of doc ids referenced in this document
            xml     = self.__getXml(docId)
            refList = self.__getRefs(xml)

            # Check to see if any of them are newly published docs
            groupThisDoc = self.__NOT_ASSIGNED
            for refId in refList:
                if self.__newDoc2Group.has_key(refId):
                    # Got a hit, is it the first new doc referred to?
                    newGroup = self.__newDoc2Group[refId]
                    if groupThisDoc == self.__NOT_ASSIGNED:
                        # Yes, we now have a group
                        groupThisDoc = newGroup
                    elif groupThisDoc == newGroup:
                        # A second reference to the same doc already
                        #  already referred to.  That's fine
                        pass
                    else:
                        # This doc refers to two newly published docs
                        # We can't assign one doc to two groups, so we
                        #  have to merge the groups
                        # We put them all into the first group encountered
                        #  (It's arbitrary which one we choose)
                        # groupThisDoc remains unchanged
                        self.__mergeGroups(groupThisDoc, newGroup)

            # If we have assigned the doc to a group, record the assignment
            if groupThisDoc != self.__NOT_ASSIGNED:
                self.__group2Doc[groupThisDoc].append(docId)
            else:
                # Create a new group of one for just this doc
                self.__doc2Group[docId] = self.__nextGroupNum
                self.__group2Doc[self.__nextGroupNum] = [docId,]
                self.__nextGroupNum += 1

        # All of the groups have been created but only the groups of one
        #   are recorded in self.__doc2Group
        # Fill in the rest
        self.__invertNewGroups()

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
                raise StandardError( \
                  "GroupNums: Unable to fetch xml for doc %d, rowcount=%d" % \
                   (docId, len(rows)))
            return rows[0][0]
        except cdrdb.Error, info:
            raise StandardError( \
             "GroupNums: Database error fetching list of docs in job %d: %s" %\
                (jobNum, str(info)))


    def __getRefs(self, xml):
        """
        Get a sequence of all of the CDR IDs referenced by this vendor
        format XML.  Finds values of "ref" and "href" attributes,
        stripped of any '#' fragment extensions.

        Does not worry about de-duplication.  That's handled easily
        by the caller for this application.

        Uses regular expression to find the docs.  A test comparing that
        to SAX parsing appeared 2 orders of magnitude faster and found
        all the same CDR IDs.

        Pass:
            xml - Vendor format text in utf-8 ( XXX - UNICODE?)

        Return:
            List of CDR IDs, as integers.
        """
        refList = []
        for match in self.__refPat.finditer(xml):
            cdrId = match.group(1)
            refList.append(cdr.exNormalize(cdrId)[1])

        return refList

    def __mergeGroups(self, destGrp, srcGrp):
        """
        Merge all docs in srcGrp into destGrp and eliminate srcGrp.

        Pass:
            destGrp - A group number, all docs wind up here.
            srcGrp  - Another group number, no docs wind up here and the
                      group itself is deleted.
        """
        # The first docId in the group is the head of the group, i.e.,
        #   the newly published doc for which this group is established
        srcDocId = self.__group2Doc[srcGrp][0]

        # That docId now points to the destination group
        self.__newDoc2Group[srcDocId] = destGrp

        # Python copies the srcGrp doc list in one operation
        self.__group2Doc[destGrp] += self.__group2Doc[srcGrp]

        # Delete the source grp, all docs are now in destGrp
        del (self.__group2Doc[srcGrp])

    def __invertNewGroups(self):
        """
        After all groups have been assigned, we have created lists of
        doc IDs for all newly published docs, and all docs that link
        to them.

        We didn't put the mappings for newly published docs in
        self.__doc2Group because merges might have changed things.
        It's simpler to wait until the end and do it then.

        This routine does that, making self.__doc2Group a complete mapping
        of docId -> group for the entire publishing job.
        """
        # Invert the lists of new docs and the docs that link to them
        for grp in self.__group2Doc.keys():
            for docId in self.__group2Doc[grp]:
                self.__doc2Group[docId] = grp

    #--------------------------------------------------------------
    # Public methods
    #--------------------------------------------------------------
    def getNewDocs(self):
        """
        Return a list of docIds for newly published documents in the job.
        """
        newDocs = self.__newDoc2Group.keys()
        newDocs.sort()
        return newDocs

    def getAllDocs(self):
        """
        Return a list of all docIds, newly published or not.
        """
        allDocs = self.__doc2Group.keys()
        allDocs.sort()
        return allDocs

    def getDocGroupNum(self, docId):
        """
        Return the group number for any particular document.
        """
        return self.__doc2Group[docId]

    def genNewUniqueNum(self):
        """
        Return a new number that won't conflict with any existing number
        now or later.  Used for docs to remove.
        """
        newNum = self.__nextGroupNum
        self.__nextGroupNum += 1
        return newNum

    def isDocNew(self, docId):
        """
        Tell the caller if this doc id is newly published in this job.
        """
        if self.__newDoc2Group.has_key(docId):
            return True
        return False

    def getNewDocCount(self):
        """
        Report how many docs were newly published.
        """
        return self.__newDocCount

    def getDocCount(self):
        """
        Report how many docs were processed (docs that didn't fail)
        """
        return self.__docCount


# Main routine for test purposes only
if __name__ == "__main__":

    if len(sys.argv) != 2:
        sys.stderr.write( \
            "usage: AssignGroupNums.py job_number_of_last_export job\n")
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
