#----------------------------------------------------------------------
#
# $Id: cdrpub.py,v 1.1 2002-02-20 12:59:22 bkline Exp $
#
# Public services for CDR publishing.
#
# $Log: not supported by cvs2svn $
# Revision 1.2  2001/10/12 20:32:39  bkline
# Simplified output directory naming convention.
#
# Revision 1.1  2001/10/01 20:37:03  bkline
# Initial revision
#
#----------------------------------------------------------------------

import cdr, cdrdb, os, time, xml.dom.minidom

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
