#----------------------------------------------------------------------
# class ExternalMapPatternCheck
#
# This class reads through all of the values in the external_map
# and checks them against the table of external_map_nomap_patterns.
# Any that should be made non-mappable can be made mappable.
#
# The constructor does all of the work of checking patterns.
# At the end of construction, the object contains all information
# needed to update the database and or produce reports.  The caller
# then calls specific methods to do what he wants with the info.
#
# $Id: extMapPatChk.py,v 1.1 2008-08-22 04:03:14 ameyer Exp $
#
# $Log: not supported by cvs2svn $
#----------------------------------------------------------------------

import re, cdr, cdrdb

class NomapPatterns:
    """
    Object to hold all of the pattern information.

    Regex Notes:
        We will support '_' to indicate any single character wildcard
        and '%' to represent any string of zero or more chars.  These
        translate to "." and ".*" in Python regex syntax.

        "\_" and "\%" will be interpreted as literal underscore and percent
        characters.
    """
    def __init__(self):
        """
        Load all of the patterns from the external_map_nomap_pattern table.

        Produce regular expressions suitable for testing all at once, or
        one at a time.
        """
        self.__bigPatStr   = ""        # All patterns OR'd together
        self.__smallPatStr = []        # Individual patterns

        # Characters special to regex processing
        ESCAPE_CHARS = "^$()[]*+.|\\"

        conn = cdrdb.connect()
        cursor = conn.cursor()
        try:
            cursor.execute("""
                SELECT pattern
                  FROM external_map_nomap_pattern""")
            rows = cursor.fetchall()
            cursor.close()
        except cdrdb.Error, info:
            raise cdr.Exception("Error fetching pattern table information %s"
                                % str(info))

        # Convert patterns from SQL to Python regex
        for row in rows:
            patStr = row[0]

            # Escape any chars that have special meaning
            regStr = ""
            for c in patStr:
                if c in ESCAPE_CHARS:
                    regStr += "\\"

                # Convert SQL wildcards, unless they're escaped
                if c == '%' or c == '_':
                    if regStr[-1:] != '\\':
                        if c == '%':
                            regStr += '.'
                            c = '*'
                        else:
                            c = '.'
                regStr += c

            # OR in the pattern to a single big regex
            if self.__bigPatStr:
                self.__bigPatStr += '|'
            self.__bigPatStr += "^" + regStr

            # Also save the original info, and a compiled version
            # Tuple contains:
            #   0 = compiled regex
            # The rest are used for debugging or reporting
            #   1 = regex as a string
            #   2 = original pattern from external_map_nomap_pattern table
            #   3 = id from external_map_nomap_pattern table
            self.__smallPatStr.append((re.compile(patStr), regStr, patStr, id))

        # Compile the big pattern
        self.__bigPatComp = re.compile(self.__bigPatStr, re.IGNORECASE)

    def getBigPat(self):
        """
        Return compiled pattern for finding string matching any pattern.
        """
        return self.__bigPatComp

class ExternalMapPatternCheck:

    def __init__(self, usrName=None, pw=None, session=None):
        """
        Perform all of the checks.

        Pass:
            usrName - Name in usr table.  This is the name of the person
                      who canDo this action, and who will be recorded as
                      having done it.
            pw      - Password.
            session - Alternative to usrName + pw.

        Raises:
            cdr.Exception if error.
        """
        self.__nomapPats     = None     # Object containing all patterns
        self.__matchingRows  = []       # Tuples of mapId, docId, value
        self.__checkedCount  = 0        # external_map values checked
        self.__matchedCount  = 0        # Count matching a nomap pattern
        self.__mappedCount   = 0        # Of those, count already mapped
        self.__updatedCount  = 0        # Count made non-mappable in DB

        # Arg check
        if not (session or (usrName and pw)):
            raise cdr.Exception("ExternalMapPatternCheck instantiated "
                "without either uid/pw or session")

        # Get a session if we don't have one
        if not session:
            session = cdr.login(usrName, pw)
            if session.find("Err") >= 0:
                raise cdr.Exception('Unable to login: "%s"' % session)

        # Check authorization
        if not cdr.canDo(session, "EDIT CTGOV MAP"):
            raise cdr.Exception("User not authorized to EDIT CTGOV MAP")

        # Collect all the patterns
        self.__nomapPats = NomapPatterns()
        bigPat = self.__nomapPats.getBigPat()

        # Database access
        conn = cdrdb.connect()
        cursor = conn.cursor()
        try:
            # Select all the entries to be checked against the regex
            cursor.execute("""
                SELECT m.id, m.doc_id, m.value
                  FROM external_map m
                  JOIN external_map_usage u
                    ON m.usage = u.id
                 WHERE mappable = 'Y'
                   AND u.name = 'CT.gov Facilities'
              ORDER BY value""")
        except cdrdb.Error, info:
            raise cdr.Exception("Error fetching pattern table information %s"
                                % str(info))

        # Check each one
        while True:
            row = cursor.fetchone()
            if not row:
                break
            (mapId, docId, value) = row
            self.__checkedCount += 1

            # Check against the big alterntion of all patterns
            if bigPat.search(value):

                # Got one, save it
                # Database update for mappable flag will be done later,
                #   if and only if requested
                self.__matchingRows.append(row)

                # Record stats
                self.__matchedCount += 1
                if docId:
                    self.__mappedCount += 1

        # Done with database
        cursor.close()

    def getCheckedCount(self):
        return self.__checkedCount

    def getMatchedCount(self):
        return self.__matchedCount

    def getMappedCount(self):
        return self.__mappedCount

    def getUpdatedCount(self):
        return self.__updatedCount

    def updateDatabase(self):
        """
        Update the database.

        For every external_map value that matched one of our nomap patterns:
            If it is not mapped to a document:
                Make the value non-mappable in the external_map table.
        Return:
            Number of updates performed.
        """
        cdr.logwrite("Updating database, count at start = %d" %
                      self.__updatedCount)
        conn = cdrdb.connect()
        cursor = conn.cursor()
        for row in self.__matchingRows:
            (mapId, docId, value) = row
            if not docId:
                try:
                    cdr.logwrite("Updating mapId=%d, value=%s" %
                                 (mapId, value))
                    cursor.execute("""
                        UPDATE external_map
                           SET mappable = 'N',
                               last_mod = GETDATE()
                         WHERE id = %d""" % mapId)
                    self.__updatedCount += 1
                except cdrdb.Error, info:
                    raise cdr.Exception("Error updating external_map table\n"
                            "mapId=%d value=%s\n"
                            "error=%s\n" % (mapId, value, str(info)))

        cdr.logwrite("Updating database, count at end = %d" %
                      self.__updatedCount)
        conn.commit()
        cursor.close()

    def getValues(self):
        """
        Get a list of values that were marked mappable but matched a
        nomap pattern.  All of these values are not currently mapped
        to a doc ID.  See getMappedValues() for the ones that are mapped.

        Return:
            Sequence of values.
        """
        nomaps = []
        for row in self.__matchingRows:
            # If there's no doc ID, it's one we want
            if not row[1]:
                nomaps.append(row[2])

        return nomaps

    def getMappedValues(self):
        """
        Get a list of values that were marked mappable but matched a
        nomap pattern and have in fact already been mapped to document
        IDs.  Any values found like this are errors.  Either they shouldn't
        have been mapped, or the nomap pattern was too extensive, covering
        some values that should have been mapped and some that should not.

        Return:
            Sequence of pairs of:
                (docId, value)
        """
        maps = []
        for row in self.__matchingRows:
            # If there is a doc ID, it's one we want
            if row[1]:
                maps.append((row[1], row[2]))

        return maps
