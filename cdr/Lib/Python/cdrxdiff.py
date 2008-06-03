#---------------------------------------------------------
# Diff module for differencing XML documents
#
# This module provides two (perhaps more in the future) methods
# for detecting and reporting differences between XML documents.
#
# It is intended that all existing programs that perform differencing
# should convert to it and new programs should use it.  If more
# advanced differencing techniques become available, we hope to
# be able to plug them in through this module so that all programs
# performing diffs will be automatically updated.
#
# Initial output program is intended to be used in HTML reports, but
# interfaces are provided for future text mode reports.
#
# To perform a diff, a program must:
#   Instantiate one of the _Diff subclasses, currently XDiff or UDiff.
#   Optionally set any options desired.
#   Call getStyleHtml() to retrieve CSS styling information for
#     inclusion in the header of the output report.
#   Call the diff() method to perform the diff and get the result.
#
# Callers may diff any pair of:
#
#   Passed XML documents.
#   CDR document ID and version number.
#
# Callers may also specify a filter list (by name, id, or set name) and
# parameter list to be invoked in pre-filtering documents before diff'ing
# them.
#
# $Id: cdrxdiff.py,v 1.4 2008-06-03 21:14:49 bkline Exp $
#
# $Log: not supported by cvs2svn $
# Revision 1.3  2005/12/16 05:22:52  ameyer
# Changed from inefficient string buffer accumulation to sequence
# accumulation with conversion to string only upon request.
#
# Revision 1.2  2005/11/30 04:56:55  ameyer
# Added getDiffText() to retrieve buffer contents whenever.
#
# Revision 1.1  2005/11/11 00:31:09  ameyer
# Differencing module for XML docs.
#
#
#---------------------------------------------------------
import os, sys, cgi, re, difflib, textwrap, cdr, cdrcgi

_DEBUG = False

# Tags for text types for display
_TT_TAG     = "rTag"
_TT_CONTEXT = "rTxt"
_TT_ADDTEXT = "rAdd"
_TT_DELTEXT = "rDel"
_TT_DBGTEXT = "rDbg"

# Don't treat smaller equal sections as worth showing
_MIN_EQUAL = 15

# Styles
_BODYSTYLE= "background-color: white;"
_TAGSTYLE = "font-size: large; color: blue;"
_ADDSTYLE = "background-color: #FFFFBB"
_DELSTYLE = "background-color: #BBBBFF"
_DBGSTYLE = "background-color: red; color: white;"

# Default margin for text style word wrap
_WRAPMARGIN = 90

#--------------------------------------------------------------------
# Wrap long lines
#--------------------------------------------------------------------
def wrap(text, margin=90):
    """
    Wrap text to a particular margin using the textwrap module.
    """
    text     = text.replace("\r", "")
    oldLines = text.split("\n")
    newLines = []
    for line in oldLines:
        # Wrap, terminate, and begin each line with a space
        line = " " + "\n ".join(textwrap.wrap(line, margin))
        newLines.append(line)

    # Return them in a unified string
    return ("\n".join(newLines))

#---------------------------------------------------------
# Base class for differencing XML documents.
#---------------------------------------------------------
class _Diff:
    """
    Base class subclassed by XDiff and UDiff.

    Diff should not be instantiated by callers.  Instantiate
    the particular subclass instead.
    """

    def __init__(self, format="html", fullDoc=False):
        """
        Create an XDiff object for use with a particular output format.

        Pass:
            format:
                "html" = Output html.
                "text" = Output plain text.
            fullDoc:
                True   = Display first doc with diffs interspersed.
                False  = Display only diffs.
        """
        # Validate
        if format not in ("html", "text"):
            raise Exception("Invalid format: %s" % format)

        # Parms
        self._format  = format
        self._fullDoc = fullDoc

        # Default styles
        if format == "html":
            self.setStyleHtml(tagS=_TAGSTYLE, addS=_ADDSTYLE,
                              delS=_DELSTYLE, bodyS=_BODYSTYLE)
        else:
            self.__tagStyle = "@@ "
            self.__addStyle = "++ "
            self.__delStyle = "-- "

        # Construct difference here
        self.buf = []


    def setStyleHtml(self, tagS=None, addS=None, delS=None, bodyS=None):
        """
        Modify the default styles for html.

        Pass:
            tagS = String of semicolon separated CSS properties for tag
                   display.
            addS = Added text style.
            delS = Deleted text style.
            bodyS= Body style.
        """
        if tagS:
            self.__tagStyle = tagS
        if addS:
            self.__addStyle = addS
        if delS:
            self.__delStyle = delS
        if bodyS:
            self.__bodyStyle = bodyS

        self.__style = """
<style type="text/css">
 body { %s }
 .%s { %s }
 .%s { %s }
 .%s { %s }
 .%s { %s }
</style>
""" % (self.__bodyStyle,
       _TT_TAG, self.__tagStyle,
       _TT_ADDTEXT, self.__addStyle,
       _TT_DELTEXT, self.__delStyle,
       _TT_DBGTEXT, _DBGSTYLE)

    #-----------------------------------------------------------
    # Get style info.
    #-----------------------------------------------------------
    def getStyleHtml(self):
        """
        Retrieve html style information for inclusion in a report

        Return:
            String of html.
        """
        return self.__style

    #-----------------------------------------------------------
    # Get the contents of the difference buffer.
    #-----------------------------------------------------------
    def getDiffText(self, clearBuf=True):
        """
        Retrieve the contents of the difference buffer.
        Clear it after retrieval if requested.

        Pass:
            clearBuf = True = Reset buffer to empty.

        Return:
            Whatever text is in the buffer.
        """
        text = "".join(self.buf)
        if (clearBuf):
            self.buf = []
        return text

    #-----------------------------------------------------------
    # Add to the report buffer
    #-----------------------------------------------------------
    def _show(self, txt, txtType, startNew=False, endNew=False):
        """
        Append information to the diff report, with appropriate
        formatting.

        Pass:
            Text to display
            Type of text - determines formatting
            Put new line before text
            Put new line after text
        """
        if self._format == "html":
            if startNew:
                self.buf.append("<br />\n")
            # Only if text is not empty
            if txt:
                self.buf.append("<span class='%s'>%s</span>" % \
                            (txtType, cgi.escape(txt)))
            if endNew:
                self.buf.append("<br />\n")
        else:
            if startNew:
                self.buf.append("\n")
            if txt:
                self.buf.append("%s%s" % (self._format[txtType], txt))
            if endNew:
                self.buf.append("\n")

    #-----------------------------------------------------------
    # Interpret color for a difference report
    #-----------------------------------------------------------
    def showColors(self, oldName, newName):
        """
        Show the old and new names, e.g., file names or CDR IDs
        in the colors in which they will display in the report.

        Pass:
            oldName - ID or filename, or whatever.
            newName - Same.
        """
        self._show("Color coding of text found only in %s" % oldName,
                     _TT_DELTEXT, True, True)
        self._show("Color coding of text found only in %s" % newName,
                     _TT_ADDTEXT, False,True)

    #-----------------------------------------------------------
    # Obtain a document for filtering
    #-----------------------------------------------------------
    def _getDiffDoc(self, doc=None, docId=None, docVer=None,
                    filter=None, fltrParms=[]):
        """
        Get, and possibly filter, a document.

        This might do nothing if the document is passed in the doc
        parameter and there is no filtering to do.  Or it might
        fetch the current working document or a version and filter it.

        Pass:
            doc       = Document XML.  Ignored if docId passed.
            docId     = CDR doc ID.  If exists, retrieve this doc.
            docVer    = Document version number.  If None or 0, use CWD.
            filter    = If present, filter each doc with this.
                        Can be in any format acceptable to cdr.filterDoc().
            fltrParms = Parameters to filter, or None.

        Return:
            Document as a unicode string.

        Raise:
            Standard error if failure fetching or filtering doc.
        """
        # Must pass doc or docID
        if not (doc or docId):
            raise Exception("No doc or docID passed to _getDiffDoc")

        if filter:
            # If filtering, let our filter function resolve everything
            inline = False
            if not docId:
                inline = True
            resp = cdr.filterDoc("guest", filter, docId=docId, doc=doc,
                                  docVer=docVer, inline=inline, parm=fltrParms)

            # Check result
            if type(resp) == type("") or type(resp) == type(u""):
                errs = cdr.getErrors(resp, errorsExpected=False)
                if errs:
                    raise Exception("_getDiffDoc filter error: %s" % errs)
                else:
                    raise Exception("Unexpected response from _getDiffDoc: %s"
                                    % resp)

            # Must be okay
            xmlText = resp[0]

        elif docId:
            # Else resolve docId with fetch
            xmlText = cdr.getDoc("guest", docId, version=docVer)
            if cdr.getErrors(xmlText, False):
                raise Exception("Error fetching doc %s: %s" % (docId, xmlText))
        else:
            # It's just a pass through
            xmlText = doc

        # Return xml converted to unicode
        return xmlText.decode("utf-8")


    #-----------------------------------------------------------
    # Get two documents, ready for differencing
    #-----------------------------------------------------------
    def _get2Docs(self, doc1=None, doc1Id=None, doc1Ver=None,
                  doc2=None, doc2Ver=None, filter=None, fltrParms=[]):
        """
        Produce a difference report.

        This part handles retrieval of the documents.  Actual
        production of the diff is done in one of the subclasses.

        Pass:
            See diff() in subclasses.
        """
        # Fetch versions for compare
        self._xml1 = self._getDiffDoc(doc1, doc1Id, doc1Ver, filter, fltrParms)
        self._xml2 = self._getDiffDoc(doc2, doc1Id, doc2Ver, filter, fltrParms)


#---------------------------------------------------------
# Python difflib based document differencer
#---------------------------------------------------------
class XDiff(_Diff):

    def __init__(self, format="html", fullDoc=False):
        """
        Create an XDiff object for use with a particular output format.

        Pass:
            Same as _Diff.
        """
        # Construct superclass
        _Diff.__init__(self, format, fullDoc)

        # Default context characters
        if not self._fullDoc:
            self.__maxLead  = 25
            self.__maxTrail = 15

        # Regexps
        self.__linepat  = re.compile(u"\s+", re.MULTILINE)
        self.__emptypat = re.compile(u"> <")
        self.__tagpat   = re.compile(u"(?P<tag><[^<]+>)[^<]*$")

    #-----------------------------------------------------------
    # Produce a difference report
    #-----------------------------------------------------------
    def diff(self, doc1=None, doc1Id=None, doc1Ver=None,
                   doc2=None, doc2Ver=None, filter=None, fltrParms=[]):
        """
        Produce a difference report.

        Pass:
            doc1      = Document XML.  Ignored if doc1Id passed.
            doc1Id    = CDR doc ID.  If not none, retrieve this doc.
            doc1Ver   = Document version number, if None or 0, use CWD.
            doc2      = 2nd document XML, ignored if doc2Ver given.
            doc2Ver   = If present, use version of doc1Id.
            filter    = See _getDiffDoc()
            fltrParms = See _getDiffDoc()

        Return:
            String of text or html.
        """
        # Delete previous diff text, if any
        self.buf = []

        # Get the two documents to compare
        self._get2Docs(doc1, doc1Id, doc1Ver, doc2, doc2Ver,
                       filter, fltrParms)

        # Normalize to remove line ends and empty content
        # This isn't perfectly right.  Should only remove empty content
        #   from elements without mixed content
        #   Maybe later.
        self._xml1 = re.sub(self.__linepat, " ", self._xml1)
        self._xml1 = re.sub(self.__emptypat, "><", self._xml1)
        self._xml2 = re.sub(self.__linepat, " ", self._xml2)
        self._xml2 = re.sub(self.__emptypat, "><", self._xml2)

        addText   = ""
        delText   = ""
        tagText   = ""
        newTag    = ""
        leadText  = ""
        lastLead  = ""

        # Calculate differences
        sm     = difflib.SequenceMatcher(None, self._xml1.lower(),
                                               self._xml2.lower())
        rawOps = sm.get_opcodes()

        # Merge small bits of equal text into larger sections
        idx = 0
        ops = []
        for [op, i1, i2, j1, j2] in rawOps:
            if op == 'equal' and (i2 - i1) < _MIN_EQUAL and idx > 0:
                ops[idx-1][2] += (i2-i1)
                ops[idx-1][4] += (i2-i1)
                ops.append([op, 0, 0, 0, 0])
            else:
                ops.append([op, i1, i2, j1, j2])
            idx += 1

        for (op, i1, i2, j1, j2) in ops:

            # DEBUG
            if _DEBUG:
                self._show("%s %d:%d (%s) %d:%d (%s)" % \
                            (op, i1, i2, self._xml1[i1:i2],
                                 j1, j2, self._xml2[j1:j2]),
                            _TT_DBGTEXT, True, True)

            if op == 'equal':

                # Get the tag from the previous equal
                tagText = newTag
                newTag  = ""

                # Find preceding tag, if any
                tagStart = 0
                tagEnd   = 0
                m = re.search(self.__tagpat, self._xml1[i1:i2])
                if m:
                    # Extract tag
                    newTag   = m.group("tag")
                    tagStart = m.start("tag")
                    tagEnd   = m.end("tag")

                # Extract possible leading difference context
                leadStart = i2 - self.__maxLead
                if leadStart < i1 + tagEnd:
                    leadStart = i1 + tagEnd
                lastLead = leadText
                leadText = self._xml1[leadStart:i2]

                # Extract possible trailing difference context
                if not (addText or delText):
                    trailText = ""
                else:
                    trailEnd = i1 + self.__maxTrail
                    if trailEnd > leadStart - tagStart:
                        trailEnd = leadStart = tagStart
                    trailStart = trailEnd - self.__maxLead
                    if trailStart < i1:
                        trailStart = i1
                    trailText = self._xml1[trailStart:trailEnd]

                # Show tag or blank line if we haven't got one
                if (addText or delText):
                    if tagText:
                        self._show(tagText, _TT_TAG, True, True)
                        tagText = ""
                    else:
                        # No actual span output, just newline
                        self._show("", _TT_TAG, True, False)

                # DEBUG
                # leadText = "!!" + leadText + "!!"
                # trailText = "**" + trailText + "**"
                # Show everything
                if delText:
                    self._show(lastLead, _TT_CONTEXT)
                    self._show(delText, _TT_DELTEXT)
                    if not addText:
                        self._show(trailText, _TT_CONTEXT, False, False)
                if addText:
                    if not delText:
                        self._show(lastLead, _TT_CONTEXT)
                    self._show(addText, _TT_ADDTEXT)
                    self._show(trailText, _TT_CONTEXT, False, False)

            # Save different texts
            if op == 'delete':
                delText = self._xml1[i1:i2]
                addText = ""

            if op == 'insert':
                addText = self._xml2[j1:j2]
                delText = ""

            if op == 'replace':
                delText = self._xml1[i1:i2]
                addText = self._xml2[j1:j2]

        # Return it to caller
        text = "".join(self.buf)
        return text.encode('Latin-1', 'replace')


#---------------------------------------------------------
# GNU/UNIX diff based document differencer
#---------------------------------------------------------
class UDiff(_Diff):

    def __init__(self, format="html", fullDoc=False):
        """
        Create a UDiff object for use with a particular output format.

        Pass:
            See _Diff.__init__()
        """
        _Diff.__init__(self, format, fullDoc)

    #-----------------------------------------------------------
    # Produce a difference report
    #-----------------------------------------------------------
    def diff(self, doc1=None, doc1Id=None, doc1Ver=None,
                   doc2=None, doc2Ver=None, filter=None, fltrParms=[]):
        """
        Produce a difference report using an external diff program.

        Pass:
            See XDiff.diff().

        Return:
            Output of diff as a Latin-1 string.
        """
        # Resolve the documents, filtered if need be
        # Get the two documents to compare
        self._get2Docs(doc1, doc1Id, doc1Ver, doc2, doc2Ver,
                       filter, fltrParms)

        # Allow any exception to bubble up
        name1 = "CWD.xml"
        name2 = "LPV.xml"
        cmd   = "diff -a -i -w -B -U 1 %s %s" % (name2, name1)
        workDir = cdr.makeTempDir('diff')
        os.chdir(workDir)
        f1 = open(name1, "wb")
        f1.write(self._xml1.encode('Latin-1', 'replace'))
        f1.close()
        f2 = open(name2, "wb")
        f2.write(self._xml2.encode('Latin-1', 'replace'))
        f2.close()
        result = cdr.runCommand(cmd)
        try:
            os.chdir("..")
            cdr.runCommand("rm -rf %s" % workDir)
        except:
            pass

        # Return pre-formatted, colorized output, or None.
        if not result.output:
            diffText = None
        else:
            diffText = "<pre>\n"+cdrcgi.colorDiffs(cgi.escape(result.output)) \
                     + "\n</pre>\n"
        return diffText


# Test driver for stand-alone testing with files
if __name__ == "__main__":

    if len(sys.argv) < 4 or sys.argv[3] not in ('u','x'):
        sys.stderr.write("argc=%d\n" % len(sys.argv))
        sys.stderr.write("usage: cdrxdiff file1 file2 {u/x} {dbg}\n")
        sys.stderr.write("       x=XDiff style, u=UDiff style\n")
        sys.exit(1)
    if len(sys.argv) > 4:
        _DEBUG = True

    fp = open(sys.argv[1], "r")
    text1 = fp.read()
    fp.close()
    fp = open(sys.argv[2], "r")
    text2 = fp.read()
    fp.close()

    if sys.argv[3] == 'x':
        df = XDiff()
    else:
        df = UDiff()

    print("<html><head><title>Testing diffs</title></head>")
    print(df.getStyleHtml())
    print("<body>")
    print(df.showColors(sys.argv[1], sys.argv[2]))
    print(df.diff(doc1=text1, doc2=text2))
    print("</body></html>")
