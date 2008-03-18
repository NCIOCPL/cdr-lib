#!/usr/bin/python

"""

    Module for generating RTF documents

    Summary

        import RtfWriter
        doc = RtfWriter.Document(title = 'Dear John', subject = 'Goodbye')
        doc.addPara('Dear John,')
        doc.addPara('I must bid you adieu ....')
        doc.addPara('Sincerely, Abigail')
        doc.write('DearJohn.rtf')
        
        Also provides a subclass of Document, FormLetter, which is
        used for generating CDR Board Member Correspondence mailers.

    Limitations:

        The current version only supports single-level lists.

    To do:

        Add support for ExternalRef elements in miscellaneous docs.
"""

#----------------------------------------------------------------------
#
# $Id: RtfWriter.py,v 1.5 2008-03-18 12:40:16 bkline Exp $
#
# Module for generating RTF documents.
#
# $Log: not supported by cvs2svn $
# Revision 1.4  2005/03/03 14:42:30  bkline
# Fixed documentation typos.
#
# Revision 1.3  2005/02/24 04:32:04  bkline
# Cleanup of pydoc comments.
#
# Revision 1.2  2005/02/24 02:20:35  bkline
# Fleshed out comments for pydoc.
#
#----------------------------------------------------------------------
import time, re

#----------------------------------------------------------------------
# Regular expression pattern to recognize non-ASCII characters.
#----------------------------------------------------------------------
_nonAsciiPattern = re.compile(u"([\u0080-\uFFFD])")

def fix(text):
    
    """
    Escape problematic characters for RTF.  Returns a cleaned-up
    version of the passed string, converted to 7-bit ASCII,
    suitable for insertion into an RTF document.
    """
    
    text = text.replace("\\", "\\\\")
    text = text.replace("{", "\\{").replace("}", "\\}")
    text = _nonAsciiPattern.sub(_replace, text)
    if isinstance(text, unicode):
        text = text.encode("ascii")
    return text

def _replace(match):
    
    "Function passed to regular expression sub() method."
    
    return _rtfForUnicodeChar(match.group(1))

def _rtfForUnicodeChar(ch):
    
    "Convert a Unicode character into its RTF representation."
    
    code = ord(ch)
    return "{\\uc1\\u%d?}" % code

class Font:
    
    """
    Object for representing a single font in an RTF document.

    Switch to a font by inserting the command \\fN in the body of
    the document, where N is the ID (number) of the desired font.
    
    A default RtfWriter Document has three fonts pre-registered:

        Document.SERIF     (ID 0: Times New Roman; family roman)
        Document.SANSSERIF (ID 1: Arial; family swiss)
        Document.FIXED     (ID 2: Arial; family swiss)

    Use the Document.addFont() method to register additional fonts
    in a document.  This method ensures that unique IDs are
    generated for the registered fonts.
    """
    
    def __init__(self, id, name, family = "nil", alt = None):

        """
        Creates a new RTF Font object.  Pass in a unique
        font number and the name by which the font will be recognized
        by the user's system (e.g., 'Arial').  Optionally pass
        the generic font type (roman, swiss, modern, script, etc.),
        and/or an alternate font to be used if the named font is
        not found.
        """
        
        self.__id     = id
        self.__name   = name
        self.__family = family
        self.__alt    = alt

    def getId(self):

        "Access method for ID used to set the font later in the document."
        
        return self.__id
    
    def getName(self):

        "Access method for the font's name (e.g., 'Times New Roman')."
        
        return self.__name
    
    def getFamily(self):

        "Access method for the generic font category (e.g., 'swiss')."
        
        return self.__family
    
    def getAlternateFont(self):

        """Access method for name of alternate font to be used if the
        specified font in the font table is not available."""
        
        return self.__alt

    def getRtf(self):

        "Generate the RTF code for the font."

        rtf = "{\\f%d\\f%s %s" % (self.__id, self.__family, self.__name)
        if self.__alt:
            rtf += "{\\*\\falt %s}" % self.__alt
        return rtf + ";}"

class List:

    """
    Object for representing a single list in an RTF document.

    A default RtfWriter Document is initialized with a single
    list unnumbered (bulleted) list type.  Use this list type
    for all bulleted lists.  Each numbered list must have its
    type registered separately (otherwise all the numbered
    lists will share the same numbering sequence).  Use the
    Document.addList() method to create these lists.

    To insert the list into the document, create a block with
    the RTF command \lsN, where N is the ID of the registered
    list type.  I find it helpful to also add a left indent
    command (e.g., \li580); otherwise the list looks like it's
    jammed too far to the left of the page.  Inside the block
    follow each list item with a \par command.  For example
    (backslashes doubled for pydoc):

        listId = doc.addList(List.ARABIC)
        aList  = '{\\\\li580\\\\ls%d\\n' % listId
        aList += 'First item on the list\\\\par\\n'
        aList += 'Second item on the list\\\\par\\n'
        aList += '}\\n'
        doc.addRawContent(aList)

    See also MiscellaneousDoc.addList()
    """
    
    BULLETED  = 1   # listid for un-numbered lists
    ARABIC    = 0   # number type for numbered lists
    ROMAN_UC  = 1   # (I., II., III., IV., etc.)
    ROMAN_LC  = 2   # (i., ii., etc.)
    LETTER_UC = 3   # (A., B., C., etc.)
    LETTER_LC = 4   # (a., b., c., etc.)
    ORDINAL   = 5   # (1st, 2nd, 3rd, etc.)
    CARDINAL  = 6   # (One, Two, Three)
    ORD_TEXT  = 7   # (First, Second, Third)
    NONE      = 23  # number type for bulleted lists
    __nextId  = 0   # class variable for generating list IDs

    def __init__(self, numberType):

        """
        Creates a new list type.  Pass in List.NONE for an
        unnumbered list, or a real number type (e.g., List.ARABIC).
        Typically, user code will invoke this indirectly,
        through the Document.addList() method (q.v.).

        Attributes:

            id   - The identifier used to create the list
                   in the document.
        """
        
        self.id                 = List.__getNextId()
        self.__numberType       = numberType
        if self.__numberType != List.NONE:
            self.__levelText    = "\\'02\\'00."
            self.__levelNumbers = "\\'01"
        else:
            self.__levelText    = "\\'01\\u8226 *"
            self.__levelNumbers = ""

    def __getNextId(listClass):
        listClass.__nextId += 1
        return listClass.__nextId
    __getNextId = classmethod(__getNextId)

    def getRtf(self):

        "Generates the RTF code to register the list type."

        return """\
{\\list\\listtemplateid%d\\listid%d\\listsimple1
{\\listlevel\\levelnfc%d\\leveljc2\\levelstartat1\\levelfollow1
{\\leveltext %s;}
{\\levelnumbers %s;}}}
""" % (self.id, self.id, self.__numberType,
       self.__levelText, self.__levelNumbers)

class Document:

    """
    Object for an RTF document.

    Attributes:

        title       - document title stored with document metadata;
                      set in constructor
        author      - name of document author, stored with metadata;
                      set in constructor
        company     - name of company for or by which the document was
                      written; stored with document metadata; set in
                      constructor
        subject     - topic of document; stored with docinfo; set in
                      constructor
        rtfVersion  - always 1; do not alter
        charset     - initialized to 'ansi'
        defaultFont - initialized to Document.SERIF
        generator   - initialized to 'RTF Writer'
        extraHeader - initialized to an empty string; can be used for
                      custom RTF header information
        defaultLang - initialized to 1033 (U.S. English)
        colors      - 0 = default color of application
                      1 = black
                      2 = white (can be used for invisible text)
        fonts       - seeded with SERIF, SANSSERIF, and FIXED; use
                      doc.addFont() method to augment the list of
                      available fonts
        lists       - initialized with a bulleted list type;
                      manipulate with addList() method
        margL       - left margin in twips (initialized to 1 inch)
        margR       - right margin in twips (initialized to 1 inch)
        margT       - top margin in twips (initialized to 1 inch)
        margB       - bottom margin in twips (initialized to 1 inch)
        fSize       - default document font size in 1/2 points;
                      initialized to 22 (11 pt)

    Notes:

        The margin and default document font size attributes can be modified
        directly, as can the defaultFont and extraHeader attributes.

    """
    
    #------------------------------------------------------------------
    # Class constants.
    #------------------------------------------------------------------
    SERIF     = 0
    SANSSERIF = 1
    FIXED     = 2
    BLACK     = 1
    WHITE     = 2

    def __init__(self,
                 title   = None,
                 author  = None,
                 company = None,
                 subject = None):

        """
        Creates a new RTF document object.  All arguments are optional.
        """
        
        self.title       = title
        self.author      = author
        self.company     = company
        self.subject     = subject
        self.rtfVersion  = 1                 # major version of RTF spec
        self.charset     = "ansi"
        self.defaultFont = self.SERIF
        self.generator   = "RTF Writer"
        self.extraHeader = ""
        self.defaultLang = 1033              # U.S. English
        self.colors      = [(),              # application's default color
                            (0, 0, 0),       # black
                            (255, 255, 255)] # white
        self.fonts       = [Font(self.SERIF,     "Times New Roman", "roman"),
                            Font(self.SANSSERIF, "Arial",           "swiss"),
                            Font(self.FIXED,     "Courier New",     "modern")]
        self.lists       = [List(List.NONE)]
        self.margL       = 1440              # 1-inch left margin
        self.margR       = 1440              # 1-inch right margin
        self.margT       = 1440              # 1-inch top margin
        self.margB       = 1440              # 1-inch bottom margin
        self.fSize       = 22                # 11-point default font size
        self.__bodyParts = []

    def write(self, file):

        """
        Serializes the RTF document to disk.  Argument can either be a 
        path (absolute or relative) name for the file, or an open file
        object.
        """
        
        if isinstance(file, (str, unicode)):
            f = open(file, "wb")
        else:
            f = file
        rtf = self.getRtf()
        if isinstance(file, (str, unicode)):
            f.write(rtf)
            f.close()

    def getRtf(self):

        """
        Serializes the document to an in-memory string.  Useful if
        another target than a disk file will be used for output
        (for example, a database table or a CGI response from a
        web server).  This method will also be used if further
        processing must be done to the document before saving it
        (for example, plugging in values for delimited placeholders).
        """
        
        rtf = '{' + self.__getHeader() + self.__getInfo() + self.__getDocFmt()
        for bodyPart in self.__bodyParts:
            rtf += bodyPart
        return rtf + '}'

    def addRawContent(self, rawContent):

        """
        Appends a string containing RTF markup to be used in the
        body of the document.
        """
        
        self.__bodyParts.append(rawContent)

    def addPara(self, text):

        """
        Append passed text as a separate paragraph, terminated by
        two paragraph marks.  Override this method to alter this
        behavior.
        """
        
        self.__bodyParts.append(fix(text) + "\\par\\par\n")

    def addFont(self, name, family = "nil", alt = None):

        """
        Registers a new font for use in the document.  Returns the
        ID to be embedded in the \\fN command to use the font in
        the document (where N is the font's ID).  Pass in the
        name by which the font will be recognized by the system
        on the user's machine.  Optionally pass the name of the
        generic font type (e.g., roman, swiss, modern, etc.),
        and/or the name of an alternate font to be used if the
        desired font is not present.
        """
        
        id = len(self.fonts)
        self.fonts.append(Font(id, name, family, alt))
        return id

    def addList(self, numberType):

        """
        Registers a new list type.  Pass in the style of numbering to
        be used (currently only List.ARABIC is supported).  The ID
        of the new list is returned to be used in the \lsN command
        in the document for the actual list, where N is the ID returned
        by this method.
        2008-03-17: added support for most other list types.
        """
        
        newList = List(numberType)
        self.lists.append(newList)
        return newList.id
    
    def __getHeader(self):

        """
        Private method to assemble the header for the document.
        Invoked by getRtf().
        """

        return ("\\rtf%d"                    # RTF version
                "\\%s"                       # Character set used in document
                "\\deff%d"                   # Index of default font
                "\\deflang%d\n"              # Default language
                % (self.rtfVersion, self.charset, self.defaultFont,
                   self.defaultLang) +
                self.__getFontTable()  +
                self.__getColorTable() +
                self.__getListTables()  +
                self.__getGenerator())
    
    def __getFontTable(self):

        """
        Private method to assemble the header's font table for the
        document.
        """
        
        fontTable = "{\\fonttbl\n"
        for font in self.fonts:
            fontTable += "%s\n" % font.getRtf()
        return fontTable + "}\n"
    
    def __getColorTable(self):

        """
        Private method to assemble the header's color table for the
        document.
        """
        
        if not self.colors:
            return ""
        colorTable = "{\\colortbl\n"
        for triplet in self.colors:
            if triplet:
                colorTable += "\\red%d\\green%d\\blue%d" % (triplet[0],
                                                            triplet[1],
                                                            triplet[2])
            colorTable += ";\n"
        return colorTable + "}\n"

    def __getListTables(self):

        """
        Private method used to assemble the header information for
        tables found in the document.
        """
        
        listTables = "{\\*\\listtable\n"
        overrideTables = "{\\listoverridetable\n"
        for listDef in self.lists:
            id = listDef.id
            listTables += listDef.getRtf()
            overrideTables += ("{\\listoverride\\listid%d"
                               "\\listoverridecount0\\ls%d}\n" % (id, id))
        return listTables + "}\n" + overrideTables + "}\n"

    def __getGenerator(self):

        """
        Private method to create the string identifying the software
        used to generate the RTF document.
        """
        
        return "{\\*\\generator %s;}\n" % fix(self.generator)

    def __getDocFmt(self):

        "Private method to assemble the document formatting information."
        
        # Add \truncatefontheight to have font sizes rounded down, not up
        # Add \lytprtmet to use printer metrics for page layout
        return ("\\pard\\plain"              # clear out settings
                "\\fs%d\n"                   # font size in 1/2 points
                "\\margl%d"                  # left margin
                "\\margr%d"                  # right margin
                "\\margt%d"                  # top margin
                "\\margb%d\n"                # bottom margin
                % (self.fSize, self.margL, self.margR, self.margT, self.margB))

    def __getInfo(self):

        "Private method to assemble the optional document metadata."
        
        info = "{\\info\n"
        if self.title:
            info += "{\\title %s}\n" % fix(self.title)
        if self.author:
            info += "{\\author %s}\n" % fix(self.author)
        if self.company:
            info += "{\\company %s}\n" % fix(self.company)
        info += "{\\creatim %s}\n" % self.__getTime()
        if self.subject:
            info += "{\\subject %s}\n" % fix(self.subject)
        return info + '}\n'
    
    def __getTime(self):

        "Private method to generate the document creation time."
        
        now = time.localtime()
        return "\\yr%d \\mo%d \\dy%d \\hr%d \\min%d \\sec%d" % (now[0],
                                                                now[1],
                                                                now[2],
                                                                now[3],
                                                                now[4],
                                                                now[5])

class FormLetter(Document):
    
    """
    Subclass for RTF document used to generate PDQ Board Member mailers.

    This is the class which is used directly by the mailer software.
    The processing for a set of correspondence mailers works like this:

        1. Determine which template to use for the main body of the
           letter.

        2. Create a new FormLetter object, passing in the name of the
           template, the title, the subject, the pathname for the
           location of the DHHS logo, and (for Editorial Board
           invitation letters) the specific invitation document
           used for this board.

        3. Serialize the FormLetter object to an in-memory string,
           using the toRtf() method of the base class.

        4. Gather the values for the following placeholders common
           to all instances which will be printed for this batch of
           letters (see the spreadsheet with additional documentation
           for these placeholders):

               @@DATE@@
               @@BOARDNAME@@
               @@MEETINGDATE@@
               @@MEETINGDATES@@
               @@BMNAME@@
               @@BMEMAIL@@
               @@ECNAME@@
               @@ATECNAME@@
               @@SUMMARYTYPE@@
               @@WORKGROUPS@@
               @@EDBOARDNAME@@
               @@ADVBOARDNAME@@
               @@DATEPLUS1MONTH@@
               @@DATEPLUS2WEEKS@@
               @@SUMMARYTOPICS@@ [if appropriate]

        5. Replace these batch-level placeholders with the collected
           values and save this copy as the template from which the
           individual letters will be customized for each recipient.

        6. For each recipient:

           * Gather the values for the following recipient-specific
             placeholders:

               @@ADDRBLOCK@@
               @@FORENAME@@
               @@SURNAME@@
               @@MEMBERNAME@@
               @@TERMYEARS@@
               @@SUMMARYLIST@@
               @@CONTACTINFO@@

           * Replace these recipient-specific placeholders with the
             collected values, saving the result in a separate string.

           * Create a new file in the output directory for the job,
             and write the string for this letter into the file.
    """
    
    def __init__(self, 
                 title      = "[CDR Form Letter]",
                 subject    = None,
                 template   = None,
                 pngName    = "dhhslogo.png",
                 author     = "cdr",
                 company    = "CIPS",
                 binImage   = False,
                 invitePara = ""):

        """
        Creates a new instance of the derived FormLetter class.  The
        caller should override the pngName parameter with the correct
        location of the DHHS logo image if it is not in the current
        working directory.  Set binImage if the image is not to be
        stored using only 7-bit ASCII characters (though this causes
        the resulting RTF document to deviate slightly from the base
        standard).  The template argument specifies the pathname
        location for the file which holds the RTF template for the
        body of this letter.  For editorial board invitation letters,
        each board has a section which is specific to that board,
        and the name of miscellaneous document containing this
        custom section is passed as the optional invitePara parameter.
        The remaining parameters are self-explanatory.
        """
        
        Document.__init__(self, title, author, company, subject)
        self.generator       = "CDR RTF Writer"
        self.pngName         = pngName       # location of bitmap file for logo
        self.margL           = 1080          # 3/4-inch left margin
        self.margR           = 720           # 1/2-inch right margin
        self.margT           = 1080          # 5/8-inch top margin
        self.margB           = 1260          # 7/8-inch bottom margin
        self.fSize           = 21            # 10.5-point default font size
        self.baskervilleFont = self.addFont("Baskerville Old Face",
                                            "roman",
                                            "Times New Roman")
        self.__addLetterHead(binImage)
        if template:
            try:
                fp   = file(template, "rb")
                body = fp.read()
                fp.close()
            except Exception, e:
                raise Exception("loading %s: %s" % (template, str(e)))
            if invitePara:
                replacement = "@@MISCDOC:%s@@" % invitePara
                body = body.replace("@@INVITATION@@", replacement)
            pattern = re.compile(u"@@MISCDOC:(.*?)@@")
            body    = pattern.sub(lambda p: self.__loadMiscDoc(p), body)
            self.addRawContent(body)

    def __writeImageBytes(self, image, bin = False):

        """
        Private method used to embed a serialized representation of
        an image (in this case, the DHHS logo used in the letterhead).
        """
        
        if not image:
            raise Exception("image not found")
        if bin:
            return "\\bin%d %s" % (len(image), image)
        result = ""
        i = 0
        while i < len(image):
            if i % 39 == 0:
                result += "\n"
            result += "%02X" % ord(image[i])
            i += 1
        return result

    def __addLetterHead(self, binImage):

        """
        Private method used to insert the letterhead as the start
        of the letter's body.
        """
        f    = open(self.pngName, "rb")
        logo = f.read()
        f.close()
        dhhs = "DEPARTMENT OF HEALTH & HUMAN SERVICES"
        phs  = "\\tab Public Health Service\\par"
        nih  = "\\tab National Institutes of Health\\par"
        nci  = "\\tab National Cancer Institute\\par"
        line = "{\\sa5\\sl-1\\slmult0\\brdrb\\brdrs\\brdrw10\\brsp20\\par}"
        tab  = 6000                          # tab stop at ~4"
        self.addRawContent("{\\absw1152\n"   # create ~2cm-wide frame
                           "\\*\\shppict"    # insert a picture shape
                           "{\\pict"         # the picture object
                           "\\pngblip"       # PNG image
                           "\\picscalex25"   # reduce to 1/4 size ...
                           "\\picscaley25"   # ... in both dimensions
                           "%s}\\par}\n"     # the image data
                           % self.__writeImageBytes(logo, binImage))
        self.addRawContent("{\\pard"         # clear out paragraph settings
                           "\\pvpg\\phpg"    # page-relative positioning
                           "\\posx2300"      # left edge of logo text
                           "\\posy1400"      # top edge of same
                           "\\absw9000"      # 6 inches wide
                           "\\tx%d\n"        # tab stop to position names
                           "{\\f%d"          # font with old-style ampersand
                           "\\b"             # turn on bold font
                           "\\kerning2 "     # turn on kerning
                           "%s}\n"           # DHHS
                           "\\f%d"           # switch to sans-serif font
                           "%s\n"            # PHS
                           "\\par\n"         # insert extra vertical space
                           "%s\n"            # line (empty para with border)
                           "\\sa5"           # a little more space after line
                           "%s\n"            # NIH
                           "%s}\n"           # NCI
                           % (tab, self.baskervilleFont, dhhs,
                              self.SANSSERIF, phs, line, nih, nci))
        
    def __loadMiscDoc(self, match):

        """
        Private method used to retrieve a CDR MiscellaneousDocument and
        and insert the contents into the body of RTF document for the
        letter.  The template for each correspondence mailer will
        have one or more instances of the pattern '@@MISCDOC:xxxx@@'
        where xxxx is the title of miscellaneous document to be
        retrieved and inserted into the body of the letter.  The
        single required parameter is the Python regular expression
        match object used to locate this pattern.  This method is
        invoked indirectly by the sub() method of the regular
        expression object for the pattern.
        """
        
        miscDoc = MiscellaneousDoc(self, match.group(1))
        return miscDoc.getRtf()

class MiscellaneousDoc:

    "Object that knows how to convert a CDR Miscellaneous document to RTF."
    
    def __init__(self, letter, name):

        """
        Creates a new MiscellaneousDoc object for insertion into
        a correspondence mailer RTF document.  Pass in the reference
        to the object for the letter into which the contents of
        the miscellaneous document are to be inserted, and the
        title of the CDR document to be loaded.
        """
        
        import xml.dom.minidom, cdr
        self.letter = letter
        attr  = 'CdrAttr/MiscellaneousDocument/MiscellaneousDocumentTitle'
        query = '%s = "%s"' % (attr, name)
        resp  = cdr.search('guest', query)
        if isinstance(resp, (str, unicode)):
            raise Exception("failure loading misc doc '%s': %s" % (name, resp))
        if not resp:
            raise Exception("Miscellaneous document '%s' not found" % name)
        self.docId = resp[0].docId
        doc = cdr.getDoc('guest', self.docId, getObject = True)
        if isinstance(doc, (str, unicode)):
            raise Exception("Retrieving %s: %s" % (self.docId, doc))
        self.dom = xml.dom.minidom.parseString(doc.xml)

    def getRtf(self):

        """
        Parses the MiscellaneousDocument XML document and converts the
        contents to RTF markup.
        """
        self.pieces = []
        for node in self.dom.documentElement.childNodes:
            if node.nodeName == "MiscellaneousDocumentText":
                for child in node.childNodes:
                    if child.nodeName == 'Para':
                        self.__addPara(child)
                    elif child.nodeName in ('ItemizedList', 'OrderedList'):
                        self.__addList(child, child.nodeName)
        return "".join(self.pieces)

    def __addPara(self, node):

        """
        Private method to convert an XML Para element to the equivalent
        RTF markup.
        """
        
        self.pieces.append(MiscellaneousDoc.__getText(node).strip())
        self.pieces.append("\\par\\par\n")

    def __getText(node):

        """
        Private method to extract the text nodes from the XML
        document and convert them to RTF marked-up content.
        """
        
        pieces = []
        for child in node.childNodes:
            if child.nodeType == child.TEXT_NODE:
                pieces.append(fix(child.nodeValue))
            elif child.nodeName == 'Strong':
                MiscellaneousDoc.__addMarkup(pieces, child, "b")
            elif child.nodeName == "Emphasis":
                MiscellaneousDoc.__addMarkup(pieces, child, "i")
            elif child.nodeName == "Superscript":
                MiscellaneousDoc.__addMarkup(pieces, child, "super")
            elif child.nodeName == "Subscript":
                MiscellaneousDoc.__addMarkup(pieces, child, "sub")
            elif child.nodeName == "ExternalRef":
                MiscellaneousDoc.__addRef(child)
        return "".join(pieces)
    __getText = staticmethod(__getText)

    def __addMarkup(pieces, node, code):

        """
        Private method to wrap a string of text with RTF markup to
        alter its appearance (for example, to apply boldface or
        italic rendering for the text).
        """
        
        pieces.append("{\\%s " % code)
        for child in node.childNodes:
            if child.nodeType == child.TEXT_NODE:
                pieces.append(fix(child.nodeValue))
        pieces.append("}")
    __addMarkup = staticmethod(__addMarkup)

    def __addRef(self, node):

        """
        Private method to convert an XML ExternalRef element to the
        RTF equivalent.  Not yet implemented.
        """
        
        raise Exception("__addRef not yet implemented")
    __addRef = staticmethod(__addRef)
    
    def __addList(self, node, name):

        """
        Private method to convert an XML list to the RTF equivalent,
        registering a new list for the document if the list is a
        numbered list.
        """
        
        if name == "OrderedList":
            listId = self.letter.addList(List.ARABIC)
        else:
            listId = List.BULLETED
        self.pieces.append("{\\li580{\\ls%d " % listId)
        for child in node.childNodes:
            if child.nodeName == "ListItem":
                self.pieces.append(MiscellaneousDoc.__getText(child).strip())
                self.pieces.append("\\par\n")
        self.pieces.append("}}\n\\par\n")

#----------------------------------------------------------------------
# Test driver.  Doesn't do anything useful.
#----------------------------------------------------------------------
if __name__ == '__main__':
    doc = Document(title = 'Dear John', subject = 'Goodbye')
    doc.addPara('Dear John,')
    doc.addPara('I must bid you adieu ....')
    doc.addPara('Sincerely, Abigail')
    doc.write('DearJohn.rtf')
