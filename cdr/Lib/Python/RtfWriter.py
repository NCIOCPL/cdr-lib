#!/usr/bin/python

"""

    Module for generating RTF documents

    Summary

        import RtfWriter
        doc = RtfWriter.RTF(title = 'Dear John',
                            author = 'Suzie Wong',
                            subject = 'Goodbye')
        
        
        
"""

#----------------------------------------------------------------------
#
# $Id: RtfWriter.py,v 1.1 2005-02-24 02:19:22 bkline Exp $
#
# Module for generating RTF documents.
#
# $Log: not supported by cvs2svn $
#----------------------------------------------------------------------
import time, re

#----------------------------------------------------------------------
# Escape problematic characters for RTF.
##----------------------------------------------------------------------
def fix(text):
    text = text.replace("\\", "\\\\")
    text = text.replace("{", "\\{").replace("}", "\\}")
    text = _nonAsciiPattern.sub(_replace, text)
    if isinstance(text, unicode):
        text = text.encode("ascii")
    return text

#----------------------------------------------------------------------
# Regular expression pattern to recognize non-ASCII characters.
#----------------------------------------------------------------------
_nonAsciiPattern = re.compile(u"([\u0080-\uFFFD])")

#----------------------------------------------------------------------
# Function passed to regular expression sub() method.
#----------------------------------------------------------------------
def _replace(match):
    return _rtfForUnicodeChar(match.group(1))

#----------------------------------------------------------------------
# Convert a Unicode character into its RTF representation.
#----------------------------------------------------------------------
def _rtfForUnicodeChar(ch):
    code = ord(ch)
    return "{\\uc1\\u%d?}" % code

#----------------------------------------------------------------------
# Invoked by regular expression engine for inserting a miscellaneous doc.
#----------------------------------------------------------------------

#----------------------------------------------------------------------
# Object for representing a single font in an RTF document.
#----------------------------------------------------------------------
class Font:
    def __init__(self, id, name, family = "nil", alt = None):
        self.__id     = id
        self.__name   = name
        self.__family = family
        self.__alt    = alt

    #------------------------------------------------------------------
    # Access methods.
    #------------------------------------------------------------------
    def getId(self):
        return self.__id
    def getName(self):
        return self.__name
    def getFamily(self):
        return self.__family
    def getFamily(self):
        return self.__alt

    #------------------------------------------------------------------
    # Generate the RTF code for the font.
    #------------------------------------------------------------------
    def getRtf(self):
        rtf = "{\\f%d\\f%s %s" % (self.__id, self.__family, self.__name)
        if self.__alt:
            rtf += "{\\*\\falt %s}" % self.__alt
        return rtf + ";}"

#----------------------------------------------------------------------
# Object for representing a single list in an RTF document.
#----------------------------------------------------------------------
class List:
    BULLETED  = 1   # listid for un-numbered lists
    ARABIC    = 0   # number type for numbered lists
    NONE      = 23  # number type for bulleted lists
    __nextId  = 0   # class variable for generating list IDs

    def __init__(self, numberType):
        self.id         = List.__getNextId()
        self.numberType = numberType
        if self.numberType != List.NONE:
            self.levelText    = "\\'02\\'00."
            self.levelNumbers = "\\'01"
        else:
            self.levelText    = "\\'01\\u8226 *"
            self.levelNumbers = ""

    def __getNextId(listClass):
        listClass.__nextId += 1
        return listClass.__nextId
    __getNextId = classmethod(__getNextId)

    #------------------------------------------------------------------
    # Generate the RTF code for the list.
    #------------------------------------------------------------------
    def getRtf(self):
        return """\
{\\list\\listtemplateid%d\\listid%d\\listsimple1
{\\listlevel\\levelnfc%d\\leveljc2\\levelstartat1\\levelfollow1
{\\leveltext %s;}
{\\levelnumbers %s;}}}
""" % (self.id, self.id, self.numberType, self.levelText, self.levelNumbers)

#----------------------------------------------------------------------
# Object for an RTF document.
#----------------------------------------------------------------------
class RTF:

    #------------------------------------------------------------------
    # Class constants.
    #------------------------------------------------------------------
    SERIF     = 0
    SANSSERIF = 1
    FIXED     = 2
    BLACK     = 1
    WHITE     = 2

    def __init__(self,
                 title   = "[Generated RTF Document]",
                 author  = "rmk",
                 company = "RK Systems",
                 subject = None):
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
        self.bodyParts   = []
    def addFont(self, name, family = "nil", alt = None):
        id = len(self.fonts)
        self.fonts.append(Font(id, name, family, alt))
        return id
    def addList(self, numberType):
        newList = List(numberType)
        self.lists.append(newList)
        return newList.id
    def getHeader(self):
        return ("\\rtf%d"                    # RTF version
                "\\%s"                       # Character set used in document
                "\\deff%d"                   # Index of default font
                "\\deflang%d\n"              # Default language
                % (self.rtfVersion, self.charset, self.defaultFont,
                   self.defaultLang) +
                self.getFontTable()  +
                self.getColorTable() +
                self.getListTables()  +
                self.getGenerator())
    def getFontTable(self):
        fontTable = "{\\fonttbl\n"
        for font in self.fonts:
            fontTable += "%s\n" % font.getRtf()
        return fontTable + "}\n"
    def getColorTable(self):
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

        generator    = "{\\*\\generator CDR RTF Writer;}\n"
    def getListTables(self):
        listTables = "{\\*\\listtable\n"
        overrideTables = "{\\listoverridetable\n"
        for listDef in self.lists:
            id = listDef.id
            listTables += listDef.getRtf()
            overrideTables += ("{\\listoverride\\listid%d"
                               "\\listoverridecount0\\ls%d}\n" % (id, id))
        return listTables + "}\n" + overrideTables + "}\n"
    def getGenerator(self):
        return "{\\*\\generator %s;}\n" % fix(self.generator)
    def getDocFmt(self):
        # Add \truncatefontheight to have font sizes rounded down, not up
        # Add \lytprtmet to use printer metrics for page layout
        return ("\\pard\\plain"              # clear out settings
                "\\fs%d\n"                   # font size in 1/2 points
                "\\margl%d"                  # left margin
                "\\margr%d"                  # right margin
                "\\margt%d"                  # top margin
                "\\margb%d\n"                # bottom margin
                % (self.fSize, self.margL, self.margR, self.margT, self.margB))
    def write(self, file):
        if isinstance(file, (str, unicode)):
            f = open(file, "wb")
        else:
            f = file
        rtf = self.getRtf()
        if isinstance(file, (str, unicode)):
            f.write(doc)
            f.close()
    def getRtf(self):
        rtf = '{' + self.getHeader() + self.getInfo() + self.getDocFmt()
        for bodyPart in self.bodyParts:
            rtf += bodyPart
        return rtf + '}'
    def addRawContent(self, rawContent):
        self.bodyParts.append(rawContent)
    def getInfo(self):
        info = "{\\info\n"
        if self.title:
            info += "{\\title %s}\n" % fix(self.title)
        if self.author:
            info += "{\\author %s}\n" % fix(self.author)
        if self.company:
            info += "{\\company %s}\n" % fix(self.company)
        info += "{\\creatim %s}\n" % self.getTime()
        if self.subject:
            info += "{\\subject %s}\n" % fix(self.subject)
        return info + '}\n'
    def getTime(self):
        now = time.localtime()
        return "\\yr%d \\mo%d \\dy%d \\hr%d \\min%d \\sec%d" % (now[0],
                                                                now[1],
                                                                now[2],
                                                                now[3],
                                                                now[4],
                                                                now[5])

#----------------------------------------------------------------------
# Subclass for RTF document used to generate PDQ Board Member mailers.
#----------------------------------------------------------------------
class FormLetter(RTF):
    def __init__(self, 
                 title      = "[CDR Form Letter]",
                 author     = "cdr",
                 company    = "CIPS",
                 subject    = None,
                 pngName    = "dhhslogo.png",
                 binImage   = 1,
                 template   = None,
                 invitePara = ""):
        RTF.__init__(self, title, author, company, subject)
        self.generator   = "CDR RTF Writer"
        self.pngName     = pngName           # location of bitmap file for logo
        self.margL       = 1080              # 3/4-inch left margin
        self.margR       = 720               # 1/2-inch right margin
        self.margT       = 1080              # 5/8-inch top margin
        self.margB       = 1260              # 7/8-inch bottom margin
        self.fSize       = 21                # 10.5-point default font size
        self.baskervilleFont = self.addFont("Baskerville Old Face",
                                            "roman",
                                            "Times New Roman")
        self.addLetterHead(binImage)
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

    def writeImageBytes(self, image, bin = 1):
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

    def addLetterHead(self, binImage):
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
                           % self.writeImageBytes(logo, binImage))
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
        miscDoc = MiscellaneousDoc(self, match.group(1))
        return miscDoc.getRtf()

#----------------------------------------------------------------------
# Object that knows how to convert a CDR Miscellaneous document to RTF.
#----------------------------------------------------------------------
class MiscellaneousDoc:
    def __init__(self, letter, name):
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
        self.pieces = []
        for node in self.dom.documentElement.childNodes:
            if node.nodeName == "MiscellaneousDocumentText":
                for child in node.childNodes:
                    if child.nodeName == 'Para':
                        self.addPara(child)
                    elif child.nodeName in ('ItemizedList', 'OrderedList'):
                        self.addList(child, child.nodeName)
        return "".join(self.pieces)

    def addPara(self, node):
        self.pieces.append(MiscellaneousDoc.getText(node).strip())
        self.pieces.append("\\par\\par\n")

    def getText(node):
        pieces = []
        for child in node.childNodes:
            if child.nodeType == child.TEXT_NODE:
                pieces.append(fix(child.nodeValue))
            elif child.nodeName == 'Strong':
                MiscellaneousDoc.addMarkup(pieces, child, "b")
            elif child.nodeName == "Emphasis":
                MiscellaneousDoc.addMarkup(pieces, child, "i")
            elif child.nodeName == "Superscript":
                MiscellaneousDoc.addMarkup(pieces, child, "super")
            elif child.nodeName == "Subscript":
                MiscellaneousDoc.addMarkup(pieces, child, "sub")
            elif child.nodeName == "ExternalRef":
                addRef(child)
        return "".join(pieces)
    getText = staticmethod(getText)

    def addMarkup(pieces, node, code):
        pieces.append("{\\%s " % code)
        for child in node.childNodes:
            if child.nodeType == child.TEXT_NODE:
                pieces.append(fix(child.nodeValue))
        pieces.append("}")
    addMarkup = staticmethod(addMarkup)

    def addRef(self, node):
        raise Exception("addRef not yet implemented")
    
    def addList(self, node, name):
        if name == "OrderedList":
            listId = self.letter.addList(List.ARABIC)
        else:
            listId = List.BULLETED
        self.pieces.append("{\\li580{\\ls%d " % listId)
        for child in node.childNodes:
            if child.nodeName == "ListItem":
                self.pieces.append(MiscellaneousDoc.getText(child).strip())
                self.pieces.append("\\par\n")
        self.pieces.append("}}\n\\par\n")
