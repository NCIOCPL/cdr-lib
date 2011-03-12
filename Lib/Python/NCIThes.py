#----------------------------------------------------------------------
#
# $Id$
#
# Contains routines for updating Drug Term documents from the NCI thesaurus
# and routines for adding new terms to CDR from the NCI thesaurus.
#
# BZIssue::4656
# BZIssue::5004
#
#----------------------------------------------------------------------

import cgi, cdr, cdrdb, urllib2, time, cdrcgi, re, lxml.etree as etree

#----------------------------------------------------------------------
# Charlie used globals here.  I'm keeping them in case outside code
# depends on them. :-(
#----------------------------------------------------------------------
err = ""
bChanged = False
changes = ''

#----------------------------------------------------------------------
# Macros used for parsing the NCIt concept document.
#----------------------------------------------------------------------
CONCEPTS = "org.LexGrid.concepts"
COMMON   = "org.LexGrid.commonTypes"
ENTITY   = "%s.Entity" % CONCEPTS
PRES     = "%s.Presentation" % CONCEPTS
DEF      = "%s.Definition" % CONCEPTS
TEXT     = "%s.Text" % COMMON
SOURCE   = "%s.Source" % COMMON
PROPERTY = "%s.Property" % COMMON
CDRNS    = "cips.nci.nih.gov/cdr"
NSMAP    = { "cdr" : CDRNS }

#----------------------------------------------------------------------
# Prepare string for comparison of term names.
#----------------------------------------------------------------------
def normalize(me):
    if not me:
        return u""
    return me.upper().strip()

#----------------------------------------------------------------------
# Convert NCI/t type to CDR term type; used for FullSynonym term groups.
#----------------------------------------------------------------------
def mapType(nciThesaurusType):
    return {
        "PT"               : "Synonym", # "Preferred term",
        "AB"               : "Abbreviation",
        "AQ"               : "Obsolete name",
        "BR"               : "US brand name",
        "CN"               : "Code name",
        "FB"               : "Foreign brand name",
        "SN"               : "Chemical structure name",
        "SY"               : "Synonym",
        "INDCode"          : "IND code",
        "NscCode"          : "NSC code",
        "CAS_Registry_Name": "CAS Registry name" 
    }.get(nciThesaurusType, "????")

#----------------------------------------------------------------------
# Construct an OtherName block.
#----------------------------------------------------------------------
def makeOtherNameNode(name, termType, sourceTermType, code=None,
                      reviewed="Reviewed"):
    otherName = etree.Element("OtherName")
    etree.SubElement(otherName, "OtherTermName").text = fix(name)
    etree.SubElement(otherName, "OtherNameType").text = fix(termType)
    info = etree.SubElement(otherName, "SourceInformation")
    vocabularySource = etree.SubElement(info, "VocabularySource")
    etree.SubElement(vocabularySource, "SourceCode").text = "NCI Thesaurus"
    etree.SubElement(vocabularySource,
                     "SourceTermType").text = fix(sourceTermType)
    if code:
        etree.SubElement(vocabularySource, "SourceTermId").text = fix(code)
    etree.SubElement(otherName, "ReviewStatus").text = reviewed
    return otherName

#----------------------------------------------------------------------
# Object for a thesaurus concept definition.
#----------------------------------------------------------------------
class Definition:
    def __init__(self, node, isDrugTerm=True):
        self.drugTerm = isDrugTerm
        self.source = Concept.extractSource(node)
        self.text = Concept.extractValue(node)

    #------------------------------------------------------------------
    # Remove leading 'NCI|' and trailing (NCI[...])
    #------------------------------------------------------------------
    def fixText(self):
        text = re.sub(r"^NCI\|", "", self.text.strip())
        return re.sub(r"\s*\(NCI[^)]*\)", "", text)

    #------------------------------------------------------------------
    # Build a Definition node for insertion into a CDR document.
    #------------------------------------------------------------------
    def toNode(self):
        reviewed = self.drugTerm and "Unreviewed" or "Reviewed"
        node = etree.Element("Definition")
        etree.SubElement(node, "DefinitionText").text = self.fixText()
        etree.SubElement(node, "DefinitionType").text = "Health professional"
        source = etree.SubElement(node, "DefinitionSource")
        etree.SubElement(source, "DefinitionSourceName").text = "NCI Thesaurus"
        etree.SubElement(node, "ReviewStatus").text = reviewed
        return node

#----------------------------------------------------------------------
# Object for an articulated synonym from the NCI thesaurus.
#----------------------------------------------------------------------
class FullSynonym:
    def __init__(self, node):
        self.termName = Concept.extractValue(node)
        self.termGroup = Concept.getFieldValue(node, "_representationalForm")
        self.mappedTermGroup = mapType(group)
        self.termSource = Concept.extractSource(node)

    #------------------------------------------------------------------
    # For informing the user about inserted other name blocks.
    #------------------------------------------------------------------
    def __str__(self):
        s = (u"%s (%s)" % (self.termName, self.termGroup)).encode("utf-8")

    #------------------------------------------------------------------
    # Build an OtherName node for insertion into a CDR Term document.
    #------------------------------------------------------------------
    def toNode(self, conceptCode, drugTerm, cdrPreferredName):
        sourceCode = None
        termType = self.mappedTermGroup
        if self.termGroup == "PT":
            sourceCode = conceptCode
            termType = "Lexical variant"
            if not drugTerm:
                if normalize(cdrPreferredName) != normalize(termName):
                    termType = "Synonym"
        return makeOtherNameNode(self.termName, termType, self.termGroup,
                                 sourceCode, "Unreviewed")

#----------------------------------------------------------------------
# Object for the OtherName element extracted from a CDR document.
#----------------------------------------------------------------------
class OtherName:
    def __init__(self, termName, nameType):
        self.termName   = termName
        self.nameType   = nameType

#----------------------------------------------------------------------
# Object for an NCI Thesaurus concept.
#----------------------------------------------------------------------
class Concept:
    def __init__(self, tree):
        self.code = self.preferredName = self.semanticType = None
        self.fullSyn = []
        self.definitions = []
        self.synonyms = []
        self.casCodes = []
        self.nscCodes = []
        self.indCodes = []
        for entity in tree.findall("queryResponse/class[@name='%s']" % ENTITY):
            self.code = Concept.getFieldValue(entity, '_entityCode')
            for field in entity.findall("field[@name='_presentationList']"):
                self.__extractNames(field)
            for field in entity.findall("field[@name='_definitionList']"):
                self.__extractDefinitions(field)
            for field in entity.findall("field[@name='_propertyList']"):
                self.__extractProperties(field)
    def __extractNames(self, node):
        for child in node.findall("class[@name='%s']" % PRES):
            name = Concept.getPropertyName(child)
            if name == "Preferred_Name":
                self.preferredName = Concept.extractValue(child)
            elif name == "FULL_SYN":
                self.fullSyn.append(FullSynonym(child))
    def __extractDefinitions(self, node):
        for child in node.findall("class[@name='%s']" % DEF):
            if Concept.getPropertyName(child) == "DEFINITION":
                self.definitions.append(Definition(child))
    def __extractProperties(self, node):
        for child in node.findall("class[@name='%s']" % PROPERTY):
            name = Concept.getPropertyName(child)
            if name == "Semantic_Type":
                self.semanticType = Concept.extractValue(child)
            elif name == "Synonym":
                self.synonyms.append(Concept.extractValue(child))
            elif name == "CAS_Registry":
                self.casCodes.append(Concept.extractValue(child))
            elif name == "NSC_Code":
                self.nscCodes.append(Concept.extractValue(child))
            elif name == "IND_Code":
                self.indCodes.append(Concept.extractValue(child))
    @staticmethod
    def getFieldValue(node, name):
        for child in node.findall("field[@name='%s']" % name):
            return child.text
        return None
    @staticmethod
    def getPropertyName(node):
        return Concept.getFieldValue(node, "_propertyName")
    @staticmethod
    def extractValue(node):
        pieces = []
        for value in node.findall("field[@name='_value']"):
            for text in value.findall("class[@name='%s']" % TEXT):
                for content in text.findall("field[@name='_content']"):
                    if content.text is not None:
                        pieces.append(content.text)
        return u"".join(pieces) or None
    @staticmethod
    def extractSource(node):
        pieces = []
        for sourceList in node.findall("field[@name='_sourceList']"):
            for source in sourceList.findall("class[@name='%s']" % SOURCE):
                for content in source.findall("field[@name='_content']"):
                    pieces.append(content.text)
        return u"".join(pieces) or None

#----------------------------------------------------------------------
# Connect to the CDR database.
#----------------------------------------------------------------------
def connectToDB():
    global err
    try:
        err = ""
        conn = cdrdb.connect('CdrGuest')
        return conn;
    except cdrdb.Error, info:
        err = '<error>Failure connecting to CDR: %s</error>' % info[1][0]
        return None

#----------------------------------------------------------------------
# Store what we got from the EVS service.
#----------------------------------------------------------------------
def storeEvsResponse(code, response):
    try:
        now  = time.strftime("%Y%m%d%H%M%S")
        name = cdr.DEFAULT_LOGDIR + "/ncit-%s-%s.html" % (code, now)
        fp = open(name, "wb")
        fp.write(page)
        fp.close()
    except:
        pass

#----------------------------------------------------------------------
# Retrieve a concept document from the NCI Thesaurus.
#----------------------------------------------------------------------
def fetchConcept(code):
    global err
    err   = ""
    code  = code.strip()
    host  = "lexevsapi60.nci.nih.gov"
    app   = "/lexevsapi60/GetXML"
    parms = "query=org.LexGrid.concepts.Entity[@_entityCode=%s]" % code
    url   = "http://%s/%s?%s" % (host, app, parms)
    try:
        conn = urllib2.urlopen(url)
        doc  = conn.read()
    except Exception, e:
        err = "<error>EVS server unavailable: %s</error>" % e
        return None
    try:
        tree    = etree.XML(doc)
        concept = Concept(tree)
    except Exception, e:
        err = "<error>Failure parsing concept: %s</error>" % e
        storeEvsResponse(code, doc)
        return None
    if not concept.code:
        err = "<error>Concept document for %s not found</error>" % code
        storeEvsResponse(code, doc)
        return None
    return concept

#----------------------------------------------------------------------
# See if citation already exists.  XXX Modify after we populate the
# new NCIThesaurusConcept element.
#----------------------------------------------------------------------
def findExistingConcept(conn, code):
    global err
    try:
        err = ""
        cursor = conn.cursor()
        cursor.execute("""\
                SELECT c.doc_id
                  FROM query_term c
                  JOIN query_term t
                    ON c.doc_id = t.doc_id
                   AND LEFT(c.node_loc, 8) = LEFT(t.node_loc, 8)
                 WHERE c.path = '/Term/OtherName/SourceInformation'
                              + '/VocabularySource/SourceTermId'
                   AND t.path = '/Term/OtherName/SourceInformation'
                              + '/VocabularySource/SourceCode'
                   AND t.value = 'NCI Thesaurus'
                   AND c.value = ?""", code)
        rows = cursor.fetchall()
        if not rows: return None
        return rows[0][0]
    except cdrdb.Error, info:
        err = ('<error>Failure checking for existing document: '
               '%s</error>' % info[1][0])
        return None

#----------------------------------------------------------------------
# Prepare definition for comparison.
#----------------------------------------------------------------------
def normalizeDefinitionText(definitionText):
    if not definitionText:
        return u""
    return re.sub(r"\s+", " ", definitionText.strip())

#----------------------------------------------------------------------
# Update the definition
#----------------------------------------------------------------------
def updateDefinition(tree, definition):
    global bChanged
    global changes
    for node in tree.findall("Definition"):
        for text in node.findall("DefinitionText"):
            oldText = normalizeDefinitionText(child.text)
            newText = definition.fixText()
            if oldText != normalizeDefinitionText(newText):
                bChanged = True
                changes += " Definition update."
                child.text = newText
                for status in node.findall("ReviewStatus"):
                    status.text = "Unreviewed"
            return

    #------------------------------------------------------------------
    # Definition not found, so add it.
    #------------------------------------------------------------------
    insertPosition = 0
    for node in tree:
        if node.tag == "TermType":
            break
        insertPosition += 1
    bChanged = True
    changes += ' Definition added.'
    tree.insert(insertPosition, definition.toNode())

#----------------------------------------------------------------------
# Determine correct position and add new OtherName element there.
#----------------------------------------------------------------------
def addOtherName(tree, fullSyn, conceptCode, drugTerm, cdrPreferredName):
    global bChanged
    global changes
    bChanged = True
    changes += (" Other Name: %s added" % fullSyn)
    position = 0
    for node in tree:
        if node.tag not in ("PreferredName", "ReviewStatus", "Comment",
                            "OtherName"):
            break
        position += 1
    newNode = fullSyn.toNode(conceptCode, drugTerm, cdrPreferredName)
    tree.insert(position, newNode)

#----------------------------------------------------------------------
# Extract OtherName information from a CDR document.
#----------------------------------------------------------------------
def getOtherNames(tree):
    otherNames = []
    for node in tree.findall("OtherName"):
        termName = nameType = None
        for child in node:
            if child.tag == "OtherTermName":
                termName = child.text
            elif child.tag == "OtherNameType":
                nameType = child.text
                if nameType == "Lexical variant":
                    nameType = "Synonym"
        otherNames.append(OtherName(termName, nameType))
    return otherNames

#----------------------------------------------------------------------
# Change the text content of a CDR Term document's TermStatus element.
#----------------------------------------------------------------------
def updateTermStatus(tree, status):
    for node in tree.findall("TermStatus"):
        node.text = status

#----------------------------------------------------------------------
# Get the preferred name of a concept in the NCI Thesaurus.
#----------------------------------------------------------------------
def getNCITPreferredName(conceptCode):
    concept = fetchConcept(conceptCode)
    if err:
        return err
    return concept.preferredName

#----------------------------------------------------------------------
# Get the semantic type value for a CDR document.  Some of these
# functions are no longer used in this module, but I'm keeping them
# in case outside code depends on them (RMK, 2011-03-12).
#----------------------------------------------------------------------
def getCDRSemanticType(session, docId):
    doc = cdr.getDoc(session, docId, 'N', getObject=True)
    error = cdr.checkErr(doc)
    if error:
        return "<error>Unable to retrieve %s - %s</error>" % (docId, error)
    tree = etree.XML(doc.xml)
    for node in tree.findall("SemanticType"):
        return node.text
    return ""

#----------------------------------------------------------------------
# Retrieve the preferred name of a CDR Term document.
#----------------------------------------------------------------------
def getCDRPreferredName(session, docId):
    doc = cdr.getDoc(session, docId, 'N', getObject=True)
    error = cdr.checkErr(doc)
    if error:
        return "<error>Unable to retrieve %s - %s</error>" % (docId, error)
    tree = etree.XML(doc.xml)
    for node in tree.findall("PreferredName"):
        return node.text
    return ""


#----------------------------------------------------------------------
# Update an existing CDR Term document.
#----------------------------------------------------------------------
def updateTerm(session, cdrId, conceptCode, doUpdate=False,
               doUpdateDefinition=True, doImportTerms=True, drugTerm=True):

    #------------------------------------------------------------------
    # Initialize global variables.
    #------------------------------------------------------------------
    global bChanged
    global err
    global changes
    bChanged = False
    err = ""
    changes = ''

    #------------------------------------------------------------------
    # Fetch the Thesaurus concept and the CDR document.
    #------------------------------------------------------------------
    concept = fetchConcept(conceptCode)
    if err:
        return err
    docId = cdr.normalize(cdrId)
    doc = cdr.getDoc(session, docId, doUpdate and "Y" or "N", getObject=True)
    error = cdr.checkErr(doc)
    if error:
        return "<error>Unable to retrieve %s - %s</error>" % (docId, error)
    tree = etree.XML(docObject.xml)
    
    #------------------------------------------------------------------
    # This check may be obsolete, but Charlie left no documentation
    # behind, so I'm leaving it in place.  XXX Check with the users.
    #------------------------------------------------------------------
    if drugTerm:
        semanticType = None
        for node in tree.findall("SemanticType"):
            semanticType = node.text
        if (semanticType != "Drug/agent"):
            return ("<error>Semantic Type is %s. The importing only works "
                    "for Drug/agent</error>" % semanticType)

    #------------------------------------------------------------------
    # If the user wants the definition updated and we have one, do it.
    #------------------------------------------------------------------
    if doUpdateDefinition:
        for definition in concept.definitions:
            if definition.source == 'NCI':
                definition.drugTerm = drugTerm
                updateDefinition(tree, definition)
                break

    #------------------------------------------------------------------
    # Plug in any new names if we're asked to.
    #------------------------------------------------------------------
    if doImportTerms:
        otherNames = getOtherNames(tree)
        for syn in concept.fullSyn:
            if syn.termSource != 'NCI-GLOSS':
                found = False
                for otherName in otherNames:
                    if syn.termName.upper() == otherName.termName.upper():
                        if syn.mappedTermGroup == otherName.nameType:
                            found = True
                            break
                                
                #------------------------------------------------------
                # It's not already in the document; add it.
                #------------------------------------------------------
                if not found:
                    cdrPreferredName = ''
                    if not drugTerm:
                        for node in tree.findall("PreferredName"):
                            cdrPreferredName = node.text
                    addOtherName(tree, syn, conceptCode, drugTerm,
                                 cdrPreferredName)

    #------------------------------------------------------------------
    # If the document changed update the status and store it if requested.
    #------------------------------------------------------------------
    if bChanged:
        updateTermStatus(dom, 'Unreviewed')

        oldDoc.xml = dom.toxml().encode('utf-8')
          
        if doUpdate:
            doc.xml = etree.tostring(tree, pretty_print=True)
            
            #--------------------------------------------------------------
            # Charlie's code never made publishable versions for drug terms.
            # I'm assuming that was a mistake.  XXX Check with the users.
            #--------------------------------------------------------------
            versions = cdr.lastVersions(session, docId)
            publishVer = versions[1] == -1 and "N" or "Y"
            updateComment = "NCI Thesaurus Update"
            resp = cdr.repDoc(session, doc=str(doc), val='Y', ver='Y',
                              verPublishable=publishVer, showWarnings=True,
                              comment=updateComment)
            cdr.unlock(session, docId)
            if not resp[0]:
                return ("<error>Failure adding concept %s: %s</error>" %
                        (cdrId, cdr.checkErr(resp[1])))
            return ("Term %s updated. Publishable = %s. %s" %
                    (cdrId, publishVer, changes))
        return ("Term %s will change. Publishable = %s. %s" %
                (cdrId, publishVer, changes))
    else:
        cdr.unlock(session,docId)
        return "No updates needed for term %s." % cdrId

#----------------------------------------------------------------------
# Add a new CDR Term document.
#----------------------------------------------------------------------
def addNewTerm(session, conceptCode, updateDefinition=True, importTerms=True):
    conn = connectToDB()
    if err:
        return err
    concept = fetchConcept(conceptCode)
    if err:
        return err
    docId = findExistingConcept(conn, conceptCode)
    if err:
        return err
    if docId:
        return ("<error>Concept has already been imported as CDR%010d</error>"
                % docId)
    root = etree.Element("Term", nsmap=NSMAP)
    preferredName == concept.preferredName or u""
    etree.SubElement(root, "PreferredName").text = preferredName
    if importTerms:
        for syn in concept.fullSyn:
            if syn.termSource == "NCI":
                code = syntermGroup == "PT" and conceptCode or None
                termType = mapType(syn.termGroup)
                root.append(makeOtherNameNode(syn.termName, termType,
                                              syn.termGroup, code))
        for code in concept.indCodes:
            root.append(makeOtherNameNode(code, "IND code", "IND_Code"))
        for code in concept.nscCodes:
            root.append(makeOtherNameNode(code, "NSC code", "NSC_Code"))
        for code in concept.casCodes:
            root.append(makeOtherNameNode(code, "CAS Registry name",
                                          "CAS_Registry"))
    if updateDefinition:
        for definition in concept.definitions:
            if definition.source == 'NCI':
                root.append(definition.toNode())
    termTypeNode = etree.SubElement(root, "TermType")
    etree.SubElement(termTypeNode, "TermTypeName").text = "Index term"
    etree.SubElement(root, "TermStatus").text = "Unreviewed"
    docXml = etree.tostring(root, pretty_print=True)
    doc = cdr.Doc(docXml, "Term")
    comment = "Importing Term document from NCI Thesaurus"
    resp = cdr.addDoc(session, doc=str(doc), val='Y', showWarnings=True,
                      comment=comment)
    if not resp[0]:
        cdrcgi.bail("Failure adding new term document: %s" % resp[1])
    cdr.unlock(session, resp[0])
    return "Concept added as %s" % resp[0]

#----------------------------------------------------------------------
# The rest of the code in this module is Charlie's, and I'm leaving it
# pretty much the way I found it.  Not sure it's still used any more.
#----------------------------------------------------------------------

class DocConceptPair:
    def __init__(self, doc_id, concept, updateDef):
        self.doc_id = doc_id
        self.concept = concept
        self.updateDef = updateDef

docConceptPairs = []

def getThingsToUpdate(isDrugUpdate, excelSSName):
    import os.path, ExcelReader
    if isDrugUpdate:

        #first get a list of all the drug/agent concept codes
        query = """\
            SELECT distinct qt.doc_id,qt.value
              FROM query_term qt
              JOIN query_term semantic
                ON semantic.doc_id = qt.doc_id
             WHERE qt.path = '/Term/OtherName/SourceInformation'
                           + '/VocabularySource/SourceTermId'
               AND semantic.path = '/Term/SemanticType/@cdr:ref'
               AND semantic.int_val = 256166
          ORDER BY qt.doc_id"""

        #submit the query to the database.
        try:
            conn = cdrdb.connect('CdrGuest')
            cursor = conn.cursor()
            cursor.execute(query, timeout=300)
            rows = cursor.fetchall()
        except cdrdb.Error, info:
            return 'Failure retrieving Summary documents: %s' % info[1][0]
             
        if not rows:
            return 'No Records Found for Selection'

        cdridsToSkipDef = []
        updateDef = True

        #get a list of cdrid's that won't have their definition updated
        if (len(excelSSName) > 3 ):
            if not os.path.exists(excelSSName):
                return 'Unable to open %s on the server.' % excelSSName
            book = ExcelReader.Workbook(excelSSName)
            sheet = book[0]
            for row in sheet.rows:
                cdridsToSkipDef.append(row[0].val)
     
        for doc_id, value in rows:
            value = value.strip()
            if doc_id in cdridsToSkipDef:
                updateDef = False
            else:
                updateDef = True
            if value[0] == 'C':
                docConceptPairs.append(DocConceptPair(doc_id, value, updateDef))

    # disease terms
    else:
        #get a list of cdrid's and NCITConceptIDs
        if (len(excelSSName) > 3 ):
            if not os.path.exists(excelSSName):
                cdrcgi.bail('Unable to open %s on the server.' % excelSSName)
            book = ExcelReader.Workbook(excelSSName)
            sheet = book[0]
            bRow1 = True
            for row in sheet.rows:
                # skip the first row
                if not bRow1:
                    cell = row[0]
                    ncitCode = cell.val
                    cell = row[1]
                    doc_id = cell.val
                    doc_id = cdr.exNormalize(doc_id)[1]
                    docConceptPairs.append(DocConceptPair(doc_id, ncitCode,
                                                          True))
                bRow1 = False
    return ""

def updateAllTerms(job, session, excelNoDefFile, excelOutputFile, doUpdate,
                   drugTerms):
    import ExcelWriter
    ret = getThingsToUpdate(drugTerms, excelNoDefFile)
    if ret:
        return ret

    wb = ExcelWriter.Workbook()
    b = ExcelWriter.Border()
    borders = ExcelWriter.Borders(b, b, b, b)
    font = ExcelWriter.Font(name = 'Times New Roman', size=10)
    align = ExcelWriter.Alignment('Left', 'Top', wrap=True)
    style1 = wb.addStyle(alignment=align, font=font, borders=borders)
    wsName = "Update NCIT"
    if drugTerms:
        wsName += " Drug Terms"
    else:
        wsName += " Disease Terms"
    ws = wb.addWorksheet(wsName, style1, 40, 1)

    ws.addCol(1, 232.5)
    ws.addCol(2, 100)
    ws.addCol(3, 127.5)

    # Set up the title and header cells in the spreadsheet's top rows.
    font = ExcelWriter.Font(name = 'Times New Roman', bold=True, size=10)
    align = ExcelWriter.Alignment('Center', 'Center', wrap=True)
    interior = ExcelWriter.Interior('#CCFFCC')
    style3 = wb.addStyle(alignment=align, font=font, borders=borders,
                         interior=interior)
    headings = (
        'Concept Code',
        'CDRID',
        'Results'
        )
    row = ws.addRow(1, style3, 40)
    cellName = "Update NCIT"
    if drugTerms:
        cellName += " Drug Terms"
    else:
        cellName += " Disease Terms"
    if not doUpdate:
        cellName += " (See what will change)"
    row.addCell(1, cellName, mergeAcross = len(headings) - 1)
    row = ws.addRow(2, style3, 40)
    for i in range(len(headings)):
        row.addCell(i + 1, headings[i])

    rowNum = 2        
    
    itemCnt = 0
    for docConceptPair in docConceptPairs:
        row = ws.addRow(rowNum, style1, 40)
        rowNum += 1
        result = updateTerm(session, docConceptPair.doc_id,
                            docConceptPair.concept, doUpdate=doUpdate,
                            doUpdateDefinition=docConceptPair.updateDef,
                            doImportTerms=True, drugTerm=drugTerms)
        itemCnt += 1
        if doUpdate:
            msg = "Updated "
        else:
            msg = "Checked "
        msg += "%d of %d terms" % (itemCnt, len(docConceptPairs))
        job.setProgressMsg(msg)
        row.addCell(1, docConceptPair.concept)
        row.addCell(2, docConceptPair.doc_id)
        row.addCell(3, result)

    fobj = file(excelOutputFile, "wb")
    wb.write(fobj, asXls=True, big=True)
    fobj.close()
