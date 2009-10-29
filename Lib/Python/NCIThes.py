#----------------------------------------------------------------------
#
# $Id$
#
# Contains routines for updating Drug Term documents from the NCI thesaurus
# and routines for adding new terms to CDR from the NCI thesaurus.
#
# BZIssue::4656
#
#----------------------------------------------------------------------

import cgi, cdr, cdrdb, xml.dom.minidom, httplib, time, cdrcgi

err = ""
bChanged = False
changes = ''

#----------------------------------------------------------------------
# Prepare string for living in an XML document.
#----------------------------------------------------------------------
def fix(s):
    return s and cgi.escape(s) or u''

def normalize(me):
    if not me:
        return u""
    return me.upper().strip()

def extractError(node):
    return node.toxml()

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

def makeOtherName(name, termType, sourceTermType, code = None):
    xmlFrag = u"""\
 <OtherName>
  <OtherTermName>%s</OtherTermName>
  <OtherNameType>%s</OtherNameType>
  <SourceInformation>
   <VocabularySource>
    <SourceCode>NCI Thesaurus</SourceCode>
    <SourceTermType>%s</SourceTermType>
""" % (fix(name), fix(termType), fix(sourceTermType))
    if code:
        xmlFrag += """\
    <SourceTermId>%s</SourceTermId>
""" % fix(code)
    xmlFrag += """\
   </VocabularySource>
  </SourceInformation>
  <ReviewStatus>Reviewed</ReviewStatus>
 </OtherName>
"""
    return xmlFrag

#----------------------------------------------------------------------
# Object for a thesaurus concept definition.
#----------------------------------------------------------------------
class Definition:
    def __init__(self, value, source):
        self.drugTerm = True # XXX Why??? Charlie doesn't explain this default.
        self.source = source
        self.text   = value
        self.removeTrailingParenText()
        self.removePrecedingNCIText()

    # remove the opening text 'NCI|' , if it exists
    def removePrecedingNCIText(self):
        self.text = self.text.strip()
        if self.text.startswith("NCI|"):
            self.text = self.text[4:]
        return        

    # remove the trailing paren text like (NCI04), if it exists
    def removeTrailingParenText(self):
        self.text = self.text.strip()
        if self.text[-1] == ')':
            i = -2
            while self.text[i] != '(':
                i = i - 1
                posi = 0 - i
                if posi > len(self.text) - 2:
                    return
            tempStr = self.text[i:]
            if len(tempStr) > 8:
                return
            if tempStr.startswith("(NCI"):
                self.text = self.text[:i]
                self.text = self.text.strip()
        return
        
    def toXml(self):
        if self.drugTerm:
            reviewedTxt = 'Unreviewed'
        else:
            reviewedTxt = 'Reviewed'
        return """\
 <Definition>
  <DefinitionText>%s</DefinitionText>
  <DefinitionType>Health professional</DefinitionType>
  <DefinitionSource>
   <DefinitionSourceName>NCI Thesaurus</DefinitionSourceName>
  </DefinitionSource>
  <ReviewStatus>%s</ReviewStatus>
 </Definition>
""" % (fix(self.text), reviewedTxt)

    def toNode(self,dom):
        node = dom.createElement('Definition')
        
        child = dom.createElement('DefinitionText')
        text = dom.createTextNode(self.text)
        child.appendChild(text)        
        node.appendChild(child)

        child = dom.createElement('DefinitionType')
        text = dom.createTextNode('Health professional')
        child.appendChild(text)        
        node.appendChild(child)

        child = dom.createElement('DefinitionSource')
        child2 = dom.createElement('DefinitionSourceName')
        text = dom.createTextNode('NCI Thesaurus')
        child2.appendChild(text)
        child.appendChild(child2)
        node.appendChild(child)

        child = dom.createElement('ReviewStatus')
        if self.drugTerm:
            text = dom.createTextNode('Unreviewed')
        else:
            text = dom.createTextNode('Reviewed')
        child.appendChild(text)
        node.appendChild(child)     
        
        return node

#----------------------------------------------------------------------
# Object for an articulated synonym from the NCI thesaurus.
#----------------------------------------------------------------------
class FullSynonym:
    def __init__(self, value, group, source):
        self.termName        = value
        self.termGroup       = group
        self.mappedTermGroup = mapType(group)
        self.termSource      = source

    def toDescription(self):
        return self.termName + ' (' + self.termGroup + ')'

    def toNode(self, dom, conceptCode, drugTerm, cdrPreferredName):
        termName = self.termName
        mappedTermGroup = self.mappedTermGroup
        termGroup = self.termGroup
        sourceCode = None
        
        if drugTerm:
            if self.termGroup == 'PT':
                mappedTermGroup = 'Lexical variant'
                sourceCode = conceptCode
        else:
            if self.termGroup == 'PT':
                if normalize(cdrPreferredName) != normalize(termName):
                    mappedTermGroup = 'Synonym'
                else:
                    mappedTermGroup = 'Lexical variant'
                sourceCode = conceptCode
        
        node = dom.createElement('OtherName')
        
        child = dom.createElement('OtherTermName')
        text = dom.createTextNode(termName)
        child.appendChild(text)
        node.appendChild(child)

        child = dom.createElement('OtherNameType')
        text = dom.createTextNode(mappedTermGroup)
        child.appendChild(text)        
        node.appendChild(child)

        child = dom.createElement('SourceInformation')
        child2 = dom.createElement('VocabularySource')
        
        child3 = dom.createElement('SourceCode')        
        text = dom.createTextNode('NCI Thesaurus')
        child3.appendChild(text)
        child2.appendChild(child3)

        child3 = dom.createElement('SourceTermType')
        text = dom.createTextNode(termGroup)
        child3.appendChild(text)
        child2.appendChild(child3)

        if sourceCode:
            child3 = dom.createElement('SourceTermId')
            text = dom.createTextNode(sourceCode)
            child3.appendChild(text)
            child2.appendChild(child3)
        
        child.appendChild(child2)
        node.appendChild(child)

        child = dom.createElement('ReviewStatus')
        text = dom.createTextNode('Unreviewed')
        child.appendChild(text)
        node.appendChild(child)     
        
        return node            

#----------------------------------------------------------------------
# Object for the other name element in the document
#----------------------------------------------------------------------
class OtherName:
    def __init__(self, termName, nameType):
        self.termName   = termName
        self.nameType   = nameType

#----------------------------------------------------------------------
# Object for a NCI Thesaurus concept.
#----------------------------------------------------------------------
class Concept:
    def __init__(self, node):
        if node.nodeName != 'Concept':
            cdrcgi.bail(extractError(node))
        self.code          = node.getAttribute('code')
        self.preferredName = None
        self.semanticType  = None
        self.fullSyn       = []
        self.definitions   = []
        self.synonyms      = []
        self.casCodes      = []
        self.nscCodes      = []
        self.indCodes      = []
        for child in node.childNodes:
            if child.nodeName == 'Property':
                name = value = group = source = None
                for grandchild in child.childNodes:
                    if grandchild.nodeName == 'Name':
                        name = cdr.getTextContent(grandchild)
                    elif grandchild.nodeName == 'Value':
                        value = cdr.getTextContent(grandchild)
                    elif grandchild.nodeName == 'Group':
                        group = cdr.getTextContent(grandchild)
                    elif grandchild.nodeName == 'Source':
                        source = cdr.getTextContent(grandchild)
                if name == 'Preferred_Name':
                    self.preferredName = value
                elif name == 'Semantic_Type':
                    self.semanticType = value
                elif name == 'Synonym':
                    self.synonyms.append(value)
                elif name == 'CAS_Registry':
                    self.casCodes.append(value)
                elif name == 'NSC_Code':
                    self.nscCodes.append(value)
                elif name == 'DEFINITION':
                    self.definitions.append(Definition(value, source))
                elif name == 'FULL_SYN':
                    self.fullSyn.append(FullSynonym(value, group, source))

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
# Retrieve a concept document from the NCI Thesaurus.
#----------------------------------------------------------------------
def fetchConcept(code):
    global err
    err   = ""
    code  = code.strip()
    host  = "lexevsapi.nci.nih.gov"
    port  = httplib.HTTP_PORT
    app   = "/lexevsapi50/GetXML"
    parms = "query=org.LexGrid.concepts.Concept[@_entityCode=%s]" % code
    url   = "%s?%s" % (app, parms)
    tries = 3
    while tries:
        try:
            conn = httplib.HTTPConnection(host, port)

            # Submit the request and get the headers for the response.
            conn.request("GET", url)
            response = conn.getresponse()

            # Skip past any "Continue" responses.
            while response.status == httplib.CONTINUE:
                response.msg = None
                response.begin()

            # Check for failure.
            if response.status != httplib.OK:
                try:
                    page = response.read()
                    now  = time.strftime("%Y%m%d%H%M%S")
                    name = cdr.DEFAULT_LOGDIR + "/ncit-%s-%s.html" % (code, now)
                    f = open(name, "wb")
                    f.write(page)
                    f.close()
                except:
                    pass
                err =  ("<error>Failure retrieving concept %s; "
                        "HTTP response %s: %s</error>" %
                        (code, response.status, response.reason))
                return None

            # We can stop trying now, we got it.
            docXml = response.read()
            tries = 0

        except Exception, e:
            tries -= 1
            if not tries:
                err =  "<error>EVS server unavailable: %s</error>" % e
                return None

    filt = ["name:EVS Concept Filter"]
    result = cdr.filterDoc('guest', filt, doc = docXml)
    if type(result) in (str, unicode):
        now = time.strftime("%Y%m%d%H%M%S")
        f = open("d:/tmp/ConceptDoc-%s-%s.xml" % (code, now), "wb")
        f.write(docXml)
        f.close()
        err =  "<error>Error in EVS response: %s</error>" % result
        return None
    docXml = result[0]
    try:
        dom = xml.dom.minidom.parseString(docXml)
        return Concept(dom.documentElement)
    except Exception, e:
        err =  "<error>Failure parsing concept: %s</error>" % str(e)
        return None

#----------------------------------------------------------------------
# See if citation already exists.
#----------------------------------------------------------------------
def findExistingConcept(conn,code):
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
# Get the preferred name for the cdr document
#----------------------------------------------------------------------
def getPreferredName(dom):
    docElem = dom.documentElement
    for node in docElem.childNodes:
        if node.nodeName == 'PreferredName':
            return cdr.getTextContent(node)
    return ""

#----------------------------------------------------------------------
# Get the semantic type text for the cdr document
#----------------------------------------------------------------------
def getSemanticType(dom):
    docElem = dom.documentElement
    for node in docElem.childNodes:
        if node.nodeName == 'SemanticType':
            return cdr.getTextContent(node)
    return ""

def removeAllSpaces(strToRemove):
    if not strToRemove:
        return ""
    return strToRemove.replace(' ', '')

#----------------------------------------------------------------------
# Update the definition
#----------------------------------------------------------------------
def updateDefinition(dom, definition):
    global bChanged
    global changes
    bFound = False
    docElem = dom.documentElement
    insertPosition = 0
    for node in docElem.childNodes:
        if node.nodeName == 'OtherName':
            insertPosition = docElem.childNodes.index(node) + 1
        elif node.nodeName == 'TermType':
            insertPosition = docElem.childNodes.index(node)
        elif node.nodeName == 'Definition':
            for n in node.childNodes:
                if n.nodeName == 'DefinitionText':
                    for nn in n.childNodes:
                        if nn.nodeType == nn.TEXT_NODE:
                            dt = removeAllSpaces(nn.nodeValue)
                            if dt != removeAllSpaces(definition.text):
                                bChanged = True
                                changes += ' Definition updated.'
                                nn.nodeValue = definition.text
                                bFound = True
                elif n.nodeName == 'ReviewStatus':
                    for nn in n.childNodes:
                        if nn.nodeType == xml.dom.minidom.Node.TEXT_NODE:
                            if bChanged:
                                nn.nodeValue = 'Unreviewed'

    # Definition not found, need to add it
    if not bFound:
        bChanged = True
        changes += ' Definition added.'
        docElem.childNodes.insert(insertPosition, definition.toNode(dom))

#----------------------------------------------------------------------
# addOtherName
#----------------------------------------------------------------------
def addOtherName(dom, fullSyn, conceptCode, drugTerm, cdrPreferredName):
    global bChanged
    global changes
    bChanged = True
    docElem = dom.documentElement
    for node in docElem.childNodes:
        if node.nodeName == 'OtherName':
            changes += ' Other Name: ' + fullSyn.toDescription() + ' added.'
            docElem.insertBefore(fullSyn.toNode(dom, conceptCode, drugTerm,
                                                cdrPreferredName), node);
            return;
        
    docElem.appendChild(fullSyn.toNode(dom, conceptCode, drugTerm,
                                       cdrPreferredName))

#----------------------------------------------------------------------
# getOtherNames
#----------------------------------------------------------------------
def getOtherNames(dom):
    otherNames = []
    docElem = dom.documentElement
    for node in docElem.childNodes:
        if node.nodeName == 'OtherName':
            for n in node.childNodes:
                if n.nodeName == 'OtherTermName':
                    termName = cdr.getTextContent(n)
                elif n.nodeName == 'OtherNameType':
                    nameType = cdr.getTextContent(n)
                    if (nameType == 'Lexical variant'):
                        nameType = 'Synonym'
                    otherNames.append(OtherName(termName, nameType))
    return otherNames

#----------------------------------------------------------------------
# updateTermStatus
#----------------------------------------------------------------------
def updateTermStatus(dom,status):
    docElem = dom.documentElement
    elems = docElem.getElementsByTagName("TermStatus")
    for elem in elems:
        for node in elem.childNodes:
            if node.nodeType == xml.dom.minidom.Node.TEXT_NODE:
                node.nodeValue = status

#----------------------------------------------------------------------
# getCDRSemanticType
#----------------------------------------------------------------------
def getCDRSemanticType(session,CDRID):
    docId = cdr.normalize(CDRID)
    oldDoc = cdr.getDoc(session, docId, 'N')
    if oldDoc.startswith("<Errors"):
        return "<error>Unable to retrieve %s - %s</error>" % (CDRID,oldDoc)
    oldDoc = cdr.getDoc(session, docId, 'N',getObject=1)
    dom = xml.dom.minidom.parseString(oldDoc.xml)
    semanticType = getSemanticType(dom)
    return semanticType

#----------------------------------------------------------------------
# getNCITPreferredName
#----------------------------------------------------------------------
def getNCITPreferredName(conceptCode):
    concept = fetchConcept(conceptCode)
    if err:
        return err
    return concept.preferredName

#----------------------------------------------------------------------
# getCDRPreferredName
#----------------------------------------------------------------------
def getCDRPreferredName(session, CDRID):
    docId = cdr.normalize(CDRID)
    oldDoc = cdr.getDoc(session, docId, 'N')
    if oldDoc.startswith("<Errors"):
        return "<error>Unable to retrieve %s - %s</error>" % (CDRID, oldDoc)
    oldDoc = cdr.getDoc(session, docId, 'N', getObject=True)
    dom = xml.dom.minidom.parseString(oldDoc.xml)
    preferredName = getPreferredName(dom)
    return preferredName

class DocConceptPair:
    def __init__(self, doc_id, concept, updateDef):
        self.doc_id = doc_id
        self.concept = concept
        self.updateDef = updateDef

docConceptPairs = []

def getThingsToUpdate(isDrugUpdate, excelSSName):
    import os.path, ExcelReader
    if isDrugUpdate:

        #first, get a list of all the drug/agent concept codes
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

        #----------------------------------------------------------------------
        # Submit the query to the database.
        #----------------------------------------------------------------------
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
                cell = row[0]
                rawValue = cell.val
                cdridsToSkipDef.append(rawValue)
     
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
    wb.write(fobj, asXls = True, big = True)
    fobj.close()

#----------------------------------------------------------------------
# Update an existing concept/term
#----------------------------------------------------------------------
def updateTerm(session, CDRID, conceptCode, doUpdate=False,
               doUpdateDefinition=True, doImportTerms=True, drugTerm=True):
    global bChanged
    global err
    global changes
    checkOut = 'N'
    bChanged = False
    err = ""
    changes = ''
    publishVer = 'N'

    if doUpdate:
        checkOut = 'Y'

    docId = cdr.normalize(CDRID)
    cdr.unlock(session,docId)
    
    if drugTerm:
        semanticType = getCDRSemanticType(session, CDRID)
        if (semanticType != "Drug/agent"):
            return ("<error>Semantic Type is %s. The importing only works "
                    "for Drug/agent</error>" % semanticType)
    else:
        query = """SELECT publishable
                     FROM doc_version
                    WHERE num = (SELECT max(num)
                                   FROM doc_version
                                  WHERE id = %s)
                      AND id = %s""" % (CDRID, CDRID)
        try:
            conn = cdrdb.connect('CdrGuest')
            cursor = conn.cursor()
            cursor.execute(query, timeout=300)
            rows = cursor.fetchall()
        except cdrdb.Error, info:
            return 'Failure retrieving Summary documents: %s' % info[1][0]
             
        if not rows:
            return 'No Records Found for Selection'

        for publishableVer in rows:
            publishVer =  publishableVer[0]

    #conn = connectToDB()
    if err:
        return err
    oldDoc = cdr.getDoc(session, docId, checkOut)
    if oldDoc.startswith("<Errors"):
        return ("<error>Unable to retrieve %s - %s, "
                "session = %s</error>" % (CDRID,oldDoc,session))
    cdr.unlock(session, docId)
    oldDoc = cdr.getDoc(session, docId, checkOut, getObject=True)

    concept = fetchConcept(conceptCode)
    if err:
        return err

    dom = xml.dom.minidom.parseString(oldDoc.xml)
    # update the definition, if there is one.
    if doUpdateDefinition:
        for definition in concept.definitions:
            if definition.source == 'NCI':
                definition.drugTerm = drugTerm
                updateDefinition(dom, definition)
                break

    if doImportTerms:
        # fetch the other names
        otherNames = getOtherNames(dom)
        for syn in concept.fullSyn:
            if syn.termSource != 'NCI-GLOSS':
                bfound = False
                for otherName in otherNames:
                    if syn.termName.upper() == otherName.termName.upper():
                        if syn.mappedTermGroup == otherName.nameType:
                            bfound = True
                                
                # Other Name not found, add it
                if not bfound:
                    cdrPreferredName = ''
                    if not drugTerm:
                        cdrPreferredName = getCDRPreferredName(session, CDRID)
                    addOtherName(dom, syn, conceptCode, drugTerm,
                                 cdrPreferredName)

    #set the TermStatus to Unreviewed, if a change was made
    if bChanged:
        updateTermStatus(dom, 'Unreviewed')

        oldDoc.xml = dom.toxml().encode('utf-8')
          
        if doUpdate:
            strDoc = str(oldDoc)
            updateComment = "NCI Thesaurus Update"
            resp = cdr.repDoc(session, doc=strDoc, val='Y', ver='Y',
                              verPublishable=publishVer, showWarnings=True,
                              comment=updateComment)
            cdr.unlock(session, docId)
            if not resp[0]:
                return ("<error>Failure adding concept %s: %s</error>" %
                        (CDRID, cdr.checkErr(resp[1])))
            return ("Term %s updated. Publishable = %s. %s" %
                    (CDRID, publishVer, changes))
        return ("Term %s will change. Publishable = %s. %s" %
                (CDRID, publishVer, changes))
    else:
        cdr.unlock(session,docId)
        return "No updates needed for term %s." % CDRID

#----------------------------------------------------------------------
# Add a new Term
#----------------------------------------------------------------------
def addNewTerm(session, conceptCode, updateDefinition=True, importTerms=True):
    conn = connectToDB()
    if err:
        return err
    concept = fetchConcept(conceptCode)
    if err:
        return err
    docId = findExistingConcept(conn,conceptCode)
    if err:
        return err
    if docId:
        return ("<error>Concept has already been imported as CDR%010d</error>"
                % docId)
    doc = [u"""\
<Term xmlns:cdr='cips.nci.nih.gov/cdr'>
 <PreferredName>%s</PreferredName>
""" % fix(concept.preferredName)]
    if importTerms:
        for syn in concept.fullSyn:
            if syn.termSource == 'NCI':
                code = syn.termGroup == 'PT' and conceptCode or None
                termType = mapType(syn.termGroup)
                doc.append(makeOtherName(syn.termName, termType, syn.termGroup,
                                         code))
        for indCode in concept.indCodes:
            doc.append(makeOtherName(indCode, 'IND code', 'IND_Code'))
        for nscCode in concept.nscCodes:
            doc.append(makeOtherName(nscCode, 'NSC code', 'NSC_Code'))
        for casCode in concept.casCodes:
            doc.append(makeOtherName(casCode, 'CAS Registry name',
                                     'CAS_Registry'))
            
    if updateDefinition:
        for definition in concept.definitions:
            if definition.source == 'NCI':
                doc.append(definition.toXml())
                
    doc.append(u"""\
 <TermType>
  <TermTypeName>Index term</TermTypeName>
 </TermType>
 <TermStatus>Unreviewed</TermStatus>
</Term>
""")
    wrapper = u"""\
<CdrDoc Type='Term' Id=''>
 <CdrDocCtl>
  <DocType>Term</DocType>
  <DocTitle>%s</DocTitle>
 </CdrDocCtl>
 <CdrDocXml><![CDATA[%%s]]></CdrDocXml>
</CdrDoc>
""" % fix(concept.preferredName)
    doc = (wrapper % u"".join(doc)).encode('utf-8')
    resp = cdr.addDoc(session, doc = doc, val = 'Y', showWarnings = True)
    if not resp[0]:
        cdrcgi.bail("Failure adding new term document: %s" % resp[1])
    cdr.unlock(session, resp[0])
    return "Concept added as %s" % resp[0]
