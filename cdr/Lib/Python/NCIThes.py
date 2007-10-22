#------------------------------------------------------
# NCIThes.py
#
# Contains routines for updating Drub Term documents
# from the NCI thesaurus and routines for adding
# new terms to CDR from the NCI thesaurus.
#------------------------------------------------------

import cgi, cdr, re, cdrdb, xml.dom.minidom, httplib, time

err = ""
bChanged = 0
changes = ''

#----------------------------------------------------------------------
# Prepare string for living in an XML document.
#----------------------------------------------------------------------
def fix(s):
    return s and cgi.escape(s) or u''

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
    pattern = re.compile(u"(<def-source>(.*)</def-source>)?"
                         u"(<def-definition>(.*)</def-definition>)?",
                         re.DOTALL)
    def __init__(self, value):
        match = Definition.pattern.search(value)
        self.source = None
        self.text   = None
        if match:
            self.source = match.group(2)
            self.text   = match.group(4)
    def toXml(self):
        return """\
 <Definition>
  <DefinitionText>%s</DefinitionText>
  <DefinitionType>Health professional</DefinitionType>
  <DefinitionSource>
   <DefinitionSourceName>NCI Thesaurus</DefinitionSourceName>
  </DefinitionSource>
  <ReviewStatus>Reviewed</ReviewStatus>
 </Definition>
""" % fix(self.text or u'')

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
        text = dom.createTextNode('Reviewed')
        child.appendChild(text)
        node.appendChild(child)     
        
        return node

#----------------------------------------------------------------------
# Object for an articulated synonym from the NCI thesaurus.
#----------------------------------------------------------------------
class FullSynonym:
    pattern = re.compile(u"(<term-name>(.*)</term-name>)?"
                         u"(<term-group>(.*)</term-group>)?"
                         u"(<term-source>(.*)</term-source>)?"
                         u"(<source-code>(.*)</source-code>)?",
                         re.DOTALL)
    def __init__(self, value):
        match = FullSynonym.pattern.search(value)
        self.termName   = None
        self.termGroup  = None
        self.mappedTermGroup  = None
        self.termSource = None
        self.sourceCode = None
        if match:
            self.termName   = match.group(2)
            self.termGroup  = match.group(4)
            self.termSource = match.group(6)
            self.sourceCode = match.group(8)
            self.mappedTermGroup = mapType(self.termGroup)

    def toDescription(self):
        return self.termName + ' (' + self.termGroup + ')'

    def toNode(self,dom,conceptCode):
        termName = self.termName
        mappedTermGroup = self.mappedTermGroup
        termGroup = self.termGroup
        sourceCode = self.sourceCode

        if self.termGroup == 'PT':
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

        if sourceCode is not None and self.termGroup == 'PT':
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
                name = None
                value = None
                for grandchild in child.childNodes:
                    if grandchild.nodeName == 'Name':
                        name = cdr.getTextContent(grandchild)
                    elif grandchild.nodeName == 'Value':
                        value= cdr.getTextContent(grandchild)
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
                elif name == 'IND_Code':
                    self.indCodes.append(value)
                elif name == 'DEFINITION':
                    self.definitions.append(Definition(value))
                elif name == 'FULL_SYN':
                    self.fullSyn.append(FullSynonym(value))

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
        err = '<error>Failure connecting to CDR: %s' % info[1][0]
        return 0

#----------------------------------------------------------------------
# Retrieve a concept document from the NCI Thesaurus.
#----------------------------------------------------------------------
def fetchConcept(code):
    global err
    #cmd = ("java -classpath d:/cdr/lib;d:/usr/lib/evs-client.jar;"
    #       "d:/usr/lib/log4j.jar "
    #       "RetrieveConceptFromEvs %s" % code)
    #result = cdr.runCommand(cmd)
    #if result.code:
    #    cdrcgi.bail("Failure fetching concept: %s" %
    #                (result.output or "unknown failure"))
    err = ""
    code  = code.strip()
    host  = "cabio-qa.nci.nih.gov"
    host  = "cabio.nci.nih.gov" # temporary fix while cabio-qa is broken
    port  = httplib.HTTP_PORT
    parms = "?query=DescLogicConcept&DescLogicConcept[@code=%s]" % code
    url = ("http://cabio-qa.nci.nih.gov/cacore32/GetXML?"
           "query=DescLogicConcept&DescLogicConcept[@code=%s]" % code)
    url   = "/cacore32/GetXML%s" % parms
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
                err =  """\<error>Failure retrieving concept %s; 
                                  HTTP response %s: %s""" % (code, response.status,response.reason)
                return 0
            # We can stop trying now, we got it.
            docXml = response.read()
            tries = 0

        except Exception, e:
            tries -= 1
            if not tries:
                err =  "<error>EVS server unavailable: %s" % e
                return 0
    filt = ["name:EVS Concept Filter"]
    result = cdr.filterDoc('guest', filt, doc = docXml)
    if type(result) in (str, unicode):
        now = time.strftime("%Y%m%d%H%M%S")
        f = open("d:/tmp/ConceptDoc-%s-%s.xml" % (code, now), "wb")
        f.write(docXml)
        f.close()
        err =  "<error>Error in EVS response: %s" % result
        return 0
    docXml = result[0]
    try:
        dom = xml.dom.minidom.parseString(docXml)
        return Concept(dom.documentElement)
    except Exception, e:
        err =  "<error>Failure parsing concept: %s" % str(e)
        return 0

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
        err =  '<error>Failure checking for existing document: %s' % info[1][0]
        return 0

#----------------------------------------------------------------------
# Get the preferred name for the cdr document
#----------------------------------------------------------------------
def getPreferredName(dom):
    docElem = dom.documentElement
    for node in docElem.childNodes:
        if node.nodeType == xml.dom.minidom.Node.ELEMENT_NODE:
            if node.nodeName == 'PreferredName':
                for n in node.childNodes:
                    if n.nodeType == xml.dom.minidom.Node.TEXT_NODE:
                        return n.nodeValue

    return ""

#----------------------------------------------------------------------
# Get the semantic type text for the cdr document
#----------------------------------------------------------------------
def getSemanticType(dom):
    docElem = dom.documentElement
    for node in docElem.childNodes:
        if node.nodeType == xml.dom.minidom.Node.ELEMENT_NODE:
            if node.nodeName == 'SemanticType':
                for n in node.childNodes:
                    if n.nodeType == xml.dom.minidom.Node.TEXT_NODE:
                        return n.nodeValue

    return ""

#----------------------------------------------------------------------
# update the definition
#----------------------------------------------------------------------
def updateDefinition(dom,definition):
    global bChanged
    global changes
    bFound = 0
    docElem = dom.documentElement
    for node in docElem.childNodes:
        if node.nodeType == xml.dom.minidom.Node.ELEMENT_NODE:
            if node.nodeName == 'Definition':
                for n in node.childNodes:
                    if n.nodeType == xml.dom.minidom.Node.ELEMENT_NODE:
                        if n.nodeName == 'DefinitionText':
                            for nn in n.childNodes:
                                if nn.nodeType == xml.dom.minidom.Node.TEXT_NODE:
                                    if nn.nodeValue != definition.text:
                                        bChanged = 1
                                        changes += ' Definition updated.'
                                        nn.nodeValue = definition.text
                                        bFound = 1
                                    else:
                                        bFound = 1
                        elif n.nodeName == 'ReviewStatus':
                            for nn in n.childNodes:
                                if nn.nodeType == xml.dom.minidom.Node.TEXT_NODE:
                                    if bChanged == 1:
                                        nn.nodeValue = 'Unreviewed'
                                
    # definition not found, need to add it
    if bFound == 0:
        changes += ' Definition added.'
        docElem.appendChild(definition.toNode(dom))

#----------------------------------------------------------------------
# addOtherName
#----------------------------------------------------------------------
def addOtherName(dom,fullSyn,conceptCode):
    global bChanged
    global changes
    bChanged = 1
    docElem = dom.documentElement
    for node in docElem.childNodes:
        if node.nodeName == 'OtherName':
            changes += ' Other Name: ' + fullSyn.toDescription() + ' added.'
            docElem.insertBefore(fullSyn.toNode(dom,conceptCode),node);
            return;
        
    docElem.appendChild(fullSyn.toNode(dom))

#----------------------------------------------------------------------
# getOtherNames
#----------------------------------------------------------------------
def getOtherNames(dom):
    otherNames = []
    docElem = dom.documentElement
    for node in docElem.childNodes:
        if node.nodeType == xml.dom.minidom.Node.ELEMENT_NODE:
            if node.nodeName == 'OtherName':
                for n in node.childNodes:
                    if n.nodeType == xml.dom.minidom.Node.ELEMENT_NODE:
                        if n.nodeName == 'OtherTermName':
                            for nn in n.childNodes:
                                if nn.nodeType == xml.dom.minidom.Node.TEXT_NODE:
                                    termName = nn.nodeValue
                        elif n.nodeName == 'OtherNameType':
                            for nn in n.childNodes:
                                if nn.nodeType == xml.dom.minidom.Node.TEXT_NODE:
                                    nameType = nn.nodeValue
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
    #conn = connectToDB()
    docId = cdr.normalize(CDRID)
    oldDoc = cdr.getDoc(session, docId, 'N')
    if oldDoc.startswith("<Errors"):
        return "<error>Unable to retrieve %s" % CDRID
    oldDoc = cdr.getDoc(session, docId, 'N',getObject=1)
    dom = xml.dom.minidom.parseString(oldDoc.xml)
    semanticType = getSemanticType(dom)
    return semanticType

#----------------------------------------------------------------------
# getNCITPreferredName
#----------------------------------------------------------------------
def getNCITPreferredName(conceptCode):
    concept = fetchConcept(conceptCode)
    if len(err) > 1:
        return err
    return concept.preferredName

#----------------------------------------------------------------------
# getCDRPreferredName
#----------------------------------------------------------------------
def getCDRPreferredName(session,CDRID):
    #conn = connectToDB()
    docId = cdr.normalize(CDRID)
    oldDoc = cdr.getDoc(session, docId, 'N')
    if oldDoc.startswith("<Errors"):
        return "<error>Unable to retrieve %s" % CDRID
    oldDoc = cdr.getDoc(session, docId, 'N',getObject=1)
    dom = xml.dom.minidom.parseString(oldDoc.xml)
    preferredName = getPreferredName(dom)
    return preferredName

#----------------------------------------------------------------------
# Update an existing concept\term
#----------------------------------------------------------------------
def updateTerm(session,CDRID,conceptCode,doUpdate=0):
    global bChanged
    global err
    global changes
    bChanged = 0

    docId = cdr.normalize(CDRID)
    cdr.unlock(session,docId)

    semanticType = getCDRSemanticType(session,CDRID)
    if (semanticType != """Drug/agent"""):
        return """<error>Semantic Type is %s. The importing only works for Drug/agent""" % semanticType

    #conn = connectToDB()
    if len(err) > 1:
        return err
    oldDoc = cdr.getDoc(session, docId, 'Y')
    if oldDoc.startswith("<Errors"):
        return "<error>Unable to retrieve %s" % CDRID
    cdr.unlock(session,docId)
    oldDoc = cdr.getDoc(session, docId, 'Y',getObject=1)

    concept = fetchConcept(conceptCode)
    if len(err) > 1:
        return err

    dom = xml.dom.minidom.parseString(oldDoc.xml)
    # update the definition, if there is one.
    for definition in concept.definitions:
        if definition.source == 'NCI':
            updateDefinition(dom,definition)
            break

    # fetch the other names
    otherNames = getOtherNames(dom)
    for syn in concept.fullSyn:
        bfound = 0
        for otherName in otherNames:
            if syn.termName == otherName.termName:
                if syn.mappedTermGroup == otherName.nameType:
                    bfound = 1
                        
        # Other Name not found, add it
        if bfound == 0:                
            addOtherName(dom,syn,conceptCode)

    #set the TermStatus to Unreviewed, if a change was made
    if bChanged == 1:
        updateTermStatus(dom,'Unreviewed')

        oldDoc.xml = dom.toxml().encode('utf-8')
            
        if doUpdate:
            strDoc = str(oldDoc)
            resp = cdr.repDoc(session, doc = strDoc, val = 'Y', ver = 'Y', verPublishable = 'N', showWarnings = 1)
            cdr.unlock(session,docId)
            if not resp[0]:
                return "<error>Failure adding concept %s: %s" % (updateCDRID, cdr.checkErr(resp[1]) ) 
                            
            return "Citation %s updated. %s" % (CDRID,changes)
        return "Citation %s will change. %s" % (CDRID,changes)
    else:
        cdr.unlock(session,docId)
        return "No updates needed for citation %s." % CDRID

#----------------------------------------------------------------------
# Add a new Term
#----------------------------------------------------------------------
def addNewTerm(session,conceptCode):
    #conn = connectToDB()
    if len(err) > 1:
        return err
    docId = findExistingConcept(conn,conceptCode)
    if len(err) > 1:
        return err
    if docId:
        return "<error>Concept has already been imported as CDR%010d" % docId
    doc = u"""\
            <Term xmlns:cdr='cips.nci.nih.gov/cdr'>
           <PreferredName>%s</PreferredName>
           """ % fix(concept.preferredName)
    for syn in concept.fullSyn:
        if syn.termSource == 'NCI':
            code = syn.termGroup == 'PT' and conceptCode or None
            termType = mapType(syn.termGroup)
            doc += makeOtherName(syn.termName, termType, syn.termGroup,
                                     code)
    for indCode in concept.indCodes:
        doc += makeOtherName(indCode, 'IND code', 'IND_Code')
    for nscCode in concept.nscCodes:
        doc += makeOtherName(nscCode, 'NSC code', 'NSC_Code')
    for casCode in concept.casCodes:
        doc += makeOtherName(casCode, 'CAS Registry name', 'CAS_Registry')
    for definition in concept.definitions:
        if definition.source == 'NCI':
            doc += definition.toXml()
    doc += u"""\
     <TermType>
      <TermTypeName>Index term</TermTypeName>
     </TermType>
     <TermStatus>Unreviewed</TermStatus>
    </Term>
    """
    wrapper = u"""\
    <CdrDoc Type='Term' Id=''>
     <CdrDocCtl>
      <DocType>Term</DocType>
      <DocTitle>%s</DocTitle>
     </CdrDocCtl>
     <CdrDocXml><![CDATA[%%s]]></CdrDocXml>
    </CdrDoc>
    """ % fix(concept.preferredName)
    doc = (wrapper % doc).encode('utf-8')
    resp = cdr.addDoc(session, doc = doc, val = 'Y', showWarnings = 1)

    cdr.unlock(session, resp[0])
        
    return "Concept added as %s" % resp[0]
