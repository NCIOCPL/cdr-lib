#!/usr/bin/python

#----------------------------------------------------------------------
#
# $Id$
#
# Common code for processing clinical trial documents from CTRP
# (Clinical Trials Reporting Program).  The original plan was to
# import these documents as a new CTRPProtocol document type.
# Eventually, after unsuccessful attempt to resolve the problems
# that CTRP was having, that plan was abandoned, and we now just
# import the participating site information from the CTRP documents
# into the corresponding CTGovProtocol documents that we already
# have.
#
# BZIssue::4942
#
#----------------------------------------------------------------------

import cdr, cdrdb, lxml.etree as etree, sys, time

CDR_NAMESPACE = 'cips.nci.nih.gov/cdr'
NSMAP = { 'cdr' : CDR_NAMESPACE }
POID = 'CTRP_PO_ID'

cursor = cdrdb.connect('CdrGuest').cursor()
session = None

#----------------------------------------------------------------------
# Gets the mapping table's type ID for CTRP person/org ID mappings.
#----------------------------------------------------------------------
def getPoidUsage():
    cursor.execute("SELECT id FROM external_map_usage WHERE name = ?", POID)
    rows = cursor.fetchall()
    return rows and rows[0][0] or None

#----------------------------------------------------------------------
# Loads the dictionary of CTRP person/org ID mappings from the mapping
# table.
#----------------------------------------------------------------------
def getPoidMappings():
    cursor.execute("SELECT value, doc_id FROM external_map WHERE usage = ?",
                   getPoidUsage())
    mappings = {}
    for poId, docId in cursor.fetchall():
        mappings[poId] = docId
    return mappings

#----------------------------------------------------------------------
# CTRP sometimes wraps text in an extra 'textblock' element, sometimes
# not (it's random).  This function gets that text whichever way it's
# stored.
#----------------------------------------------------------------------
def extractText(node):
    for child in node.findall('textblock'):
        return child.text
    return node.text

#----------------------------------------------------------------------
# Another workaround for the random wrapping of text in 'textblock'
# elements by CTRP.
#----------------------------------------------------------------------
def getParagraphs(node):
    paras = [child.text for child in node.findall('textblock')]
    return paras or [node.text]

#----------------------------------------------------------------------
# Represents a person, organization, state, or country found in a
# CTRP trial document for which we can't find a corresponding mapped
# CDR document.  The ctrpId attribute refers to the ID CTRP uses
# for the unmapped entity, not the trial document itself.
#----------------------------------------------------------------------
class MappingProblem:
    def __init__(self, docType, value, ctrpId=None):
        self.docType = docType
        self.value = value
        self.ctrpId = ctrpId
    @staticmethod
    def getChildText(node, tag):
        for child in node.findall(tag):
            return child.text
        return None
    @staticmethod
    def extractStrings(node):
        values = []
        for path in ('name', 'first_name', 'middle_initial', 'last_name',
                     'address/street', 'address/city', 'address/state',
                     'address/zip', 'address/country'):
            for child in node.findall(path):
                if child.text is not None:
                    text = child.text.strip()
                    if text:
                        values.append(text)
        return u" ".join(values)
    @staticmethod
    def findMappingProblems(session, tree, poIds, geoMappings, orgsOnly=False):
        """
        Builds a dictionary of persons, organizations, states, and countries
        in a CTRP trial for which we can't identify the corresponding CDR
        documents.

        Passed:
            session     - ID of a CDR login session
            tree        - parsed XML document (from lxml.etree parser)
            poIds       - object holding known person/org mappings
            geoMappings - object holding known country and state mappings
            orgsOnly    - True=don't bother reporting problems for persons
        """
        problems = {}
        tags = { 'location/facility': 'Organization',
                 'location/contact': 'Person',
                 'location/investigator': 'Person',
                 'overall_official': 'Person' }
        if orgsOnly:
            tags = { 'location/facility': 'Organization' }
        for path, docType in tags.iteritems():
            for node in tree.findall(path):
                poId = MappingProblem.getChildText(node, 'po_id')
                if not poId:
                    raise Exception("missing po_id element for %s" %
                                    repr(MappingProblem.extractStrings(node)))
                cdrId = poIds.get(poId)
                if not cdrId:
                    key = (docType, poId)
                    value = MappingProblem.extractStrings(node)
                    problems[key] = MappingProblem(docType, value, poId)
                    if poId not in poIds:
                        cdr.addExternalMapping(session, POID, poId)
                        poIds[poId] = None
                for node in node.findall("address"):
                    country = MappingProblem.getChildText(node, 'country')
                    if geoMappings.lookupCountryId(country) is None:
                        problem = MappingProblem('Country', country)
                        problems[('Country', country)] = problem
                    state = MappingProblem.getChildText(node, 'state')
                    if geoMappings.lookupStateId(state, country) is None:
                        value = u"%s|%s" % (state, country)
                        key = ('State/Province', value)
                        problem = MappingProblem('State/Province', value)
                        problems[key] = problem
        return problems

#----------------------------------------------------------------------
# Tools for looking up CTRP download and import disposition codes
# and names.
#----------------------------------------------------------------------
class Dispositions:
    downloadCodes = {}
    downloadNames = {}
    importCodes   = {}
    importNames   = {}
    @staticmethod
    def loadMaps():
        cursor = cdrdb.connect("CdrGuest").cursor()
        table = "ctrp_download_disposition"
        cursor.execute("SELECT disp_id, disp_name FROM %s" % table)
        for code, name in cursor.fetchall():
            Dispositions.downloadNames[code] = name
            Dispositions.downloadCodes[name] = code
        table = "ctrp_import_disposition"
        cursor.execute("SELECT disp_id, disp_name FROM %s" % table)
        for code, name in cursor.fetchall():
            Dispositions.importNames[code] = name
            Dispositions.importCodes[name] = code
    @staticmethod
    def lookupDownloadName(code):
        if not Dispositions.downloadNames:
            Dispositions.loadMaps()
        return Dispositions.downloadNames.get(code)
    @staticmethod
    def lookupDownloadCode(name):
        if not Dispositions.downloadCodes:
            Dispositions.loadMaps()
        return Dispositions.downloadCodes.get(name)
    @staticmethod
    def lookupImportName(code):
        if not Dispositions.importNames:
            Dispositions.loadMaps()
        return Dispositions.importNames.get(code)
    @staticmethod
    def lookupImportCode(name):
        if not Dispositions.importCodes:
            Dispositions.loadMaps()
        return Dispositions.importCodes.get(name)

#----------------------------------------------------------------------
# Object holding known state and country mappings between CTRP values
# and the matching CDR IDs.
#----------------------------------------------------------------------
class GeographicalMappings:

    def __init__(self):
        cursor = cdrdb.connect('CdrGuest').cursor()
        cursor.execute("""\
SELECT doc_id, value
  FROM query_term
 WHERE path = '/Country/ISOShortCountryName'""")
        self.countryIds = {}
        countryNames = {}
        for docId, value in cursor.fetchall():
            value = value.lower().strip()
            self.countryIds[value] = docId
            countryNames[docId] = value
        self.stateIds = {}
        for length in ('Short', 'Full'):
            cursor.execute("""\
SELECT s.doc_id, s.value, c.int_val
  FROM query_term s
  JOIN query_term c
    ON s.doc_id = c.doc_id
 WHERE s.path = '/PoliticalSubUnit/PoliticalSubUnit%sName'
   AND c.path = '/PoliticalSubUnit/Country/@cdr:ref'""" % length)
            for docId, name, countryId in cursor.fetchall():
                countryName = countryNames.get(countryId)
                if countryName:
                    key = u"%s|%s" % (name.strip().lower(), countryName)
                    self.stateIds[key] = docId
        self.countryNames = self.loadDocTitles(cursor, "Country")
        self.stateNames = self.loadDocTitles(cursor, "PoliticalSubUnit")

    def lookupCountryId(self, name):
        if not name:
            return None
        return self.countryIds.get(name.lower())

    def lookupStateId(self, name, country):
        if not name or not country:
            return None
        key = u"%s|%s" % (name.strip().lower(), country.strip().lower())
        return self.stateIds.get(key)

    def lookupCountryName(self, countryId):
        return self.countryNames.get(countryId)

    def lookupStateName(self, stateId):
        return self.stateNames.get(stateId)

    @staticmethod
    def loadDocTitles(cursor, docType):
        cursor.execute("""\
SELECT d.id, d.title
  FROM active_doc d
  JOIN doc_type t
    ON t.id = d.doc_type
 WHERE t.name = ?""", docType)
        ids = {}
        for docId, docTitle in cursor.fetchall():
            ids[docId] = docTitle
        return ids

#----------------------------------------------------------------------
# Module object holding mappings for states and countries.
#----------------------------------------------------------------------
geoMap = GeographicalMappings()

#----------------------------------------------------------------------
# Adds an attribute with its value to an XML element we've created.
#----------------------------------------------------------------------
def addAttribute(node, name, value):
    if value:
        node.set(name, value)

#----------------------------------------------------------------------
# Adds an XML child element, optionally with text content, to a parent
# element.
#----------------------------------------------------------------------
def addChild(parent, name, value):
    if value:
        etree.SubElement(parent, name).text = value

#----------------------------------------------------------------------
# Adds an XML child element, optionally with Para grandchildren, to a
# parent element.
#----------------------------------------------------------------------
def addChildWithParas(parent, name, paras):
    if paras:
        child = etree.Element(name)
        for para in paras:
            etree.SubElement(child, 'Para').text = para
        parent.append(child)

#----------------------------------------------------------------------
# Adds Phase elements to a CTRPProtocol document we're building.
#----------------------------------------------------------------------
def addPhases(parent, ctrpPhase):
    if not ctrpPhase or ctrpPhase == 'N/A':
        addChild(parent, 'Phase', 'No phase specified')
    elif ctrpPhase == 'I/II':
        addChild(parent, 'Phase', 'Phase I')
        addChild(parent, 'Phase', 'Phase II')
    elif ctrpPhase == 'I/II':
        addChild(parent, 'Phase', 'Phase II')
        addChild(parent, 'Phase', 'Phase III')
    elif ctrpPhase in ('0', 'I', 'II', 'III', 'IV'):
        addChild(parent, 'Phase', 'Phase %s' % ctrpPhase)
    else:
        raise Exception(u"invalid phase '%s'" % ctrpPhase)

#----------------------------------------------------------------------
# Looks up the DocTitle for a CDR document.
#----------------------------------------------------------------------
def getDocTitle(docId):
    cursor.execute("SELECT title FROM document WHERE id = ?", docId)
    rows = cursor.fetchall()
    return rows and rows[0][0] or None

#----------------------------------------------------------------------
# Adds a new document to the CDR.  Returns the CdrDoc object representing
# the new document.
#----------------------------------------------------------------------
def createCdrDoc(docXml, docType):
    doc = cdr.Doc(docXml, docType)
    comment = "Adding %s doc for CTRP import" % docType
    response = cdr.addDoc(session, doc=str(doc), comment=comment, ver='Y',
                          verPublishable='N', checkIn='Y')
    err = cdr.checkErr(response)
    if err:
        raise Exception("creating %s document: %s" % (docType, err))
    docId = cdr.exNormalize(response)[1]
    docTitle = getDocTitle(docId)
    return CdrDoc(docId, docTitle)

#----------------------------------------------------------------------
# Holds document ID and title for a CDR document.
#----------------------------------------------------------------------
class CdrDoc:
    def __init__(self, docId, docTitle):
        self.cdrId = docId
        self.title = docTitle

    @staticmethod
    def lookupExternalMapValue(value, usage):
        "Returns CdrDoc object matching CTRP ID (if any)"
        if not value or not usage:
            return None
        cursor.execute("""\
SELECT m.doc_id, d.title
  FROM external_map m
  JOIN external_map_usage u
    ON u.id = m.usage
  JOIN document d
    ON m.doc_id = d.id
 WHERE m.value = ?
   AND u.name = ?""", (value, usage))
        rows = cursor.fetchall()
        if not rows:
            return None
        return CdrDoc(rows[0][0], rows[0][1])

#----------------------------------------------------------------------
# Base class for elements which have po_id and ctep_id attributes.
#----------------------------------------------------------------------
class Ids:
    def __init__(self, node):
        self.poId = self.ctepId = None
        for child in node:
            if child.tag == 'po_id':
                self.poId = child.text
            elif child.tag == 'ctep_id':
                self.ctepId = child.text
    def convert(self, node):
        addAttribute(node, 'po_id', self.poId)
        addAttribute(node, 'ctep_id', self.ctepId)

#----------------------------------------------------------------------
# Base class for blocks which have contact elements (phone, email, etc.).
#----------------------------------------------------------------------
class ContactInfo:
    def convert(self, node, standaloneDoc=False):
        if self.address:
            node.append(self.address.convert(standaloneDoc))
        for number in self.phoneNumbers:
            etree.SubElement(node, 'Phone').text = number
        for number in self.faxNumbers:
            etree.SubElement(node, 'Fax').text = number
        for address in self.emailAddresses:
            etree.SubElement(node, 'Email').text = address
    def __init__(self, node):
        self.address = None
        self.phoneNumbers = []
        self.faxNumbers = []
        self.emailAddresses = []
        for child in node:
            if child.tag == 'address':
                self.address = Protocol.Address(child)
            elif child.tag == 'phone':
                self.phoneNumbers.append(child.text)
            elif child.tag == 'fax':
                self.faxNumbers.append(child.text)
            elif child.tag == 'email':
                self.emailAddresses.append(child.text)

#----------------------------------------------------------------------
# Represents a block for a person (e.g., overall_contact) in a CTRP
# document.
#----------------------------------------------------------------------
class Person(Ids, ContactInfo):
    def __init__(self, node):
        Ids.__init__(self, node)
        ContactInfo.__init__(self, node)
        self.name = Person.Name(node)
        self.role = None
        for child in node.findall('role'):
            self.role = child.text
    def convert(self, name):
        node = etree.Element(name)
        Ids.convert(self, node)
        self.addPdqPerson(node)
        addChild(node, 'GivenName', self.name.firstName)
        addChild(node, 'MiddleInitial', self.name.middleInitial)
        addChild(node, 'Surname', self.name.lastName)
        ContactInfo.convert(self, node)
        addChild(node, 'Role', self.role)
        return node
    def addPdqPerson(self, parent):
        doc = CdrDoc.lookupExternalMapValue(self.poId, 'CTRP_PO_ID')
        if not doc:
            doc = CdrDoc.lookupExternalMapValue(self.ctepId, 'CTSU_Person_ID')
        if not doc:
            #doc = createCdrDoc(self.createCdrDocXml(), 'Person')
            if session:
                if self.poId:
                    cdr.addExternalMapping(session, 'CTRP_PO_ID', self.poId)
                if self.ctepId:
                    try:
                        cdr.addExternalMapping(session, 'CTSU_Person_ID',
                                               self.ctepId)
                    except:
                        pass
            # Requirements have changed; we now allow unmapped persons.
            #raise Exception("unmapped person with CTRP ID %s" % self.poId)
            return
        node = etree.SubElement(parent, 'PDQPerson')
        node.text = doc.title
        node.set("{%s}ref" % CDR_NAMESPACE, "CDR%010d" % doc.cdrId)
    def createCdrDocXml(self):
        top = etree.Element('Person', nsmap=NSMAP)
        child = etree.SubElement(top, 'PersonNameInformation')
        addChild(child, 'GivenName', self.name.firstName)
        addChild(child, 'MiddleInitial', self.name.middleInitial)
        addChild(child, 'SurName', self.name.lastName)
        locs = etree.SubElement(top, 'PersonLocations')
        pp = etree.SubElement(locs, 'PrivatePractice')
        ppLoc = etree.SubElement(pp, 'PrivatePracticeLocation')
        ContactInfo.convert(self, ppLoc, True)
        ppLoc.set('{%s}id' % CDR_NAMESPACE, 'loc1')
        addChild(locs, 'CIPSContact', 'loc1')
        child = etree.SubElement(top, 'Status')
        etree.SubElement(child, 'CurrentStatus').text = u'Active'
        etree.SubElement(child, 'EnteredBy').text = u'CTRP Import'
        etree.SubElement(child, 'EntryDate').text = time.strftime('%Y-%m-%d')
        return etree.tostring(top, pretty_print=True)
    class Name:
        def __init__(self, node):
            self.firstName = self.middleInitial = self.lastName = None
            for child in node:
                if child.tag == 'first_name':
                    self.firstName = child.text
                elif child.tag == 'middle_initial':
                    self.middleInitial = child.text
                elif child.tag == 'last_name':
                    self.lastName = child.text

#----------------------------------------------------------------------
# Holds the information parsed from a CTRP clinical trial document.
#----------------------------------------------------------------------
class Protocol:
    def makeInfoBlock(self, ctrpId):
        block = etree.Element('CTRPInfo')
        block.set("ctrp_id", ctrpId)
        if self.overallOfficial:
            block.append(self.overallOfficial.convert())
        for location in self.locations:
            block.append(location.convert())
        return block
    def convert(self, returnTree=False):
        root = etree.Element('CTRPProtocol', nsmap=NSMAP)
        root.append(self.idInfo.convert(self.acronym))
        addChild(root, 'BriefTitle', self.briefTitle)
        addChild(root, 'OfficialTitle', self.officialTitle)
        if self.leadOrg:
            root.append(self.leadOrg.convert('LeadOrg'))
        root.append(self.sponsors.convert(self.overallOfficial,
                                          self.overallContact))
        if self.owners:
            owners = etree.SubElement(root, 'TrialOwners')
            for owner in self.owners:
                etree.SubElement(owners, 'Name').text = owner
        if self.sponsors.respPerson or self.sponsors.respOrg:
            respParties = etree.Element('ResponsibleParty')
            if self.sponsors.respPerson:
                name = 'ResponsiblePartyPerson'
                rp = self.sponsors.respPerson.convert(name)
                respParties.append(rp)
            if self.sponsors.respOrg:
                name = 'ResponsiblePartyOrganization'
                ro = self.sponsors.respOrg.convert(name)
                respParties.append(ro)
            root.append(respParties)
        if self.nciInfo:
            root.append(self.nciInfo.convert())
        addChild(root, 'IsFDARegulated', self.fda)
        addChild(root, 'IsSection801', self.section801)
        addChild(root, 'DelayedPosting', self.delayed)
        if self.oversight:
            root.append(self.oversight.convert())
        if self.indInfo:
            indInfo = etree.SubElement(root, 'TrialIndIde')
            for info in self.indInfo:
                indInfo.append(info.convert())
        if self.funding:
            funding = etree.SubElement(root, 'TrialFunding')
            for info in self.funding:
                funding.append(info.convert())
        if self.status:
            self.status.convert(root)
        addChildWithParas(root, 'BriefSummary', self.briefSummary)
        addChildWithParas(root, 'DetailedDescription', self.description)
        armsOrGroups = etree.SubElement(root, 'ArmsOrGroups')
        if not self.armsAndGroups:
            armsOrGroups.set("SingleArmOrGroupStudy", "Yes")
        for armOrGroup in self.armsAndGroups:
            armsOrGroups.append(armOrGroup.convert())
        if self.enrollment:
            enrollment = etree.SubElement(root, 'CTEnrollment')
            enrollment.text = self.enrollment
            addAttribute(enrollment, 'Type', self.enrollmentType)
        if self.eligibility:
            root.append(self.eligibility.convert())
        if self.design:
            # William agreed we should keep the logical groups intact
            #addPhases(root, self.design.phase)
            #addChild(root, 'CTStudyType', self.design.studyType)
            #if self.design.haveInterventionalDesign:
            #    root.append(self.design.convert())
            root.append(self.design.convert())
        if self.outcomes:
            outcomes = etree.SubElement(root, 'CTOutcomes')
            for outcome in self.outcomes:
                outcomes.append(outcome.convert())
        if self.subgroups:
            #subgroups = etree.SubElement(indexing, 'SubGroups')
            subgroups = etree.SubElement(root, 'SubGroups')
            for subgroup in self.subgroups:
                subgroups.append(subgroup.convert())
        indexing = etree.SubElement(root, 'CTRPIndexing')
        for condition in self.conditions:
            indexing.append(condition.convert())
        for intervention in self.interventions:
            indexing.append(intervention.convert())
        for keyword in self.keywords:
            addChild(indexing, 'CTKeyword', keyword)
        for location in self.locations:
            root.append(location.convert())
        addChild(root, 'VerificationDate', self.verificationDate)
        if returnTree:
            return root
        return etree.tostring(root, pretty_print=True)
    def __init__(self, doc):
        if type(doc) is str:
            tree = etree.XML(doc)
        else:
            tree = doc
        if tree.tag != 'clinical_study':
            raise Exception("not a clinical_study document")
        self.idInfo = self.leadOrg = self.nciInfo = None
        self.fda = self.section801 = self.delayed = None
        self.briefTitle = self.officialTitle = self.acronym = None
        self.sponsors = self.oversight = self.briefSummary = None
        self.description = self.status = self.design = None
        self.enrollment = self.enrollmentType = None
        self.eligibility = self.overallOfficial = self.overallContact = None
        self.verificationDate = None
        self.owners = []
        self.funding = []
        self.indInfo = []
        self.outcomes = []
        self.conditions = []
        self.armsAndGroups = []
        self.subgroups = []
        self.interventions = []
        self.locations = []
        self.keywords = []
        for child in tree:
            if child.tag == 'id_info':
                self.idInfo = Protocol.IDInfo(child)
            elif child.tag == 'trial_owners':
                for grandchild in child.findall('name'):
                    self.owners.append(extractText(grandchild))
            elif child.tag == 'lead_org':
                self.leadOrg = Protocol.Org(child)
            elif child.tag == 'nci_specific_information':
                self.nciInfo = Protocol.NCIInfo(child)
            elif child.tag == 'is_fda_regulated':
                self.fda = child.text
            elif child.tag == 'is_section_801':
                self.section801 = child.text
            elif child.tag == 'delayed_posting':
                self.delayed = child.text
            elif child.tag == 'trial_ind_ide':
                for grandchild in child.findall('ind_info'):
                    self.indInfo.append(Protocol.IndInfo(grandchild))
            elif child.tag == 'brief_title':
                self.briefTitle = extractText(child)
            elif child.tag == 'acronym':
                self.acronym = child.text
            elif child.tag == 'official_title':
                self.officialTitle = extractText(child)
            elif child.tag == 'sponsors':
                self.sponsors = Protocol.Sponsors(child)
            elif child.tag == 'oversight_info':
                self.oversight = Protocol.Oversight(child)
            elif child.tag == 'brief_summary':
                self.briefSummary = getParagraphs(child)
            elif child.tag == 'detailed_description':
                self.description = getParagraphs(child)
            elif child.tag == 'trial_status':
                self.status = Protocol.Status(child)
            elif child.tag == 'trial_funding':
                for grandchild in child.findall('funding_info'):
                    self.funding.append(Protocol.Funding(grandchild))
            elif child.tag == 'study_design':
                self.design = Protocol.Design(child)
            elif child.tag == 'primary_outcome':
                self.outcomes.append(Protocol.Outcome(child, 'Primary'))
            elif child.tag == 'secondary_outcome':
                self.outcomes.append(Protocol.Outcome(child, 'Secondary'))
            elif child.tag == 'disease_conditions':
                for grandchild in child.findall('condition_info'):
                    self.conditions.append(Protocol.Condition(grandchild))
            elif child.tag == 'sub_groups':
                for grandchild in child.findall('sub_groups_info'):
                    self.subgroups.append(Protocol.Subgroup(grandchild))
            elif child.tag == 'enrollment':
                self.enrollment = child.text
            elif child.tag == 'enrollment_type':
                self.enrollmentType = child.text
            elif child.tag == 'arm_group':
                self.armsAndGroups.append(Protocol.ArmOrGroup(child))
            elif child.tag == 'intervention':
                self.interventions.append(Protocol.Intervention(child))
            elif child.tag == 'eligibility':
                self.eligibility = Protocol.Eligibility(child)
            elif child.tag == 'overall_official':
                self.overallOfficial = Protocol.OverallOfficial(child)
            elif child.tag == 'overall_contact':
                self.overallContact = Person(child)
            elif child.tag == 'location':
                self.locations.append(Protocol.Location(child))
            elif child.tag == 'keyword':
                self.keywords.append(child.text)
            elif child.tag == 'verification_date':
                self.verificationDate = child.text
    class IDInfo:
        def convert(self, acronym):
            node = etree.Element('IDInfo')
            addChild(node, 'OrgStudyID', self.orgStudyId)
            # See comment #27 in tracker task #4962
            #addChild(node, 'OrgName', self.orgName)
            for secondaryId in self.secondaryIds:
                node.append(secondaryId.convert())
            for ctepId in self.ctepIds:
                etree.SubElement(node, 'CTEP_ID').text = ctepId
            for dcpId in self.dcpIds:
                etree.SubElement(node, 'DCP_ID').text = dcpId
            addChild(node, 'CTAcronym', acronym)
            return node
        def __init__(self, node): 
            self.orgStudyId = self.orgName = None
            self.ctepIds = []
            self.dcpIds = []
            self.secondaryIds = []
            for child in node:
                if child.tag == 'org_study_id':
                    self.orgStudyId = child.text
                elif child.tag == 'ctep_id':
                    self.ctepIds.append(child.text)
                elif child.tag == 'dcp_id':
                    self.dcpIds.append(child.text)
                elif child.tag == 'secondary_id':
                    self.secondaryIds.append(Protocol.IDInfo.SecondaryID(child))
                elif child.tag == 'org_name':
                    self.orgName = child.text
        class SecondaryID:
            def convert(self):
                node = etree.Element('SecondaryID')
                addChild(node, 'SecondaryIDValue', self.id)
                addChild(node, 'SecondaryIDType', self.type)
                addChild(node, 'SecondaryIDDomain', self.domain)
                return node
            def __init__(self, node):
                self.id = self.type = self.domain = None
                for child in node:
                    if child.tag == 'id':
                        self.id = child.text
                    elif child.tag == 'id_type':
                        self.type = child.text
                    elif child.tag == 'id_domain':
                        self.domain = child.text
    class NCIInfo:
        def convert(self):
            node = etree.Element('NCISpecificInformation')
            addChild(node, 'ReportingDataSetMethod', self.method)
            addChild(node, 'Summary4FundingCategory', self.category)
            if self.source:
                node.append(self.source.convert('Summary4FundingSponsorSource'))
            addChild(node, 'ProgramCode', self.code)
            return node
        def __init__(self, node):
            self.method = self.category = self.source = self.code = None
            for child in node:
                if child.tag == 'reporting_data_set_method':
                    self.method = child.text
                elif child.tag == 'summary_4_funding_category':
                    self.category = child.text
                elif child.tag == 'summary_4_funding_sponsor_source':
                    self.source = Protocol.Org(child)
                elif child.tag == 'program_code':
                    self.code = child.text
    class IndInfo:
        def convert(self):
            node = etree.Element('IndInfo')
            addChild(node, 'IndIdeType', self.ideType)
            addChild(node, 'IndHolderType', self.holderType)
            addChild(node, 'IndNIHInstHolder', self.nihInstHolder)
            addChild(node, 'IndNCIDivHolder', self.nciDivHolder)
            addChild(node, 'HasExpandedAccess', self.hasExpandedAccess)
            addChild(node, 'ExpandedAccessStatus', self.expandedAccessStatus)
            addChild(node, 'IsExempt', self.isExempt)
            return node
        def __init__(self, node):
            self.ideType = self.holderType = self.nihInstHolder = None
            self.nciDivHolder = self.hasExpandedAccess = None
            self.expandedAccessStatus = self.isExempt = None
            for child in node:
                if child.tag == 'ind_ide_type':
                    self.ideType = child.text
                elif child.tag == 'ind_holder_type':
                    self.holderType = child.text
                elif child.tag == 'ind_nih_inst_holder':
                    self.nihInstHolder = child.text
                elif child.tag == 'ind_nci_div_holder':
                    self.nciDivHolder = child.text
                elif child.tag == 'has_expanded_access':
                    self.hasExpandedAccess = child.text
                elif child.tag == 'expanded_access_status':
                    self.expandedAccessStatus = child.text
                elif child.tag == 'is_exempt':
                    self.isExempt = child.text
    class Sponsors:
        def convert(self, overallOfficial, overallContact):
            # XXX need PDQSponsorship logic?
            node = etree.Element('Sponsors')
            if self.leadSponsor:
                node.append(self.leadSponsor.convert('LeadSponsor'))
            #if self.respPerson or self.respOrg:
            if False: # William doesn't want this here.
                respParties = etree.Element('ResponsibleParty')
                if self.respPerson:
                    name = 'ResponsiblePartyPerson'
                    respParties.append(self.respPerson.convert(name))
                if self.respOrg:
                    name = 'ResponsiblePartyOrganization'
                    respParties.append(self.respOrg.convert(name))
                node.append(respParties)
            if self.collaborator:
                node.append(self.collaborator.convert('Collaborator'))
            if overallOfficial:
                node.append(overallOfficial.convert())
            if overallContact:
                node.append(overallContact.convert('OverallContact'))
            return node
        def __init__(self, node):
            self.leadSponsor = self.respPerson = self.collaborator = None
            self.respOrg = None
            for child in node:
                if child.tag == 'lead_sponsor':
                    self.leadSponsor = Protocol.Org(child)
                elif child.tag == 'resp_party':
                    for grandchild in child:
                        if grandchild.tag == 'resp_party_person':
                            self.respPerson = Person(grandchild)
                        elif grandchild.tag == 'resp_party_organization':
                            self.respOrg = Protocol.Org(grandchild)
                elif child.tag == 'collaborator':
                    self.collaborator = Protocol.Org(child)
    class Oversight:
        def convert(self):
            node = etree.Element('CTOversightInfo')
            addChild(node, 'CTAuthority', self.regulatoryAuthority)
            addChild(node, 'HasDMC', self.hasDMC)
            return node
        def __init__(self, node):
            self.regulatoryAuthority = self.hasDmc = None
            for child in node:
                if child.tag == 'regulatory_authority':
                    self.regulatoryAuthority = child.text
                elif child.tag == 'has_dmc':
                    self.hasDMC = child.text
    class Status:
        def convert(self, parent):
            addChild(parent, 'OverallStatus', self.current)
            addChild(parent, 'OverallStatusDate', self.currentDate)
            addChild(parent, 'ReasonStopped', self.whyStopped)
            addChild(parent, 'StartDate', self.start)
            addChild(parent, 'StartDateType', self.startType)
            addChild(parent, 'CompletionDate', self.completion)
            addChild(parent, 'CompletionDateType', self.completionType)
        def __init__(self, node):
            self.current = self.currentDate = self.whyStopped = None
            self.start = self.startType = self.completion = None
            self.completionType = None
            for child in node:
                if child.tag == 'current_trial_status':
                    self.current = child.text
                elif child.tag == 'current_trial_status_date':
                    self.currentDate = child.text
                elif child.tag == 'why_stopped':
                    self.whyStopped = child.text
                elif child.tag == 'current_trial_start_date':
                    self.start = child.text
                elif child.tag == 'current_trial_start_date_type':
                    self.startType = child.text
                elif child.tag == 'current_trial_completion_date':
                    self.completion = child.text
                elif child.tag == 'current_trial_completion_date_type':
                    self.completionType = child.text
    class Funding:
        def convert(self):
            node = etree.Element('FundingInfo')
            addChild(node, 'FundingCode', self.code)
            addChild(node, 'FundingNIHInstCode', self.nihInstCode)
            addChild(node, 'FundingSerialNumber', self.serialNumber)
            addChild(node, 'FundingNCIDivProgram', self.nciDivProgram)
            return node
        def __init__(self, node):
            self.code = self.nihInstCode = self.serialNumber = None
            self.nciDivProgram = None
            for child in node:
                if child.tag == 'funding_code':
                    self.code = child.text
                elif child.tag == 'funding_nih_inst_code':
                    self.nihInstCode = child.text
                elif child.tag == 'funding_serial_number':
                    self.serialNumber = child.text
                elif child.tag == 'funding_nci_div_program':
                    self.nciDivProgram = child.text
    class Design:
        def convert(self):
            wrapper = etree.Element('CTStudyDesign')
            addChild(wrapper, 'CTStudyType', self.studyType)
            node = etree.Element('InterventionalDesign')
            wrapper.append(node)
            addChild(node, 'InterventionalSubtype', self.interventionalSubtype)
            addChild(node, 'InterventionalAdditionalQualifier',
                     self.interventionalAdditionalQualifier)
            addChild(node, 'InterventionalOtherText',
                     self.interventionalOtherText)
            addPhases(node, self.phase)
            addChild(node, 'PhaseAdditionalQualifier',
                     self.phaseAdditionalQualifier)
            addChild(node, 'Allocation', self.allocation)
            addChild(node, 'Masking', self.masking)
            addChild(node, 'MaskedInvestigator', self.maskedInvestigator)
            addChild(node, 'MaskedSubject', self.maskedSubject)
            addChild(node, 'MaskedCaregiver', self.maskedCaregiver)
            addChild(node, 'Assignment', self.assignment)
            addChild(node, 'Endpoint', self.endpoint)
            addChild(node, 'NumberOfArms', self.numberOfArms)
            return wrapper
        def __init__(self, node):
            self.haveInterventionalDesign = False
            self.studyType = self.interventionalSubtype = None
            self.interventionalAdditionalQualifier = None
            self.interventionalOtherText = None
            self.phaseAdditionalQualifier = self.phase = None
            self.allocation = self.masking = self.maskedInvestigator = None
            self.maskedSubject = self.maskedCaregiver = self.assignment = None
            self.endpoint = self.numberOfArms = None
            for child in node:
                if child.tag == 'study_type':
                    self.studyType = child.text
                elif child.tag == 'interventional_design':
                    self.haveInterventionalDesign = True
                    for gc in child:
                        if gc.tag == 'interventional_subtype':
                            self.interventionalSubtype = gc.text
                        elif gc.tag == 'interventional_additional_qualifier':
                            self.interventionalAdditionalQualifier = gc.text
                        elif gc.tag == 'interventional_other_text':
                            self.interventionalOtherText = gc.text
                        elif gc.tag == 'phase_additional_qualifier':
                            self.phaseAdditionalQualifier = gc.text
                        elif gc.tag == 'phase':
                            self.phase = gc.text
                        elif gc.tag == 'allocation':
                            self.allocation = gc.text
                        elif gc.tag == 'masking':
                            self.masking = gc.text
                        elif gc.tag == 'masked_investigator':
                            self.maskedInvestigator = gc.text
                        elif gc.tag == 'masked_subject':
                            self.maskedSubject = gc.text
                        elif gc.tag == 'masked_caregiver':
                            self.maskedCaregiver = gc.text
                        elif gc.tag == 'assignment':
                            self.assignment = gc.text
                        elif gc.tag == 'endpoint':
                            self.endpoint = gc.text
                        elif gc.tag == 'number_of_arms':
                            self.numberOfArms = gc.text
    class Outcome:
        def convert(self):
            node = etree.Element('CTOutcome')
            addChild(node, 'CTOutcomeMeasure', self.measure)
            addChild(node, 'CTOutcomeDescription', self.desc)
            addChild(node, 'CTOutcomeTimeFrame', self.time)
            addChild(node, 'CTOutcomeSafetyIssue', self.safety)
            addAttribute(node, 'OutcomeType', self.importance)
            return node
        def __init__(self, node, importance):
            self.importance = importance
            self.measure = self.safety = self.time = self.desc = None
            for child in node:
                if child.tag == 'outcome_measure':
                    self.measure = extractText(child)
                elif child.tag == 'outcome_description':
                    self.desc = extractText(child)
                elif child.tag == 'outcome_safety_issue':
                    self.safety = child.text
                elif child.tag == 'outcome_time_frame':
                    self.time = extractText(child)
    class Condition:
        def convert(self):
            node = etree.Element('CTCondition')
            addChild(node, 'PreferredName', self.name)
            addChild(node, 'MenuDisplayName', self.menuName)
            #addAttribute(node, "{%s}ref" % CDR_NAMESPACE, self.code)
            addAttribute(node, 'disease_code', self.code)
            addAttribute(node, "nci_thesaurus_id", self.ncitId)
            return node
        def __init__(self, node):
            self.name = self.code = self.ncitId = self.menuName = None
            for child in node:
                if child.tag == 'preferred_name':
                    self.name = child.text
                elif child.tag == 'disease_code':
                    self.code = child.text
                elif child.tag == 'nci_thesaurus_id':
                    self.ncitId = child.text
                elif child.tag == 'menu_display_name':
                    self.menuName = extractText(child)
    class Subgroup:
        def convert(self):
            node = etree.Element('SubGroupInfo')
            addChild(node, 'SubGroupNumber', self.number)
            addChild(node, 'SubGroupDescription', self.desc)
            return node
        def __init__(self, node):
            self.number = self.desc = None
            for child in node:
                if child.tag == 'group_number':
                    self.number = extractText(child)
                elif child.tag == 'description':
                    self.desc = extractText(child)
    class ArmOrGroup:
        def convert(self):
            node = etree.Element('ArmOrGroup')
            addChild(node, 'ArmOrGroupLabel', self.label)
            addChild(node, 'ArmOrGroupType', self.type)
            addChild(node, 'ArmOrGroupDescription', self.desc)
            return node
        def __init__(self, node):
            self.label = self.type = self.desc = None
            for child in node:
                if child.tag == 'arm_group_label':
                    self.label = child.text
                elif child.tag == 'arm_type':
                    self.type = child.text
                elif child.tag == 'arm_group_description':
                    self.desc = extractText(child)
    class Intervention:
        def convert(self):
            node = etree.Element('CTIntervention')
            addChild(node, 'CTInterventionType', self.type)
            addChild(node, 'CTInterventionName', self.name)
            addChild(node, 'CTInterventionDescription', self.desc)
            for label in self.armOrGroupLabels:
                addChild(node, 'ArmOrGroupLabel', label)
            for name in self.otherNames:
                addChild(node, 'CTInterventionOtherName', name)
            return node
        def __init__(self, node):
            self.type = self.name = self.desc = None
            self.otherNames = []
            self.armOrGroupLabels = []
            for child in node:
                if child.tag == 'intervention_type':
                    self.type = child.text
                elif child.tag == 'intervention_name':
                    self.name = child.text
                elif child.tag == 'intervention_description':
                    self.desc = extractText(child)
                elif child.tag == 'intervention_other_name':
                    self.otherNames.append(child.text)
                elif child.tag == 'arm_group_label':
                    self.armOrGroupLabels.append(child.text)
    class Eligibility:
        def convert(self):
            node = etree.Element('CTEligibility')
            criteria = etree.SubElement(node, 'CTCriteria')
            for criterion in self.criteria:
                criteria.append(criterion.convert())
            addAttribute(node, 'HealthyVolunteers', self.healthyVolunteers)
            addChild(node, 'CTGender', self.gender)
            addChild(node, 'CTMinimumAge', self.minAge)
            addChild(node, 'CTMaximumAge', self.maxAge)
            return node
        def __init__(self, node):
            self.criteria = []
            self.healthyVolunteers = self.gender = None
            self.minAge = self.maxAge = None
            for child in node:
                if child.tag == 'healthy_volunteers':
                    self.healthyVolunteers = child.text
                elif child.tag == 'gender':
                    self.gender = child.text
                elif child.tag == 'minimum_age':
                    self.minAge = child.text
                elif child.tag == 'maximum_age':
                    self.maxAge = child.text
            for child in node.findall('criteria/criterion'):
                self.criteria.append(Protocol.Eligibility.Criterion(child))
        class Criterion:
            def convert(self):
                node = etree.Element('CTCriterion')
                addChild(node, 'CTCriterionType', self.type)
                addChild(node, 'CTCriterionData', self.data)
                return node
            def __init__(self, node):
                self.type = self.data = None
                for child in node:
                    if child.tag == 'type':
                        self.type = child.text
                    elif child.tag == 'data':
                        self.data = extractText(child)
    class Address:
        def convert(self, standaloneDoc=False):
            node = etree.Element('PostalAddress')
            for street in self.street:
                etree.SubElement(node, 'Street').text = street
            addChild(node, 'City', self.city)
            if self.state:
                stateId = geoMap.lookupStateId(self.state, self.country)
                if not stateId:
                    if session and self.state and self.country:
                        key = u"%s|%s" % (self.state.strip().lower(),
                                          self.country.strip().lower())
                        cdr.addExternalMapping(session, 'CTRP_States', key)
                    raise Exception("unmapped state %s|%s" % (self.state,
                                                              self.country))
                stateNode = etree.Element('PoliticalSubUnit_State')
                stateNode.set('{%s}ref' % CDR_NAMESPACE, 'CDR%010d' % stateId)
                stateName = geoMap.lookupStateName(stateId) or self.state
                stateNode.text = stateName
                node.append(stateNode)
            if self.country:
                countryId = geoMap.lookupCountryId(self.country)
                if not countryId:
                    if session and self.country:
                        key = self.country.strip().lower()
                        cdr.addExternalMapping(session, 'CTRP_Countries', key)
                    raise Exception("unmapped country %s" % self.country)
                countryNode = etree.Element('Country')
                countryNode.set('{%s}ref' % CDR_NAMESPACE,
                                'CDR%010d' % countryId)
                countryName = (geoMap.lookupCountryName(countryId) or
                               self.country)
                countryNode.text = countryName
                node.append(countryNode)
                if standaloneDoc:
                    addressType = countryName == 'U.S.A.' and 'US' or 'Non-US'
                    node.set('AddressType', addressType)
            addChild(node, 'PostalCode_ZIP', self.zip)
            return node
        def __init__(self, node):
            self.street = []
            self.city = self.state = self.zip = self.country = None
            for child in node:
                if child.tag == 'street':
                    self.street += getParagraphs(child)
                elif child.tag == 'city':
                    self.city = child.text
                elif child.tag == 'state':
                    self.state = child.text
                elif child.tag == 'zip':
                    self.zip = child.text
                elif child.tag == 'country':
                    self.country = child.text
    class Org(Ids, ContactInfo):
        def convert(self, name):
            node = etree.Element(name)
            Ids.convert(self, node)
            self.addPdqOrganization(node)
            addChild(node, 'CTRPOrgName', self.name)
            ContactInfo.convert(self, node)
            return node
        def __init__(self, node):
            self.name = None
            Ids.__init__(self, node)
            ContactInfo.__init__(self, node)
            for child in node.findall('name'):
                self.name = extractText(child)
        def addPdqOrganization(self, parent):
            node = etree.SubElement(parent, 'PDQOrganization')
            doc = CdrDoc.lookupExternalMapValue(self.poId, 'CTRP_PO_ID')
            if not doc:
                doc = CdrDoc.lookupExternalMapValue(self.ctepId,
                                                    'CTEP_Institution_Code')
            if not doc:
                #doc = createCdrDoc(self.createCdrDocXml(), 'Organization')
                if session:
                    if self.poId:
                        cdr.addExternalMapping(session, 'CTRP_PO_ID', self.poId)
                    if self.ctepId:
                        try:
                            cdr.addExternalMapping(session,
                                                   'CTEP_Institution_Code',
                                                   self.ctepId)
                        except:
                            pass
                raise Exception("unmapped organization with CTRP ID %s" %
                                self.poId)
            node.text = doc.title
            node.set("{%s}ref" % CDR_NAMESPACE, "CDR%010d" % doc.cdrId)
        def createCdrDocXml(self):
            top = etree.Element('Organization', nsmap=NSMAP)
            child = etree.SubElement(top, 'OrganizationNameInformation')
            child = etree.SubElement(child, 'OfficialName')
            etree.SubElement(child, 'Name').text = self.name
            details = etree.SubElement(top, 'OrganizationDetails')
            info = etree.SubElement(details,
                                    'OrganizationAdministrativeInformation')
            etree.SubElement(info, 'IncludeInDirectory').text = 'Pending'
            info.set('Directory', 'Treatment')
            today = time.strftime('%Y-%m-%d')
            etree.SubElement(info, 'Date').text = today
            locs = etree.SubElement(top, 'OrganizationLocations')
            orgLoc = etree.SubElement(locs, 'OrganizationLocation')
            loc = etree.SubElement(orgLoc, 'Location')
            ContactInfo.convert(self, loc, True)
            loc.set('{%s}id' % CDR_NAMESPACE, 'loc1')
            addChild(locs, 'CIPSContact', 'loc1')
            child = etree.SubElement(top, 'Status')
            etree.SubElement(child, 'CurrentStatus').text = 'Active'
            addChild(top, 'OrganizationType', 'No type assigned')
            return etree.tostring(top, pretty_print=True)
    class OverallOfficial(Person):
        def convert(self):
            node = Person.convert(self, 'CTRPOverallOfficial')
            if self.affiliation:
                node.append(self.affiliation.convert('CTRPAffiliation'))
            return node
        def __init__(self, node):
            Person.__init__(self, node)
            self.affiliation = None
            for child in node.findall('affiliation'):
                self.affiliation = Protocol.Org(child)
    class Location:
        def convert(self):
            node = etree.Element('CTRPLocation')
            if self.facility:
                node.append(self.facility.convert('CTRPFacility'))
            addChild(node, 'CTRPStatus', self.status)
            if self.contact:
                node.append(self.contact.convert('CTRPContact'))
            for investigator in self.investigators:
                node.append(investigator.convert('CTRPInvestigator'))
            return node
        def __init__(self, node):
            self.facility = self.contact = self.status = None
            self.investigators = []
            for child in node:
                if child.tag == 'facility':
                    self.facility = Protocol.Org(child)
                elif child.tag == 'status':
                    self.status = child.text
                elif child.tag == 'contact':
                    self.contact = Person(child)
                elif child.tag == 'investigator':
                    self.investigators.append(Person(child))

#----------------------------------------------------------------------
# For unit testing.  Reads a CTRP trial document from the standard
# input and prints out the corresponding CDR CTRPProtocol document.
# If optional CDR user name and password are given on the command
# line, then rows are added to the mapping table for missing
# mappings (persons, organizations, states, countries).
#----------------------------------------------------------------------
def main(uid, pwd):
    global session
    if uid and pwd:
        session = cdr.login(uid, pwd)
    protocol = Protocol(sys.stdin.read())
    print protocol.convert()

if __name__ == '__main__':
    uid = len(sys.argv) > 1 and sys.argv[1] or None
    pwd = len(sys.argv) > 2 and sys.argv[2] or None
    main(uid, pwd)
