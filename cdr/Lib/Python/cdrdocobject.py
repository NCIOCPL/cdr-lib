#----------------------------------------------------------------------
#
# $Id: cdrdocobject.py,v 1.4 2008-05-06 18:06:09 bkline Exp $
#
# Types for data extracted from CDR documents of specific document types.
#
# $Log: not supported by cvs2svn $
# Revision 1.3  2008/05/06 17:40:22  bkline
# Added new classes for protocols and organizations.
#
# Revision 1.2  2006/06/08 19:10:09  bkline
# Renamed __incPersonTitle to __ptHandling in ContactInfo class.
#
# Revision 1.1  2005/07/08 21:36:10  bkline
# Python classes for data extracted from CDR documents.
#
#----------------------------------------------------------------------
import cdr, sys, xml.dom.minidom, time

#------------------------------------------------------------------
# Constants for adding a person's title to his address
#------------------------------------------------------------------
TITLE_OMITTED    = 0    # Do not print person's PersonTitle
TITLE_AFTER_NAME = 1    # Print it below his name, if name present
TITLE_AFTER_ORG  = 2    # Print it below org name, if present

#------------------------------------------------------------------
# Object for a personal name.
#------------------------------------------------------------------
class PersonalName:
    def __init__(self, node):
        """
        Parameters:
            node - PersonName or Name subelement DOM node
        """
        self.__givenName     = u""
        self.__middleInitial = u""
        self.__surname       = u""
        self.__prefix        = u""
        self.__genSuffix     = u""
        self.__proSuffixes   = []
        suffixElems          = ("StandardProfessionalSuffix",
                                "CustomProfessionalSuffix")
        for child in node.childNodes:
            if child.nodeName == "GivenName":
                self.__givenName = cdr.getTextContent(child).strip()
            elif child.nodeName == "MiddleInitial":
                self.__middleInitial = cdr.getTextContent(child).strip()
            elif child.nodeName == "SurName":
                self.__surname = cdr.getTextContent(child).strip()
            elif child.nodeName == "ProfessionalSuffix":
                for grandchild in child.childNodes:
                    if grandchild.nodeName in suffixElems:
                        suffix = cdr.getTextContent(grandchild).strip()
                        if suffix:
                            self.__proSuffixes.append(suffix)
            elif child.nodeName == "Prefix":
                self.__prefix = cdr.getTextContent(child).strip()
            elif child.nodeName == "GenerationSuffix":
                self.__genSuffix = cdr.getTextContent(child).strip()

    def getGivenName       (self): return self.__givenName
    def getMiddleInitial   (self): return self.__middleInitial
    def getSurname         (self): return self.__surname
    def getPrefix          (self): return self.__prefix
    def getGenSuffix       (self): return self.__genSuffix
    def getProSuffixes     (self): return list(self.__proSuffixes)

    def format(self, useSuffixes = True, usePrefix = True):
        """
        Return value:
            String containing formatted name, e.g.:
                'Dr. John Q. Kildare, Jr.'
        """
        if usePrefix:
            name = ("%s %s" % (self.__prefix, self.__givenName)).strip()
        else:
            name = self.__givenName.strip()
        name = ("%s %s" % (name, self.__middleInitial)).strip()
        name = ("%s %s" % (name, self.__surname)).strip()
        if self.__genSuffix:
            name = "%s, %s" % (name, self.__genSuffix)
        if useSuffixes:
            rest = ", ".join(self.__proSuffixes).strip()
            if rest:
                name = "%s, %s" % (name, rest)
        return name

#----------------------------------------------------------------------
# Object to hold information about contact information for a person
# or organization.  Base class for Person.Contact and Org.Contact.
#----------------------------------------------------------------------
class ContactInfo:
    """
    Public methods:

        getStreetLines()
            Returns a tuple of strings for the street address.

        getAddressLines(getNameAndTitle)
            Assembles and returns a list of strings for the postal
            address for this set of contact information.  The caller
            can specify whether the recipient's name should be prepended
            to the address

        getOrgs()
            Returns a tuple of strings for the orgs in the address

        getAddressee()
            Returns concatenation of prefix, forename, and surname.

        setAddressee()
            Allows the caller to replace the string for the name of
            the addressee

        getPersonalName()
            Returns PersonalName object for this contact information
            (if any).

        getPersonTitle()
            Returns PersonTitle element for this address, or None.

        getCity()
            Returns string for city of this address, if any; otherwise
            None.

        getCitySuffix()
            Returns the string for the city suffix for this address,
            if any; otherwise None.

        getState()
            Returns the name of the political unit for this address,
            if any; otherwise None.

        getCountry()
            Returns the string for the country for this address, if
            any; otherwise None.

        getPostalCode()
            Returns the postal code (ZIP code for US addresses) for
            this address, if any; otherwise None.

        getCodePosition()
            One of:
                "after City"
                "after Country"
                "after PoliticalUnit_State"
                "before City"
                None

        getPhone()
            Returns the phone number, if any; otherwise None.
            
        getFax()
            Returns the fax number, if any; otherwise None.
            
        getEmail()
            Returns the email address, if any; otherwise None.

        lineIsUS(line)
            Static method to test whether line consists of abbreviation
            for United States

        wrapLines(lines, wrapAt)
            Static method to perform word-wrap on a sequence of lines.
            Returns a new (possibly modified) list of lines.
    """

    #------------------------------------------------------------------
    # Parse contact information from XML fragment.
    #------------------------------------------------------------------
    def __init__(self, xmlFragment, personTitleHandling = TITLE_OMITTED):
        """
        Parameters:
            xmlFragment    - Either DOM object for parsed address XML,
                             or the string containing the XML for the
                             address.
                             The top node should be <AddressElements>
        """
        self.__addressee      = None
        self.__personalName   = None
        self.__orgs           = []   # Main + parent orgs in right order
        self.__street         = []
        self.__city           = None
        self.__citySuffix     = None
        self.__state          = None
        self.__country        = None
        self.__postalCode     = None
        self.__codePos        = None
        self.__personTitle    = None
        self.__phone          = None
        self.__fax            = None
        self.__email          = None
        self.__ptHandling     = personTitleHandling

        if type(xmlFragment) in (str, unicode):
            dom = xml.dom.minidom.parseString(xmlFragment)
        else:
            dom = xmlFragment

        # No organization name nodes identified yet
        orgParentNode = None

        # Parse parts of an address
        if dom:
            for node in dom.documentElement.childNodes:
                if node.nodeName == 'PostalAddress':
                    self.__parsePostalAddress(node)
                elif node.nodeName in ('Name', 'PersonName'):
                    self.__personalName = PersonalName(node)
                    self.__addressee = self.__personalName.format()
                elif node.nodeName == 'OrgName':
                    self.__orgs.append (cdr.getTextContent(node).strip())
                elif node.nodeName == 'ParentNames':
                    orgParentNode = node
                elif node.nodeName == 'PersonTitle':
                    self.__personTitle = cdr.getTextContent(node).strip()
                elif node.nodeName == 'Phone':
                    self.__phone = cdr.getTextContent(node).strip()
                elif node.nodeName == 'Fax':
                    self.__fax = cdr.getTextContent(node).strip()
                elif node.nodeName == 'Email':
                    self.__email = cdr.getTextContent(node).strip()

        # If we got them, get org parent names to __orgs in right order
        if orgParentNode:
            self.__parseOrgParents(orgParentNode)

    #------------------------------------------------------------------
    # Public access methods.
    #------------------------------------------------------------------
    def getStreetLines    (self): return tuple(self.__street)
    def getOrgs           (self): return tuple(self.__orgs)
    def getCity           (self): return self.__city
    def getCitySuffix     (self): return self.__citySuffix
    def getState          (self): return self.__state
    def getCountry        (self): return self.__country
    def getPostalCode     (self): return self.__postalCode
    def getCodePosition   (self): return self.__codePos
    def getAddressee      (self): return self.__addressee
    def getPersonalName   (self): return self.__personalName
    def getPersonTitle    (self): return self.__personTitle
    def getPhone          (self): return self.__phone
    def getFax            (self): return self.__fax
    def getEmail          (self): return self.__email

    # Caller may need to manipulate the name line of the address
    def setAddressee (self, nameStr): self.__addressee = nameStr

    #------------------------------------------------------------------
    # Construct a list of strings representing the lines of a
    # formatted address.  This part of address formatting is broken
    # out separately, so we can hand out the lines without the
    # formatting for routines like the one which creates address
    # label sheets (which use uppercase versions of the address
    # line strings).
    #------------------------------------------------------------------
    def getAddressLines(self, includeNameAndTitle = True,
                        includeOrgs = True):

        #--------------------------------------------------------------
        # Start with an empty list.
        #--------------------------------------------------------------
        lines = []

        #--------------------------------------------------------------
        # Add the addressee's name if requested.
        #--------------------------------------------------------------
        if includeNameAndTitle:
            if self.__addressee:
                lines.append(self.__addressee)
            if self.__ptHandling == TITLE_AFTER_NAME:
                if self.__personTitle:
                    lines.append(self.__personTitle)

        #--------------------------------------------------------------
        # Add organization lines.
        #--------------------------------------------------------------
        if includeOrgs:
            for org in self.__orgs:
                if org:
                    lines.append(org)
        if includeNameAndTitle and self.__ptHandling == TITLE_AFTER_ORG:
            if self.__personTitle:
                lines.append(self.__personTitle)

        #--------------------------------------------------------------
        # Now we get to the actual postal address lines.
        #--------------------------------------------------------------
        city    = self.getCity()
        suffix  = self.getCitySuffix()
        state   = self.getState()
        zip     = self.getPostalCode()
        pos     = self.getCodePosition()
        country = self.getCountry()
        line    = ""
        city    = ("%s %s" % (city or "", suffix or "")).strip()
        for street in self.__street:
            if street:
                lines.append(street)
        if zip and pos == "before City":
            line = zip
            if city: line += " "
        if city: line += city
        if zip and pos == "after City":
            if line: line += " "
            line += zip
        if state:
            if line: line += ", "
            line += state
        if zip and (not pos or pos == "after PoliticalUnit_State"):
            if line: line += " "
            line += zip
        if line:
            lines.append(line)
        if country:
            if zip and pos == "after Country":
                lines.append("%s %s" % (country, zip))
            else:
                lines.append(country)
        elif zip and pos == "after Country":
            lines.append(zip)

        #--------------------------------------------------------------
        # We're done.
        #--------------------------------------------------------------
        return lines

    #------------------------------------------------------------------
    # Check to see if a line is just U.S. (or the equivalent).
    #------------------------------------------------------------------
    def lineIsUS(line):
        return line.strip().upper() in ("US", "USA", "U.S.", "U.S.A.")
    lineIsUS = staticmethod(lineIsUS)

    #------------------------------------------------------------------
    # Perform word wrap if needed.
    #------------------------------------------------------------------
    def wrapLines(lines, wrapAt):
        needWrap = False
        for line in lines:
            if len(line) > wrapAt:
                needWrap = True
                break
        if not needWrap:
            return lines
        newLines = []
        for line in lines:
            indent = 0
            while len(line) > wrapAt - indent:
                partLen = wrapAt - indent
                while partLen > 0:
                    if line[partLen] == ' ':
                        break
                    partLen -= 1
                if partLen == 0:
                    partLen = wrapAt - indent
                firstPart = line[:partLen].strip()
                line = line[partLen:].strip()
                if firstPart:
                    newLines.append(' ' * indent + firstPart)
                    indent = 2
            if line:
                newLines.append(' ' * indent + line)
        return newLines
    wrapLines = staticmethod(wrapLines)

    #------------------------------------------------------------------
    # Extract postal address element values.
    #------------------------------------------------------------------
    def __parsePostalAddress(self, node):
        """
        Extracts individual elements from street address, storing
        each in a field of the Address object.

        Pass:
            node    - DOM node of PostalAddress element
        """
        for child in node.childNodes:
            if node.nodeType == xml.dom.minidom.Node.ELEMENT_NODE:
                if child.nodeName == "Street":
                    self.__street.append(cdr.getTextContent(child).strip())
                elif child.nodeName == "City":
                    self.__city = cdr.getTextContent(child).strip()
                elif child.nodeName == "CitySuffix":
                    self.__citySuffix = cdr.getTextContent(child).strip()
                elif child.nodeName in ("State", "PoliticalSubUnit_State"):
                    self.__state = cdr.getTextContent(child).strip()
                elif child.nodeName == "Country":
                    self.__country = cdr.getTextContent(child).strip()
                elif child.nodeName == "PostalCode_ZIP":
                    self.__postalCode = cdr.getTextContent(child).strip()
                elif child.nodeName == "CodePosition":
                    self.__codePos = cdr.getTextContent(child).strip()

    #------------------------------------------------------------------
    # Extract and sort (if necessary) organization name values of parents
    #------------------------------------------------------------------
    def __parseOrgParents (self, node):
        """
        Parses a ParentNames element, extracting organization names
        and appending them, in the right order, to the list of
        organizations.

        Pass:
            node    - DOM node of ParentNames element
        """
        # Attribute tells us the order in which to place parents
        parentsFirst = False
        pfAttr = node.getAttribute("OrderParentNameFirst")
        if pfAttr == "Yes":
            parentsFirst = True

        for child in node.childNodes:
            if child.nodeName == "ParentName":
                self.__orgs.append(cdr.getTextContent(child).strip())
        if parentsFirst:
            self.__orgs.reverse()

#----------------------------------------------------------------------
# Object for a CDR Person.
#----------------------------------------------------------------------
class Person:

    def getCipsContactId(docId, conn = None):
        path = '/Person/PersonLocations/CIPSContact'
        rows = cdr.getQueryTermValueForId(path, docId, conn)
        if not rows:
            return None
        return rows[0]
    getCipsContactId = staticmethod(getCipsContactId)

    class Contact(ContactInfo):
        def __init__(self, cdrId, fragId, filt = 'Person Address Fragment'):
            filters = ['name:%s' % filt]
            parms = (('fragId', fragId),)
            result = cdr.filterDoc('guest', filters, cdrId, parm = parms)
            if type(result) in (str, unicode):
                raise Exception(u"Person.Contact(%s, %s): %s" %
                                (cdrId, fragId, result))
            ContactInfo.__init__(self, result[0])

    class CipsContact(Contact):
        def __init__(self, cdrId):
            fragId = Person.getCipsContactId(cdrId)
            if not fragId:
                raise Exception("no CIPS Contact for %s" % cdrId)
            Person.Contact.__init__(self, cdrId, fragId)

#----------------------------------------------------------------------
# Object for a CDR Organization.
#----------------------------------------------------------------------
class Organization:

    def getCipsContactId(docId, conn = None):
        path = '/Organization/OrganizationLocations/CIPSContact'
        rows = cdr.getQueryTermValueForId(path, docId, conn)
        if not rows:
            return None
        return rows[0]
    getCipsContactId = staticmethod(getCipsContactId)

    class Contact(ContactInfo):
        def __init__(self, cdrId, fragId,
                     filt = 'Organization Address Fragment'):
            filters = ['name:%s' % filt]
            parms = (('fragId', fragId),)
            result = cdr.filterDoc('guest', filters, cdrId, parm = parms)
            if type(result) in (str, unicode):
                raise Exception(u"Organization.Contact(%s, %s): %s" %
                                (cdrId, fragId, result))
            ContactInfo.__init__(self, result[0])

    class CipsContact(Contact):
        def __init__(self, cdrId):
            fragId = Organization.getCipsContactId(cdrId)
            if not fragId:
                raise Exception("no CIPS Contact for %s" % cdrId)
            Organization.Contact.__init__(self, cdrId, fragId)

#----------------------------------------------------------------------
# Object for a CDR InScopeProtocol document.
#----------------------------------------------------------------------
class Protocol:
    "Modeled on protocol information used for OPS-like reports."

    def __init__(self, id, node):
        "Create a protocol object from the XML document."
        self.id         = id
        self.leadOrgs   = []
        self.statuses   = []
        self.status     = ""
        self.primaryId  = ""
        self.otherIds   = []
        self.firstPub   = ""
        self.closed     = ""
        self.completed  = ""
        self.studyTypes = []
        self.categories = []
        self.sources    = []
        self.designs    = []
        self.pupLink    = []
        self.sponsors   = []
        self.title      = ""
        self.origTitle  = ""
        self.phases     = []
        profTitle       = ""
        patientTitle    = ""
        originalTitle   = ""
        for child in node.childNodes:
            if child.nodeName == "ProtocolSponsors":
                for grandchild in child.childNodes:
                    if grandchild.nodeName == "SponsorName":
                        value = cdr.getTextContent(grandchild)
                        if value:
                            self.sponsors.append(value)
            elif child.nodeName == 'ProtocolSources':
                for grandchild in child.childNodes:
                    if grandchild.nodeName == 'ProtocolSource':
                        for greatgrandchild in grandchild.childNodes:
                            if greatgrandchild.nodeName == 'SourceName':
                                source = cdr.getTextContent(greatgrandchild)
                                source = source.strip()
                                if source:
                                    self.sources.append(source)
            elif child.nodeName == 'ProtocolDesign':
                design = cdr.getTextContent(child).strip()
                if design:
                    self.designs.append(design)
            elif child.nodeName == "ProtocolIDs":
                for grandchild in child.childNodes:
                    if grandchild.nodeName == "PrimaryID":
                        for greatgrandchild in grandchild.childNodes:
                            if greatgrandchild.nodeName == "IDString":
                                value = cdr.getTextContent(greatgrandchild)
                                self.primaryId = value
                    if grandchild.nodeName == "OtherID":
                        for greatgrandchild in grandchild.childNodes:
                            if greatgrandchild.nodeName == "IDString":
                                value = cdr.getTextContent(greatgrandchild)
                                if value:
                                    self.otherIds.append(value)
            elif child.nodeName == "ProtocolTitle":
                titleType = child.getAttribute("Type")
                value     = cdr.getTextContent(child)
                if value:
                    if titleType == "Professional":
                        profTitle = value
                    elif titleType == "Patient":
                        patientTitle = value
                    elif titleType == "Original":
                        originalTitle = self.origTitle = value
            elif child.nodeName == "ProtocolAdminInfo":
                for grandchild in child.childNodes:
                    if grandchild.nodeName == "ProtocolLeadOrg":
                        self.leadOrgs.append(self.LeadOrg(grandchild))
                    elif grandchild.nodeName == "CurrentProtocolStatus":
                        value = cdr.getTextContent(grandchild)
                        if value:
                            self.status = value
            elif child.nodeName == "ProtocolDetail":
                for grandchild in child.childNodes:
                    if grandchild.nodeName == 'StudyCategory':
                        for greatgrandchild in grandchild.childNodes:
                            if greatgrandchild.nodeName == 'StudyCategoryName':
                                cat = cdr.getTextContent(greatgrandchild)
                                cat = cat.strip()
                                if cat:
                                    self.categories.append(cat)
                    elif grandchild.nodeName == 'StudyType':
                        studyType = cdr.getTextContent(grandchild).strip()
                        if studyType:
                            self.studyTypes.append(studyType)
            elif child.nodeName == 'ProtocolPhase':
                self.phases.append(cdr.getTextContent(child))
        if profTitle:
            self.title = profTitle
        elif originalTitle:
            self.title = originalTitle
        elif patientTitle:
            self.title = patientTitle
        orgStatuses = []
        statuses    = {}
        i           = 0
        for leadOrg in self.leadOrgs:
            if leadOrg.role == 'Primary' and leadOrg.pupLink:
                self.pupLink = leadOrg.pupLink
            orgStatuses.append("")
            for orgStatus in leadOrg.statuses:
                startDate = orgStatus.startDate
                val = (i, orgStatus.name)
                #print "val: %s" % repr(val)
                #print "orgStatuses: %s" % repr(orgStatuses)
                statuses.setdefault(startDate, []).append(val)
            i += 1
        keys = statuses.keys()
        keys.sort()
        for startDate in keys:
            for i, orgStatus in statuses[startDate]:
                try:
                    orgStatuses[i] = orgStatus
                except:
                    print "statuses: %s" % repr(statuses)
                    print "orgStatuses: %s" % repr(orgStatuses)
                    raise
            protStatus = self.getProtStatus(orgStatuses)
            if protStatus == "Active" and not self.firstPub:
                self.firstPub = startDate
            if protStatus in ("Active", "Approved-not yet active",
                              "Temporarily closed"):
                self.closed = ""
            elif not self.closed:
                self.closed = startDate
            if protStatus == 'Completed':
                self.completed = startDate
            else:
                self.completed = ""
            if self.statuses:
                self.statuses[-1].endDate = startDate
            self.statuses.append(Protocol.Status(protStatus, startDate))
        if self.statuses:
            self.statuses[-1].endDate = time.strftime("%Y-%m-%d")

    def getProtStatus(self, orgStatuses):
        "Look up the protocol status based on the status of the lead orgs."
        statusSet = set()
        for orgStatus in orgStatuses:
            statusSet.add(orgStatus.upper())
        if len(statusSet) == 1:
            # return orgStatuses.pop() BAD SIDE EFFECT!
            return tuple(orgStatuses)[0]
        for status in ("ACTIVE",
                       "TEMPORARILY CLOSED",
                       "COMPLETED",
                       "CLOSED",
                       "APPROVED-NOT YET ACTIVE"):
            if status in statusSet:
                return status
        return ""

    def hadStatus(self, start, end, statuses = ("Active",
                                                "Approved-not yet active",
                                                "Temporarily closed")):
        """
        Did this protocol have any of these status values at any time
        during the indicated range of dates?
        """
        for status in self.statuses:
            if status.endDate > start:
                if status.startDate <= end:
                    if status.name in statuses:
                        return True
        return False

    class Status:
        "Protocol status for a given range of dates."
        def __init__(self, name, startDate, endDate = None):
            self.name      = name
            self.startDate = startDate
            self.endDate   = endDate
        def __cmp__(self, other):
            diff = cmp(self.startDate, other.startDate)
            if diff:
                return diff
            return cmp(self.endDate, other.endDate)

    class LeadOrg:
        "Lead Organization for a protocol, with all its status history."
        def __init__(self, node):
            self.statuses = []
            self.role     = None
            self.pupLink  = None
            for child in node.childNodes:
                if child.nodeName == "LeadOrgProtocolStatuses":
                    for grandchild in child.childNodes:
                        if grandchild.nodeName in ("PreviousOrgStatus",
                                                   "CurrentOrgStatus"):
                            name = date = ""
                            for greatgrandchild in grandchild.childNodes:
                                if greatgrandchild.nodeName == "StatusDate":
                                    date = cdr.getTextContent(greatgrandchild)
                                elif greatgrandchild.nodeName == "StatusName":
                                    name = cdr.getTextContent(greatgrandchild)
                            if name and date:
                                ps = Protocol.Status(name, date)
                                self.statuses.append(ps)
                elif child.nodeName == "LeadOrgRole":
                    self.role = cdr.getTextContent(child).strip() or None
                elif child.nodeName == 'LeadOrgPersonnel':
                    role = link = ""
                    for grandchild in child.childNodes:
                        if grandchild.nodeName == 'PersonRole':
                            role = cdr.getTextContent(grandchild).strip()
                        elif grandchild.nodeName == 'Person':
                            link = grandchild.getAttribute('cdr:ref').strip()
                    if role.upper() == 'UPDATE PERSON':
                        self.pupLink = link
            self.statuses.sort()
            for i in range(len(self.statuses)):
                if i == len(self.statuses) - 1:
                    self.statuses[i].endDate = time.strftime("%Y-%m-%d")
                else:
                    self.statuses[i].endDate = self.statuses[i + 1].startDate
