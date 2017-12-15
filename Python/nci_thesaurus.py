#----------------------------------------------------------------------
# Interface to the NCI Thesaurus (or EVS - Enterprise Vocabulary System)
#
# BZIssue::4656
# BZIssue::5004
# BZIssue::5073
# JIRA::OCECDR-4153 - strip unwanted OtherName and Definition blocks
# JIRA::OCECDR-4226 - complete rewrite to use new EVS API
# JIRA::OCECDR-4338 - make module adaptable to volatile API
#----------------------------------------------------------------------

import datetime
import json
import re
import sys
import lxml.etree as etree
import requests
import cdr
import cdrdb

class NamedValue:
    """
    Value stored in the JSON returned by the EVS API.

    As far as I have been able to determine through reverse engineering
    (the team maintaining the API has failed so far to provide the
    documentation they promised), the bits we're interested in are
    stored using the following structure:

        dictionary
            predicate
                name -> used as the name in our own object
            value
                literal
                    value -> used as the value in our object

    Hoisted outside of the Concept namespace so it can be used as a base
    class for both properties and qualifiers.
    """

    @staticmethod
    def get_value(dictionary):
        """
        Extract the string value for the property or qualifier.

        Ensure that our assumption about how the value is stored
        is correct.
        """

        value = dictionary["value"]
        assert len(value) == 1, "property has %d values" % len(value)
        return value[0]["literal"]["value"]

class Concept:
    """
    Thesaurus concept retrieved from NCI's Enterprise Vocabulary System (EVS).

    Class values:
        logger - for recording processing information and failures
        REQUIRED - names of properties which are required singletons

    Instance values:
        code - unique identifier for the concept record in the EVS
        preferred_name - canonical name for the term in the EVS
        synonyms - FULL_SYN properties extracted from the concept JSON
        cas_codes, nsc_codes, ind_codes - specialized other names
        definitions - list of definitions for the concept, identified by source
        properties - all of the properties found in the JSON (for debugging)

    Methods:
        add() - create a new CDR Term document for the concept
        update() - refresh the concept's existing CDR Term document
    """

    logger = cdr.Logging.get_logger("nci_thesaurus", level="info")
    REQUIRED = set(["code", "Preferred_Name"])

    def __init__(self, code=None, path=None):
        """
        Fetch, parse, and validate the properties for a thesaurus concept.

        The parts of the structure we're interested in are:

            EntityDescriptionMsg
                entityDescription
                    namedEntity
                        property[, property ...]
                            predicate
                            value
                            [propertyQualifier, [...]]

        See NamedValue above for the composition of properties and
        qualifiers.

        Pass exactly one of:
            code - thesaurus concept ID (fetch the concept using the API)
            path - location of concept JSON stored in a file
        """

        self.logger.info("Concept(code=%r, path=%r)", code, path)
        assert code or path, "code or path must be provided"
        assert not(code and path), "code and path are mutually exclusive"
        for name in self.REQUIRED:
            setattr(self, name.lower(), None)
        self.synonyms = []
        self.definitions = []
        self.cas_codes = []
        self.nsc_codes = []
        self.ind_codes = []
        dictionary = path and self.load(path) or self.fetch(code)
        entity = self.get_named_entity(dictionary)
        self.properties = [self.Property(p) for p in entity["property"]]
        for property in self.properties:
            self.logger.debug("property name: %s", property.name)
            if property.name in self.REQUIRED:
                name = property.name.lower()
                old = getattr(self, name, None)
                assert old is None, "%s already set to %s" % (name, old)
                setattr(self, name, property.value)
            elif property.name in ("DEFINITION", "ALT_DEFINITION"):
                self.definitions.append(self.Definition(property))
            elif property.name == "FULL_SYN":
                self.synonyms.append(self.OtherName(property))
            elif property.name == "CAS_Registry":
                self.cas_codes.append(self.OtherName(property))
            elif property.name == "NSC_Code":
                self.nsc_codes.append(self.OtherName(property))
            elif property.name == "IND_Code":
                self.ind_codes.append(self.OtherName(property))
        for name in self.REQUIRED:
            name = name.lower()
            assert getattr(self, name, None) is not None, "%s missing" % name
        self.logger.info("loaded %s", self.code)

    def add(self, session):
        """
        Add a new CDR Term document for the concept.

        Pass:
            session - ID for a session with permission to create Term docs

        Return:
            string describing successful document create (an exception
            is thrown on failure)
        """

        return TermDoc(self, session).save()

    def update(self, session, cdr_id, **opts):
        """
        Refresh other names and definitions in the CDR Term document.

        Positional arguments:
            session - ID for a session with permission to create Term docs
            cdr_id - unique identifier for the CDR Term document

        Keyword options:
            skip_other_names - if True, leave the OtherName blocks alone
            skip_definitions - if True, leave the Definition blocks alone

        Return:
            sequence of strings describing changes made (empty if no changes
            were found, in which case no new document version is created)
        """

        return TermDoc(self, session, cdr_id, **opts).save()

    @classmethod
    def fetch(cls, code, format="json"):
        """
        Retrieve and unpack the JSON for a thesaurus concept.

        Pass:
            code - unique concept identifier in the NCI thesaurus

        Return:
            dictionary of values for the concept
        """

        url = cls.URL(code, format)
        cls.logger.info(url)
        response = requests.get(url)
        if response.status_code != 200:
            raise Exception("fetching concept %s: %d (%s)", code,
                            response.status_code, response.reason)
        elif format == "json":
            return json.loads(response.content)
        elif format == "xml":
            return etree.fromstring(response.content)

    @staticmethod
    def load(path):
        """
        Read and unpack the JSON for a thesaurus concept stored in a file.

        Pass:
            path - location of file containing JSON for the concept

        Return:
            dictionary of values for the concept
        """

        return json.loads(open(path, "rb").read())

    @staticmethod
    def normalize(text):
        """
        Prepare a string value for comparison.

        The users have decided to eliminate duplicates of OtherName blocks
        for which the term name value differs only in spacing or case. They
        subsequently decided to apply the same approach to definitions.
        See https://tracker.nci.nih.gov/browse/OCECDR-4153.

        Pass:
            text - original value

        Return:
            lowercase version of string with spacing normalized
        """

        step1 = text.strip().lower()
        step2 = re.sub(r"\s+", " ", step1, re.U)
        step3 = re.sub(r"[^\w ]", "", step2, re.U)
        return step3

    @staticmethod
    def get_named_entity(dictionary):
        """
        Dig out the node in the dictionary which contains all the values
        we need.

        Pass:
            dictionary - complete unencoded value returned by the EVS API

        Return:
            the namnedEntity node three levels deep in the dictionary
        """

        entity_description_message = dictionary["EntityDescriptionMsg"]
        entity_description = entity_description_message["entityDescription"]
        return entity_description["namedEntity"]

    @classmethod
    def fail(cls, problem, exception=False):
        """
        Log the problem and raise an exception.

        Pass:
            problem - string describing the nature of the failure
            exception - whether we should log the stack track of an exception
        """

        if exception:
            cls.logger.exception(problem)
        else:
            cls.logger.error(problem)
        raise Exception(problem)

    class OtherName:
        """
        One of the names by which this concept is known.

        Class values:
            SKIP - elements to go past when inserting new OtherName nodes
            TERM_TYPE_MAP - lookup for CDR equivalent of EVS name type

        Instance values:
            name - the term name for the concept
            group - the name type used in the EVS'
            source - the authority for recognizing this name
            include - whether this name should be added to the CDR document
        """

        SKIP = set(["PreferredName", "ReviewStatus", "Comment", "OtherName"])
        TERM_TYPE_MAP = {
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
            "CAS_Registry_Name": "CAS Registry name",
            "IND_Code"         : "IND code",
            "NSC_Code"         : "NSC code",
            "CAS_Registry"     : "CAS Registry name"
        }
        def __init__(self, property):
            """
            Extract the values we'll need for generating an OtherName block.

            Pass:
                property - dictionary of values in the EVS for the name
            """

            self.name = property.value
            self.group = property.name
            self.source = None
            self.include = True
            if property.name == "FULL_SYN":
                for qualifier in property.qualifiers:
                    if qualifier.name == "representational-form":
                        self.group = qualifier.value
                    elif qualifier.name == "property-source":
                        self.source = qualifier.value
                        self.include = self.source == "NCI"

        def convert(self, concept_code, status="Unreviewed"):
            """
            Create an OtherName block for the CDR Term document.

            Pass:
                concept_code - added as the source ID for a primary term
                status - whether CIAT needs to review the name
            """

            term_type = self.TERM_TYPE_MAP.get(self.group, "????")
            node = etree.Element("OtherName")
            etree.SubElement(node, "OtherTermName").text = self.name
            etree.SubElement(node, "OtherNameType").text = term_type
            info = etree.SubElement(node, "SourceInformation")
            source = etree.SubElement(info, "VocabularySource")
            etree.SubElement(source, "SourceCode").text = "NCI Thesaurus"
            etree.SubElement(source, "SourceTermType").text = self.group
            if self.group == "PT" and concept_code:
                child = etree.SubElement(source, "SourceTermId")
                child.text = concept_code
            etree.SubElement(node, "ReviewStatus").text = status
            return node

    class Definition:
        """
        One of the definitions for this concept.

        Class values:
            TYPE - DefinitionType value for all definitions we import
            NCIT - DefinitionSourceName value for those definitions
            SKIP - elements to go past when inserting new Definition blocks

        Instance values:
            text - the definition value (contains no markup)
            source - the creator of this definition
        """

        TYPE = "Health professional"
        NCIT = "NCI Thesaurus"
        SKIP = set(["PreferredName", "ReviewStatus", "OtherName"])

        def __init__(self, property):
            """
            Extract the values we'll need for generating a Dictionary block.

            Pass:
                property - dictionary of values in the EVS for the name
            """

            self.text = property.value
            self.source = None
            for qualifier in property.qualifiers:
                if qualifier.name == "property-source":
                    assert self.source is None, "definition source already set"
                    self.source = qualifier.value

        def convert(self, status="Unreviewed"):
            """
            Create Dictionary block for the CDR Term document.

            Pass:
                status - whether CIAT needs to review the name
            """

            node = etree.Element("Definition")
            etree.SubElement(node, "DefinitionText").text = self.fix_text()
            etree.SubElement(node, "DefinitionType").text = self.TYPE
            source = etree.SubElement(node, "DefinitionSource")
            etree.SubElement(source, "DefinitionSourceName").text = self.NCIT
            etree.SubElement(node, "ReviewStatus").text = status
            return node

        def fix_text(self):
            """
            Strip out some cruft which NCI injects into its definition strings.
            """

            text = re.sub(r"^NCI\|", "", self.text.strip())
            return re.sub(r"\s*\(NCI[^)]*\)", "", text)

    class Property(NamedValue):
        """
        A single piece of information about the thesaurus concept.

        For more information, see the documentation of the base
        class NamedValue above.

        Instance values:
            name - the name of the property (e.g., FULL_SYN or DEFINITION)
            value - string value for the property (e.g., a name for the concept)
            qualifiers - sequence of additional information about the
                         property (for example, the source of a definition,
                         or the type ("group") of a synonym)
        """

        def __init__(self, dictionary):
            """
            Extract the name, value, and qualifiers for this property.

            Pass:
                EVS node contining this property's information
            """

            self.name = dictionary["predicate"]["name"]
            self.value = self.get_value(dictionary)
            qualifiers = dictionary.get("propertyQualifier") or []
            self.qualifiers = [self.Qualifier(q) for q in qualifiers]

        class Qualifier(NamedValue):
            """
            Piece of additional information about a concept's property.

            For more information, see the documentation of the base
            class NamedValue above.

            Instance values:
                name - identifies which piece of additional information
                       this is
                value - the additional information itself
            """

            def __init__(self, dictionary):
                """
                Extract the name and value for the qualifier.

                Pass:
                    EVS node contining this qualifier's information
                """

                self.name = dictionary["predicate"]["name"]
                self.value = self.get_value(dictionary)

    class URL:
        """
        Address for retrieving the JSON for an EVS concept.

        Class values:
            SCHEME - they have recently switched to https (as mandated)
            HOST - DNS name for the service
            PATH - location of requested resource, with placeholder for
                   the unique identifier of a specific concept
            PARMS - identification of requested format (JSON)
            TEMPLATE - the assembled pattern for constructing a URL,
                       with a placeholder for the unique identifier
                       of a specific EVS concept record
        """

        GRP = cdr.getControlGroup("thesaurus")
        SCHEME = GRP.get("scheme", "https")
        HOST = GRP.get("host", "lexevscts2.nci.nih.gov")
        DFLT_PATH = "lexevscts2/codesystem/NCI_Thesaurus/entity/{self.code}"
        PATH = GRP.get("path", DFLT_PATH)
        PARMS = GRP.get("parms", "format={self.format}")
        TEMPLATE = "{}://{}/{}?{}".format(SCHEME, HOST, PATH, PARMS)

        def __init__(self, code, format="json"):
            """
            Store the concept identifier without leading or trailing spaces.

            Pass:
                code - the unique identifier of a specifie EVS concept record
            """

            self.code = code.strip()
            self.format = format

        def __str__(self):
            """
            Plug the concept code for a specific concept into the template.

            Return:
                string containing the URL for retrieving the concept record
            """

            return self.TEMPLATE.format(self=self)

    @classmethod
    def test(cls):
        """
        Run a requested command-line test of the module's functionality.

        For usage, invoke the module as follows:
            python nci_thesaurus.py --help
        """

        import argparse
        actions = (
            "save-json",
            "save-xml",
            "print-json",
            "print-xml",
            "print-changes",
            "find-changed-terms",
            "count-properties"
        )
        formatter_class = argparse.ArgumentDefaultsHelpFormatter
        parser = argparse.ArgumentParser(formatter_class=formatter_class)
        parser.add_argument("--concept-id", default="C55555")
        parser.add_argument("--cdr-id")
        parser.add_argument("--action", choices=actions, default=actions[0])
        parser.add_argument("--limit", type=int)
        parser.add_argument("--indent", type=int, default=2)
        parser.add_argument("--directory", default=".")
        parser.add_argument("--filename")
        args = parser.parse_args()
        getattr(cls, args.action.replace("-", "_"))(args)

    @classmethod
    def find_changed_terms(cls, args):
        """
        Find all of the CDR Term documents which need to be refreshed.

        Writes the concept code and CDR ID to the standard output
        on a single line, separated by the tab character. Writes
        errors to the standard error file (most of these will be
        caused by a mismatch in the concept's preferred name string).
        Can take as much as a couple of hours, unless the --limit
        option is used.

        Pass:
            args - dictionary of command line arguments (this test
                   uses only the --limit option)
        """

        start = datetime.datetime.now()
        query = cdrdb.Query("query_term", "doc_id", "value")
        query.where("path = '/Term/NCIThesaurusConcept'")
        query.where("value LIKE 'C%'")
        query.where("value NOT LIKE 'CDR%'")
        if args.limit:
            query.limit(args.limit)
        for cdr_id, concept_id in query.execute().fetchall():
            try:
                concept = cls(code=concept_id)
                term_doc = TermDoc(concept, cdr_id=cdr_id)
                if term_doc.changes:
                    print "%s\tCDR%s" % (concept_id, cdr_id)
            except Exception, e:
                error = "comparing CDR%d to %r" % (cdr_id, concept_id)
                Concept.logger.exception(error)
                sys.stderr.write("%s: %s\n" % (error, e))
        elapsed = (datetime.datetime.now() - start).total_seconds()
        sys.stderr.write("elapsed time: %s seconds\n" % elapsed)

    @classmethod
    def print_json(cls, args):
        """
        Unit test to write the formatted JSON for a concept to the
        standard output.

        Pass:
            args - dictionary of command line arguments (this test
                   uses the --concept-id and --indent options)
        """

        print json.dumps(cls.fetch(args.concept_id), indent=args.indent)

    @classmethod
    def save_json(cls, args):
        """
        Unit test to write the formatted JSON for a concept to a disk file.

        Pass:
            args - dictionary of command line arguments (this test
                   uses the --concept-id and --indent options)
        """

        name = "%s.json" % args.concept_id
        with open(name, "w") as fp:
            json.dump(cls.fetch(args.concept_id), fp, indent=args.indent)
        print "saved", name

    @classmethod
    def print_xml(cls, args):
        """
        Unit test to write the converted (or updated) CDR Term document
        to standard output.

        If the --cdr-id option is specified, an in-memory update of the
        document is performed. Otherwise a new Term document is generated
        in memory and printed (but not saved to the repository).

        Pass:
            args - dictionary of command line arguments (this test
                   uses the --concept-id and (optionally) --cdr-id options)
        """

        term_doc = TermDoc(cls(code=args.concept_id), cdr_id=args.cdr_id)
        print term_doc.doc.xml

    @classmethod
    def save_xml(cls, args):
        """
        Unit test to write the converted (or updated) CDR Term document
        to a disk file.

        If the --cdr-id option is specified, an in-memory update of the
        document is performed. Otherwise a new Term document is generated
        in memory and saved to disk (but not saved to the repository).

        Pass:
            args - dictionary of command line arguments (this test
                   uses the --concept-id and (optionally) --cdr-id options)
        """

        name = "%s.xml" % args.concept_id
        term_doc = TermDoc(cls(code=args.concept_id), cdr_id = args.cdr_id)
        with open(name, "w") as fp:
            fp.write(term_doc.doc.xml)
        print "saved", name

    @classmethod
    def print_changes(cls, args):
        """
        Unit test to update an existing CDR Term document in memory
        using the current EVS concept record and describe the changes
        performed (if any).

        Pass:
            args - dictionary of command line arguments (this test
                   uses the --concept-id and --cdr-id options)
        """

        assert args.cdr_id, "cdr-id required for print-changes action"
        term_doc = TermDoc(cls(code=args.concept_id), cdr_id = args.cdr_id)
        for change in term_doc.changes:
            if "definition" not in change.lower():
                print change
        for change in term_doc.changes:
            if "definition" in change.lower():
                print change
        if not term_doc.changes:
            print "Term document unchanged"

    @classmethod
    def count_properties(cls, args):
        """
        Unit test which parses all of the .json files in the specified
        directory and prints the counts of each type of property found
        to the standard output.

        Pass:
            args - dictionary of command line arguments (this test
                   uses only the --directory option)
        """

        import glob
        properties = {}
        for name in glob.glob("%s/*.json" % args.directory):
            try:
                concept = cls(path=name)
                counts = {}
                for p in concept.properties:
                    properties[p.name] = properties.get(p.name, 0) + 1
                print name, concept.code, concept.preferred_name
            except Exception, e:
                sys.stderr.write("%s : %s\n" % (name, e))
        for name in sorted(properties):
            print "%7d %s" % (properties[name], name)

class TermDoc:
    """
    CDR Term document, created or updated from the corresponding EVS concept.

    Class values:
        CDRNS - namespace for some of the attributes in CDR XML documents
        NSMAP - namespace map used for building new CDR Term document
        OTHER_NAMES - property names for term names/aliases

    Instance values:
        concept - object for the source EVS concept
        session - CDR session used for retrieving/saving the document
        skip_other_names - if True, do not update the OtherNames elements
                           (ignored for newly created Term documents)
        skip_definitions - if True, do not update the Definition elements
                           (ignored for newly created Term documents)
        published - if true, at least publishable version of the document
                    already exists
        cdr_id - the unique ID of the CDR Term document to be updated;
                 None when creating a new Term document
        doc - cdr.Doc object to be saved
    """

    CDRNS = "cips.nci.nih.gov/cdr"
    NSMAP = { "cdr" : CDRNS }
    OTHER_NAMES = "synonyms", "ind_codes", "nsc_codes", "cas_codes"

    def __init__(self, concept, session="guest", cdr_id=None, **opts):
        """
        Create of update the cdr.Doc object for this Term document.

        Pass:
            concept - EVS concept information to be used in the Term document
            session - CDR session used for retrieving/saving the document
            cdr_id - Term document to be updated, or None if new document
        """

        self.concept = concept
        self.session = session
        self.skip_other_names = opts.get("skip_other_names", False)
        self.skip_definitions = opts.get("skip_definitions", False)
        self.published = False
        self.cdr_id = None
        self.doc = cdr_id and self.load(cdr_id) or self.create()

    def save(self):
        """
        Create a version of the CDR Term document in the repository.

        Return:
            String describing successful creation of a new Term document
            (including the CDR ID for the new document); or sequence
            of strings describing changes made to an existing Term
            document.

        Raises an exception on failure.
        """

        verb = self.doc.id and "Updating" or "Importing"
        opts = {
            "doc": str(self.doc),
            "comment": "%s Term document from NCI Thesaurus" % verb,
            "val": "Y",
            "ver": "Y",
            "verPublishable": self.published and "Y" or "N",
            "showWarnings": True
        }
        try:
            if self.doc.id:
                if self.changes:
                    result = cdr.repDoc(self.session, **opts)
                    Concept.logger.info("repDoc() result: %r", result)
                    cdr_id, errors = result
                    if not cdr_id:
                        Concept.fail("failure versioning %s: %s",
                                     self.cdr_id, errors)
                response = self.changes or None
            else:
                result = cdr.addDoc(self.session, **opts)
                Concept.logger.info("addDoc() result: %r", result)
                self.cdr_id, errors = result
                if not self.cdr_id:
                    Concept.fail("failure adding new document: %s" % errors)
                response = "Added %s as %s" % (self.concept.code, self.cdr_id)
        except Exception:
            if self.cdr_id:
                cdr.unlock(self.session, self.cdr_id)
            raise
        cdr.unlock(self.session, self.cdr_id)
        return response

    def load(self, cdr_id):
        """
        Check out the CDR Term document, parse it, and update it with
        the current concept information.

        Pass:
            cdr_id - unique identified of the existing CDR Term document

        Return:
            cdr.Doc object with updated terms and/or definitions
        """

        try:
            self.cdr_id, self.doc_id, frag_id = cdr.exNormalize(cdr_id)
            if cdr.lastVersions(self.session, self.cdr_id)[1] != -1:
                self.published = True
        except:
            Concept.fail("invalid CDR ID %r" % cdr_id)
        self.concept.logger.info("updating %s", self.cdr_id)
        try:
            doc = cdr.getDoc(self.session, self.cdr_id, "Y", getObject=True)
        except Exception as e:
            Concept.fail("failure retrieving %s: %s" % (self.cdr_id, e))
        try:
            self.root = self.parse(doc.xml)
            doc.xml = self.update()
            return doc
        except Exception:
            cdr.unlock(self.session, self.cdr_id)
            raise

    def parse(self, xml):
        """
        Create the ElementTree node for the CDR Term document.

        This method function verifies that the Term document matches
        the concept, and throws an exception otherwise.

        Pass:
            xml - serialized XML for the CDR Term document

        Return:
            parsed ElementTree node for the document
        """

        cdr_id = self.cdr_id
        root = etree.fromstring(xml)
        node = root.find("NCIThesaurusConcept")
        if node is None or node.text is None:
            Concept.fail("%s has no concept code" % cdr_id)
        code = node.text
        if code.strip().upper() != self.concept.code.strip().upper():
            why = "%s is for %r, not %r" % (cdr_id, code, self.concept.code)
            Concept.fail(why)
        node = root.find("PreferredName")
        if node is None or node.text is None:
            Concept.fail("%s has no preferred name" % cdr_id)
        cdr_name, ncit_name = node.text, self.concept.preferred_name
        if Concept.normalize(cdr_name) != Concept.normalize(ncit_name):
            why = u"%s is for %r, not %r" % (cdr_id, cdr_name, ncit_name)
            Concept.fail(why)
        return root

    def update(self):
        """
        Modify the OtherName and Definition blocks for the Term document.

        Honor the options to avoid modifying one or the other of these
        sets of blocks.

        Return:
            serialized XML for the (possibly) updated Term document
        """

        self.changes = set()
        if not self.skip_other_names:
            self.update_names()
        if not self.skip_definitions:
            self.update_definitions()
        if self.changes:
            for node in self.root.findall("TermStatus"):
                node.text = "Unreviewed"
        return etree.tostring(self.root, pretty_print=True)

    def update_names(self):
        """
        Replace the existing OtherName elements with a fresh set.

        A sequence of changes is recorded in self.changes.
        The position for the inserts is determined by walking past
        all of the elements which precede the OtherName elements.
        Then the sequence of OtherName nodes to be inserted is
        reversed, so we can perform all of the insertions using
        the same position.
        """

        vals = self.Values(self.root, "OtherName/OtherTermName")
        if vals.dups:
            what = "OtherName block" + (vals.dups > 1 and "s" or "")
            self.changes.add("%d duplicate %s removed" % (vals.dups, what))
        nodes = []
        for attr_name in self.OTHER_NAMES:
            for n in getattr(self.concept, attr_name):
                if n.include:
                    key = Concept.normalize(n.name)
                    if key not in vals.used:
                        status = "Reviewed"
                        original = vals.original.get(key)
                        if original is None:
                            status = "Unreviewed"
                            self.changes.add("added name %r" % n.name)
                        elif original != n.name:
                            status = "Unreviewed"
                            change = "replaced %r with %r" % (original, n.name)
                            self.changes.add(change)
                        nodes.append(n.convert(self.concept.code, status))
                        vals.used.add(key)
        etree.strip_elements(self.root, "OtherName")
        position = self.find_position(Concept.OtherName.SKIP)
        for node in reversed(nodes):
            self.root.insert(position, node)
        for key in (set(vals.original) - vals.used):
            self.changes.add("dropped name %r" % vals.original[key])

    def update_definitions(self):
        """
        Replace the existing Definition elements with a fresh set.

        A sequence of changes is recorded in self.changes.
        The position for the inserts is determined by walking past
        all of the elements which precede the Definition elements.
        Then the sequence of Definition nodes to be inserted is
        reversed, so we can perform all of the insertions using
        the same position.
        """

        vals = self.Values(self.root, "Definition/DefinitionText")
        if vals.dups:
            what = "definition" + (vals.dups > 1 and "s" or "")
            self.changes.add("%d duplicate %s eliminated" % (vals.dups, what))
        nodes = []
        for d in self.concept.definitions:
            if d.source == 'NCI':
                key = Concept.normalize(d.text)
                if key not in vals.used:
                    status = "Reviewed"
                    original = vals.original.get(key)
                    if original is None:
                        status = "Unreviewed"
                    elif original != d.text:
                        status = "Unreviewed"
                        vals.updated += 1
                    nodes.append(d.convert(status))
                    vals.used.add(key)
        etree.strip_elements(self.root, "Definition")
        position = self.find_position(Concept.Definition.SKIP)
        for node in reversed(nodes):
            self.root.insert(position, node)
        vals.record_definition_changes(self.changes)

    def create(self):
        """
        Create a new CDR Term document for the concept.

        Return:
            cdr.Doc object for a new CDR Term document
        """

        code = self.concept.code
        cdr_id = self.lookup(code)
        if cdr_id:
            Concept.fail("%s already imported as CDR%d" % (code, cdr_id))
        root = etree.Element("Term", nsmap=self.NSMAP)
        node = etree.SubElement(root, "PreferredName")
        node.text = self.concept.preferred_name
        done = set()
        for name in self.OTHER_NAMES:
            for other_name in getattr(self.concept, name):
                if other_name.include:
                    key = Concept.normalize(other_name.name)
                    if key not in done:
                        root.append(other_name.convert(self.concept.code))
                        done.add(key)
        done = set()
        for definition in self.concept.definitions:
            if definition.source == "NCI":
                key = Concept.normalize(definition.text)
                if key not in done:
                    root.append(definition.convert())
                    done.add(key)
        term_type = etree.SubElement(root, "TermType")
        etree.SubElement(term_type, "TermTypeName").text = "Index term"
        etree.SubElement(root, "TermStatus").text = "Unreviewed"
        code = etree.SubElement(root, "NCIThesaurusConcept", Public="Yes")
        code.text = self.concept.code
        return cdr.Doc(etree.tostring(root, pretty_print=True), "Term")

    def find_position(self, skip):
        """
        Find the position where new nodes should be inserted.

        Pass:
            skip - set of element names which precede the nodes being
                   inserted
        """

        position = 0
        for node in self.root:
            if node.tag not in skip:
                return position
            position += 1
        return position

    @staticmethod
    def lookup(code):
        """
        Find the CDR ID for the Term document for an EVS concept.

        Pass:
            code - string for unique identifier of EVS concept record

        Return:
            integer CDR document ID for the Term document; None if not found
        """

        query = cdrdb.Query("query_term", "doc_id").unique()
        query.where("path = '/Term/NCIThesaurusConcept'")
        query.where(query.Condition("value", code))
        row = query.execute().fetchone()
        return row and row[0] or None

    class Values:
        """
        Set of OtherName or Definition element in a CDR Term document.

        Instance values:
            dups - integer count of duplicates found
            updated - integer count of modified elements
            original - map of normalized values to original values
            used - set of normalized values for elements queued for
                   re-insertion into the element
        """

        def __init__(self, root, path):
            """
            Gather the unique OtherName or Definition string values,
            indexed by the normalized values for those values.
            """

            self.dups = self.updated = 0
            self.original = {}
            self.used = set()
            for node in root.findall(path):
                value = self.Value(node)
                if value.normalized in self.original:
                    self.dups += 1
                else:
                    self.original[value.normalized] = value.text

        def record_definition_changes(self, changes):
            """
            Update the sequence of change descriptions to reflect
            what happened to the Term document's definitions.

            Unlike the OtherName changes, for which we show the
            actual original and new name strings, we only show
            counts of replaced, added, and dropped definitions,
            because the definition strings will be too lengthy
            in most cases to show to the user or put in the logs.
            If the actual values need to be examined, the CDR
            version history table has everything we need.

            Pass:
                changes - sequence of strings describing modifications
                          to the CDR Term documents
            """

            self.added = len(self.used - set(self.original))
            self.dropped = len(set(self.original) - self.used)
            if self.added == self.dropped:
                self.replaced = self.added
                self.added = self.dropped = 0
            elif self.added > self.dropped:
                self.replaced = self.dropped
                self.added -= self.replaced
                self.dropped = 0
            else:
                self.replaced = self.added
                self.dropped -= self.replaced
                self.added = 0
            for verb in ("added", "dropped", "updated", "replaced"):
                count = getattr(self, verb)
                if count:
                    what = "definition" + (count > 1 and "s" or "")
                    changes.add("%s %d %s" % (verb, count, what))

        class Value:
            """
            Original and normalized versions of an OtherName or
            Definition element.

            Instance values:
                text - string value actually stored in the element
                normalized - transformed version of the value used
                             for identifying what the users regard
                             as duplicates
            """

            def __init__(self, node):
                """
                Assemble the string for the node's value, create
                a normalized version of the string, and save both.
                """

                self.text = u"".join(node.itertext())
                self.normalized = Concept.normalize(self.text)

if __name__ == "__main__":
    """
    Normally this file is imported as a module, but it can be run
    from the command line for unit testing.
    """

    Concept.test()
