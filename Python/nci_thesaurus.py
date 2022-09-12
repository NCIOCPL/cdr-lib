"""Interface to the NCI Thesaurus (or EVS - Enterprise Vocabulary System)

BZIssue::4656
BZIssue::5004
BZIssue::5073
JIRA::OCECDR-4153 - strip unwanted OtherName and Definition blocks
JIRA::OCECDR-4226 - complete rewrite to use new EVS API
JIRA::OCECDR-4338 - make module adaptable to volatile API
JIRA::OCECDR-5038 - rewrite for yet another EVS API (our sixth!)
"""

from datetime import date, datetime
from difflib import SequenceMatcher
from functools import cached_property
from json import load, dump
from re import compile
from time import sleep
from uuid import uuid1
from lxml import etree
from requests import get
from cdrapi import db
from cdrapi.docs import Doc
from ModifyDocs import Job


class EVS:
    """Wrapper for the EVS API."""

    BASE_URL = "https://api-evsrest.nci.nih.gov/api/v1/concept/ncit"
    BATCH_SIZE = 100
    CTRP_AGENT_TERMINOLOGY = "C116978"
    NCI_DRUG_DICTIONARY_TERMINOLOGY = "C176424"
    SUBSET_PARENTS = CTRP_AGENT_TERMINOLOGY, NCI_DRUG_DICTIONARY_TERMINOLOGY
    MAX_REQUESTS_ALLOWED_PER_SECOND = 3
    SLEEP = 1 / MAX_REQUESTS_ALLOWED_PER_SECOND
    DRUG_AGENT = 256166
    DRUG_AGENT_CATEGORY = 256164
    DRUG_AGENT_COMBINATION = 256171
    SEMANTIC_TYPES = DRUG_AGENT, DRUG_AGENT_CATEGORY, DRUG_AGENT_COMBINATION

    def fetch(self, code, /, **opts):
        """Retrieve the values for a single EVS concept record.

        Pass:
          code - string for the concept's unique code
          include - indicator of how much data to return
            comma-separated list of any of the following values: minimal,
            summary, full, associations, children, definitions, disjointWith,
            inverseAssociations, inverseRoles, maps, parents, properties,
            roles, synonyms; default is "full"

        Return:
          reference to `Concept` object

        Raise:
          `Exception` on failure, including failure to find the concept
        """

        include = opts.get("include", "full")
        url = f"{self.BASE_URL}/{code}?include={include}"
        response = get(url)
        if response.status_code != 200:
            message = f"{url}: {response.status_code}, ({response.reason})"
            raise Exception(message)
        return Concept(response.json())

    def fetchmany(self, codes, /, **opts):
        """Load drug concept documents from tne NCI EVS.

        Pass:
            codes - concept codes for the records to be fetched
            include - see documentation above for `fetch()`
            batch_size - optional override of the number to fetch at once

        Return:
            dictionary of `Concept` objects indexed by concept code
        """

        include = opts.get("include") or "full"
        batch_size = opts.get("batch_size") or self.BATCH_SIZE
        concepts = {}
        offset = 0
        base = f"{self.BASE_URL}?include={include}&list="
        while offset < len(codes):
            subset = codes[offset:offset+batch_size]
            offset += batch_size
            api = base + ",".join(subset)
            response = get(api)
            for values in response.json():
                concept = Concept(values)
                concepts[concept.code] = concept
            if offset < len(codes):
                sleep(self.SLEEP)
        return concepts

    def load_from_cache(self, path, logger, /):
        """Load drug concept documents from values cached in the file system.

        Pass:
            path - where the concept values have been cached
            logger - used to record what we do

        Return:
            dictionary of `Concept` objects indexed by concept code
        """

        with open(path, encoding="utf-8") as fp:
            concepts = load(fp)
        logger.info("loaded concepts from %s", path)
        return self.__map_concepts(concepts)

    def load_drug_concepts(self, path, logger, /):
        """Load drug concept documents from the Enterprise Vocabulary System.

        Pass:
            path - where to cache the loaded concept values
            logger - used to record what we do

        Return:
            dictionary of `Concept` objects indexed by concept code
        """

        start = datetime.now()
        parms = dict(
            fromRecord=0,
            include="full",
            pageSize=self.BATCH_SIZE,
            subset=",".join(self.SUBSET_PARENTS),
        )
        done = False
        concepts = []
        api = f"{self.BASE_URL}/search"
        while not done:

            # Don't give up right away when an error is encountered.
            tries = 5
            while tries > 0:
                try:
                    response = get(api, params=parms)
                    if not response.ok:
                        raise Exception(response.reason)
                    values = response.json()
                    if not values.get("total"):
                        done = True
                        break
                    concepts += values.get("concepts")
                    parms["fromRecord"] += self.BATCH_SIZE
                    sleep(self.SLEEP)
                    break
                except Exception:
                    tries -= 1
                    if tries < 1:
                        self.bail("EVS not available")
                    logger.exception("failure fetching concepts")
                    sleep(self.SLEEP)
        args = len(concepts), datetime.now() - start
        logger.info("fetched %d concepts in %s", *args)
        with open(path, "w", encoding="utf-8") as fp:
            dump(concepts, fp, indent=2)
        return self.__map_concepts(concepts)

    @property
    def cache_path(self):
        """Unique path to cached concepts (different value for each access)."""
        return f"d:/tmp/evs-{uuid1()}.json"

    @cached_property
    def cursor(self):
        """Cursor for database queries."""
        return db.connect().cursor()

    @cached_property
    def drug_doc_ids(self):
        """Set of IDs for active drug index term CDR documents."""

        query = db.Query("query_term s", "s.doc_id").unique()
        query.join("query_term t", "t.doc_id = s.doc_id")
        query.join("active_doc a", "a.id = s.doc_id")
        query.where("s.path = '/Term/SemanticType/@cdr:ref'")
        query.where(query.Condition("s.int_val", self.SEMANTIC_TYPES, "IN"))
        query.where("t.value = 'Index term'")
        return {row.doc_id for row in query.execute(self.cursor).fetchall()}

    @cached_property
    def linked_concepts(self):
        """Map of concept codes to active CDR active drug index terms."""

        query = db.Query("query_term", "doc_id", "value").unique()
        query.where("path = '/Term/NCIThesaurusConcept'")
        docs_for_codes = {}
        for doc_id, code in query.execute(self.cursor).fetchall():
            if doc_id in self.drug_doc_ids:
                code = code.strip().upper()
                if code not in docs_for_codes:
                    docs_for_codes[code] = []
                docs_for_codes[code].append(doc_id)
        return docs_for_codes

    @staticmethod
    def show_changes(B, old, new, /):
        """Highlight deltas in the EVS definition versus the CDR definition.

        If there are any insertions or replacements, we show those in red.
        Otherwise, we just show the deletions with strikethrough.

        Pass:
            B - HTML builder from the lxml package
            old - definition currently in the CDR document
            new - definition found in the EVS concept record

        Return:
            HTML div object
        """

        sm = SequenceMatcher(None, old, new)
        pieces = []
        new_segments_shown = False
        for tag, i1, i2, j1, j2 in sm.get_opcodes():
            if tag in ("replace", "insert"):
                segment = new[j1:j2]
                pieces.append(B.SPAN(segment, B.CLASS("insertion")))
                new_segments_shown = True
            elif tag == "equal":
                segment = new[j1:j2]
                pieces.append(B.SPAN(segment))
        if not new_segments_shown:
            sm = SequenceMatcher(None, old, new)
            pieces = []
            for tag, i1, i2, j1, j2 in sm.get_opcodes():
                if tag in ("replace", "delete"):
                    segment = old[i1:i2]
                    pieces.append(B.SPAN(segment, B.CLASS("deletion")))
                elif tag == "equal":
                    segment = new[j1:j2]
                    pieces.append(B.SPAN(segment))
        return B.DIV(*pieces)

    @staticmethod
    def show_updates(control, page, refreshes, creates=None):
        """Perform and show requested drug term refresh/create actions.

        Pass:
            page - object on which results are displayed
            refreshes - list of values in the form code-cdrid
            creates - list of concept codes for creating new Term docs
                      (None for the RefreshDrugTermsFromEVS.py script)
        """

        # Fetch the concepts we've been asked to use, to get fresh values.
        refresh_pairs = [value.split("-") for value in refreshes]
        codes = [row[0] for row in refresh_pairs]
        if creates is not None:
            codes += creates
            set_codes = True
        else:
            set_codes = False
        concepts = control.evs.fetchmany(codes)

        # Start the table for displaying the refreshes performed.
        if refreshes:
            body = page.B.TBODY()
            table = page.B.TABLE(
                page.B.CAPTION("Actions" if creates is None else "Updates"),
                page.B.THEAD(
                    page.B.TH("CDR ID"),
                    page.B.TH("Code"),
                    page.B.TH("Name"),
                    page.B.TH("Notes"),
                ),
                body
            )

            # Make sure we will be able to check out the CDR documents.
            docs = {}
            for code, doc_id in refresh_pairs:
                doc_id = int(doc_id)
                if code not in concepts:
                    docs[doc_id] = code
                else:
                    docs[doc_id] = concepts[code]
                    try:
                        doc = Doc(control.session, id=doc_id)
                        doc.check_out()
                    except Exception:
                        concepts[code].unavailable = True

            # Invoke the global change harness to perform the updates.
            control.successes = set()
            control.failures = {}
            Updater(control, docs, set_codes).run()

            # Populate the table reporting the results.
            for doc_id in sorted(docs):
                concept = docs[doc_id]
                if isinstance(concept, str):
                    # This would be a very rare and odd edge case, in which
                    # the concept was removed from the EVS between the time
                    # the form was displayed and the time the refresh request
                    # was submitted.
                    values = doc_id, concept, "", "Concept not found"
                else:
                    if doc_id in control.failures:
                        note = control.failures[doc_id]
                    elif concept.unavailable:
                        note = "Term document checked out to another user."
                    elif doc_id not in control.successes:
                        note = "CDR document unavailable for update"
                    else:
                        note = "Refreshed from and associated with EVS concept"
                    values = doc_id, concept.code, concept.name, note
                row = page.B.TR()
                for value in values:
                    row.append(page.B.TD(str(value)))
                body.append(row)
            page.form.append(table)

        # Add any new CDR Term documents requested.
        if creates:
            body = page.B.TBODY()
            table = page.B.TABLE(
                page.B.CAPTION("New CDR Drug Term Documents"),
                page.B.THEAD(
                    page.B.TH("Code"),
                    page.B.TH("Name"),
                    page.B.TH("CDR ID"),
                    page.B.TH("Notes"),
                ),
                body
            )
            for code in creates:
                if code not in concepts:
                    # See note on comparable condition in the previous table.
                    values = code, "", "", "Concept not found"
                else:
                    try:
                        concept = concepts[code]
                        xml = concept.xml
                        doc = Doc(control.session, doctype="Term", xml=xml)
                        opts = dict(
                            version=True,
                            publishable=False,
                            val_types=("schema", "links"),
                            unlock=True,
                        )
                        doc.save(**opts)
                        values = code, concept.name, doc.cdr_id, "Created"
                    except Exception as e:
                        control.logger.exception(f"Saving doc for {code}")
                        values = code, concept.name, "", str(e)
                row = page.B.TR()
                for value in values:
                    row.append(page.B.TD(value))
                body.append(row)
            page.form.append(table)

    @staticmethod
    def __map_concepts(concepts):
        """Load dictionary of `Concept` objects from sequence of value sets.

        Pass:
            concepts - sequence of value dictionaries

        Return:
            dictionary of `Concept` objects indexed by concept code
        """

        concept_map = {}
        for values in concepts:
            concept = Concept(values)
            concept_map[concept.code] = concept
        return concept_map


class Normalizer:
    """Base class for `Concept` and `Term` classes."""

    WHITESPACE = compile(r"\s+")
    NON_BREAKING_SPACE = chr(160)
    THIN_SPACE = chr(8201)
    ZERO_WIDTH_SPACE = chr(8203)
    FUNKY_WHITESPACE = NON_BREAKING_SPACE, THIN_SPACE, ZERO_WIDTH_SPACE

    @classmethod
    def normalize(cls, text):
        """Prepare a string value for comparison.

        The most recent decision is to go with a less aggressive approach
        to normalization, leaving punctuation in place. So now a normalized
        string will have case differences squashed and space normalized.
        The space normalization has already been applied before the string
        reaches this method, so all we have to do is lowercase the string.

        Pass:
            text - original value

        Return:
            lowercase version of the caller's string
        """

        return text.lower()

    @classmethod
    def normalize_space(cls, text):
        """Fold Unicode space characters into ASCII space and call strip().

        Pass:
            text - original string

        Return:
            original string with whitespace normalized
        """

        for c in cls.FUNKY_WHITESPACE:
            text = text.replace(c, " ")
        return cls.WHITESPACE.sub(" ", text).strip()

    @cached_property
    def normalized_definitions(self):
        """Sequence of normalized definition strings."""

        # pylint: disable=no-member
        return {self.normalize(d) for d in self.definitions}

    @cached_property
    def normalized_name(self):
        """Normalized version of the preferred name."""
        return self.normalize(self.name)  # pylint: disable=no-member

    @cached_property
    def normalized_other_names(self):
        """Dictionary of other names indexed by normalized key."""

        normalized_other_names = {}
        for other_name in self.other_names:  # pylint: disable=no-member
            key = self.normalize(other_name.name)
            if key != self.normalized_name:
                if key not in normalized_other_names:
                    normalized_other_names[key] = other_name
        return normalized_other_names


class Concept(Normalizer):
    """Parsed concept record from the EVS."""

    NAME_PROPS = "CAS_Registry", "NSC_CODE", "IND_Code"
    DEFINITION_TYPES = "DEFINITION", "ALT_DEFINITION"
    SUFFIX = compile(r"\s*\(NCI\d\d\)$")

    def __init__(self, values):
        """Remember the caller's values.

        We also initialize a flag indicating whether the matching CDR document
        is checked out to another user.

        Pass:
            values - nested values extracted from the serialized JSON string
        """

        self.__values = values
        self.unavailable = False

    def __lt__(self, other):
        """Support sorting by the normalized name of the concept.

        Pass:
            other - concept being compared with this one

        Return:
            `True` if this concept should be sorted before the other one
        """

        return self.key < other.key

    @cached_property
    def code(self):
        """Concept code for this EVS record."""
        return self.__values.get("code", "").strip().upper()

    @cached_property
    def definitions(self):
        """Primary or alternate definitions for the concept."""

        definitions = []
        for values in self.__values.get("definitions", []):
            if values.get("type") in self.DEFINITION_TYPES:
                if values.get("source") == "NCI":
                    definition = values.get("definition", "").strip()
                    if definition:
                        definition = self.SUFFIX.sub("", definition)
                        definition = definition.removeprefix("NCI|")
                        definition = self.normalize_space(definition)
                        definitions.append(definition)
        return definitions

    @cached_property
    def key(self):
        """Tuple of the concept's normalized name string and code."""
        return self.normalized_name, self.code

    @cached_property
    def name(self):
        """Preferred name string for the concept."""
        return self.normalize_space(self.__values.get("name", ""))

    @cached_property
    def other_names(self):
        """Sequence of `Concept.OtherName` objects."""

        other_names = []
        for synonym in self.__values.get("synonyms", []):
            if synonym.get("type") == "FULL_SYN":
                name = synonym.get("name", "").strip()
                if name:
                    source = synonym.get("source", "").strip()
                    if source == "NCI":
                        name = self.normalize_space(name)
                        if self.normalize(name) != self.normalized_name:
                            group = synonym.get("termType", "").strip()
                            other_name = self.OtherName(name, group, "NCI")
                            other_names.append(other_name)
        for prop_name in self.NAME_PROPS:
            for code in self.properties.get(prop_name, []):
                code = self.normalize_space(code.strip())
                if code and self.normalize(code) != self.normalized_name:
                    other_names.append(self.OtherName(code, prop_name))
        return other_names

    @cached_property
    def properties(self):
        """Dictionary of named properties."""

        properties = {}
        for prop in self.__values.get("properties", []):
            name = prop.get("type")
            if name:
                value = prop.get("value", "").strip()
                if value:
                    if name not in properties:
                        properties[name] = []
                    properties[name].append(value)
        return properties

    @cached_property
    def xml(self):
        """CDR new document xml created using this EVS concept's values."""

        root = etree.Element("Term", nsmap=Doc.NSMAP)
        node = etree.SubElement(root, "PreferredName")
        node.text = self.name
        names = set([self.normalized_name])
        for key in sorted(self.normalized_other_names):
            if key not in names:
                names.add(key)
                other_name = self.normalized_other_names[key]
                root.append(other_name.convert(self.code))
        for definition in self.definitions:
            root.append(Updater.make_definition_node(definition))
        term_type = etree.SubElement(root, "TermType")
        etree.SubElement(term_type, "TermTypeName").text = "Index term"
        etree.SubElement(root, "TermStatus").text = "Reviewed-retain"
        code = etree.SubElement(root, "NCIThesaurusConcept", Public="Yes")
        code.text = self.code
        opts = dict(pretty_print=True, encoding="Unicode")
        return etree.tostring(root, **opts)

    def differs_from(self, doc):
        """Are the normalized names and definitions different?

        Pass:
          doc - reference to `Term` document object being compared

        Return:
          `True` if differences are found which justify a refresh
        """

        if self.normalized_name != doc.normalized_name:
            return True
        if self.normalized_definitions != doc.normalized_definitions:
            return True
        return self.other_names_differ(doc)

    def other_names_differ(self, doc):
        """Are the normalized other names different?

        If the existing document has an "other" name which the EVS
        concept doesn't have, and that "other" name is approved,
        we're going to keep it anyway, so we ignore that discrepancy.

        Pass:
          doc - reference to `Term` document object being compared

        Return:
          `True` if differences are found which justify a refresh
        """

        for key in self.normalized_other_names:
            if key not in doc.normalized_other_names:
                return True
        for key, name in doc.normalized_other_names.items():
            if not name.approved and key not in self.normalized_other_names:
                return True
        return False

    class OtherName:
        """Synonym or code found in an EVS concept record."""

        SKIP = {"PreferredName", "ReviewStatus"}
        TERM_TYPE_MAP = {
            "PT": "Synonym",
            "AB": "Abbreviation",
            "AQ": "Obsolete name",
            "BR": "US brand name",
            "CN": "Code name",
            "FB": "Foreign brand name",
            "SN": "Chemical structure name",
            "SY": "Synonym",
            "INDCode": "IND code",
            "NscCode": "NSC code",
            "CAS_Registry_Name": "CAS Registry name",
            "IND_Code": "IND code",
            "NSC_Code": "NSC code",
            "CAS_Registry": "CAS Registry name"
        }

        def __init__(self, name, group, source=None):
            """
            Extract the values we'll need for generating an OtherName block.

            Pass:
                name - string for the other name
                group - string for the type of name
                source - string for the name source
            """

            self.name = name
            self.group = group
            self.source = source
            self.include = source == "NCI" if source else True

        def __lt__(self, other):
            """Support sorting the concept's names.

            Pass:
                other - reference to other name being compared with this one

            Return:
                `True` if this name should sort before the other one
            """

            return self.name < other.name

        def convert(self, concept_code, status="Reviewed"):
            """
            Create an OtherName block for the CDR Term document.

            Pass:
                concept_code - added as the source ID for a primary term
                status - whether CIAT needs to review the name

            Return:
                reference to lxml `_Element` object
            """

            term_type = self.TERM_TYPE_MAP.get(self.group, "????" + self.group)
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


class Term(Normalizer):
    """Parsed CDR drug term document."""

    def __init__(self, cdr_id, root):
        """Remember the caller's values.

        Pass:
            cdr_id - unique ID integer for the CDR Term document
            root - top-level node of the parsed XML document
        """

        self.cdr_id = cdr_id
        self.root = root

    @cached_property
    def definitions(self):
        """Sequence of definition strings found in the CDR document."""

        definitions = []
        for node in self.root.findall("Definition/DefinitionText"):
            definition = Doc.get_text(node, "").strip()
            if definition:
                definitions.append(self.normalize_space(definition))
        return definitions

    @cached_property
    def name(self):
        """Preferred name for the CDR drug term document."""

        name = Doc.get_text(self.root.find("PreferredName"))
        return self.normalize_space(name)

    @cached_property
    def other_names(self):
        """Synonyms for the drug term."""

        other_names = set()
        for node in self.root.findall("OtherName"):
            other_name = self.OtherName(node)
            if other_name.name:
                other_names.add(other_name)
        return other_names

    class OtherName:
        """Capture the name string and review status of an OtherName node."""

        def __init__(self, node):
            self.__node = node

        def __lt__(self, other):
            """Support case-insensitive sorting."""
            return self.normalized_name < other.normalized_name

        @cached_property
        def approved(self):
            """`True` if the name has been approved in review."""
            child = self.__node.find("ReviewStatus")
            return Doc.get_text(child, "") == "Reviewed"

        @cached_property
        def name(self):
            """Name string with space normalized."""

            child = self.__node.find("OtherTermName")
            return Normalizer.normalize_space(Doc.get_text(child, ""))

        @cached_property
        def normalized_name(self):
            """Name string normalized for sorting."""
            return self.name.lower()


class Updater(Job):
    """Global change job used to update CDR term documents from the EVS."""

    LOGNAME = "updates-from-evs"
    COMMENT = f"Term document refreshed from EVS {date.today()}"
    NCIT = "NCI Thesaurus"
    TYPE = "Health professional"

    def __init__(self, control, docs, set_codes, /):
        """Capture the caller's values.

        Pass:
            control - used to record successes and failures
            docs - dictionary of `EVSConcept` objects indexded by CDR ID
            set_codes - `True` if the concept codes needs to be set
        """

        self.__control = control
        self.__docs = docs
        self.__set_codes = set_codes
        opts = dict(session=control.session, mode="live", console=False)
        Job.__init__(self, **opts)

    def select(self):
        """Return sequence of CDR ID integers for documents to transform."""
        return sorted([id for id in self.__docs if self.__docs[id]])

    def transform(self, doc):
        """Refresh the CDR document with values from the EVS concept.

        Pass:
            doc - reference to `cdr.Doc` object

        Return:
            serialized XML for the modified document
        """

        # Find the concept whose values we will apply to the CDR document.
        int_id = Doc.extract_id(doc.id)
        concept = self.__docs[int_id]

        # Catch any failures.
        try:

            # Make sure the document has the correct preferred name, statuses.
            root = etree.fromstring(doc.xml)
            root.find("PreferredName").text = concept.name
            node = root.find("TermStatus")
            if node is not None:
                node.text = "Reviewed-retain"
            node = root.find("ReviewStatus")
            if node is not None:
                node.text = "Reviewed"

            # Don't duplicate the preferred name in the OtherName blocks.
            names = set([concept.normalized_name])

            # Start with a clean slate.
            etree.strip_elements(root, "Definition")
            if self.__set_codes:
                etree.strip_elements(root, "NCIThesaurusConcept")
            for node in root.findall("OtherName"):
                other_name = Term.OtherName(node)
                if other_name.approved:
                    names.add(Normalizer.normalize(other_name.name))
                else:
                    root.remove(node)

            # Find where our new elements will be inserted.
            term_type_node = root.find("TermType")
            if term_type_node is None:
                raise Exception("Term document has no TermType element")

            # Insert the nodes for other names and definitions.
            for key in sorted(concept.normalized_other_names):
                if key not in names:
                    names.add(key)
                    other_name = concept.normalized_other_names[key]
                    node = other_name.convert(concept.code)
                    term_type_node.addprevious(node)
            for definition in concept.definitions:
                node = self.make_definition_node(definition)
                term_type_node.addprevious(node)

            # Add the concept code if necessary.
            if self.__set_codes:
                position = 0
                for node in root:
                    if node.tag in ("Comment", "DateLastModified", "PdqKey"):
                        break
                    position += 1
                code_node = etree.Element("NCIThesaurusConcept", Public="Yes")
                code_node.text = concept.code
                root.insert(position, code_node)

            # Record the transformation and return the results.
            self.__control.successes.add(int_id)
            return etree.tostring(root)

        except Exception as e:

            # Record the failure.
            self.logger.exception("CDR%s", int_id)
            self.__control.failures[int_id] = str(e)
            raise

    @classmethod
    def make_definition_node(cls, text):
        """Create the block for the definition being added.

        Pass:
            text - the string for the definition

        Return:
            `_Element` created using the lxml package
        """

        node = etree.Element("Definition")
        etree.SubElement(node, "DefinitionText").text = text
        etree.SubElement(node, "DefinitionType").text = cls.TYPE
        source = etree.SubElement(node, "DefinitionSource")
        etree.SubElement(source, "DefinitionSourceName").text = cls.NCIT
        etree.SubElement(node, "ReviewStatus").text = "Reviewed"
        return node
