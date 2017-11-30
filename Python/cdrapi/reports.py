from datetime import datetime
import re
from dateutil.relativedelta import relativedelta
from lxml import etree
from cdrapi.db import Query, connect as db_connect
from cdrapi.docs import Doc

class Report:

    def __init__(self, session, name, **opts):
        """
        Capture the report name and options

        Pass:
          session - reference to object with tools we need for the report
          name - string identifying which report is being requested
          opts - keyword parameters controlling report processing behavior

        See individual report methods for documentation of which keyword
        options are support for each report.
        """

        self.__session = session
        self.__name = name
        self.__opts = opts

    @property
    def cursor(self):
        if not hasattr(self, "_cursor"):
            self._cursor = db_connect().cursor()
        return self._cursor

    @property
    def name(self):
        return self.__name

    @property
    def session(self):
        return self.__session

    def __start_body(self):
        body = etree.Element("ReportBody")
        etree.SubElement(body, "ReportName").text = self.name
        return body

    def run(self):
        """
        Generate a report and return it to the caller

        Return:
          string for the report body
        """

        stripper = re.compile("[^a-z0-9 ]+")
        normalized = stripper.sub("", self.name.lower()).replace(" ", "_")
        handler = getattr(self, "_" + normalized)
        if handler is None:
            raise Exception("Report {!r} not implemented".format(self.name))
        self.session.logger.info("Running report {!r}".format(self.name))
        return handler()

    # ------------------------------------------------------------------
    # INDIVIDUAL REPORT METHODS START HERE.
    # ------------------------------------------------------------------

    def _board_member(self):
        """
        Find corresponding person document linked from board member document

        Parameters:
          PersonID - required CDR ID for the Person document for which
                     we want to find the corresponding PDQBoardMemeberInfo
                     document

        Return:
          XML document node with the following structure:
            ReportBody
              ReportName
              BoardMember
        """

        person_id = self.__opts.get("PersonId")
        if not person_id:
            raise Exception("Missing required 'PersonId' parameter")
        doc = Doc(self.session, id=person_id)
        query = Query("query_term", "doc_id")
        query.where("path = '/PDQBoardMemberInfo/BoardMemberName/@cdr:ref'")
        query.where(query.Condition("int_val", doc.id))
        row = query.execute(self.cursor).fetchone()
        if not row:
            message = "No board member found for {}".format(person_id)
            raise Exception(message)
        body = self.__start_body()
        member_id = Doc.normalize_id(row.doc_id)
        etree.SubElement(body, "BoardMember").text = member_id
        return body

    def _dated_actions(self):
        """
        Find the to-do list for documents of a particular type

        Parameters:
          DocType - required string naming the document type for
                    which documents should be included

        Return:
          XML document node with the following structure:
            ReportBody
              ReportName
              ReportRow*
                DocId
                DocTitle
        """

        doctype = self.__opts.get("DocType")
        if not doctype:
            raise Exception("Missing required 'DocType' parameter")
        path = "/{}/DatedAction/ActionDate".format(doctype)
        query = Query("document d", "d.id", "d.title").unique()
        query.join("query_term a", "a.doc_id = d.id")
        query.where(query.Condition("a.path", path))
        rows = query.execute(self.cursor).fetchall()
        body = self.__start_body()
        for id, title in rows:
            wrapper = etree.SubElement(body, "ReportRow")
            etree.SubElement(wrapper, "DocId").text = Doc.normalize_id(id)
            etree.SubElement(wrapper, "DocTitle").text = title
            result = Doc(self.session, id=id).filter("name:Dated Actions")
            wrapper.append(result.result_tree.getroot())
        return body

    def _genetics_syndromes(self):
        """
        Find Term documents used to represent genetics syndromes

        Parameters:
          TitlePattern - optional string narrowing report to syndrome
                         names which match the pattern

        Return:
          XML document node with the following structure:
            ReportBody
              ReportName
              ReportRow*
                DocId
                DocTitle
        """

        pattern = self.__opts.get("TitlePattern")
        query = Query("document d", "d.id", "d.title")
        query.unique().order("d.title")
        query.join("query_term t", "t.doc_id = d.id")
        query.join("query_term s", "s.doc_id = t.doc_id",
                   "LEFT(s.node_loc, 8) = LEFT(t.node_loc, 8)")
        query.where("t.path = '/Term/MenuInformation/MenuItem/MenuType'")
        query.where("s.path = '/Term/MenuInformation/MenuItem/MenuStatus'")
        query.where("s.value = 'Online'")
        query.where("t.value = 'Genetics Professionals--GeneticSyndrome'")
        if pattern:
            query.where(query.Condition("d.title", pattern, "LIKE"))
        body = self.__start_body()
        for id, title in query.execute(self.cursor).fetchall():
            wrapper = etree.SubElement(body, "ReportRow")
            etree.SubElement(wrapper, "DocId").text = Doc.normalize_id(id)
            etree.SubElement(wrapper, "DocTitle").text = title
        return body

    def _glossary_term_names(self):
        """
        Find GlossaryTermName documents linked to a GlossaryTermConcept doc

        Parameters:
          ConceptID - required CDR ID for the master concept document

        Return:
          XML document node with the following structure:
            ReportBody
              ReportName
              GlossaryTermName*
                @ref
        """

        concept_id = self.__opts.get("ConceptId")
        if not concept_id:
            raise Exception("Missing required 'ConceptId' parameter")
        path = "/GlossaryTermName/GlossaryTermConcept/@cdr:ref"
        doc = Doc(self.session, id=concept_id)
        query = Query("document d", "d.id", "d.title")
        query.join("query_term n", "n.doc_id = d.id")
        query.where(query.Condition("n.path", path))
        query.where(query.Condition("n.int_val", doc.id))
        body = self.__start_body()
        for id, title in query.execute(self.cursor).fetchall():
            cdr_id = Doc.normalize_id(id)
            etree.SubElement(body, "GlossaryTermName", ref=cdr_id)
        return body

    def _inactive_checked_out_documents(self):
        """
        Report on locked documents which haven't seen any action recently

        Parameters:
          InactivityLength - required string for how long a document
                             can be active before showing up on the report
                             in the form YYYY-MM-DD

        Return:
          XML document node with the following structure:
            ReportBody
              ReportName
              ReportRow*
                DocId
                DocType
                CheckedOutTo
                WhenCheckedOut
                LastActivity
                  ActionType
                  ActionWhen
        """

        deltas = self.__opts.get("InactivityLength")
        if not deltas:
            raise Exception("Missing required 'InactivityLength' parameter")
        try:
            years, months, days = [int(digits) for digits in deltas.split("-")]
        except:
            message = "InactivityLength parameter must be in YYYY-MM-DD format"
            raise Exception(message)
        delta = relativedelta(years=years, months=months, days=days)
        cutoff = (datetime.now() - delta).replace(microsecond=0)
        subquery = Query("audit_trail", "MAX(dt)")
        subquery.where("document = a.document")
        fields = "d.id", "t.name", "c.dt_out", "u.name", "w.name", "a.dt"
        query = Query("document d", *fields)
        query.join("doc_type t", "t.id = d.doc_type")
        query.join("checkout c", "c.id = d.id")
        query.join("open_usr u", "u.id = c.usr")
        query.join("audit_trail a", "a.document = d.id")
        query.join("action w", "w.id = a.action")
        query.where("c.dt_in IS NULL")
        query.where(query.Condition("c.dt_out", cutoff, "<"))
        query.where(query.Condition("a.dt", cutoff, "<"))
        query.where(query.Condition("a.dt", subquery))
        rows = query.order("c.id").execute(self.cursor).fetchall()
        body = self.__start_body()
        for id, doctype, locked, locker, what, when in rows:
            wrapper = etree.SubElement(body, "ReportRow")
            etree.SubElement(wrapper, "DocId").text = Doc.normalize_id(id)
            etree.SubElement(wrapper, "DocType").text = doctype
            etree.SubElement(wrapper, "CheckedOutTo").text = locker
            etree.SubElement(wrapper, "WhenCheckedOut").text = str(locked)
            last_activity = etree.SubElement(wrapper, "LastActivity")
            etree.SubElement(last_activity, "ActionType").text = what
            etree.SubElement(last_activity, "ActionWhen").text = str(when)
        return body

    def _locked_documents(self):
        """
        Report on documents locked by specified user

        Parameters:
          UserId - required string identifying a user account;
                   this is the value often referred to as the 'name'
                   rather than the integer primary key for the row
                   in the usr table

        Return:
          XML document node with the following structure:
            ReportBody
              ReportName
              ReportRow*
                DocId
                DocType
                WhenCheckedOut
        """

        user_name = self.__opts.get("UserId")
        if not user_name:
            raise Exception("Missing required 'UserId' parameter")
        query = Query("document d", "d.id", "t.name", "c.dt_out")
        query.join("doc_type t", "t.id = d.doc_type")
        query.join("checkout c", "c.id = d.id")
        query.join("open_usr u", "u.id = c.usr")
        query.where("c.dt_in IS NULL")
        query.where(query.Condition("u.name", user_name))
        rows = query.order("c.id").execute(self.cursor).fetchall()
        body = self.__start_body()
        for id, doctype, locked in rows:
            wrapper = etree.SubElement(body, "ReportRow")
            etree.SubElement(wrapper, "DocId").text = Doc.normalize_id(id)
            etree.SubElement(wrapper, "DocType").text = doctype
            etree.SubElement(wrapper, "WhenCheckedOut").text = str(locked)
        return body

    def _menu_term_tree(self):
        """
        Report on the menu term hierarchy

        Parameters:
          MenuType - required string restricting report to a single menu type

        Return:
          XML document node with the following structure:
            ReportBody
              ReportName
              MenuItem*
                TermId
                TermName
                MenuType
                MenuStatus
                DisplayName?
                ParentId?
                SortString
        """

        menutype = self.__opts.get("MenuType", "%")
        operand = "LIKE" if "%" in menutype else "="
        fields = (
            "n.doc_id AS id",
            "n.value AS name",
            "t.value AS type",
            "s.value AS status",
            "d.value AS display",
            "p.int_val AS parent",
            "k.value AS sort_key"
        )
        i_path = "/Term/MenuInformation/MenuItem"
        n_path = "/Term/PreferredName"
        t_path = "{}/MenuType".format(i_path)
        s_path = "{}/MenuStatus".format(i_path)
        d_path = "{}/DisplayName".format(i_path)
        p_path = "{}/MenuParent/@cdr:ref".format(i_path)
        k_path = "{}/@SortOrder".format(i_path)
        query = Query("query_term n", *fields).unique().order("n.doc_id")
        query.join("query_term t", "t.doc_id = n.doc_id")
        query.join("query_term s", "s.doc_id = t.doc_id",
                   "LEFT(s.node_loc, 8) = LEFT(t.node_loc, 8)")
        query.outer("query_term d", "d.doc_id = t.doc_id",
                    "d.path = '{}'".format(d_path),
                   "LEFT(d.node_loc, 8) = LEFT(t.node_loc, 8)")
        query.outer("query_term p", "p.doc_id = t.doc_id",
                    "p.path = '{}'".format(p_path),
                   "LEFT(d.node_loc, 8) = LEFT(t.node_loc, 8)")
        query.outer("query_term k", "k.doc_id = t.doc_id",
                    "k.path = '{}'".format(k_path),
                   "LEFT(k.node_loc, 8) = LEFT(t.node_loc, 8)")
        query.where("n.path = '{}'".format(n_path))
        query.where("t.path = '{}'".format(t_path))
        query.where("s.path = '{}'".format(s_path))
        query.where(query.Condition("t.value", menutype, operand))
        query.where(query.Condition("s.value", "Offline", "<>"))
        body = self.__start_body()
        for row in query.execute(self.cursor).fetchall():
            sort_key = row.sort_key if row.sort_key is not None else row.name
            wrapper = etree.SubElement(body, "MenuItem")
            etree.SubElement(wrapper, "TermId").text = str(row.id)
            etree.SubElement(wrapper, "TermName").text = row.name
            etree.SubElement(wrapper, "MenuType").text = row.type
            etree.SubElement(wrapper, "MenuStatus").text = row.status
            if row.display is not None:
                etree.SubElement(wrapper, "DisplayName").text = row.display
            if row.parent is not None:
                etree.SubElement(wrapper, "ParentId").text = str(row.parent)
            etree.SubElement(wrapper, "SortString").text = sort_key
        return body

    def _patient_summary(self):
        """
        Find corresponding patient version for HP summary

        Parameters:
          HPSummary - required CDR ID for the Health Professional
                      for which we want to find the patient verion

        Return:
          XML document node with the following structure:
            ReportBody
              ReportName
              PatientSummary
        """

        hp_id = self.__opts.get("HPSummary")
        if not hp_id:
            raise Exception("Missing required 'HPSummary' parameter")
        doc = Doc(self.session, id=hp_id)
        query = Query("query_term", "doc_id")
        query.where("path = '/Summary/PatientVersionOf/@cdr:ref'")
        query.where(query.Condition("int_val", doc.id))
        row = query.execute(self.cursor).fetchone()
        if not row:
            message = "No patient summary found for {}".format(hp_id)
            raise Exception(message)
        body = self.__start_body()
        patient_id = Doc.normalize_id(row.doc_id)
        etree.SubElement(body, "PatientSummary").text = patient_id
        return body

    def _person_address_fragment(self):
        """
        Fetch the details for an address identified by fragment ID

        Parameters:
          Link - string in the form CDR0000999999#XXX, representing
                 the unique ID of a CDR document, and the ID for
                 a node withing that document's subtree (required)

        Return:
          XML document node with the following structure:
            ReportBody
              [sequence of contact information elements]
        """

        link = self.__opts.get("Link")
        if not link:
            raise Exception("Missing required 'Link' parameter")
        try:
            doc_id, frag_id = link.split("#", 1)
        except:
            raise Exception("Link parameter must include fragment ID")
        parms = dict(fragId=etree.XSLT.strparam(frag_id))
        doc = Doc(self.session, id=doc_id)
        result = doc.filter("name:Person Address Fragment", parms=parms)
        return result.result_tree.getroot()

    def _person_locations_picklist(self):
        """
        Support picking a Person address to link to

        Parameters:
          DocId - unique identifier of the Person document to link to
                  (required)
          PrivatePracticeOnly - if "no" (the default) picklist includes
                                other practice locations (in addition to
                                private practice locations); otherwise
                                only private practice locations are included

        Return:
          XML document node with the following structure:
            ReportBody
              ReportName
              ReportRow*
                Link
                Data
        """

        doc_id = self.__opts.get("DocId")
        if not doc_id:
            raise Exception("Missing required 'DocId' parameter")
        doc = Doc(self.session, id=doc_id)
        private_practice_only = self.__opts.get("PrivatePracticeOnly", "no")
        parms = dict(
            docId=etree.XSLT.strparam(str(doc.id)),
            privatePracticeOnly=etree.XSLT.strparam(private_practice_only),
            repName=etree.XSLT.strparam(self.name)
        )
        filter_name = "Person Locations Picklist"
        result = doc.filter("name:"+filter_name, parms=parms)
        return result.result_tree.getroot()

    def _term_sets(self):
        """
        Collect named sets of CDR terms

        Parameters:
          SetType - optional string restricting report to term sets
                    whose type name matches the pattern passed
                    (any wildcards in the pattern must be included,
                    the report code does not wrap the string in a
                    pair of wildcards); as of this writing, this
                    parameter is of limited use, as all of the
                    existing term sets have the set type of
                    "diagnosis macro"

        Return:
          XML document node with the following structure:
            ReportBody
              ReportName
              TermSet*
                Name
                Members [string of CDR term IDs]
        """

        query = Query("doc_version v", "v.id", "MAX(v.num) AS num")
        query.join("query_term_pub t", "t.doc_id = v.id")
        query.where("t.path = '/TermSet/TermSetType'")
        query.where("v.publishable = 'Y'")
        query.group("v.id")
        pattern = self.__opts.get("SetType")
        if pattern:
            query.where(query.Condition("t.value", pattern, "LIKE"))
        body = self.__start_body()
        for id, num in query.execute(self.cursor).fetchall():
            doc = Doc(self.session, id=id, version=num)
            result = doc.filter("name:Get TermSet Name and Members")
            body.append(result.result_tree.getroot())
        return body

    def _translated_summary(self):
        """
        Find corresponding translation of English summary

        Parameters:
          EnglishSummary - required CDR ID for the document for which
                           we want to find the Spanish translation

        Return:
          XML document node with the following structure:
            ReportBody
              ReportName
              TranslatedSummary
        """

        english_id = self.__opts.get("EnglishSummary")
        if not english_id:
            raise Exception("Missing required 'EnglishSummary' parameter")
        doc = Doc(self.session, id=english_id)
        query = Query("query_term", "doc_id")
        query.where("path = '/Summary/TranslationOf/@cdr:ref'")
        query.where(query.Condition("int_val", doc.id))
        row = query.execute(self.cursor).fetchone()
        if not row:
            message = "No translated summary found for {}".format(english_id)
            raise Exception(message)
        body = self.__start_body()
        spanish_id = Doc.normalize_id(row.doc_id)
        etree.SubElement(body, "PatientSummary").text = spanish_id
        return body

    def _values_for_path(self):
        """
        Find values located in document at specified path

        Parameters:
          DocId - required string indicating which document to look in
          Path - required string for location of values to be returned
          Pub - if present (regardless of value) look for values in
                the latest publishable version of the document

        Return:
          XML document node with the following structure:
            ReportBody
              ReportName
              Value*
                @Loc
        """

        doc_id = self.__opts.get("DocId")
        if not doc_id:
            raise Exception("Missing required 'DocId' parameter")
        path = self.__opts.get("Path")
        if not doc_id:
            raise Exception("Missing required 'Path' parameter")
        table = "query_term_pub" if "Pub" in self.__opts else "query_term"
        query = Query(table, "value", "node_loc").order("node_loc")
        query.where(query.Condition("doc_id", Doc(self.session, id=doc_id).id))
        query.where(query.Condition("path", path))
        body = self.__start_body()
        for value, loc in query.execute(self.cursor).fetchall():
            etree.SubElement(body, "Value", Loc=loc).text = value
        return body
