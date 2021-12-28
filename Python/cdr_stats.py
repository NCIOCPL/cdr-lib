"""
Management report to list a variety of counts (typically run for the
previous month) regarding the number of documents published, updated, etc.
See https://tracker.nci.nih.gov/browse/OCECDR-3478 for original requirements
for this report.
"""

# Standard library modules
import datetime


# Local modules
import cdr
from cdrapi import db


class DocBase:
    """
    Default functionality for objects representing a single CDR document.
    Customized as appropriate by derived classes for each of the entities
    represented in the report (sometimes with more than one derived class
    for a single CDR document type, representing different aspects of the
    documents).
    """

    def __init__(self, control, values):
        """
        Save a reference to the control object for later use and
        capture the common attributes shared by most document objects.

        control     reference to object controlling behavior for this run
        values      dictionary of values representing a single row in
                    the result set from a database query
        """
        self.control = control
        self.id = values.get("id")
        self.title = values.get("title")

    def row(self, *extra):
        """
        Contruct an object representing an HTML table row. The first
        column contains the document ID, if display of document IDs
        has been requested for the report. The next column contains
        the document's title. Any additional values passed by the
        caller are also included.

        extra    possibly empty sequence of string values to be included
                 in the HTML table row
        """

        cols = []
        if self.control.ids:
            cols = [Control.td(cdr.normalize(self.id))]
        cols.append(Control.td(self.title))
        return Control.B.TR(*(cols + [Control.td(val) for val in extra]))

    def __lt__(self, other):
        """
        Support sorting the rows in a table of CDR documents. Sort by
        document ID if the IDs are included in the table; otherwise
        sort by title.
        2016-08-25: Robin decided to change the logic so even when
        the ID column is present, the rows are sorted by the titles.

        other    document object to be compared with this one
        """

        return self.title.lower() < other.title.lower()

    @staticmethod
    def fix_date_time(dt):
        """
        Replace NULL dates with and empty string, and strip off any fractions
        of a second.

        dt      date/time value to be normalized
        """

        if not dt:
            return ""
        return str(dt).split(".")[0]


class Section:
    """
    Default behavior for a section of the report.

    NEW_ONLY    If true, we modify captions and labels to indicate that
                all of the counts/documents are new
    HEADERS     Additional table headers to be added to those for the
                document ID and title
    cursor      singleton database cursor used by all derived Section
                class objects
    """

    NEW_ONLY = False
    HEADERS = []
    cursor = db.connect(user="CdrGuest").cursor()

    def show_counts(self):
        """
        Add a row to the table for report counts. Overridden by
        the derived classes for all but the simplest sections.
        """

        new = self.NEW_ONLY and " - New" or ""
        return [self.count_row("%s%s" % (self.TITLE, new), len(self.docs))]

    def list_docs(self):
        """
        Create a table showing all of the documents for the report section.
        Overridden by the derived classes for most section types.
        """

        if not self.docs:
            return [self.no_docs()]
        new = self.NEW_ONLY and "new " or ""
        caption = "%d %s%s" % (len(self.docs), new, self.TITLE)
        headers = self.headers(*self.HEADERS)
        rows = [doc.row() for doc in sorted(self.docs)]
        return [self.table(caption, headers, rows)]

    def no_docs(self, what=None):
        """
        Supply a small paragraph indicating that there are no documents
        to be show for the section (replacing the table that would appear
        if there were documents).

        what   string overriding the default identification of the type
               of documents represented by the section
        """
        what = what or ("%s%s" % (self.NEW_ONLY and "new " or "", self.TITLE))
        style = "font-family: Arial; margin-top: 30px; font-style: italic;"
        return Control.B.P("No %s found during this time" % what, style=style)

    def headers(self, *extra):
        """
        Assemble the list column headers, taking into account whether the
        report is supposed to be showing document IDs in the first column.

        extra   possibly empty sequence of header strings to be appended
                to the default headers for CDR ID and Title
        """

        headers = []
        if self.control.ids:
            headers = ["CDR ID"]
        headers.append("Title")
        return headers + list(extra)

    def table(self, caption, headers, rows):
        """
        Assemble an object representing an HTML table. Truncate the number
        of rows if that has been requested. It would be more efficient
        to truncate the number of records further upstream, before creating
        the TR objects for the ones we won't display, but the code is cleaner
        and easier to understand and maintain if we do it here, and the
        processing is plenty fast (and the option to limit the number of
        rows displayed is rarely used).

        caption    title string for the table
        headers    sequence of strings for the column headers
        rows       sequence of objects representing HTML table rows
                   for the data, one row for each CDR document in
                   the section
        """

        if self.control.max_docs and len(rows) > self.control.max_docs:
            if self.control.max_docs == 1:
                caption += " (showing first row)"
            else:
                caption += " (showing first %d rows)" % self.control.max_docs
            rows = rows[:self.control.max_docs]
        return Control.B.TABLE(
            Control.caption(caption),
            Control.B.TR(*[Control.th(h) for h in headers]),
            *rows,
            style=Control.TSTYLE
        )

    @classmethod
    def execute(cls, query):
        """
        Build a sequences of dictionaries from the query results set

        Pass:
          query - db.Query object

        Return:
          sequence of dictionaries of column name -> row value for column
        """

        rows = query.execute(cls.cursor).fetchall()
        cols = [col[0] for col in cls.cursor.description]
        return [dict(zip(cols, row)) for row in rows]

    @staticmethod
    def query(*cols):
        """
        Create an object for database query to select documents which
        are active and published. The query will be refined and executed
        by the derived classes as appropriate. We select from the document
        view and explicity check the active_status column, rather than
        selecting from the active_doc view, which does that check for us,
        because many of the queries will need the first_pub column, and
        that column isn't exposed by the active_doc view.

        cols    sequence of specifiers for the columns to be retrieved
                by the query
        """

        query = db.Query("document d", *cols)
        query.where("d.active_status = 'A'")
        query.join("pub_proc_cg c", "c.id = d.id")
        return query

    @staticmethod
    def count_row(label="\xa0", count=None):
        """
        Create an object representing a two-column HTML table row, showing
        classes of documents in the first column and a count in the
        second column.

        label    string identifying what is being counted; defaults to
                 a non-breaking space so that empty separator rows do
                 not have their height collapsed
        count    optional integer representing the number of things
                 (documents, revised definitions, etc.) being counted
        """

        if count is None:
            count = ""
        return Control.B.TR(
            Control.td(label),
            Control.td(str(count))
        )

    class Doc(DocBase):
        """
        Each section has at least one nested class for its documents.
        For the simplest sections, the default base class is sufficient.
        """

        pass


class Summary(Section):
    """
    Section which counts new and modified CDR Summary documents.

    HEADERS    additional column headers added to the document table
    ABBR       key used for requesting the inclusion of this section
    AUDIENCES  map of audience values to display abbreviations
    LANGUAGES  map of language values to display abbreviations
    """

    HEADERS = ("Language", "Audience")
    ABBR = "summary"
    AUDIENCES = {"Health professionals": "HP", "Patients": "Pat."}
    LANGUAGES = {"English": "EN", "Spanish": "ES"}

    def __init__(self, control):
        """
        Fetch the new and modified summaries using separate queries.
        Note that it is possible for the same summary to be included
        as both new and modified (this happens if a summary is first
        published in the date range for the report, and subsequently
        modified during the date range).

        control   reference to object controlling behavior for this run
        """

        self.control = control
        self.new = self.fetch_new(control)
        self.mod = self.fetch_mod(control)

    def fetch_new(self, control):
        """
        Use the first_pub column of the document view to find summaries
        published for the first time during the report's date range.
        """

        query = self.query()
        query.where(control.date_check("d.first_pub"))
        control.logger.debug("new summary query:\n%s", query)
        return [self.Doc(control, row) for row in self.execute(query)]

    def fetch_mod(self, control):
        """
        Use the summary's DateLastModified element to find summaries
        last modified during the report's date range. Note that if you
        run the report for an older time period, you won't pick up all
        of the summaries which were modified during that period, because
        the DateLastModified value will have been overwritten with a later
        date for summaries modified after the date range.
        """

        query = self.query()
        query.join("query_term_pub m", "m.doc_id = d.id",
                   "m.path = '/Summary/DateLastModified'")
        query.where(control.date_check("m.value"))
        control.logger.debug("new summary query:\n%s", query)
        return [self.Doc(control, row) for row in self.execute(query)]

    def query(self):
        """
        Factor out the common logic for a query to find new or
        modified CDR Summary documents. The query is refined by the
        caller before execution to narrow the results to the target.
        """

        # Create the query object.
        cols = ("d.id", "t.value AS title", "l.value AS language",
                "a.value AS audience")
        query = Section.query(*cols)

        # Add the join for the Summary title.
        query.join("query_term_pub t", "t.doc_id = d.id")
        query.where("t.path = '/Summary/SummaryTitle'")

        # Add the join for the Summary audience.
        query.join("query_term_pub a", "a.doc_id = d.id")
        query.where("a.path = '/Summary/SummaryMetaData/SummaryAudience'")

        # Add the join for the Summary language.
        query.join("query_term_pub l", "l.doc_id = d.id")
        query.where("l.path = '/Summary/SummaryMetaData/SummaryLanguage'")

        # Let the caller add the final tweaks before execution.
        return query

    def show_counts(self):
        """
        Add the two blocks to the top report table, one block for new
        Summaries, and a second block for revised Summaries.
        """

        rows = self._show_counts(self.new, "New")
        rows.append(self.count_row())
        return rows + self._show_counts(self.mod, "Revised")

    def _show_counts(self, docs, label):
        """
        Factor out the common logic to show separate counts for Summaries
        broken out for each audience/language combination. Invoked once
        for new Summaries, and a second time for revised Summaries.

        docs    sequence of objects for the block's documents
        label   string display in a separate row above the actual
                count rows, identifying which block this is (new
                or revised)
        """

        # Show the row identifying which block this is.
        rows = [self.count_row("Total Summaries - %s" % label, len(docs))]

        # Loop through the document objects to roll up the counts.
        counts = {
            "HP": {"EN": 0, "ES": 0},
            "Pat.": {"EN": 0, "ES": 0}
        }
        for doc in docs:
            audience = self.AUDIENCES.get(doc.audience)
            language = self.LANGUAGES.get(doc.language)
            if audience and language:
                counts[audience][language] += 1

        # Add the actual count rows and return the results.
        for audience in ("HP", "Pat."):
            for language in ("EN", "ES"):
                label = " - %s Summaries (%s)" % (audience, language)
                rows.append(self.count_row(label, counts[audience][language]))
        return rows

    def list_docs(self):
        """
        Add two tables to the report, one for new Summaries, and a second
        for revised Summaries.
        """

        return (self._list_docs(self.new, "New") +
                self._list_docs(self.mod, "Revised"))

    def _list_docs(self, docs, label):
        """
        Factor out the common logic for creating a table of Summary
        documents (called twice, once for new Summaries, and then
        again for the revised Summaries).

        docs    sequence of objects for the table's documents
        label   string used for the table's caption element
        """

        if not docs:
            return [self.no_docs("%s Summaries" % label)]
        caption = "%d %s Summaries" % (len(docs), label)
        headers = self.headers(*self.HEADERS)
        rows = [doc.row() for doc in sorted(docs)]
        return [self.table(caption, headers, rows)]

    class Doc(DocBase):
        """
        Customized class for Summary documents, which need to
        know what language and audience each belongs to.
        """

        def __init__(self, control, values):
            """
            Invoke the base class constructor and then pick up the
            Summary-specific values.

            control   reference to object controlling behavior for this run
            values    dictionary of values representing a single row in
                      the result set from a database query
            """

            DocBase.__init__(self, control, values)
            self.language = values.get("language")
            self.audience = values.get("audience")

        def row(self):
            """
            Invoke the base class method for constructing an object
            for an HTML table row, passing in the extra values
            needed for the Summary tables.
            """

            return DocBase.row(self, self.language, self.audience)


class Glossary(Section):
    """
    Represents the statistics on dictionary entities. Includes separate
    reporting for the general dictionary entries and for genetics-
    specific terms, as well as documents for pronunciation of the
    term names. This is by far the most complex section of the
    report, in terms of tricky logic.

    There are five sets of documents needed for this section, two for
    new GlossaryTermName documents (one of these for term name documents
    linked to concept documents with new genetics definitions), and two
    for GlossaryTermConcept documents with new definitions (one of these
    specifically for concept documents with new genetics definitions),
    and a fifth group for new documents for audio pronunciations of
    glossary term names.

    ABBR     key used for requesting the inclusion of this section
    DLM      path used in database queries to get the DateLastModified values
    CONCEPT  path used for linking a GlossaryTermName document to the
             GlossaryTermConcept document to which it belongs
    """

    ABBR = "glossary"
    DLM = "/GlossaryTermConcept/%TermDefinition/DateLastModified"
    CONCEPT = "/GlossaryTermName/GlossaryTermConcept/@cdr:ref"

    def __init__(self, control):
        """
        Assemble the five sets of documents needed for this section of
        the report. Honor option to show new glossary pronunciation
        audio without the rest of this section, if so specified.

        control   reference to object controlling behavior for this run
        """

        self.control = control
        self.audio = self.fetch_audio(control)
        if control.audio_alone():
            return
        self.new_terms = self.fetch_new_terms(control)
        self.mod_concepts = self.fetch_mod_concepts(control)
        self.new_genetics_terms = self.fetch_new_genetics_terms(control)
        self.mod_genetics_concepts = self.fetch_mod_genetics_concepts(control)

    def show_counts(self):
        """
        Assemble rows for the counts table, representing new dictionary
        term documents and revised definitions, as well as new Media
        documents for glossary term name pronunciations.  Show just
        the audio counts if so requested.
        """

        if self.control.audio_alone():
            return [self.count_row("Audio - New", len(self.audio))]
        return [
            self.count_row("Dictionary - New Terms"),
            self.count_row("- New (EN)", len(self.new_terms)),
            self.count_row("- New (ES)", self.NewTerm.es_count),
            self.count_row("Dictionary - Revised Definitions"),
            self.count_row("- Revised (EN)", self.RevisedConcept.rev_en_defs),
            self.count_row("- Revised (ES)", self.RevisedConcept.rev_es_defs),
            self.count_row("Audio - New", len(self.audio)),
            self.count_row(),
            self.count_row("Genetics Dictionary - New Terms",
                           len(self.new_genetics_terms)),
            self.count_row("Genetics Dictionary - Revised Definitions",
                           len(self.mod_genetics_concepts))
        ]

    def list_docs(self):
        """
        Display the five tables showing GlossaryTermName, Media,
        and GlossaryTermConcept documents tracked by this report.
        Show just the new pronunciation documents if so requested.
        """

        if self.control.audio_alone():
            return self._list_docs("New Dictionary w/ pronunciation (Audio)",
                                   self.audio)
        return (
            self._list_docs("New Dictionary Terms", self.new_terms, True) +
            self._list_docs("GTCs with Revised Dictionary Definitions",
                            self.mod_concepts, True) +
            self._list_docs("New Dictionary w/ pronunciation (Audio)",
                            self.audio) +
            self._list_docs("New Genetics Dictionary Terms",
                            self.new_genetics_terms) +
            self._list_docs("Revised Genetics Dictionary Definitions",
                            self.mod_genetics_concepts)
        )

    def _list_docs(self, label, docs, spanish=False):
        """
        Factor out the common logic for creating each of the five
        tables in this section of the report.
        """

        if not docs:
            return [self.no_docs(label)]
        languages = spanish and " (EN and ES)" or ""
        caption = "%d %s%s" % (len(docs), label, languages)
        headers = self.headers(*(spanish and ["Title (Spanish)"] or []))
        rows = [doc.row() for doc in sorted(docs)]
        return [self.table(caption, headers, rows)]

    def fetch_new_terms(self, control):
        """
        Find all of the active GlossaryTermName documents published for
        the first time during the date range for this report.
        """

        query = Section.query("d.id", "n.value AS title")
        query.join("query_term_pub n", "n.doc_id = d.id")
        query.where("n.path = '/GlossaryTermName/TermName/TermNameString'")
        query.where(control.date_check("d.first_pub"))
        control.logger.debug("new glossary term query:\n%s", query)
        return [self.NewTerm(control, v) for v in self.execute(query)]

    def fetch_mod_concepts(self, control):
        """
        Find all of the GlossaryTermConcept documents containing at
        least one definition which was modified during the date range
        for this report (using the DateLastModified element of the
        individual definition blocks).
        """

        query = db.Query("active_doc d", "d.id").unique()
        query.join("query_term_pub m", "m.doc_id = d.id")
        query.where("m.path LIKE '%s'" % self.DLM)
        query.where(control.date_check("m.value"))
        control.logger.debug("mod concepts query:\n%s", query)
        rows = self.execute(query)
        return [self.RevisedConcept(control, row) for row in rows]

    def fetch_new_genetics_terms(self, control):
        """
        Try to find all of the GlossaryTermName documents linked to
        a GlossaryTermConcept with a new genetics definition.  There
        is no perfectly accurate way to do this, because we don't
        track either a creation or a first publishing date for the
        definitions. We have the date the definition was last modified
        (which is not present when the definition is first created)
        and a definition status date, set when the definition is
        created, but overwritten when the definition is updated.
        So we only know when a definition was created until it gets
        updated. Therefore for definitions created and then
        subsequently modified during the date range of the report,
        we do not refect their linked term name documents in this
        document set, but in the set of modified genetics concept
        documents collected below.
        """

        # Create the query.
        query = Section.query("d.id", "n.value AS title")

        # Add the join to get the name for the term.
        query.join("query_term_pub n", "n.doc_id = d.id")
        query.where("n.path = '/GlossaryTermName/TermName/TermNameString'")

        # Add the join to find the document's GlossaryTermConcept doc.
        query.join("query_term_pub gc", "gc.doc_id = d.id")
        query.where("gc.path = '%s'" % self.CONCEPT)

        # Make sure the definition is for the genetics dictionary.
        self.join_genetics_definition(query, "gc.int_val", True)

        # Make sure the StatusDate is in the date range and the
        # DateLastModifiedhas not been set yet.
        query.where("m.value IS NULL")
        query.where(control.date_check("s.value"))

        # Construct and return the sequence of objects for the documents.
        control.logger.debug("new genetics terms query:\n%s", query)
        return [DocBase(control, row) for row in self.execute(query)]

    def fetch_mod_genetics_concepts(self, control):
        """
        Find all of the GlossaryTermConcept documents with a genetics
        definition which was modified during the report's date range.
        """

        # Create the query.
        query = db.Query("active_doc d", "d.id").unique()

        # Make sure the definition is for the genetics dictionary.
        self.join_genetics_definition(query, "d.id")

        # Make sure the genetics definition was last modified during
        # the report's date range.
        query.where(control.date_check("m.value"))

        # Construct and return the sequence of objects for the documents.
        control.logger.debug("mod genetics concepts query:\n%s", query)
        rows = self.execute(query)
        return [self.RevisedConcept(control, row, True) for row in rows]

    def join_genetics_definition(self, query, id_column, status_date=False):
        """
        Factor out the common logic to make sure we get concept
        documents with genetics definitions, and join to the
        date(s) we need to determine whether the definitions
        are in scope for the report.

        query        database query object to be refined
        id_column    column spec used to join the query_term_pub rows
                     for our concepts (this column holds the document
                     ID for the concept)
        status_date  if True, we add a join to pick up the StatusDate
                     values for the definitions (only needed for
                     finding the new genetics definitions, not the
                     modified definitions)
        """

        # Path for a single English definition.
        d_path = "/GlossaryTermConcept/TermDefinition"

        # Make sure the definition is for the Genetics dictionary.
        query.join("query_term_pub g", "g.doc_id = %s" % id_column)
        query.where("g.path = '%s/Dictionary'" % d_path)
        query.where("g.value = 'Genetics'")

        # Make sure the audience is health professionals.
        query.join("query_term_pub a", "a.doc_id = g.doc_id",
                   "LEFT(a.node_loc, 4) = LEFT(g.node_loc, 4)")
        query.where("a.path = '%s/Audience'" % d_path)
        query.where("a.value = 'Health professional'")

        # Find out when the definition was modified (if ever).
        query.outer("query_term_pub m", "m.doc_id = g.doc_id",
                    "LEFT(m.node_loc, 4) = LEFT(g.node_loc, 4)",
                    "m.path = '%s/DateLastModified'" % d_path)

        # Optionally get the StatusDate element for the definition.
        if status_date:
            query.outer("query_term_pub s", "s.doc_id = g.doc_id",
                        "LEFT(s.node_loc, 4) = LEFT(g.node_loc, 4)",
                        "s.path = '%s/StatusDate'" % d_path)

    def fetch_audio(self, control):
        """
        Find all of the pronunciation documents first published
        during the report's date range.
        """

        # Create the query.
        query = Section.query("d.id", "t.value AS title")

        # Get the name of the document.
        query.join("query_term t", "t.doc_id = d.id")
        query.where("t.path = '/Media/MediaTitle'")

        # Make sure it's a pronunciation document.
        query.join("query_term p", "p.doc_id = d.id")
        query.where("p.path = '/Media/MediaContent/Categories/Category'")
        query.where("p.value = 'pronunciation'")

        # Filter down to the ones published in our date range.
        query.where(control.date_check("d.first_pub"))

        # Construct and return the sequence of objects for the documents.
        control.logger.debug("audio query:\n%s", query)
        return [DocBase(control, row) for row in self.execute(query)]

    class NewTerm(DocBase):
        """
        Objects representing the active QueryTermName documents first
        published during the date range for the report.

        es_count    class-level count cumulating the number of
                    translated names found in the documents
        PATH        query_term path used to find the translated names
        """

        es_count = 0
        PATH = "/GlossaryTermName/TranslatedName/TermNameString"

        def __init__(self, control, values):
            """
            Use the base class constructor to capture the control object,
            the term name document ID, and the English name, which we
            use as the title. Then collect all of the translated name
            strings, remember them, and update the cumulative count
            of Spanish names for the new dictionary terms.

            control   reference to object controlling behavior for this run
            values    dictionary of values representing a single row in
                      the result set from a database query
            """

            # Save the base values.
            DocBase.__init__(self, control, values)

            # Create the database query to find the Spanish name strings.
            query = db.Query("query_term_pub", "value")
            query.where(query.Condition("doc_id", self.id))
            query.where(query.Condition("path", self.PATH))

            # Store the Spanish names.
            self.spanish_names = [r[0] for r in query.execute(control.cursor)]

            # Update the total count of Spanish names in the new term docs.
            Glossary.NewTerm.es_count += len(self.spanish_names)

        def row(self):
            """
            Override the base class row() method to add a column for
            the concatenated Spanish names.
            """

            return DocBase.row(self, "; ".join(self.spanish_names))

    class RevisedConcept(DocBase):
        """
        GlossaryTermConcept documents containing a definition of
        interest which was last modified during this report's date
        range. In this context, "of interest" means genetics
        definitions if the `genetics' parameter is True; otherwise,
        we are interested in any definition.

        rev_en_defs   class-level variable counting revised English
                      definitions
        rev_es_defs   class-level variable counting revised Spanish
                      definitions
        """

        rev_en_defs = 0
        rev_es_defs = 0

        def __init__(self, control, values, genetics=False):
            """
            Save the concept document ID, a reference to the report
            control object, and the `genetics' flag. Collect all of
            the names for the concept. If the `genetics' flag is
            False, count the revised English and Spanish definitions
            (each language counted separately).

            control   reference to object controlling behavior for this run
            values    dictionary of values representing a single row in
                      the result set from a database query
            genetics  if True, we don't need to count revised definitions,
                      because a genetics dictionary entry will have
                      exactly one English genetics definition
            """

            self.id = values.get("id")
            self.control = control
            self.genetics = genetics
            self.get_names(control.logger)
            if not genetics:
                self.count_revised_definitions(control)

        def row(self):
            """
            Override the base class row() method so we can add a column
            for the Spanish names if we're not in the Genetics Definition
            block.
            """

            extra = []
            if not self.genetics:
                extra = ["; ".join(self.es_names)]
            return DocBase.row(self, *extra)

        def get_names(self, logger):
            """
            Collect the English names (and optionally the Spanish names
            as well) for the concept.
            """

            # Create the query to find the GlossaryTermName documents.
            query = db.Query("query_term_pub n", "n.value", "n.path")

            # Link those documents to the concept document.
            query.join("query_term_pub c", "c.doc_id = n.doc_id")
            query.where("c.path = '%s'" % Glossary.CONCEPT)
            query.where("c.int_val = %d" % self.id)

            # The Genetics dictionary doesn't use Spanish names,
            # so just get the English name strings.
            if self.genetics:
                n_path = "/GlossaryTermName/TermName/TermNameString"
                query.where("n.path = '%s'" % n_path)

            # For the general dictionary block, use a path pattern
            # which will pick up both English and Spanish names.
            else:
                n_pattern = "/GlossaryTermName/T%Name/TermNameString"
                query.where("n.path LIKE '%s'" % n_pattern)

            # Collect the names, using the path to detect language.
            logger.debug("concept term names query:\n%s", query)
            self.en_names = []
            self.es_names = []
            for values in query.execute(Section.cursor):
                name = values.value
                if "translated" in values.path.lower():
                    self.es_names.append(name)
                else:
                    self.en_names.append(name)

            # Set the title attribute to the concatenated English names,
            # since that's where DocBase.row() expects to find the second
            # column's value.
            self.title = "; ".join(self.en_names)

        def count_revised_definitions(self, control):
            """
            Count the English and Spanish definitions which have
            been modified during the report's date range.

            control   reference to object controlling behavior for this run;
                      used for adding the date range condition to the
                      query
            """

            # Select the definitions' DateLastModified values.
            query = db.Query("query_term_pub", "path")
            query.where("doc_id = %d" % self.id)
            query.where("path LIKE '%s'" % Glossary.DLM)

            # Filter down to the ones modified during our date range.
            query.where(control.date_check("value"))

            # Use the path name to distinguish between English and Spanish.
            control.logger.debug("revised definitions query:\n%s", query)
            for row in query.execute(Section.cursor):
                if "translated" in row.path.lower():
                    Glossary.RevisedConcept.rev_es_defs += 1
                else:
                    Glossary.RevisedConcept.rev_en_defs += 1


class DrugInformationSummary(Section):
    """
    Collections information about the CDR DrugInformationSummary documents.

    HEADERS    additional column headers added to the document table
    ABBR       key used for requesting the inclusion of this section
    new_count  class-level count cumulating the number of new documents
    mod_count  class-level count cumulating the number of revised documents
    """

    HEADERS = ("First Pub", "Date Modified")
    TITLE = "Drug Information Summaries"
    ABBR = "dis"
    new_count = 0
    mod_count = 0

    def __init__(self, control):
        """
        Save the control object and collect the DrugInformationSummary (DIS)
        documents which were either first published or last modified at some
        point during the report's date range. Note that (unlike the genetics
        definitions counting) a DIS document can be counted both as new and
        as modified during the date range.

        control   reference to object controlling behavior for this run
        """

        # Save a reference to the report control object.
        self.control = control

        # Create a query object for finding the DIS documents.
        cols = ("d.id", "t.value AS title", "d.first_pub", "m.value AS dlm")
        query = Section.query(*cols)

        # Add a join to get the title for the document.
        query.join("query_term_pub t", "t.doc_id = d.id")
        query.where("t.path = '/DrugInformationSummary/Title'")

        # Add a join to get the optional DateLastModified value for the doc.
        query.outer("query_term_pub m", "m.doc_id = d.id",
                    "m.path = '/DrugInformationSummary/DateLastModified'")

        # Add a condition to accept documents which fulfill either or
        # both of the following two conditions:
        #  1. the document was first published in our date range
        #  2. the document was last modified in our date range
        first_pub_ok = control.date_check("d.first_pub")
        dlm_ok = control.date_check("m.value")
        query.where(query.Or(first_pub_ok, dlm_ok))

        # Fetch the documents and save them.
        control.logger.debug("drug information summaries query:\n%s", query)
        self.docs = [self.Doc(control, v) for v in self.execute(query)]

    def show_counts(self):
        """
        Show totals for the new and revised documents separately. Note
        that there can be some overlap (same documents reflected in
        both counts).
        """

        return [
            self.count_row(self.TITLE),
            self.count_row(" - New", self.new_count),
            self.count_row(" - Revised", self.mod_count)
        ]

    class Doc(DocBase):
        """
        DIS document object, aware of when the document was first published
        and last modified.
        """
        def __init__(self, control, values):
            """
            Capture the id, title, date first published, and date last
            modified, and save a reference to the report's control object.
            Update the cumulative counts for new and updated DIS documents.

            control   reference to object controlling behavior for this run
            values    dictionary of values representing a single row in
                      the result set from a database query
            """

            DocBase.__init__(self, control, values)
            self.dlm = values.get("dlm")
            self.first_pub = values.get("first_pub")
            if control.check_date(self.first_pub):
                DrugInformationSummary.new_count += 1
            if control.check_date(self.dlm):
                DrugInformationSummary.mod_count += 1

        def row(self):
            """
            Override the base class row() method to add columns for the
            date of first publication and date the document was last
            modified (if any).
            """

            first_pub = self.fix_date_time(self.first_pub)
            last_mod = self.fix_date_time(self.dlm)
            return DocBase.row(self, first_pub, last_mod)


class GeneticsProfessional(Section):
    """
    Reports on newly published Person documents included in the
    genetics professional (GP) directory. The default `Section' behaviors
    are appropriate for this section, so all we have to take care
    of is the document selection logic.

    TITLE      the string used to identify the section of the report
    ABBR       key used for requesting the inclusion of this section
    NEW_ONLY   mark this section as reflection only new documents
    """

    TITLE = "Genetics Professionals"
    ABBR = "genetics"
    NEW_ONLY = True

    def __init__(self, control):
        """
        Find all of the newly published genetics professional documents.

        control   reference to object controlling behavior for this run
        """

        # Save a reference to the report's control object.
        self.control = control

        # Create the query object.
        query = Section.query("d.id", "d.title")

        # Make sure the Person document has an `Active' status.
        query.join("query_term_pub s", "s.doc_id = d.id")
        query.where("s.path = '/Person/Status/CurrentStatus'")
        query.where("s.value = 'Active'")

        # Make sure the person has indicated (s)he wants to be included
        # in the directory of genetics professionals.
        query.join("query_term_pub i", "i.doc_id = d.id")
        query.where("i.path LIKE '/Person/Professional%/Directory/Include'")
        query.where("i.value = 'Include'")

        # Filter the documents to only include those first published
        # some time within the report's date range.
        query.where(control.date_check("d.first_pub"))

        # Fetch and save the sequence of GP documents.
        control.logger.debug("genetics professionals query:\n%s", query)
        self.docs = [self.Doc(control, v) for v in self.execute(query)]


class Drug(Section):
    """
    Reports on newly published CDR Term documents with a semantic type
    of `Drug/agent'.  The default `Section' behaviors are appropriate
    for this section, so all we have to take care of is the document
    selection logic.

    TITLE      the string used to identify the section of the report
    ABBR       key used for requesting the inclusion of this section
    NEW_ONLY   mark this section as reflection only new documents
    """

    TITLE = "NCI Drug Terms"
    ABBR = "drug"
    NEW_ONLY = True

    def __init__(self, control):
        """
        Find all of the NCI Drug Term documents.

        control   reference to object controlling behavior for this run
        """

        # Save a reference to the report's control object.
        self.control = control

        # Create the database query object for selecting the documents.
        query = Section.query("d.id", "n.value AS title")

        # Add a join to get the drug term's name.
        query.join("query_term_pub n", "n.doc_id = d.id")
        query.where("n.path = '/Term/PreferredName'")

        # Narrow the selection to term with semantic type of Drug/agent.
        query.join("query_term_pub s", "s.doc_id = d.id")
        query.where("s.path = '/Term/SemanticType/@cdr:ref'")
        query.join("query_term_pub t", "t.doc_id = s.int_val")
        query.where("t.value = 'Drug/agent'")

        # Filter the set down to document first published within
        # the report's date range.
        query.where(control.date_check("d.first_pub"))

        # Fetch and save the sequence of NCI Drug Term documents.
        control.logger.debug("drug term query:\n%s", query)
        self.docs = [self.Doc(control, v) for v in self.execute(query)]


class BoardMember(Section):
    """
    Collection of new PDQ board members whose term starts at some time
    during the date range for the report. The default behavior for
    showing the table of individual records is appropriate for this
    section, so we just have to supply the constructor and the custom
    code for displaying separate counts for editorial and advisory
    board members.

    TITLE            the string used to identify the section of the report
    ABBR             key used for requesting the inclusion of this section
    NEW_ONLY         mark this section as reflection only new documents
    HEADERS          additional column headers added to the document table
    DETAILS          path to the BoardMembershipDetails block
    BOARD_NAME       path to the element containing the name of the board
                     of which this individual is a member
    INVITATION       path to the element containing the date the member was
                     invited to join this board
    TERM_START       path to the element containing the date the current
                     membership term began
    RESPONSE         path to the element indicating whether the invitation
                     was accepted
    editorial_count  cumulative count of new editorial board members
    advisory_count   cumulative count of new advisory board members
    """

    TITLE = "PDQ Board Members"
    ABBR = "boardmembers"
    NEW_ONLY = True
    HEADERS = ["Board"]
    DETAILS = "/PDQBoardMemberInfo/BoardMembershipDetails"
    BOARD_NAME = "%s/BoardName" % DETAILS
    INVITATION = "%s/InvitationDate" % DETAILS
    TERM_START = "%s/TermStartDate" % DETAILS
    RESPONSE = "%s/ResponseToInvitation" % DETAILS
    editorial_count = 0
    advisory_count = 0

    def __init__(self, control):
        """
        Find all of the new PDQ board members.

        control   reference to object controlling behavior for this run
        """

        # Save a reference to the report's control object.
        self.control = control

        # Tests to make sure we get all of the values from the
        # same membership details block.
        i_node_loc = "LEFT(i.node_loc, 4) = LEFT(b.node_loc, 4)"
        r_node_loc = "LEFT(r.node_loc, 4) = LEFT(b.node_loc, 4)"
        t_node_loc = "LEFT(t.node_loc, 4) = LEFT(b.node_loc, 4)"

        # Create the selection query.
        cols = ("d.id", "d.title", "b.value AS board")
        query = db.Query("active_doc d", *cols)

        # Add the join for the board's name.
        query.join("query_term b", "b.doc_id = d.id")
        query.where("b.path = '%s'" % self.BOARD_NAME)

        # Find out when the individual was first invited to join this board.
        # The only way to be sure the current term is the first term of
        # membership for this board is to make sure the invitation came
        # within the last half year.
        query.join("query_term i", "i.doc_id = d.id", i_node_loc)
        query.where("i.path = '%s'" % self.INVITATION)
        query.where("i.value >= DATEADD(MONTH, -6, '%s')" % control.start)

        # Make sure the individual accepted the invitation.
        query.join("query_term r", "r.doc_id = d.id", r_node_loc)
        query.where("r.path = '%s'" % self.RESPONSE)
        query.where("r.value = 'Accepted'")

        # See if the term start is within the report's date range.
        query.join("query_term t", "t.doc_id = d.id", t_node_loc)
        query.where("t.path = '%s'" % self.TERM_START)
        query.where(control.date_check("t.value"))

        # Fetch and save the PDQ board member documents.
        control.logger.debug("pdq board member query:\n%s", query)
        self.docs = [self.Doc(control, v) for v in self.execute(query)]

    def show_counts(self):
        """
        Show separate counts for editorial and advisory board members.
        """

        return [
            self.count_row("%s - New" % self.TITLE),
            self.count_row("- Editorial", self.editorial_count),
            self.count_row("- Advisory", self.advisory_count)
        ]

    class Doc(DocBase):
        """
        Information about a single PDQ board member.
        """

        def __init__(self, control, values):
            """
            Save a reference to the report's control object, the
            board member's name (with superflous suffix stripped
            away), and the name of the board. Update the cumulative
            counts for the two types of board (editorial and advisory),
            by examining the boards' names.

            control   reference to object controlling behavior for this run
            values    dictionary of values representing a single row in
                      the result set from a database query
            """

            DocBase.__init__(self, control, values)
            self.title = self.title.split(" (board mem")[0]
            self.board = values.get("board")
            if "editorial advisory board" in self.board.lower():
                BoardMember.advisory_count += 1
            else:
                BoardMember.editorial_count += 1

        def row(self):
            """
            Override the base class row() method to add a third column
            for the board's name.
            """

            return DocBase.row(self, self.board)


class BoardMeeting(Section):
    """
    Collection of PDQ board meetings which took place during the date
    range for the report. The default behavior for showing the table
    of individual records is appropriate for this section, so we just
    have to supply the constructor and the custom code for displaying
    separate counts for WebEx versus on-site meetings.

    TITLE          the string used to identify the section of the report
    ABBR           key used for requesting the inclusion of this section
    HEADERS        additional column headers added to the document table
    NAME           path to the element containing the name of the board
                   holding the meeting
    MEETING        path to the block of information for a single meeting
    DATE           path for the element containing the meeting's date
    WEBEX          path for the attribute indicating whether this meeting
                   was conducted via WebEx
    onsite_count   cumulative count of meetings held on site
    webex_count    cumulative count of meetings conducted via WebEx
    """

    TITLE = "PDQ Board Meetings"
    ABBR = "boardmeetings"
    HEADERS = ("Date", "WebEx?")
    NAME = "/Organization/OrganizationNameInformation/OfficialName/Name"
    MEETING = "/Organization/PDQBoardInformation/BoardMeetings/BoardMeeting"
    DATE = "%s/MeetingDate" % MEETING
    WEBEX = "%s/@WebEx" % DATE
    onsite_count = 0
    webex_count = 0

    def __init__(self, control):
        """
        Find all of the meetings held within the report's date range.

        control   reference to object controlling behavior for this run
        """

        # Save a reference to the report's control object.
        self.control = control

        # Start a new query for active documents.
        cols = ("d.id", "n.value AS title", "m.value AS date",
                "w.value AS webex")
        query = db.Query("active_doc d", *cols)

        # Add a join to get the PDQ board's name and to narrow down
        # the selection organization documents.
        query.join("query_term n", "n.doc_id = d.id")
        query.where("n.path = '%s'" % self.NAME)

        # Get the date and filter down to meetings held during our date range.
        query.join("query_term m", "m.doc_id = d.id")
        query.where("m.path = '%s'" % self.DATE)
        query.where(control.date_check("m.value"))

        # Find out whether the meeting was on site or WebEx.
        query.outer("query_term w", "w.doc_id = d.id",
                    "w.path = '%s'" % self.WEBEX,
                    "LEFT(w.node_loc, 12) = LEFT(m.node_loc, 12)")

        # Collect and save the meeting information.
        control.logger.debug("pdq board meeting query:\n%s", query)
        rows = self.execute(query)
        self.docs = [self.Meeting(control, row) for row in rows]

    def show_counts(self):
        """
        Show separate counts for on-site and WebEx meetings.
        """

        return [
            self.count_row(self.TITLE),
            self.count_row("- Onsite Meetings", self.onsite_count),
            self.count_row("- WebEx Meetings", self.webex_count)
        ]

    class Meeting(DocBase):
        """
        Object representing a PDQ board meeting. There can be
        multiple such instances drawn from a single Organization
        document.
        """

        def __init__(self, control, values):
            """
            Save the document ID, title, meeting date, WebEx flag,
            and a reference to the report's control object. Update
            the cumulative counts for each of the meeting types.

            control   reference to object controlling behavior for this run
            values    dictionary of values representing a single row in
                      the result set from a database query
            """

            DocBase.__init__(self, control, values)
            self.date = values.get("date")
            self.webex = values.get("webex")
            if self.webex:
                BoardMeeting.webex_count += 1
            else:
                BoardMeeting.onsite_count += 1

        def row(self):
            """
            Override the base class row() method to add columns for the
            meeting dates and types.
            """

            return DocBase.row(self, self.date, self.webex and "Yes" or "")

        def __lt__(self, other):
            """
            Add custom sorting logic so we can arrange the meetings
            chronologically, grouped by board.
            """

            return (self.title, self.date) < (other.title, other.date)


class Image(Section):
    """
    Collection of PDQ board meetings which took place during the date
    range for the report. The default behavior for showing the table
    of individual records is appropriate for this section, so we just
    have to supply the constructor and the custom code for displaying
    separate counts for WebEx versus on-site meetings.

    TITLE          the string used to identify the section of the report
    ABBR           key used for requesting the inclusion of this section
    HEADERS        additional column headers added to the document table
    new_count      class-level count cumulating the number of new documents
    mod_count      class-level count cumulating the number of revised documents
    """

    HEADERS = ("First Pub", "Date Modified")
    TITLE = "PDQ Image Files"
    ABBR = "image"
    new_count = 0
    mod_count = 0

    def __init__(self, control):
        """
        Save a reference to the report's control object and find
        all of the image Media documents which were either first
        published or most recently published (or both) within the
        report's date range.
        """

        # Save a reference to the report's control object.
        self.control = control

        # Start a database query for active, published CDR documents.
        cols = ("d.id", "t.value AS title", "d.first_pub", "p.started AS pub")
        query = Section.query(*cols)

        # Add the join to get the document's title.
        query.join("query_term t", "t.doc_id = d.id")
        query.where("t.path = '/Media/MediaTitle'")

        # Add a join to narrow the selection down to images (we don't
        # care what the encoding is, but we know that all images will
        # have exactly one required ImageEncoding element).
        query.join("query_term e", "e.doc_id = d.id")
        query.where("e.path = '/Media/PhysicalMedia/ImageData/ImageEncoding'")

        # Find the first as well as the most recent publication dates
        # and make sure at least one of them falls within the report's
        # date range. We know that we only re-send an image to Akamai
        # (recorded in the pub_proc_cg table, joined the base class's
        # query() method) if the image or the CDR document has changed,
        # so the pub_proc.started value (the date/time the publishing
        # job linked by the pub_proc_cg table started) can be used as
        # the date the image (or its document) was last modified.
        query.join("pub_proc p", "p.id = c.pub_proc")
        query.join("pub_proc_doc pd", "pd.doc_id = d.id", "pd.pub_proc = p.id")
        first_pub_ok = control.date_check("d.first_pub")
        last_pub_ok = control.date_check("p.started")
        query.where(query.Or(first_pub_ok, last_pub_ok))

        # Fetch and save the image documents.
        control.logger.debug("image query:\n%s", query)
        self.docs = [self.Doc(control, v) for v in self.execute(query)]

    def show_counts(self):
        """
        Show totals for the new and revised documents separately. Note
        that unlike the DrugInformationSummary document counts, there
        is no overlap for the image documents, which are either counted
        as new, or as revised, but not both.
        """

        return [
            self.count_row(self.TITLE),
            self.count_row(" - New", self.new_count),
            self.count_row(" - Revised", self.mod_count)
        ]

    class Doc(DocBase):
        """
        Object representing a single Media document for an image.
        """

        def __init__(self, control, values):
            """
            Save the CDR document ID and title, as well as the
            dates for first and most recent publications, and
            a reference for the report's control object.

            control   reference to object controlling behavior for this run
            values    dictionary of values representing a single row in
                      the result set from a database query
            """

            DocBase.__init__(self, control, values)
            self.last_pub = values.get("pub")
            self.first_pub = values.get("first_pub")
            if control.check_date(self.first_pub):
                Image.new_count += 1
            else:
                Image.mod_count += 1

        def row(self):
            """
            Override the base class row() method to add columns for the
            first publication date and the last modification date.
            """

            first_pub = self.fix_date_time(self.first_pub)
            last_mod = self.fix_date_time(self.last_pub)
            return DocBase.row(self, first_pub, last_mod)


class Control:
    """
    This is the class that does the real work. It is separated out so that
    we can invoke the report from multiple interfaces, including the new
    CDR Scheduler.

    Class constants:

    TITLE           Default title for the report.
    LOGFILE         We write our log entries here.
    TODAY           The date the report is being run.
    MONTH_START     First day of the current month.
    DEFAULT_START   Fall back on this for beginning of date range for report.
    DEFAULT_END     Fall back on this for end of date range.
    CLASSES         Classes implementing the sections of the report
    SECTIONS        Default list of sections to be shown (all of them)
    SENDER          Account the mail comes from
    TSTYLE          CSS formatting rules for table elements.
    CSTYLES         Dictionary of default CSS rules for table cells
    TO_STRING_OPTS  Options used for serializing HTML report object.
    B               HTML builder module imported at Control class scope.
    HTML            HTML module imported at Control class scope.
    TIER            Where we're running

    Instance properties:

    sections        Which parts of the report should be run (if cherry-picked)
    classes         Classes to be instantiated to create those sections
    mode            Required report mode ("test" or "live").
    email           If false, don't send report to recipients; just save it.
    docs            If false, omit tables listing individual documents
    ids             If false (and listing docs) omit document ID columns
    max_docs        Maximum number of rows to show in each table.
    start           Beginning of date range for selecting documents for report.
    end             End of date range for selecting documents for report.
    test            Convenience Boolean reflecting whether mode is 'test'.
    logger          Object for recording log information about the report.
    cursor          Object for submitting queries to the database.
    """

    import lxml.html.builder as B
    import lxml.html as HTML
    from cdrapi.settings import Tier

    TIER = Tier()
    TITLE = "PCIB Statistics Report"
    LOGFILE = "%s/cdr_stats.log" % cdr.DEFAULT_LOGDIR
    TODAY = datetime.date.today()
    MONTH_START = datetime.date(TODAY.year, TODAY.month, 1)
    DEFAULT_END = MONTH_START - datetime.timedelta(1)
    DEFAULT_START = datetime.date(DEFAULT_END.year, DEFAULT_END.month, 1)
    CLASSES = [Summary, Glossary, GeneticsProfessional, Drug,
               DrugInformationSummary, BoardMember, BoardMeeting, Image]
    SECTIONS = [c.ABBR for c in CLASSES] + ["audio"]
    SENDER = "PDQ Operator <NCIPDQoperator@mail.nih.gov>"
    TSTYLE = (
        "min-width: 350px",
        "border: 1px solid #999",
        "border-collapse: collapse",
        "margin-top: 30px",
        "background-color: white"
    )
    TSTYLE = "; ".join(TSTYLE)
    CSTYLES = {
        "font-family": "Arial",
        "border": "1px solid #999",
        "vertical-align": "top",
        "padding": "1px 5px",
        "margin": "0"
    }
    TO_STRING_OPTS = {
        "pretty_print": True,
        "encoding": "unicode",
        "doctype": "<!DOCTYPE html>"
    }

    def __init__(self, options):
        """
        Create the logger object and extract and validate the settings:

        sections
            array of sections to be included (default is all)

        mode
            must be "test" or "live" (required); test mode restricts
            recipient list for report

        email
            optional Boolean, defaults to True; if False, don't email
            the report to anyone

        log-level
            "info", "debug", or "error"; defaults to "info"

        start
            start of the date range (defaults to beginning of previous month)

        end
            end of the date range (defaults to end of previous month)

        docs
            if True (default), show information about individual documents

        ids
            if True (default), include a column for document IDs (ignored if
            docs is False)

        max-docs
            limits the number of documents shown for each section
            (ignored if list-docs is False)

        recips
            overrides the list of report recipients pulled from the database
        """

        self.options = options
        log_level = options.get("log-level") or "info"
        self.sections = options.get("sections") or self.SECTIONS
        self.classes = self.get_classes()
        self.mode = options["mode"]
        self.email = options.get("email", True)
        self.docs = options.get("docs", True)
        self.ids = options.get("ids", True)
        self.max_docs = int(options.get("max-docs") or 0)
        self.recips = options.get("recips")
        self.start = str(options.get("start") or self.DEFAULT_START)
        self.end = str(options.get("end") or self.DEFAULT_END)
        self.test = self.mode == "test"
        self.title = self.get_title(options)
        self.cursor = db.connect(user="CdrGuest").cursor()
        self.logger = cdr.Logging.get_logger("cdr-stats", level=log_level)

    def run(self):
        """
        Create, save, and (optionally) send out the report.
        """

        self.logger.info("%s job started for %s", self.mode, self.title)
        for key, value in self.options.items():
            if value is not None:
                self.logger.info("%s: %s", key, value)
        report = self.create_report()
        if self.email:
            self.send_report(report)
        else:
            self.logger.info("skipping email of reports")
        self.save_report(report)
        self.logger.info("job completed")

    def create_report(self):
        """
        Create the HTML document for this report.

        The report always contains a table showing counts for each of the
        requested sub-categories of documents. In addition, the user can
        ask for details for each of the documents reflected in the report.
        """

        table = Control.B.TABLE(
            Control.B.TR(
                Control.th("Documents"),
                Control.th("From %s to %s" % (self.start, self.end)),
            ),
            style=self.TSTYLE
        )
        details = []
        for cls in self.classes:
            section = cls(self)
            for row in section.show_counts():
                table.append(row)
            table.append(Section.count_row())
            if self.docs:
                details += section.list_docs()
        report_date = "Report date: %s" % datetime.date.today()
        h3_style = "font-family: Arial;"
        p_style = "font-size: .9em; font-style: italic; font-family: Arial"
        html = Control.B.HTML(
            Control.B.HEAD(
                Control.B.META(charset="utf-8"),
                Control.B.TITLE(self.title),
            ),
            Control.B.BODY(
                self.B.H3(self.title, style=h3_style),
                self.B.P(report_date, style=p_style),
                table,
                *details,
                style="font-size: .9em"
            )
        )
        return self.serialize(html)

    def send_report(self, report):
        """
        Email the report to the right recipient list.

        report    Serialized HTML document for the report.
        """

        if self.recips:
            recips = self.recips
        elif self.test:
            recips = cdr.getEmailList("Test Publishing Notification")
        else:
            recips = cdr.getEmailList('ICRDB Statistics Notification')
        subject = "[%s] %s" % (self.TIER.name, self.title)
        self.logger.info("sending %s", subject)
        self.logger.info("recips: %s", ", ".join(recips))
        opts = dict(subject=subject, subtype="html", body=report)
        try:
            message = cdr.EmailMessage(self.SENDER, recips, **opts)
            message.send()
        except Exception:
            self.logger.exception("Failure sending report")

    def save_report(self, report):
        """
        Write the generated report to the cdr/reports directory.

        report    Serialized HTML document for the report.
        """

        now = datetime.datetime.now().isoformat()
        stamp = now.split(".")[0].replace(":", "").replace("-", "")
        test = self.test and ".test" or ""
        name = "cdr-stats-%s%s.html" % (stamp, test)
        path = "%s/reports/%s" % (cdr.BASEDIR, name)
        with open(path, "w", encoding="utf-8") as fp:
            fp.write(report)
        self.logger.info("created %s", path)

    def get_title(self, options):
        """
        Assemble the title of the report, allowing the run-time
        options to change the base portion of the title (for example,
        to be able to adapt to the inevitable organizational name
        changes within NCI without modifying the report code.

        options    dictionary of run-time options
        """

        title = options.get("title") or self.TITLE
        if self.start == str(self.DEFAULT_START):
            if self.end == str(self.DEFAULT_END):
                month = self.DEFAULT_START.strftime("%B %Y")
                return "Monthly %s for %s" % (title, month)
        return "%s from %s to %s" % (title, self.start, self.end)

    def get_classes(self):
        """
        Assemble the sequence of classes to be used for generating
        the sections of the report. The default is for all section
        to be shown. Make it possible for the report to show the
        new audio recording without the rest of the Glossary section.
        """

        classes = []
        for c in self.CLASSES:
            if c.ABBR in self.sections:
                classes.append(c)
            elif c.ABBR == "glossary" and "audio" in self.sections:
                classes.append(c)
        return classes

    def audio_alone(self):
        """
        Determine whether the report should show the new glossary
        pronunciation audio recordings without the rest of the
        glossary section.
        """

        return "audio" in self.sections and "glossary" not in self.sections

    def check_date(self, date):
        """
        Determine whether the specified date falls within the report's
        date range. This is a direct check. See the `date_check' method
        for a database query condition to make this determination.
        """

        if not date:
            return False
        date = str(date)
        return date >= self.start and date <= (self.end + " 23:59:59")

    def date_check(self, col):
        """
        Create a condition for a database query, to determine
        whether the specified column's value falls within the
        range of the report's start and end dates. Make sure
        we don't lose matches which fall on the last day of
        the range. See the `check_date' method for making this
        determination directly without a database query.

        col      string identifying the column whose value
                 is to be compared to the date range
        """

        range = "'%s' AND '%s 23:59:59'" % (self.start, self.end)
        return "ISNULL(%s, 0) BETWEEN %s" % (col, range)

    @classmethod
    def caption(cls, label, **styles):
        """
        Helper method to generate a table caption object.

        label      Display string for the table caption
        styles     Optional style tweaks. See merge_styles() method.
        """

        default_styles = {
            "font-weight": "bold",
            "font-size": "1.2em",
            "font-family": "Arial",
            "text-align": "left",
            "background-color": "white"
        }
        style = cls.merge_styles(default_styles, **styles)
        return cls.B.CAPTION(label, style=style)

    @classmethod
    def th(cls, label, **styles):
        """
        Helper method to generate a table column header.

        label      Display string for the column header
        styles     Optional style tweaks. See merge_styles() method.
        """

        style = cls.merge_styles(cls.CSTYLES, **styles)
        return cls.B.TH(label, style=style)

    @classmethod
    def td(cls, data, url=None, **styles):
        """
        Helper method to generate a table data cell.

        data       Data string to be displayed in the cell
        styles     Optional style tweaks. See merge_styles() method.
        """

        style = cls.merge_styles(cls.CSTYLES, **styles)
        if url:
            return cls.B.TD(cls.B.A(data, href=url), style=style)
        return cls.B.TD(data, style=style)

    @classmethod
    def serialize(cls, html):
        """
        Create a properly encoded string for the report.

        html       Tree object created using lxml HTML builder.
        """

        return cls.HTML.tostring(html, **cls.TO_STRING_OPTS)

    @staticmethod
    def merge_styles(defaults, **styles):
        """
        Allow the default styles for an element to be overridden.

        defaults   Dictionary of style settings for a given element.
        styles     Dictionary of additional or replacement style
                   settings. If passed as separate arguments the
                   setting names with hyphens will have to have been
                   given with underscores instead of hyphens. We
                   restore the names which CSS expects.
        """

        d = dict(defaults, **styles)
        s = [f"{k.replace('_', '-')}:{v}" for k, v in d.items()]
        return ";".join(s)


def main():
    """
    Make it possible to run this task from the command line.
    """

    import argparse
    title = Control.TITLE
    parser = argparse.ArgumentParser(description=title)
    parser.add_argument("--mode", choices=("test", "live"), required=True,
                        help="controls who gets the report")
    parser.add_argument("--recips", nargs="*", metavar="EMAIL-ADDRESS",
                        help="email recipients (omit to have --mode option "
                        "control the recipient list)")
    parser.add_argument("--start", help="optional start of date range",
                        default=Control.DEFAULT_START)
    parser.add_argument("--end", help="optional end of date range",
                        default=Control.DEFAULT_END)
    parser.add_argument("--sections", metavar="SECTION", nargs="*",
                        choices=Control.SECTIONS, help="section(s) to include"
                        "; options are %s; omit to include all sections" %
                        ", ".join(Control.SECTIONS))
    parser.add_argument("--title", help="name of report", default=title)
    parser.add_argument("--no-email", action="store_false", dest="email",
                        help="just write the report to the file system")
    parser.add_argument("--no-docs", action="store_false", dest="docs",
                        help="omit tables showing individual documents")
    parser.add_argument("--no-ids", action="store_false", dest="ids",
                        help="omit IDs column when listing documents")
    parser.add_argument("--max-docs", type=int, help="display at most N docs")
    parser.add_argument("--log-level", choices=("info", "debug", "error"),
                        default="info", help="verbosity of logging")
    args = parser.parse_args()
    opts = dict([(k.replace("_", "-"), v) for k, v in args._get_kwargs()])
    Control(opts).run()


if __name__ == "__main__":
    main()
