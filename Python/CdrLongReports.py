# ----------------------------------------------------------------------
# CDR Reports too long to be run directly from CGI.
#
# BZIssue::1264 - Different filter set for OrgProtocolReview report
# BZIssue::1319 - Modifications to glossary term search report
# BZIssue::1337 - Modified displayed string; formatting changes
# BZIssue::1702
# BZIssue::3134 - Modifications to Protocol Processing Status report
# BZIssue::3627 - Modifications for OSP report
# BZIssue::4626 - Added class ProtocolOwnershipTransfer
# BZIssue::4711 - Adding GrantNo column to report output
# BZIssue::5086 - Changes to the Transferred Protocols Report (Issue 4626)
# BZIssue::5123 - Audio Pronunciation Tracking Report
# BZIssue::5237 - Report for publication document counts fails on
#                 non-production server
# BZIssue::5244 - URL Check report not working
# JIRA::OCECDR-4183 - support searching Spanish summaries
# JIRA::OCECDR-4216 and JIRA::OCECDR-4219 - URL check mods
# Extensive reorganization and cleanup January 2017
# JIRA::OCECDR-4284 - Fix Glossary Term and Variant Search report
# JIRA::OCECDR-4547 - Add section title for summaries
# ----------------------------------------------------------------------

# Standard library modules
import argparse
import datetime
from functools import cached_property
import re
import socket
import sys
import urllib.parse

# Third-party packages
import lxml.etree as etree
import requests

# Local modules.
import cdr
import cdrbatch
import cdrcgi
from cdrapi import db
from cdrapi.docs import Doc
from cdrapi.settings import Tier
from cdrapi.users import Session


class BatchReport:
    """
    Base class for individual long-running reports.

    Class attributes:

      B - lxml module for building HTML pages
      SUMMARY_LANGUAGE - path to element identifying language of a PDQ summary
      SUMMARY_AUDIENCE - path to element identifying language of a PDQ summary
      SUMMARY_METADATA - path to metadata block in a PDQ summary document
      GTC - top level element of a CDR glossary term concept document
      GTC_LANGUAGES - ISO language values indexed by display name
      GTC_RELATED_REF - path to link element in a GlossaryTermConcept doc
      GTC_USE_WITH - path to language attribute for a GTC link element
      EMAILFROM - sender for email messages sent for the reports
      REPORTS_BASE - directory where reports are store
      CMD - path to this script
      STAMP - YYYYMMDDHHMMSS timestamp string
      LOGNAME - default name for the log (".log" will be added to the filename)
      LOGLEVEL - default verbosity for logging
      TIER - where we're running

    Instance attributes:

      logger - object for writing log entries (standard library logging model)
      start - datetime object for processing initiation
      elapsed - number of seconds since program start
      max_time - optional ceiling on the amount of time the report should run
      throttle - whether it is possible to stop the report before completion
      cursor - the database cursor provided by the batch job's object
      name - the name the report job type is know by
      title - the string used for the HTML head/title element (optional)
      banner - string displayed at the top of the report (optional)
      format - one of "html" or "excel"
      debug - if True, do extra logging
      queued - if True, the job is queued in the database (the usual case)
      verbose - if True, display progress on the console (for testing)
    """

    import lxml.html.builder as B

    TIER = Tier()
    SUMMARY_LANGUAGE = "/Summary/SummaryMetaData/SummaryLanguage"
    SUMMARY_AUDIENCE = "/Summary/SummaryMetaData/SummaryAudience"
    SUMMARY_BOARD = "/Summary/SummaryMetaData/PDQBoard/Board/@cdr:ref"
    SUMMARY_METADATA = "/Summary/SummaryMetaData"
    GTC_LANGUAGES = {"English": "en", "Spanish": "es"}
    DEFINITIONS = {"en": "TermDefinition", "es": "TranslatedTermDefinition"}
    GTC = "GlossaryTermConcept"
    GTC_RELATED_REF = "/%s/RelatedInformation/RelatedExternalRef" % GTC
    GTC_USE_WITH = "%s/@UseWith" % GTC_RELATED_REF
    EMAILFROM = 'cdr@%s' % cdr.CBIIT_NAMES[1]
    REPORTS_BASE = cdr.BASEDIR + "/reports"
    CMD = "lib/Python/CdrLongReports.py"
    STAMP = datetime.datetime.now().strftime("%Y%m%d%H%M%S")
    LOGNAME = "reports"
    LOGLEVEL = "info"

    def __init__(self, job, name, title=None, banner=None):
        """
        Collect the common parameters for the job.
        """

        self.start = datetime.datetime.now()
        self.debug = job.getParm("debug") == "True"
        self.logger = self.get_logger()
        self.job = job
        self.elapsed = 0.0
        self.throttle = job.getParm("throttle") == "True"
        self.max_time = self.int_parm("max_time")
        self.cursor = job.getCursor()
        self.name = name
        self.title = title or name
        self.banner = banner or self.title
        self.format = job.getParm("format") or "html"
        self.queued = job.getJobId() and True or False
        self.verbose = job.getParm("verbose") == "True"

    def run(self):
        """
        Create the report and let the user know where it is.
        """

        self.job.setProgressMsg("Report started")
        self.logger.info("Starting %s report", self.name)
        report_filename = self.write_report()
        self.notify_users(report_filename)
        self.logger.info("Completed %s report", self.name)

    def write_report(self):
        """
        Create and save the report and return the base file name.
        """

        name = "%s-%d" % (self.name.replace(" ", ""), self.job.getJobId())
        if self.format == "html":
            report = self.create_html_report()
            name += ".html"
            path = "%s/%s" % (self.REPORTS_BASE, name)
            open(path, "wb").write(report.encode("utf-8"))
        else:
            workbook = self.create_excel_report()
            name += ".xlsx"
            path = "%s/%s" % (self.REPORTS_BASE, name)
            workbook.save(path)
        self.logger.info("Saved %s", path)
        path = path.replace("/", "\\")
        command = f"{cdr.FIX_PERMISSIONS} {path}"
        cdr.run_command(command)
        self.logger.info("Adjusted report permissions")
        return name

    def create_report_url(self, name):
        """
        Create the link to the CGI script which serves up the report.
        """

        base = cdr.CBIIT_NAMES[2]
        if self.format == "html":
            return "%s/CdrReports/%s" % (base, name)
        else:
            base += cdrcgi.BASE
            return "%s/GetReportWorkbook.py?name=%s" % (base, name)

    def create_html_report(self):
        """
        Assemble and serialize the HTML document for the report.
        """

        report = self.B.HTML(self.head(), self.body())
        opts = {
            "encoding": "unicode",
            "pretty_print": True,
            "method": "html",
            "doctype": "<!DOCTYPE html>"
        }
        return etree.tostring(report, **opts)

    def head(self):
        """
        Assemble the head block element for the HTML report.

        Can be overridden by the derived classes, but that wouldn't
        normally be necessary.
        """

        return self.B.HEAD(
            self.B.META(charset="utf-8"),
            self.B.TITLE("%s Report" % self.title),
            self.B.STYLE(self.style())
        )

    def body(self):
        """
        Start the HTML body block and let the derived class fill it in.
        """

        body = self.B.BODY(
            self.B.H1(self.banner),
            self.B.H2(str(datetime.date.today()))
        )
        return self.fill_body(body)

    def fill_body(self, body):
        """
        Add the payload content for the report.

        Will be overridden by the derived classes. This version results
        in an empty report.
        """

        return body

    def create_excel_report(self):
        """
        Create the Excel styles object and workbook and let the caller
        add the worksheets to it.
        """

        self.excel = cdrcgi.Excel(wrap=True)
        self.add_sheets()
        return self.excel.book

    def add_sheets(self):
        """
        Derived classes implement this to add the report's worksheets.
        """

        pass

    def get_logger(self):
        """
        Create a logging object.

        Separated out so derived classes can completely customize this.
        """

        level = self.debug and "debug" or self.LOGLEVEL
        return cdr.Logging.get_logger(self.LOGNAME, level=level)

    def notify_users(self, filename):
        """
        Send a link to the new report in an email message and post it
        to the database.
        """

        url = self.create_report_url(filename)
        self.logger.info("url: %s", url)
        args = self.TIER.name, self.name
        subject = "[CDR %s] Report results - %s Report" % args
        args = self.name, url
        body = "The %s report you requested can be viewed at\n%s\n" % args
        self.send_mail(subject, body)
        link = "<a href='%s'><u>%s</u></a>" % (url, url)
        message = "%s<br>Report available at %s" % (self.activity(), link)
        self.job.setProgressMsg(message)
        self.job.setStatus(cdrbatch.ST_COMPLETED)
        return url

    def activity(self):
        """
        Create the string that summarizes what was done to create the report.

        This is generally displayed in a different font at the bottom of
        the report. Typically this will be overriden for an individual
        report. For example

        "Processed 123 urls for 314 link elements in 2.09 seconds."
        """

        elapsed = (datetime.datetime.now() - self.start).total_seconds()
        return "processing time: %s seconds" % elapsed

    def style(self):
        """
        Create the CSS rules for an HTML report.

        Derived classes can provide customized style rules for the individual
        reports by overriding this method, or (more frequently) by implementing
        its own version of selectors (see below).
        """

        selectors = self.selectors()
        lines = []
        for selector in sorted(selectors):
            rules = selectors[selector]
            rules = [("%s: %s;" % rule) for rule in list(rules.items())]
            lines.append("%s { %s }" % (selector, " ".join(rules)))
        css = "\n".join(lines)
        if self.verbose:
            sys.stderr.write("<style>\n%s\n</style>\n" % css)
        return css

    def selectors(self):
        """
        Provide a dictionary of CSS rules indexed by selectors.

        This will be serialized for inclusion in the head block
        of the HTML document for the report by the style() method
        above. A derived class would typically provide its own
        version of this method, invoking this base class version
        and then making specific modifications before returning
        it to the caller.
        """

        return {
            "*": {"font-family": "Arial, sans-serif"},
            ".right": {"text-align": "right"},
            ".left": {"text-align": "left"},
            ".center": {"text-align": "center"},
            ".red": {"color": "red"},
            ".strong": {"font-weight": "bold"},
            ".error": {"color": "darkred"},
            "p.processing": {
                "color": "green",
                "font-style": "italic",
                "font-size": "9pt"
            },
            "h1, h2": {"text-align": "center", "font-family": "serif"},
            "h1": {"font-size": "16pt"},
            "h2": {"font-size": "13pt"},
            "table": {"border-collapse": "collapse"},
            "th, td": {
                "font-family": "Arial",
                "border": "1px solid grey",
                "padding": "2px",
                "font-size": "10pt"
            }
        }

    def quitting_time(self):
        """
        Tell the caller whether report processing should finish early.

        This is provided to allow the user to run a report in a quick
        sample version, usually for testing or a preview of the full
        report (for example, by setting a ceiling for run time).
        Derived classes can customize the method to replace or
        augment the logic provided here.
        """

        self.elapsed = (datetime.datetime.now() - self.start).total_seconds()
        if not self.throttle:
            return False
        if self.max_time and self.elapsed > self.max_time:
            if self.verbose:
                sys.stderr.write("%s > %s\n" % (self.elapsed, self.max_time))
            return True

    def int_parm(self, name):
        """
        Pull an integer value from the job's parameter set in a safe way.
        """

        try:
            return int(self.job.getParm(name))
        except Exception:
            return 0

    def get_boards(self):
        """
        Pull PDQ board IDs from the parameter set as a sequence of integers.
        """

        boards = self.job.getParm("boards")
        if not boards:
            return []
        if not isinstance(boards, (list, tuple)):
            boards = [boards]
        if "all" in boards:
            return []
        return [int(board) for board in boards]

    def get_doc_type(self, doc_id):
        """
        Find the CDR document type for a specific document.
        """

        query = db.Query("doc_type t", "t.name")
        query.join("document d", "d.doc_type = t.id")
        query.where(query.Condition("d.id", doc_id))
        rows = query.execute(self.cursor).fetchall()
        return rows[0][0] if rows else None

    def log_query(self, query):
        """
        Optionally (depending on whether debugging is turned on) log
        the SQL query and its parameters (if any).
        """

        self.logger.debug("SQL query\n%s", query)
        parms = query.parms()
        if parms:
            self.logger.debug("query parameters: %s", query.parms())

    def send_mail(self, subject, message):
        """
        Send mail to the recipients specified for the job.
        """

        email = self.job.getEmail()
        if not email:
            self.logger.error("No email address provided")
        else:
            self.logger.info("Sending email report to %s", email)
            recips = email.replace(',', ' ').replace(';', ' ').split()
            opts = dict(subject=subject, body=message)
            cdr.EmailMessage(self.EMAILFROM, recips, **opts).send()

    @classmethod
    def summary_board_subquery(cls, boards, language=None):
        """
        Assemble a database query which can be used to narrow the set
        of PDQ summaries by PDQ board(s) and optionally by language.

        The logic is complicated by the fact that the link to the
        board responsible for a Spanish summary is not stored in
        that summary's document, but is instead stored in the document
        for the English summary of which it is a translation. So we
        have to use two separate queries for finding a board's summaries
        in English and in Spanish. To find summaries for a given board
        (or set of boards) we use the SQL UNION of the two queries.

        Pass:
            boards - sequence of integers for PDQ board IDs; cannot be empty
            language - "English" or "Spanish" (optional)

        Return:
            db.Query object
        """

        # Double conversion ensures we have integers, guarding against attacks.
        boards = ", ".join([str(int(board)) for board in boards])
        if not language or language == "English":
            english_query = db.Query("query_term", "doc_id")
            english_query.where(f"path = '{cls.SUMMARY_BOARD}'")
            english_query.where(f"int_val in ({boards})")
            if language:
                return english_query
        if not language or language == "Spanish":
            spanish_query = db.Query("query_term s", "s.doc_id")
            spanish_query.join("query_term e", "e.doc_id = s.int_val")
            spanish_query.where("s.path = '/Summary/TranslationOf/@cdr:ref'")
            spanish_query.where(f"e.path = '{cls.SUMMARY_BOARD}'")
            spanish_query.where(f"e.int_val in ({boards})")
            if language:
                return spanish_query
        return english_query.union(spanish_query)

    @classmethod
    def test_filename_base(cls):
        """
        Generate the default filename base (without extension) for testing.
        """

        # pylint: disable=no-member
        return "%s-%s" % (cls.NAME.replace(" ", "_"), cls.STAMP)

    @classmethod
    def arg_parser(cls, suffix=".html"):
        """
        Create a parser for command-line options of the test harness.
        """

        default_filename = cls.test_filename_base() + suffix
        # pylint: disable=no-member
        parser = argparse.ArgumentParser(
            usage='CdrLongReports.py "%s" [options]' % cls.NAME,
            formatter_class=argparse.ArgumentDefaultsHelpFormatter,
            description="Performs a %s test report." % cls.NAME
        )
        # pylint: enable=no-member
        parser.add_argument("--full-run", action="store_false",
                            dest="throttle",
                            help="don't throttle the amount of time "
                            "or processing performed for the test run")
        parser.add_argument("--max-time", type=int, default=10,
                            help="if throttling is on, stop after MAX_TIME "
                            "seconds")
        parser.add_argument("--debug", "-d", action="store_true",
                            help="write additional logging information")
        parser.add_argument("--verbose", "-v", action="store_true",
                            help="write details to the console for testing")
        parser.add_argument("--filename", default=default_filename,
                            help="where to save test report")
        return parser

    @classmethod
    def run_test(cls, arg_parser, format="html"):
        """
        Called from test_harness for an individual report type to run
        a report from the command line and save the results.

        Suppress false positives from pylint, which doesn't understand
        that only instances of the derived classes are created, and the
        derived classes are suppliers of some of the arguments passed
        to the base class constructor.
        """

        import logging
        # pylint: disable=no-member
        logger = logging.getLogger(cls.NAME)
        logger.info("Starting %s report", cls.NAME)
        # pylint: enable=no-member
        args = vars(arg_parser.parse_args())
        cls.announce_test(args)
        filename = args.pop("filename")
        args = list(args.items())
        # pylint: disable-next=no-member
        job = cdrbatch.CdrBatch(jobName=cls.NAME, command=cls.CMD, args=args)
        if format == "html":
            # pylint: disable-next=no-value-for-parameter
            report = cls(job).create_html_report()
            open(filename, "wb").write(report.encode("utf-8"))
        else:
            # pylint: disable-next=no-value-for-parameter
            cls(job).create_excel_report().save(filename)
        sys.stderr.write("\nsaved %s\n\n" % filename)
        logger.info("Saved %s", filename)
        # pylint: disable-next=no-member
        logger.info("Completed %s report", cls.NAME)

    @classmethod
    def announce_test(cls, args):
        sys.stderr.write("\n%s\n\n" % ("-" * 78))
        # pylint: disable-next=no-member
        sys.stderr.write("Running %s test report\n\n" % cls.NAME)
        if args:
            sys.stderr.write("Run-time options:\n")
            for name in sorted(args):
                sys.stderr.write("%25s: %s\n" % (name, args[name]))
            sys.stderr.write("\n")


class URLChecker(BatchReport):
    """
    Base class for two reports on links with problems.
    """

    def __init__(self, job, name, title=None, banner=None):
        """
        Collect the options stored for this report job.
        """

        BatchReport.__init__(self, job, name, title, banner)
        self.doc_type = job.getParm("doc_type") or "Summary"
        self.doc_id = job.getParm("doc_id")
        if self.doc_id:
            self.doc_type = self.get_doc_type(self.doc_id)
        self.boards = self.get_boards()
        self.max_urls = self.int_parm("max_urls")
        self.connect_timeout = self.int_parm("connect_timeout") or 5
        self.read_timeout = self.int_parm("read_timeout") or 10
        self.check_certs = job.getParm("check_certs") == "True"
        self.audience = job.getParm("audience")
        self.language = job.getParm("language")
        self.show_redirects = False
        if not self.check_certs:
            self.suppress_cert_warning()
        if self.verbose:
            sys.stderr.write("throttle is %s\n" % self.throttle)
            sys.stderr.write("max_urls is %s\n" % self.max_urls)
            sys.stderr.write("max_time is %s\n" % self.max_time)

    def table(self):
        """
        Fetch the data rows and insert them into the report's table.

        As a side effect, the string describing the processing performed
        to generate the report is assembled and stored in the report
        object here.
        """

        self.links_tested = 0
        rows = self.get_report_rows()
        elapsed = (datetime.datetime.now() - self.start).total_seconds()
        self.message = ("Checked %d urls for %d of %d links in %s seconds." %
                        (len(self.pages), self.links_tested,
                         len(self.links), elapsed))
        # pylint: disable=no-member
        headers = [self.B.TH(header) for header in self.COLUMN_HEADERS]
        table_class = self.B.CLASS(self.TABLE_CLASS)
        # pylint: enable=no-member
        return self.B.TABLE(table_class, self.B.TR(*headers), *rows)

    def get_report_rows(self):
        """
        Select the links to be checked and put the ones with problems in the
        rows for the report's table.
        """

        self.Page.REQUEST_OPTS = {
            "timeout": (self.connect_timeout, self.read_timeout),
            "verify": self.check_certs,
            "allow_redirects": not self.show_redirects
        }
        try:
            self.links = self.find_links()  # pylint: disable=no-member
        except Exception as e:
            self.logger.exception("fetching external links for report")
            self.job.fail("Fetching external links for report: %s" % e)
        if self.verbose:
            sys.stderr.write("%d links fetched\n" % len(self.links))
        self.pages = {}
        rows = []
        for link in self.links:
            page = self.pages.get(link.url)
            if not page:
                if self.quitting_time():
                    break
                page = self.pages[link.url] = self.Page(self, link.url)
                if self.verbose:
                    message_strings = self.elapsed, repr(link.url)
                    sys.stderr.write("%s -- checked %s\n" % message_strings)
            if link.in_scope(page):
                rows.append(link.make_row(page))
            self.links_tested += 1
            if self.queued:
                message_strings = self.links_tested, len(self.links)
                message = "Checked %d of %d links" % message_strings
                self.job.setProgressMsg(message)
        return rows

    def quitting_time(self):
        """
        Determine whether we can quit early.

        The base class implementation is invoked first to see if we've
        passed the time threshold. We also check to see if we have already
        processed the maximum number of URLs the user wants us to check.
        """

        if not self.throttle:
            return False
        if BatchReport.quitting_time(self):
            return True
        if self.max_urls and len(self.pages) >= self.max_urls:
            if self.verbose:
                sys.stderr.write("%s >= %s\n" % (len(self.pages),
                                                 self.max_urls))
            return True

    def activity(self):
        """
        Return the string describing report processing information.

        This string is constructed by the table() method above.
        """

        return self.message

    def suppress_cert_warning(self):
        """
        Prevent security warning output from garbling HTML report pages.
        """

        # pylint: disable=import-error, no-member
        from urllib3.exceptions import InsecureRequestWarning
        requests.packages.urllib3.disable_warnings(InsecureRequestWarning)

    class Page:
        """
        Information about the health of the web resource addressed by a URL.
        """

        dead_hosts = {}

        def __init__(self, report, url):
            """
            Determine whether the URL has problems.

            We look for:
              1. malformed URLs
              2. unresponsive web servers
              3. wrong URL schema
              4. response code other than OK (200)
              5. host name lookup failure
              6. request timeout
              7. (optionally) problems with invalid SSL certificates

            We cache the results of checking each URL so we don't try
            to process the same URL more than once. We also remember
            web servers we know aren't alive as another optimization.
            The URL cache is in the report object. The dead host
            cache is stored in this class. (Dead host cache disabled;
            see note below on remember_dead_host() method.)

            The derived classes are responsible for closing the response
            object, if that is appropriate.
            """

            self.url = url
            self.problem = self.response = None
            components = urllib.parse.urlparse(url)
            host = components.netloc
            if not host:
                self.problem = "Malformed URL"
            elif components.netloc in self.dead_hosts:
                self.problem = self.dead_hosts[host]
            elif components.scheme not in ("http", "https"):
                self.problem = "Unexpected scheme"
            else:
                try:
                    self.response = requests.get(url, **self.REQUEST_OPTS)
                    code = self.response.status_code
                    if code != 200:
                        try:
                            reason = str(self.response.reason, "utf-8")
                        except Exception:
                            reason = str(self.response.reason)
                        self.problem = "%d: %s" % (code, reason)
                except IOError as e:
                    problem = str(e)
                    if "getaddrinfo failed" in problem:
                        self.problem = "host name lookup failure"
                        self.remember_dead_host(host, self.problem)
                    elif "ConnectTimeoutError":
                        self.problem = "connection timeout"
                    else:
                        self.problem = "IOError: %s" % e
                    report.logger.error("%s, %s", url, self.problem)
                except socket.error:  # pylint: disable=no-member
                    self.problem = "Host not responding"
                    self.remember_dead_host(host, self.problem)
                    report.logger.error("%s not responding", host)
                except Exception as e:
                    self.problem = str(e)
                    report.logger.error("%s: %s", url, self.problem)

        @classmethod
        def remember_dead_host(cls, host, problem):
            """
            Cache the information about a problem with a particular web server.

            XXX 2017-02-19 ============================================
            We've had to eliminate the dead host optimization, because
            of a bug somewhere in the stack (the Windows firewall,
            perhaps?). For some reason, the 301 response we get back
            from www.cancer.gov when we submit a "GET /cam" request
            never reaches the socket layer, resulting in an exception
            being thrown. If we take that exception to mean that the
            web server for www.cancer.gov is off line, we'll make the
            wrong assumption about all subsequent URLs for that host.
            See https://github.com/psf/requests/issues/3880 and
            https://github.com/python/cpython/issues/73853.
            XXX 2017-02-19 ============================================
            """

            return
            cls.dead_hosts[host] = problem

    @classmethod
    def test_harness(cls):
        """
        Run a test report from the command line, bypassing the reporting queue.
        """

        parser = cls.arg_parser(".html")
        parser.add_argument("--doc-type", default="Summary",
                            help="CDR document type for report selections")
        parser.add_argument("--max-urls", type=int, default=100,
                            help="stop after checking this many URLs "
                            "if throttling")
        parser.add_argument("--connect-timeout", type=int, default=5,
                            help="wait this long for the socket connection")
        parser.add_argument("--read-timeout", type=int, default=30,
                            help="wait READ_TIMEOUT seconds for web server")
        parser.add_argument("--check-certs", action="store_true",
                            help="report invalid SSL certificates as errors")
        parser.add_argument("--audience", default="Patient",
                            help="restrict report to this audience",
                            choices=("Patient", "Health Professional"))
        parser.add_argument("--language", default="English",
                            help="restrict report to one language",
                            choices=("English", "Spanish"))
        parser.add_argument("--boards", metavar="BOARD-ID", nargs="*",
                            help="restrict report by PDQ Board ID(s)")
        if cls is BrokenExternalLinks:
            parser.add_argument("--show-redirects", action="store_true",
                                help="report redirected URLs as problems")
        if cls is PageTitleMismatches:
            parser.add_argument("--show-all", action="store_true",
                                help="also display matching titles")
        cls.run_test(parser, "html")


class BrokenExternalLinks(URLChecker):
    """
    Report on external links with problems. BZIssues 903 and 3374.
    Modified extensively for JIRA ticket OCECDR-4219.
    """

    NAME = "Broken URLs"
    TITLE = "URL Check"
    BANNER = "CDR Report on Inactive Hyperlinks"
    TABLE_CLASS = "url-check"
    COLUMN_HEADERS = ("CDR ID", "Title", "Stored URL", "Problem", "Element")

    def __init__(self, job):
        """
        Collect the options stored for this report job.
        """

        URLChecker.__init__(self, job, self.NAME, self.TITLE, self.BANNER)
        self.show_redirects = job.getParm("show_redirects") == "True"
        if self.doc_type == "Summary":
            BrokenExternalLinks.COLUMN_HEADERS += ("Section Title",)

    def fill_body(self, body):
        """
        Add the heading, table, and processing activity summary to
        the page HTML.
        """

        return self.B.BODY(
            self.B.H1(self.banner),
            self.B.H2(self.doc_type),
            self.table(),
            self.B.P(self.message, self.B.CLASS("processing"))
        )

    def find_links(self):
        """
        Find the links to be checked, using the user-supplied selection
        criteria.

        The selection logic is delicately tricky. Exactly one document type
        must be specified for the report, unless a document ID is provided
        to narrow the report to links in a single document, in which case
        the document type is retrieved from the database. Depending on the
        document type, other filtering criteria may be provided.

        For glossary term concept documents, we look in two places: the
        links stored in language-specific definition blocks, and links
        stored outside the definition blocks in RelatedExternalRef element
        with a @UseWith attribute specifying language usage for the links.
        The links in the definition are audience specific (patients or
        health professionals), whereas the links outside the definitions
        are not. We use an SQL UNION of two separate queries to implement
        this logic. Note that even if a CDR ID is specified to narrow
        the report to links in a single document, we still apply this
        logic to the GlossaryTermConcept documents to filter by language
        and/or audience as appropriate.

        Summary documents are somewhat less complicated in one sense:
        if a summary document is selected, then all of the external
        links in that document are selected for checking. Unlike the
        GlossaryTermConcept documents, a PDQ summary document has
        exactly one language and one audience. This simplification
        is paid for by the complexity of supporting narrowing of the
        result set to summaries linked to a specific PDQ board (a
        requirement added in January 2017). The summary_board_subquery()
        in the base class for a fuller explanation of how this is done.

        For any other document type, no other special filtering is needed.
        """

        fields = "u.doc_id", "d.title", "u.value", "u.path", "u.node_loc"
        query = db.Query("query_term u", *fields).unique()
        query.where("u.value LIKE 'http%'")
        query.join("document d", "d.id = u.doc_id")
        if not self.doc_id:
            query.where("d.active_status = 'A'")
        if self.doc_type == "GlossaryTermConcept":
            if self.language or self.audience:
                query.where("u.path = '%s/@cdr:xref'" % self.GTC_RELATED_REF)
                if self.language:
                    language = self.GTC_LANGUAGES[self.language]
                    query.join("query_term l", "l.doc_id = u.doc_id",
                               "l.node_loc = u.node_loc")
                    query.where("l.path = '%s'" % self.GTC_USE_WITH)
                    query.where("l.value = '%s'" % language)
                    definition = self.DEFINITIONS[language]
                    def_path = "/%s/%s" % (self.GTC, definition)
                else:
                    def_path = "/GlossaryTermConcept%TermDefinition"
                query2 = db.Query("query_term u", *fields)
                query2.where("u.path LIKE '%s%%/@cdr:xref'" % def_path)
                query2.join("document d", "d.id = u.doc_id")
                if not self.doc_id:
                    query2.where("d.active_status = 'A'")
                if self.audience:
                    query2.join("query_term a", "a.doc_id = u.doc_id",
                                "a.node_loc = u.node_loc")
                    query2.where("a.path LIKE '%s%%/Audience'" % def_path)
                    query2.where("a.value = '%s'" % self.audience)
                    query2.where("u.value LIKE 'http%'")
                    query.union(query2)
            else:
                query.where("u.path LIKE '/GlossaryTermConcept/%/@cdr:xref'")
        else:
            query.where("u.path LIKE '/%s/%%/@cdr:xref'" % self.doc_type)
        if self.doc_id:
            query.where(query.Condition("u.doc_id", self.doc_id))
        elif self.doc_type == "Summary":
            if self.boards:
                sq = self.summary_board_subquery(self.boards, self.language)
                query.where(query.Condition("u.doc_id", sq, "IN"))
            elif self.language:
                query.join("query_term l", "l.doc_id = u.doc_id")
                query.where("l.path = '%s'" % self.SUMMARY_LANGUAGE)
                query.where("l.value = '%s'" % self.language)
            if self.audience:
                query.join("query_term a", "a.doc_id = u.doc_id")
                query.where("a.path = '%s'" % self.SUMMARY_AUDIENCE)
                query.where("a.value = '%ss'" % self.audience)
        self.log_query(query)
        rows = query.execute(self.cursor).fetchall()
        return [self.Link(self, *row) for row in rows]

    def selectors(self):
        """
        Customize the style rules applied the report's display.
        """

        selectors = BatchReport.selectors(self)
        selectors["table.url-check"] = {"width": "100%"}
        selectors["table.url-check th"] = {
            "background-color": "silver",
            "text-align": "left"
        }
        selectors["table.url-check th, table.url-check td"] = {
            "border": "solid white 1px"
        }
        return selectors

    class Link:
        """
        External links found in the CDR documents.
        """

        def __init__(self, report, doc_id, title, url, path, node_loc):
            """
            Store the values for a row in the result set from the
            database query.
            """

            self.report = report
            self.doc_id = doc_id
            self.title = title
            self.url = url
            self.path = path
            self.node_loc = node_loc

        @property
        def section_title(self):
            """
            Title of innermost enclosing summary section where link is found

            None if this is not a link in a Cancer Information Summary.
            An empty string if it is a CIS, but no section title is found
            (very unlikely).
            """

            if not hasattr(self, "_section_title"):
                self._section_title = None
                if self.path.startswith("/Summary/"):
                    self._section_title = ""
                    top_loc = self.node_loc[:4]
                    query = db.Query("query_term", "value", "node_loc")
                    query.where(query.Condition("LEFT(node_loc, 4)", top_loc))
                    query.where("path LIKE '/Summary/%SummarySection/Title'")
                    query.where(query.Condition("doc_id", self.doc_id))
                    rows = query.execute(self.report.cursor).fetchall()
                    loc_len = 0
                    for title, node_loc in rows:
                        section_loc = node_loc[:-4]
                        if self.node_loc.startswith(section_loc):
                            if loc_len < len(section_loc):
                                self._section_title = title
                                loc_len = len(section_loc)
            return self._section_title

        def in_scope(self, page):
            """
            Should this link be included on the report?
            """

            return page.problem and True or False

        def make_row(self, page):
            """
            Assemble the table row describing problems with a link to this URL.
            """

            B = self.report.B
            element = self.path.split("/")[-2]
            link = B.A(page.url, href=page.url, target="_blank")
            url = "/cgi-bin/cdr/QcReport.py?DocId={:d}".format(self.doc_id)
            qclink = B.A(str(self.doc_id), href=url, target="_blank")
            row = B.TR(
                B.TD(qclink, B.CLASS("center")),
                B.TD(self.title, B.CLASS("left")),
                B.TD(link, B.CLASS("left")),
                B.TD(page.problem, B.CLASS("left")),
                B.TD(element, B.CLASS("left"))
            )
            if self.section_title is not None:
                row.append(B.TD(self.section_title, B.CLASS("left")))
            return row


class PageTitleMismatches(URLChecker):
    """
    Class for comparing stored ExternalRef/@SourceTitle values
    html head titles of the referenced web pages.

    This report selects all ExternalRef elements that match user entered
    report parameters (see CheckUrls.py) that contain a SourceTitle
    attribute.  The referenced (via @cdr:xref) web page is fetched and
    parsed. and a normalized value of the /html/head/title element is
    compared to a similarly normalized @SourceTitle.

    If the two do not match, an error is reported.
    """

    NAME = "Page Title Mismatches"
    TITLE = "External Page Title"
    BANNER = "Comparison of Stored Page Titles with Actual Titles in Web Pages"
    TABLE_CLASS = "page-title-mismatches"
    COLUMN_HEADERS = (
        "CDR ID",
        "CDR Doc Title",
        "CDR URL",
        "CDR Stored External Reference Title",
        "Web Page Title"
    )

    def __init__(self, job):
        """
        Collect the options stored for this report job.
        """

        URLChecker.__init__(self, job, self.NAME, self.TITLE, self.BANNER)
        self.show_all = job.getParm("show_all") == "True"

    def fill_body(self, body):
        """
        Add the heading, table, and processing activity summary to the
        page HTML.
        """

        body.append(self.show_parameters())
        body.append(self.table())
        body.append(self.B.P(self.message, self.B.CLASS("processing")))
        body.append(self.B.DIV(self.stats_table()))
        return body

    def find_links(self):
        """
        Find the links to be checked, using the user-supplied selection
        criteria.

        The logic is similar to that in BrokenExternalLinks.find_links (q.v.),
        but is significantly simplified by the fact that we're only
        interested in links which have a SourceTitle attribute,
        which none of the external links in the GlossaryTermConcept
        documents have.
        """

        fields = "d.id", "u.value", "d.title", "t.value"
        query = db.Query("query_term t", *fields).unique()
        query.join("query_term u", "u.doc_id = t.doc_id",
                   "u.node_loc = t.node_loc")
        query.join("document d", "d.id = t.doc_id")
        if not self.doc_id:
            query.where("d.active_status = 'A'")
        query.where("t.path LIKE '/%s/%%/@SourceTitle'" % self.doc_type)
        query.where("u.path LIKE '/%s/%%/@cdr:xref'" % self.doc_type)
        if self.doc_id:
            query.where(query.Condition("d.id", self.doc_id))
        elif self.doc_type == "Summary":
            if self.boards:
                sq = self.summary_board_subquery(self.boards, self.language)
                query.where(query.Condition("u.doc_id", sq, "IN"))
            elif self.language:
                query.join("query_term l", "l.doc_id = u.doc_id")
                query.where("l.path = '%s'" % self.SUMMARY_LANGUAGE)
                query.where("l.value = '%s'" % self.language)
            if self.audience:
                query.join("query_term a", "a.doc_id = u.doc_id")
                query.where("a.path = '%s'" % self.SUMMARY_AUDIENCE)
                query.where("a.value = '%ss'" % self.audience)
        if self.doc_type == self.GTC:
            if self.language:
                language = self.GTC_LANGUAGES[self.language]
                query.join("query_term l", "l.doc_id = t.doc_id",
                           "l.node_loc = t.node_loc")
                query.where("l.path = '%s'" % self.GTC_USE_WITH)
                query.where("l.value = '%s'" % language)
        self.log_query(query)
        rows = query.execute(self.cursor).fetchall()
        return [self.Link(self, *row) for row in rows]

    def selectors(self):
        """
        Add report-specific formatting for the values in the last column,
        the statistics table at the bottom, and the column headers of the
        primary report table.
        """

        selectors = BatchReport.selectors(self)
        selectors["td.ok"] = {"color": "cyan"}
        selectors["td.mismatch"] = {"color": "red"}
        selectors["td.error"] = {"color": "darkred"}
        selectors["table.stats"] = {"width": "150px", "margin": "25px auto"}
        selectors["table.titles th"] = {"white-space": "nowrap"}
        return selectors

    def stats_table(self):
        """
        Assemble the table showing statistics for the number of links in
        each state category.
        """

        matched = self.Link.stats["matched"]
        mismatched = self.Link.stats["mismatched"]
        errors = self.Link.stats["errors"]
        return self.B.TABLE(
            self.B.CAPTION("Statistics"),
            self.B.TR(
                self.B.TH("Matched Titles", self.B.CLASS("right")),
                self.B.TD(str(matched), self.B.CLASS("right")),
            ),
            self.B.TR(
                self.B.TH("Mismatched Titles", self.B.CLASS("right")),
                self.B.TD(str(mismatched), self.B.CLASS("right")),
            ),
            self.B.TR(
                self.B.TH("Retrieval Errors", self.B.CLASS("right")),
                self.B.TD(str(errors), self.B.CLASS("right")),
            ),
            self.B.CLASS("stats")
        )

    def show_parameters(self):
        """
        Show the parameters used to request the report.
        """

        report_type = self.show_all and "All Titles" or "Problem Titles"
        parms = [
            self.B.LI("Show: %s" % report_type),
            self.B.LI("Doc Type: %s" % self.doc_type)
        ]
        if self.doc_id:
            parms.append(self.B.LI("Doc ID: %s" % self.doc_id))
        elif self.doc_type == "Summary":
            parms.append(self.B.LI("Language: %s" % (self.language or "Any")))
            parms.append(self.B.LI("Audience: %s" % (self.audience or "Any")))
            if not self.boards:
                parms.append(self.B.LI("Board: Any"))
            else:
                boards = cdr.Board.get_boards().values()
                boards = dict([(b.id, b.short_name) for b in boards])
                names = [boards[id] for id in self.boards]
                for name in names:
                    parms.append(self.B.LI("Board: %s" % name))
        if self.doc_type == "GlossaryTermConcept":
            parms.append(self.B.LI("Language: %s" % (self.language or "Any")))
        check_certs = self.check_certs and "On" or "Off"
        parms.append(self.B.LI("Certificate Checking: %s" % check_certs))
        parms.append(self.B.LI("Connect Timeout: %d seconds" %
                               self.connect_timeout))
        parms.append(self.B.LI("Read Timeout: %d seconds" % self.read_timeout))
        return self.B.P(self.B.B("Parameters:"), self.B.UL(*parms))

    class Page(URLChecker.Page):
        """
        This class does what the base class does for the broken URLs report
        (finding dead servers and links), and then for the successful
        retrievals, parses the document to extract the title for the
        linked page.
        """

        import lxml.html as HTML

        def __init__(self, report, url):
            """
            Invoke the base class constructor to see if we can retrieve the
            page, and if we are successful, then parse the page to extract
            it's title from the html/head block.
            """

            URLChecker.Page.__init__(self, report, url)
            self.title = None
            if not self.problem:
                try:
                    content_type = self.response.headers["content-type"]
                    if "html" not in content_type:
                        self.problem = "content is %s" % content_type
                except Exception as e:
                    self.problem = "content type: %s" % e
            if not self.problem and not self.response.text:
                self.problem = "no content returned by host"
            if not self.problem:
                try:
                    root = self.HTML.fromstring(self.response.text)
                except Exception:
                    try:
                        root = self.HTML.fromstring(self.response.content)
                    except Exception:
                        self.problem = "unable to parse page HTML"
            if self.response:
                self.response.close()
            if not self.problem:
                for node in root.findall("head/title"):
                    self.title = cdr.get_text(node).strip()
                if not self.title:
                    self.problem = "no page title found"

    class Link:
        """
        External links found in the CDR documents, including the
        value stored in the CDR document for the link's page.
        """

        stats = {"errors": 0, "mismatched": 0, "matched": 0}

        def __init__(self, report, doc_id, url, doc_title, stored_title):
            """
            Store the values for a row in the result set from the
            database query.
            """

            self.report = report
            self.doc_id = doc_id
            self.url = url
            self.doc_title = doc_title
            self.stored_title = stored_title
            self.mismatched = False

        def in_scope(self, page):
            """
            Determine whether this link be included on the report.

            As a side effect, updates the statistics for the different
            link states.
            """

            if page.problem:
                self.stats["errors"] += 1
                return True
            if self.stored_title.lower() != page.title.lower():
                self.mismatched = True
                self.stats["mismatched"] += 1
                return True
            self.stats["matched"] += 1
            return self.report.show_all

        def make_row(self, page):
            """
            Assemble the table row describing problems with a link to this URL.
            """

            B = self.report.B
            title_class = "ok"
            title = page.title
            if page.problem:
                title = page.problem
                title_class = "error"
            elif self.mismatched:
                title_class = "mismatch"
            link = B.A(page.url, href=page.url, target="_blank")
            url = "/cgi-bin/cdr/QcReport.py?DocId={:d}".format(self.doc_id)
            qclink = B.A(str(self.doc_id), href=url, target="_blank")
            return B.TR(
                B.TD(qclink, B.CLASS("center")),
                B.TD(self.doc_title, B.CLASS("left")),
                B.TD(link, B.CLASS("left")),
                B.TD(self.stored_title, B.CLASS("left")),
                B.TD(title, B.CLASS(title_class))
            )


class GlossaryTermSearch(BatchReport):
    """
    Find phrases which match a specified glossary term.
    """

    NAME = "Glossary Term Search"
    TYPES = "HPSummaries", "PatientSummaries"
    LANGUAGES = "English", "Spanish"
    punct = "]['.,?!:;\u201c\u201d(){}<>"
    squeeze = re.compile("[{punct}]")
    non_blanks = re.compile("[^\\s_-]+")

    def __init__(self, job):
        """
        Gather the report request options.
        """

        BatchReport.__init__(self, job, self.NAME)
        doc_id = job.getParm("id")
        self.types = job.getParm("types") or self.TYPES
        if isinstance(self.types, str):
            self.types = self.types.split()
        digits = re.sub(r"[^\d]", "", doc_id)
        self.doc_id = int(digits)
        self.language = job.getParm("language") or "English"
        self.limit = int(self.job.getParm("limit") or 0)

        # For testing, divide the alloted time limit between the audiences.
        self.time_per_audience = self.max_time / len(self.types)
        self.max_time = self.time_per_audience

    def body(self):
        """
        Override the base class method to supply the report's table(s).
        """

        self.msg = "Glossary tree built"
        term = self.GlossaryTerm(self.cursor, self.doc_id)
        phrases = term.get_phrases(self.language)
        self.tree = self.GlossaryTree(phrases)
        self.job.setProgressMsg(self.msg)
        if self.language == "Spanish":
            names = ", ".join(term.spanish_names)
        else:
            names = term.name
        body = self.B.BODY(
            self.B.H1("Glossary Term Search Report"),
            self.B.H2("%s Term: %s" % (self.language, names))
        )
        if "HPSummaries" in self.types:
            body.append(self.make_table("Health professional"))
        if "PatientSummaries" in self.types:
            body.append(self.make_table("Patient"))
        return body

    def make_table(self, audience):
        """
        Build a table for glossary phrases in one set of CDR documents.
        """

        caption = "Cancer Information %s Summaries" % audience
        if self.msg:
            self.msg += "<br>"
        cursor = db.connect(user="CdrGuest").cursor()
        self.make_query(audience).execute(cursor)
        row = cursor.fetchone()
        num_rows = 0
        matches = []
        while row:
            if self.quitting_time():
                self.logger.info("table for %s truncated after %s seconds",
                                 audience, self.elapsed)
                self.max_time += self.time_per_audience
                break
            self.tree.clear_flags()
            doc_id, doc_xml, doc_title = row
            try:
                root = etree.fromstring(doc_xml)
            except Exception:
                root = etree.fromstring(doc_xml.encode("utf-8"))
            for node in root.findall("SummarySection"):
                text = " ".join(node.itertext("*")).strip()
                sec_title = cdr.get_text(node.find("Title")) or "[None]"
                self.tree.clear_flags()
                for p in self.tree.find_phrases(text):
                    mp = self.MatchingPhrase(p, doc_title, doc_id, sec_title)
                    matches.append(mp)
            row = cursor.fetchone()
            num_rows += 1
            new_msg = "Searched %d %s" % (num_rows, caption)
            self.job.setProgressMsg(self.msg + new_msg)
        self.msg += new_msg
        return GlossaryTermSearch.B.TABLE(
            GlossaryTermSearch.B.CLASS("matching-phrases"),
            GlossaryTermSearch.B.CAPTION(caption),
            GlossaryTermSearch.B.TR(
                GlossaryTermSearch.B.TH("Matching phrase"),
                GlossaryTermSearch.B.TH("DocTitle"),
                GlossaryTermSearch.B.TH("DocId"),
                GlossaryTermSearch.B.TH("Section Title")
            ),
            *[match.tr() for match in sorted(matches)]
        )

    def make_query(self, audience):
        """
        Build a DB query for finding summary documents.
        """

        query = db.Query("active_doc d", "d.id", "d.xml", "d.title")
        query.join("query_term a", "a.doc_id = d.id")
        query.join("query_term l", "l.doc_id = d.id")
        query.where("a.path = '/Summary/SummaryMetaData/SummaryAudience'")
        query.where("l.path = '/Summary/SummaryMetaData/SummaryLanguage'")
        query.where(query.Condition("a.value", audience + "s"))
        query.where(query.Condition("l.value", self.language))
        query.order("d.title")
        if self.limit:
            query.limit(self.limit)
            self.logger.info("limit to %d summary documents for testing",
                             self.limit)
        return query

    def selectors(self):
        """
        Customize the style rules applied the report's display.
        """

        selectors = BatchReport.selectors(self)
        selectors[".matching-phrases caption"] = {
            "padding": "25px 10px 10px 10px",
            "font-weight": "bold"
        }
        return selectors

    class GlossaryTerm:
        """
        Glossary term and all phrases used for it.
        """

        ENGLISH_NAME_PATH = "/GlossaryTermName/TermName/TermNameString"
        SPANISH_NAME_PATH = "/GlossaryTermName/TranslatedName/TermNameString"

        def __init__(self, cursor, id):
            """
            Assemble the names and variants for the glossary term.
            """

            query = db.Query("query_term", "value")
            query.where("path = '%s'" % self.ENGLISH_NAME_PATH)
            query.where(query.Condition("doc_id", id))
            rows = query.execute(cursor).fetchall()
            if not rows:
                raise Exception("GlossaryTermName %d not found" % id)
            self.id = id
            self.name = rows[0][0]
            query = db.Query("query_term", "value")
            query.where("path = '%s'" % self.SPANISH_NAME_PATH)
            query.where(query.Condition("doc_id", id))
            rows = query.execute(cursor).fetchall()
            self.spanish_names = [row[0] for row in rows]
            query = db.Query("external_map m", "m.value", "u.name")
            query.join("external_map_usage u", "u.id = m.usage")
            query.where(query.Condition("m.doc_id", id))
            query.where("u.name LIKE '%GlossaryTerm Phrases'")
            rows = query.execute(cursor).fetchall()

            class Variant:
                def __init__(self, value, usage):
                    self.name = value
                    self.language = "English"
                    if "Spanish" in usage:
                        self.language = "Spanish"
            self.variants = [Variant(*row) for row in rows]

        def get_phrases(self, language):
            """
            Return a sequence of language-specific names and variants for
            this term.
            """

            Phrase = GlossaryTermSearch.Phrase
            if language == "English":
                phrases = [Phrase(self.name, self.id)]
            else:
                phrases = [Phrase(n, self.id) for n in self.spanish_names]
            for variant in self.variants:
                if variant.language == language:
                    phrases.append(Phrase(variant.name, self.id))
            return phrases

    class Word:
        "Normalized token for one word in a phrase."
        def __init__(self, match):
            self.match = match
            lower_word = match.group().lower()
            self.value = GlossaryTermSearch.squeeze.sub("", lower_word)

    class Phrase:
        "Sequence of Word objects."
        def __init__(self, text, id):
            self.id, self.text = id, text
            self.words = GlossaryTermSearch.get_words(text)

    class MatchingPhrase:
        "Remembers where a glossary term phrase was found."
        def __init__(self, phrase, title, id, section):
            self.phrase = phrase
            self.title = title
            self.doc_id = id
            self.section = section

        def tr(self):
            return GlossaryTermSearch.B.TR(
                GlossaryTermSearch.B.TD(self.phrase),
                GlossaryTermSearch.B.TD(self.title),
                GlossaryTermSearch.B.TD(str(self.doc_id)),
                GlossaryTermSearch.B.TD(self.section)
            )

        def __lt__(self, other):
            return (self.title, self.phrase) < (other.title, other.phrase)

    class GlossaryNode:
        "Node in the tree of known glossary terms and their variant phrases."
        def __init__(self):
            self.doc_id, self.node_map, self.seen = None, {}, False

        def clear_flags(self):
            self.seen = False
            for node in list(self.node_map.values()):
                node.clear_flags()

    class GlossaryTree(GlossaryNode):
        "Known glossary terms and their variant phrases."
        def __init__(self, phrases):
            GlossaryTermSearch.GlossaryNode.__init__(self)
            for phrase in phrases:
                current_map, current_node = self.node_map, None
                for word in phrase.words:
                    value = word.value
                    if value:
                        if value in current_map:
                            current_node = current_map[value]
                        else:
                            current_node = GlossaryTermSearch.GlossaryNode()
                            current_map[value] = current_node
                        current_map = current_node.node_map
                if current_node:
                    current_node.doc_id = phrase.id

        def find_phrases(self, text):
            "Returns sequence of strings for matching phrases."
            phrases = []
            words = GlossaryTermSearch.get_words(text)
            words_left = len(words)
            current_map = self.node_map
            current_word = 0
            while words_left > 0:
                nodes = []
                current_map = self.node_map

                # Find the longest chain of matching words from this point.
                while len(nodes) < words_left:
                    word = words[current_word + len(nodes)]
                    node = current_map.get(word.value)
                    if not node:
                        break
                    nodes.append(node)
                    current_map = node.node_map

                # See if the chain (or part of it) matches a glossary term.
                while nodes:
                    lastNode = nodes[-1]

                    # A doc_id means this node is the end of a glossary term.
                    if lastNode.doc_id and not lastNode.seen:
                        start = words[current_word].match.start()
                        end = words[current_word + len(nodes) - 1].match.end()
                        phrase = text[start:end]
                        phrase = phrase.strip(GlossaryTermSearch.punct)
                        phrases.append(phrase)
                        lastNode.seen = True
                        break
                    nodes.pop()

                # Skip past the matched term (if any) or the current word.
                words_to_move = nodes and len(nodes) or 1
                words_left -= words_to_move
                current_word += words_to_move
            return phrases

    @staticmethod
    def get_words(text):
        "Extract Word tokens from phrase or text block."
        words = []
        for w in GlossaryTermSearch.non_blanks.finditer(text):
            words.append(GlossaryTermSearch.Word(w))
        return words

    @classmethod
    def test_harness(cls):
        """
        Perform a test run from the command line.
        """

        lung_cancer = 445043
        parser = cls.arg_parser()
        parser.add_argument("--limit", type=int, default=100,
                            help="maximum number of documents to process for "
                            "each audience")
        parser.add_argument("--doc-id", dest="id", default=str(lung_cancer),
                            help="CDR ID of glossary term document")
        parser.add_argument("--types", choices=cls.TYPES, nargs="*")
        parser.add_argument("--language", choices=cls.LANGUAGES,
                            default="English")
        cls.run_test(parser)


class PronunciationRecordingsReport(BatchReport):
    """
    Report for tracking audio pronunciation recordings.
    """

    NAME = "Audio Pronunciation Recordings Tracking Report"
    LANGS = ("ALL", "en", "es")

    def __init__(self, job):
        BatchReport.__init__(self, job, self.NAME)
        self.logger.info("Starting %s job", self.NAME)
        self.job = job
        self.begin = job.getParm('start')
        self.end = job.getParm('end')
        self.language = job.getParm('language')
        self.verbose = job.getParm('verbose') == "True"
        self.limit = int(self.job.getParm("limit") or 0)
        self.format = "excel"

    def add_sheets(self):
        """
        Add the worksheet for the report.

        Date parameters have already been scrubbed by the front end.
        """

        begin, end = self.begin, self.end
        fields = "t.doc_id", "t.value", "d.created"
        query = db.Query("query_term t", *fields).unique()
        query.join("query_term c", "c.doc_id = t.doc_id")
        query.join("query_term e", "e.doc_id = t.doc_id")
        query.join("doc_created d", "d.doc_id = t.doc_id")
        query.where("t.path = '/Media/MediaTitle'")
        query.where("c.path = '/Media/MediaContent/Categories/Category'")
        query.where("e.path = '/Media/PhysicalMedia/SoundData/SoundEncoding'")
        query.where("e.value = 'MP3'")
        query.where("c.value = 'pronunciation'")
        query.where(f"d.created BETWEEN '{begin}' and '{end} 23:59:59'")
        if self.language == 'en':
            query.where("t.value NOT LIKE '%-Spanish'")
        elif self.language == 'es':
            query.where("t.value LIKE '%-Spanish'")
        rows = query.execute(self.cursor).fetchall()
        self.logger.info("fetched %d rows from the database", len(rows))
        docs = []
        for doc_id, title, created in rows:
            docs.append(self.MediaDoc(self.cursor, doc_id, title, created))
            message = f"processed {len(docs):d} of {len(rows):d}"
            self.job.setProgressMsg(message)
            if self.verbose:
                sys.stderr.write(f"\r{message}")
            if self.limit and len(docs) >= self.limit:
                self.logger.info("abbrieviating test run at %d of %d docs",
                                 len(docs), len(rows))
                break
            if self.quitting_time():
                self.logger.info("test concluded after %s seconds",
                                 self.elapsed)
                break
        self.excel.add_sheet("Pronunciations")
        widths = 10, 35, 35, 30, 15, 30, 15, 15, 15, 15
        headers = ("CDRID", "Title", "Proposed Glossary Terms",
                   "Processing Status", "Processing Status Date",
                   "Comments", "Last Version Publishable?",
                   "Date First Published", "Date Last Modified",
                   "Published Date")
        assert len(widths) == len(headers)
        for i, width in enumerate(widths, start=1):
            self.excel.set_width(i, width)
        lang = {"en": "English", "es": "Spanish"}.get(self.language, "ALL")
        title = f"Audio Pronunciation Recordings Tracking Report - {lang}"
        if self.throttle:
            title += " [TRUNCATED FOR TESTING]"
        styles = dict(alignment=self.excel.center_middle, font=self.excel.bold)
        self.excel.merge(1, 1, 1, len(headers))
        self.excel.write(1, 1, title, styles)
        dates = f"From {self.begin} - {self.end}"
        self.excel.merge(2, 1, 2, len(headers))
        self.excel.write(2, 1, dates, styles)
        for col, header in enumerate(headers, start=1):
            self.excel.write(3, col, header, styles)
        row = 4
        for doc in sorted(docs):
            row = doc.add_row(self.excel, row)

    class MediaDoc:
        """
        CDR Media document for an audio pronunciation file.

        Instance values:
            doc_id - unique CDR ID for the document
            title - title of the Media document
            created - the date the document was first save in the repository
            status - processing status for the Media document
            first_pub - the date the document was first published
            pub_date - the date the document was last published
            last_mod - the date the document most recently changed
            glossary_terms - glossary terms linked from this Media document
            last_ver_publishable - "Y" if the latest version is publishable
        """

        def __init__(self, cursor, doc_id, title, created):
            """
            Collect the values needed on the report for this Media document.
            """

            self.doc_id = doc_id
            self.title = title
            self.created = created
            self.status = self.status_date = None
            self.pub_date = self.first_pub = self.last_mod = None
            self.glossary_terms = []
            self.comments = []
            self.last_ver_publishable = "N"
            versions = cdr.lastVersions("guest", "CDR%010d" % doc_id)
            if versions[0] == versions[1] and versions[0] > 0:
                self.last_ver_publishable = "Y"
            query = db.Query("last_doc_publication", "dt")
            query.where(query.Condition("doc_id", doc_id))
            rows = query.execute(cursor).fetchall()
            if rows:
                self.pub_date = rows[0][0]
            query = db.Query("query_term n", "n.value")
            query.join("query_term u", "u.int_val = n.doc_id")
            query.where(query.Condition("u.doc_id", doc_id))
            query.where("n.path = '/GlossaryTermName/TermName/TermNameString'")
            query.where("u.path = '/Media/ProposedUse/Glossary/@cdr:ref'")
            for row in query.order("n.value").execute(cursor).fetchall():
                self.glossary_terms.append(row[0])
            query = db.Query("document", "first_pub", "xml")
            query.where(query.Condition("id", doc_id))
            self.first_pub, xml = query.execute(cursor).fetchone()
            try:
                root = etree.fromstring(xml)
            except Exception:
                root = etree.fromstring(xml.encode("utf-8"))
            for node in root.findall('DateLastModified'):
                self.last_mod = node.text
            for node in root.findall('ProcessingStatuses/ProcessingStatus'):
                for child in node:
                    if child.tag == 'ProcessingStatusValue':
                        self.status = child.text
                    elif child.tag == 'ProcessingStatusDate':
                        self.status_date = child.text
                    elif child.tag == 'Comment':
                        self.comments.append(child.text)
                break

        def add_row(self, book, row):
            """
            Add a row to the worksheet for this Media document.
            """

            left = dict(alignment=book.left)
            center = dict(alignment=book.center)
            book.write(row, 1, self.doc_id, center)
            book.write(row, 2, self.title, left)
            book.write(row, 3, "; ".join(self.glossary_terms), left)
            book.write(row, 4, self.status, left)
            book.write(row, 5, self.fix_date(self.status_date), center)
            book.write(row, 6, "; ".join(self.comments), left)
            book.write(row, 7, self.last_ver_publishable, center)
            book.write(row, 8, self.fix_date(self.first_pub), center)
            book.write(row, 9, self.fix_date(self.last_mod), center)
            book.write(row, 10, self.fix_date(self.pub_date), center)
            return row + 1

        def __lt__(self, other):
            """
            Support sorting of the Media documents.
            """

            return self.sortkey < other.sortkey

        @property
        def sortkey(self):
            """
            Articulated sort key for a media document.
            """

            last_ver_publishable = 0 if self.last_ver_publishable == "Y" else 1
            last_mod = self.last_mod or "0000-00-00"
            status = self.status or ""
            return status, last_ver_publishable, last_mod

        @staticmethod
        def fix_date(date):
            """
            Prepare a date value for display on the report.
            """

            return date and str(date)[:10] or ""

    @classmethod
    def test_harness(cls):
        """
        Perform a test run from the command line.
        """

        parser = cls.arg_parser(".xlsx")
        parser.add_argument("--start", default="2015-01-01")
        parser.add_argument("--end", default=str(datetime.date.today()))
        parser.add_argument("--language", choices=cls.LANGS, default="ALL")
        parser.add_argument("--limit", type=int, default=10)
        cls.run_test(parser, format="excel")


class CitationsInSummaries(BatchReport):
    """
    Generates Excel workbook showing Summaries linked to Citation documents.
    """

    NAME = "Citations In Summaries"
    COLS = (
        cdrcgi.Reporter.Column("Citation ID", width="60px"),
        cdrcgi.Reporter.Column("Citation Title", width="1000px"),
        cdrcgi.Reporter.Column("PMID", width="70px"),
        cdrcgi.Reporter.Column("Summary ID", width="60px"),
        cdrcgi.Reporter.Column("Summary Title", width="500px"),
        cdrcgi.Reporter.Column("Summary Boards", width="500px"),
    )
    OPTS = dict(columns=COLS, caption=NAME, sheet_name=NAME)
    LINK_PATH = "/Summary/%CitationLink/@cdr:ref"

    def __init__(self, job):
        """Initialize the report object."""

        BatchReport.__init__(self, job, self.NAME)
        self.logger.info("Starting %s job", self.NAME)
        self.limit = self.int_parm("limit")
        self.debug = self.job.getParm("debug")
        self.format = "excel"
        self.session = Session("guest")

    def add_sheets(self):
        """Populate the workbook for the report."""

        self.table.add_worksheet(self.excel)
        self.logger.info("report added to workbook")

    @cached_property
    def rows(self):
        """Rows for the report's table."""

        query = db.Query("active_doc c", "c.id").order("c.id DESC").unique()
        query.join("query_term q", "q.int_val = c.id")
        query.join("active_doc s", "s.id = q.doc_id")
        query.where(f"q.path LIKE '{self.LINK_PATH}'")
        if self.limit:
            query.limit(self.limit)
            message = "limit to %d summary documents for testing"
            self.logger.info(message, self.limit)
        ids = [row.id for row in query.execute(self.cursor).fetchall()]
        self.logger.info("found %d citations", len(ids))
        rows = []
        done = 0
        for id in ids:
            if self.quitting_time():
                self.logger.info("test stopped after %s seconds", self.elapsed)
                break
            rows += self.Citation(self, id).rows
            done += 1
            message = (
                f"assembled {len(rows)} report rows from {done} of "
                f"{len(ids)} citations"
            )
            self.job.setProgressMsg(message)
        self.logger.info("collected %d rows for report", len(rows))
        message += "; building Excel report from these rows"
        message += " (takes about an hour)"
        self.job.setProgressMsg(message)
        return rows

    @cached_property
    def summaries(self):
        """Cache summary info here so we don't have to keep fetching it."""
        return {}

    @cached_property
    def table(self):
        """Table for the report's single worksheet."""

        opts = dict(logger=self.logger, **self.OPTS)
        return cdrcgi.Reporter.Table(self.rows, **opts)

    class Document:
        """Base class for Citation and Summary"""

        HOST = Tier("PROD").hosts["APPC"]
        BASE = cdrcgi.Controller.BASE
        URL = f"https://{HOST}{BASE}/QcReport.py?DocId={{:d}}"

        @cached_property
        def title(self):
            """Title from the Citation document."""
            return Doc(self.control.session, id=self.id).title

    class Citation(Document):
        """Creates spanned rows for summaries which link to the Citation."""

        PMID_PATH = "/Citation/PubmedArticle/MedlineCitation/PMID"

        def __init__(self, control, id):
            """Save the caller's values.

            Required positional arguments:
              control - access to the database and the cache of summary info
              id - unique CDR ID for the Citation document
            """

            self.control = control
            self.id = id

        @cached_property
        def pmid(self):
            """PubMed ID for the citation."""

            query = db.Query("query_term", "value")
            query.where(f"path = '{self.PMID_PATH}'")
            query.where(query.Condition("doc_id", self.id))
            for row in query.execute(self.control.cursor).fetchall():
                pmid = row.value.strip()
                if pmid:
                    return pmid
            return None

        @cached_property
        def rows(self):
            """This citation's Table rows for the report."""

            url = self.URL.format(self.id)
            rowspan = len(self.summaries)
            opts = {} if rowspan < 2 else dict(rowspan=str(rowspan))
            row = [
                cdrcgi.Reporter.Cell(self.id, center=True, href=url, **opts),
                cdrcgi.Reporter.Cell(self.title.strip(), **opts),
                cdrcgi.Reporter.Cell(self.pmid, **opts),
                *self.summaries[0].row,
            ]
            rows = [row]
            for summary in self.summaries[1:]:
                rows.append(summary.row)
            self.control.logger.debug("citation has %d rows", len(rows))
            return rows

        @cached_property
        def summaries(self):
            """The summaries linked to this citation."""

            query = db.Query("active_doc s", "s.id").order("s.id")
            query.join("query_term q", "q.doc_id = s.id")
            query.where(query.Condition("q.int_val", self.id))
            query.where(f"q.path LIKE '{self.control.LINK_PATH}'")
            summaries = []
            for row in query.unique().execute(self.control.cursor).fetchall():
                if row.id not in self.control.summaries:
                    summary = self.control.Summary(self.control, row.id)
                    self.control.summaries[row.id] = summary
                summaries.append(self.control.summaries[row.id])
            return summaries

    class Summary(Document):
        """Summary linking to one or more citations included in the report."""

        def __init__(self, control, id):
            """Save the caller's values.

            Required positional arguments:
              control - access to the database
              id - unique CDR ID for the Summary document
            """

            self.control = control
            self.id = id

        @cached_property
        def boards(self):
            """Board names for this summary (or its English original)."""

            query = db.Query("query_term", "int_val")
            query.where("path = '/Summary/TranslationOf/@cdr:ref'")
            query.where(query.Condition("doc_id", self.id))
            rows = query.execute(self.control.cursor).fetchall()
            summary_id = rows[0][0] if rows else self.id
            query = db.Query("query_term", "value").order("value")
            query.where("path = '/Summary/SummaryMetaData/PDQBoard/Board'")
            query.where(query.Condition("doc_id", summary_id))
            rows = query.unique().execute(self.control.cursor).fetchall()
            return [row.value for row in rows]

        @cached_property
        def row(self):
            """Table row with report information for this summary document."""

            url = self.URL.format(self.id)
            link = cdrcgi.Reporter.Cell(self.id, center=True, href=url)
            return [link, self.title, self.boards]

    @classmethod
    def test_harness(cls):
        """Perform a test run from the command line."""

        parser = cls.arg_parser(suffix=".xlsx")
        help = "limit the maximum number of citations for testing"
        parser.add_argument("--limit", type=int, default=100, help=help)
        cls.run_test(parser, format="excel")


class Control:
    """
    Top-level router for report requests.

    To implement and install a new report:
      1. Create a new class in this file.
      2. Provided the new class with a unique class-level NAME value.
      3. Implement a run() method which takes a cdrbatch.CdrBatch object.
      4. Add the name of the new class to the CLASSES sequence below.
      5. Implement a CGI web interface which queues up requests for the report.

    It isn't necessary to derive the report's class from BatchReport at
    the top of the file, but it will be much easier to do it that way
    (and for the programmers who come after you to maintain).
    """

    CLASSES = (
        BrokenExternalLinks,
        PageTitleMismatches,
        GlossaryTermSearch,
        PronunciationRecordingsReport,
        CitationsInSummaries,
    )

    @classmethod
    def run(cls):
        """
        Perform a live or test run of a batch CDR report.
        """

        logger = cdr.Logging.get_logger("reports", level="info")
        job_id = sys.argv.pop(1)
        if job_id.isdigit():
            logger.info("CdrLongReports: job id %s", job_id)
            job = cdrbatch.CdrBatch(job_id)
            try:
                job_name = job.getJobName()
                job_class = cls.get_job_class(job_name)
                if not job_class:
                    raise Exception(f"Job class for {job_name} not found")
                job_class(job).run()
            except Exception as e:
                logger.exception("failure executing job %s", job_id)
                job.fail("Caught exception: %s" % e)
        else:
            job_name = job_id
            job_class = cls.get_job_class(job_name)
            if not job_class:
                sys.stderr.write("job type %s not found\n" % job_name)
            elif not hasattr(job_class, "test_harness"):
                sys.stderr.write("%s test harness not implemented" % job_name)
            else:
                job_class.test_harness()

    @classmethod
    def get_job_class(cls, name):
        """
        Find the report class which matched the report type name.
        """

        for job_class in cls.CLASSES:
            if job_class.NAME == name:
                return job_class
        return None

    @classmethod
    def run_tests(cls):
        for report_class in cls.get_testable_report_classes():
            report_class.test_harness()

    @classmethod
    def usage(cls):
        """
        Remind the user how to run tests from the command line.
        """

        explanation = (
            "",
            "usage: CdrLongReports.py JOB-ID",
            "   or: CdrLongReports.py JOB-NAME [OPTIONS ...]",
            "   or: CdrLongReports.py --run-tests",
            "",
            "The first form is used by the production batch report system.",
            "It could also be used to re-run a failed job by hand.",
            "The second form is for testing a job type from the command line.",
            "Be sure to enclose the job name in double quotes if the name",
            "includes spaces. To see the options available for a particular",
            "job type, add the option --help after the job type name. For",
            "example:",
            "",
            "    CdrLongReports.py \"Broken URLs\" --help",
            "",
            "All job types provide reasonable default test options.",
            "",
            "Here are the available reports:",
            "",
        )
        indent = " " * 8
        for line in explanation:
            if line:
                sys.stderr.write(indent)
            sys.stderr.write("%s\n" % line)
        for job_class in cls.get_testable_report_classes():
            sys.stderr.write("%s%s\n" % (indent * 2, job_class.NAME))
        tail = "The last form of the command tests all of these job types."
        sys.stderr.write("\n%s%s\n" % (indent, tail))
        sys.stderr.write("\n")

    @classmethod
    def get_testable_report_classes(cls):
        """
        Create a sequence of report classes which support testing.
        """

        testable = []
        for job_class in cls.CLASSES:
            if hasattr(job_class, "test_harness"):
                testable.append(job_class)
        return testable


if __name__ == "__main__":
    """
    Make it possible to load this file as a module, without executing
    anything but the class definitions. Also, give some help if needed.
    """

    if len(sys.argv) == 2 and sys.argv[1] == "--run-tests":
        sys.argv.pop(1)
        Control.run_tests()
    elif len(sys.argv) > 1:
        Control.run()
    else:
        Control.usage()
