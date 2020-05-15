"""Common routines for creating CDR web forms.

This module has the following sections:

    Imports and global values

    Tools for creating CGI forms and reports

        Controller
            Base class for top-level controller of CGI scripts
        FormFieldFactory
            Provides convenient class methods for creating HTML form fields
        HTMLPage
            Creates a CDR web page for forms and reports using a common
            customizable layout
        Reporter
            Creates CDR reports which can be sent to the web browser or
            to Excel
        Excel
            Wrapper for building Excel workbooks using OpenPyXl
        AdvancedSearch
            Search forms for each of the CDR document types

    Older legacy versions of these tools
        Control, Page, Report, ExcelStyle

        Keep these older class as long as we still have scripts
        which haven't been converted to their successors

    Miscellaneous global function

        Most of these should be gradually retired, in favor of
        the core CDR API functionality.
"""

# Packages from the standard library.
import cgi
import cgitb
import collections
import copy
import datetime
from email.utils import parseaddr as parse_email_address
from html import escape as html_escape
from io import BytesIO
from operator import itemgetter
import os
import re
import sys
import textwrap
import time
import urllib.request, urllib.parse, urllib.error
import xml.sax.saxutils

# Third-party libraries/packages.
import lxml.etree as etree
import lxml.html
import lxml.html.builder
import openpyxl
import xlsxwriter
import xlwt

# Project modules.
import cdr
from cdrapi import db
from cdrapi.settings import Tier
from cdrapi.users import Session


# Turn on debugging for CGI scripts.
cgitb.enable(display = cdr.isDevHost(), logdir = cdr.DEFAULT_LOGDIR)


# Global values
VERSION = "202003230906"
CDRCSS = "/stylesheets/cdr.css?v=%s" % VERSION
DATETIMELEN = len("YYYY-MM-DD HH:MM:SS")
TAMPERING = "CGI parameter tampering detected"
USERNAME = "UserName"
PASSWORD = "Password"
PORT     = "Port"
SESSION  = "Session"
REQUEST  = "Request"
DOCID    = "DocId"
FILTER   = "Filter"
BASE     = "/cgi-bin/cdr"
MAINMENU = "Admin Menu"
DEVTOP   = "Developer Menu"
TIER     = Tier()
WEBSERVER= os.environ.get("SERVER_NAME") or TIER.hosts.get("APPC")
SPLTNAME = WEBSERVER.lower().split(".")
THISHOST = SPLTNAME[0]
ISPLAIN  = "." not in THISHOST
DOMAIN   = "." + ".".join(SPLTNAME[1:])
DAY_ONE  = cdr.URDATE


#----------------------------------------------------------------------#
#                        NEW CLASSES -- USE THESE                      #
#----------------------------------------------------------------------#


class Controller:
    """Base class for top-level controller for a CGI script.

    Includes methods for displaying a form (typically for a report)
    and for rendering the requested report.

    This will gradually replace the older `Control` class, which
    is built around the use of `Page` objects, which created
    HTML pages using direct string manipulation instead of real
    HTML parser objects.

    Typically, a web script will create a derived class using this
    class as its base. It is possible to override the constructor,
    but this is not necessary. If you do, be sure to invoke the
    base class constructor from the overriding implementation.

    The `run()` method is the top-level processing starting point.
    That method calls the `show_report()` method if the Submit
    button was clicked, or `show_form()` if no button was clicked.
    The derived class implements `populate_form()` to add the
    fields it needs for its job, and overrides the `build_tables()`
    method to provide the report's data.
    """

    PAGE_TITLE = "CDR Administration"
    TITLE = PAGE_TITLE
    SUBTITLE = None
    REPORTS_MENU = SUBMENU = "Reports Menu"
    ADMINMENU = MAINMENU
    DEVMENU  = DEVTOP
    SUBMIT = "Submit"
    LOG_OUT = "Log Out"
    FORMATS = "html", "excel"
    LOGNAME = "reports"
    LOGLEVEL = "INFO"
    METHOD = "post"
    AUDIENCES = "Health Professional", "Patient"
    LANGUAGES = "English", "Spanish"
    INCLUDE_ANY_LANGUAGE_CHECKBOX = INCLUDE_ANY_AUDIENCE_CHECKBOX = False
    SUMMARY_SELECTION_METHODS = "id", "title", "board"
    EMAIL_PATTERN = re.compile(r"[^@]+@[^@\.]+\.[^@]+$")
    KEEP_COMPLETE_TITLES = False

    def __init__(self, **opts):
        """Set up a skeletal controller."""

        self.__started = datetime.datetime.now()
        self.__opts = opts
        self.logger.info("started %s", self.subtitle or "controller")

    def run(self):
        """Override in derived class if there are custom actions."""

        try:
            if self.request:
                if self.request == self.ADMINMENU:
                    self.redirect("Admin.py")
                elif self.request == self.REPORTS_MENU:
                    self.redirect("Reports.py")
                elif self.request == self.DEVMENU:
                    self.redirect("DevSA.py")
                elif self.request == self.LOG_OUT:
                    logout(self.session.name)
                elif self.request and self.request == self.SUBMIT:
                    self.show_report()
                else:
                    self.show_form()
            else:
                self.show_form()
        except Exception as e:
            self.logger.exception("Controller.run() failure")
            bail(e)

    def show_form(self):
        """Populate an HTML page with a form and fields and send it."""
        self.populate_form(self.form_page)
        self.form_page.send()

    def show_report(self):
        """Override this method if you need to make some tweaks.

        A typical case would be:

            def show_report(self):
                self.report.body.set("id", "activity-report")
                self.report.send(self.format)

        Another situation in which you would override this method
        would be if you have a non-tabular report to be sent.
        """

        elapsed = self.report.page.html.get_element_by_id("elapsed", None)
        if elapsed is not None:
            elapsed.text = str(self.elapsed)
        self.report.send(self.format)

    def populate_form(self, page):
        """Stub, to be overridden by real controllers."""

    def build_tables(self):
        """Stub, to be overridden by real controllers."""
        return []

    def log_elapsed(self):
        """Record how long this took."""
        self.logger.info(f"elapsed: {self.elapsed.total_seconds():f}")

    def redirect(self, where, session=None, **params):
        """Send the user to another page.

        Pass:
            where - base URL, up to but not including parameters
            session - session string or object to override this session (opt)
            params - dictionary of other named parameters
        """

        session = session or self.session
        self.navigate_to(where, session, **params)

    def load_group(self, group):
        """Fetch the active members of a named user group.

        Pass:
            group - name of group to fetch

        Return:
            dictionary of user names indexed by user ID
        """

        query = db.Query("usr u", "u.id", "u.fullname", "u.name")
        query.join("grp_usr j", "j.usr = u.id")
        query.join("grp g", "g.id = j.grp")
        query.where("u.expired IS NULL")
        query.where(query.Condition("g.name", group))
        rows = query.execute(self.cursor).fetchall()
        class Group:
            def __init__(self, rows):
                self.map = {}
                for row in rows:
                    self.map[row.id] = row.fullname or row.name
                key = lambda pair: pair[1].lower()
                self.items = sorted(self.map.items(), key=key)
            def __getvalue__(self, key):
                return self.map.get(key)
        return Group(rows)

    def load_valid_values(self, table_name):
        """Factor out logic for collecting a valid values set.

        This works because our tables for valid values have the
        same structure.

        Pass:
            table_name - name of the database table for the values

        Return:
            a populated `Values` object
        """

        query = self.Query(table_name, "value_id", "value_name")
        rows = query.order("value_pos").execute(self.cursor).fetchall()
        class Values:
            def __init__(self, rows):
                self.map = {}
                self.values = []
                for value_id, value_name in rows:
                    self.map[value_id] = value_name
                    self.values.append((value_id, value_name))
        return Values(rows)

    def make_url(self, script, **params):
        """Create a URL.

        Pass:
            script - string for base of url (can be relative or absolute)
            params - dictionary of named parameters for the URL

        Return:
            value appropriate for the href attribute of a link
        """

        if SESSION not in params:
            params[SESSION] = self.session.name
        params = urllib.parse.urlencode(params)
        return f"{script}?{params}"

    def add_summary_selection_fields(self, page, **kwopts):
        """
        Display the fields used to specify which summaries should be
        selected for a report, using one of several methods:

            * by summary document ID
            * by summary title
            * by summary board

        There are two branches taken by this method. If the user has
        elected to select a summary by summary title, and the summary
        title fragment matches more than one summary, then a follow-up
        page is presented on which the user selects one of the summaries
        and re-submits the report request. Otherwise, the user is shown
        options for choosing a selection method, which in turn displays
        the fields appropriate to that method dynamically. We also add
        JavaScript functions to handle the dynamic control of field display.

        Pass:
            page     - Page object on which to show the fields
            titles   - an optional array of SummaryTitle objects
            audience - if False, omit Audience buttons (default is True)
            language - if False, omit Language buttons (default is True)
            id-label - optional string for the CDR ID field (defaults
                       to "CDR ID" but can be overridden, for example,
                       to say "CDR ID(s)" if multiple IDs are accepted)
            id-tip   - optional string for the CDR ID field for popup
                       help (e.g., "separate multiple IDs by spaces")

        Return:
            nothing (the form object is populated as a side effect)
        """

        #--------------------------------------------------------------
        # Show the second stage in a cascading sequence of the form if we
        # have invoked this method directly from build_tables(). Widen
        # the form to accomodate the length of the title substrings
        # we're showing.
        #--------------------------------------------------------------
        titles = kwopts.get("titles")
        if titles:
            page.form.append(page.hidden_field("selection_method", "id"))
            page.form.append(page.hidden_field("format", self.format))
            fieldset = page.fieldset("Choose Summary")
            page.add_css("fieldset { width: 600px; }")
            for t in titles:
                opts = dict(label=t.display, value=t.id, tooltip=t.tooltip)
                fieldset.append(page.radio_button("cdr-id", **opts))
            page.form.append(fieldset)
            self.new_tab_on_submit(page)

        else:
            # Fields for the original form.
            fieldset = page.fieldset("Selection Method")
            methods = "PDQ Board", "CDR ID", "Summary Title"
            checked = True
            for method in methods:
                value = method.split()[-1].lower()
                opts = dict(label=f"By {method}", value=value, checked=checked)
                fieldset.append(page.radio_button("selection_method", **opts))
                checked = False
            page.form.append(fieldset)
            self.add_board_fieldset(page)
            if opts.get("audience", True):
                self.add_audience_fieldset(page)
            if opts.get("language", True):
                self.add_language_fieldset(page)
            fieldset = page.fieldset("Summary Document ID")
            fieldset.set("class", "by-id-block")
            label = kwopts.get("id-label", "CDR ID")
            opts = dict(label=label, tooltip=kwopts.get("id-tip"))
            fieldset.append(page.text_field("cdr-id", **opts))
            page.form.append(fieldset)
            fieldset = page.fieldset("Summary Title")
            fieldset.set("class", "by-title-block")
            tooltip = "Use wildcard (%) as appropriate."
            fieldset.append(page.text_field("title", tooltip=tooltip))
            page.form.append(fieldset)
            page.add_script(self.summary_selection_js)

    def add_board_fieldset(self, page):
        """Add checkboxes for the PDQ Editorial Boards.

        Pass:
            page - object on which we place the fields
        """

        fieldset = page.fieldset("Board")
        fieldset.set("class", "by-board-block")
        fieldset.set("id", "board-set")
        opts = dict(label="All Boards", value="all", checked=True)
        fieldset.append(page.checkbox("board", **opts))
        boards = self.get_boards()
        for id in sorted(boards, key=boards.get):
            opts = dict(value=id, label=boards[id], classes="ind")
            fieldset.append(page.checkbox("board", **opts))
        page.form.append(fieldset)

    def add_audience_fieldset(self, page):
        """Add radio buttons for PDQ audience.

        Pass:
            page - object on which we place the fields
        """

        fieldset = page.fieldset("Audience")
        fieldset.set("class", "by-board-block")
        fieldset.set("id", "audience-block")
        default = self.default_audience
        if self.INCLUDE_ANY_AUDIENCE_CHECKBOX:
            checked = False if default else True
            opts = dict(label="Any", value="", checked=checked)
            fieldset.append(page.radio_button("audience", **opts))
        elif not default:
            default = self.AUDIENCES[0]
        for value in self.AUDIENCES:
            checked = True if value == default else False
            opts = dict(value=value, checked=checked)
            fieldset.append(page.radio_button("audience", **opts))
        page.form.append(fieldset)

    def add_language_fieldset(self, page):
        """Add radio buttons for summary language.

        Pass:
            page - object on which we place the fields
        """

        fieldset = page.fieldset("Language")
        fieldset.set("class", "by-board-block")
        fieldset.set("id", "language-block")
        checked = True
        if self.INCLUDE_ANY_LANGUAGE_CHECKBOX:
            opts = dict(label="Any", value="", checked=True)
            fieldset.append(page.radio_button("language", **opts))
            checked = False
        for value in self.LANGUAGES:
            opts = dict(value=value, checked=checked)
            fieldset.append(page.radio_button("language", **opts))
            checked = False
        page.form.append(fieldset)

    def get_boards(self):
        """Construct a dictionary of PDQ board names indexed by CDR ID."""

        boards = cdr.Board.get_boards().values()
        return dict([(board.id, board.short_name) for board in boards])

    def new_tab_on_submit(self, page):
        """
        Take over the onclick event for the Submit button in order to
        show the report in a new tab. This avoids the problem of the
        request to resubmit a form unnecessarily when navigating back
        to the base report request page through an intermediate page
        (such as the one to choose from multiple matching titles).

        Pass:
            page - reference to the page object to which the script is added
        """

        page.add_script(f"""\
jQuery("input[value='Submit']").click(function(e) {{
    var parms = jQuery("form").serialize();
    if (!/Request=Submit/.test(parms)) {{
        if (parms)
            parms += "&";
        parms += "Request=Submit";
    }}
    var url = "{self.script}?" + parms;
    window.open(url, "_blank");
    e.preventDefault();
}});""")

    @staticmethod
    def add_date_range_to_caption(caption, start, end):
        """Format caption with date range (we do this a lot).

        Pass:
            caption - base string for the start of the caption
            start - optional beginning of the date range
            end - optional finish of the date range

        Return:
            possibly altered string for the table caption
        """

        if start:
            if end:
                return f"{caption} Between {start} and {end}"
            return f"{caption} Since {start}"
        elif end:
            return f"{caption} Through {end}"
        return caption

    @staticmethod
    def navigate_to(where, session, **params):
        """Send the user to another page.

        This is the non-instance version.
        Pass:
            where - base URL, up to but not including parameters (required)
            session - session string or object (required)
            params - dictionary of other named parameters
        """

        params[SESSION] = session
        params = urllib.parse.urlencode(params)
        print(f"Location:https://{WEBSERVER}{BASE}/{where}?{params}\n")
        sys.exit(0)

    @staticmethod
    def send_page(page, text_type="html"):
        """Send a string back to the web server using UTF-8 encoding.

        Pass:
            page - Unicode string for the page
            text_type - typically "html" but sometimes "xml"
        """

        string = f"Content-type: text/{text_type};charset=utf-8\n\n{page}"
        sys.stdout.buffer.write(string.encode("utf-8"))
        sys.exit(0)

    @staticmethod
    def parse_date(iso_date):
        """Convert a date string to a `datetime.date` object.

        Pass:
            iso_date - optional string for the date

        Return:
            None if the string is None or empty, otherwise a date object
        """

        if iso_date is None or not iso_date.strip():
            return None
        year, month, date = iso_date.strip().split("-")
        return datetime.date(int(year), int(month), int(date))

    @classmethod
    def parse_email_address(cls, address):
        """Pull out an email address from a string.

        Performs a very simple validation, may improve it later, but full
        RFC requires an incredible thousand-character regular expression.

        Pass:
            address - string which might have a display portion
                      (e.g., "Joe Blow <joe@example.com>")

        Return:
            address portion of the string if validation passes
            None if no valid address is found
        """

        realname, address = parse_email_address(address)
        if address and cls.EMAIL_PATTERN.match(address) and ".." not in address:
            return address
        return None

    @staticmethod
    def toggle_display(function_name, show_value, class_name):
        """Create JavaScript function to show or hide elements.

        Pass:
            function_name  - name of the JavaScript function to create
            show_value     - controlling element's value causing show
            class_name     - class of which the controlled blocks are members
        Return:
            source code for JavaScript function
        """

        return f"""\
function {function_name}(value) {{
    if (value == "{show_value}")
        jQuery(".{class_name}").show();
    else
        jQuery(".{class_name}").hide();
}}"""

    @property
    def HTMLPage(self):
        """Allow overriding of page class."""
        return HTMLPage

    @property
    def Query(self):
        """Convenience reference to database query class object."""
        return db.Query

    @property
    def Reporter(self):
        """Allow overriding of page class."""
        return Reporter

    @property
    def banner(self):
        """Title displayed boldly at the top of the page."""
        return self.__opts.get("banner") or self.title

    @property
    def buttons(self):
        """Sequence of names for request buttons to be provided

        By default, SUBMIT, SUBMENU, ADMINMENU and LOG_OUT will be used.
        Any of these can be suppressed by setting the class value to None
        in the derived class. Alternatively, the "buttons" keyword arg
        can be passed with a list of button names. Downstream these
        names will be converted to HTML input elements with the name
        attribute set to "Submit". Pass an empty list to the constructor
        for the "buttons" keyword argument to have no buttons displayed
        (None won't have the same effect).
        """

        if not hasattr(self, "_buttons"):
            buttons = self.SUBMIT, self.SUBMENU, self.ADMINMENU, self.LOG_OUT
            self._buttons = self.__opts.get("buttons")
            if self._buttons is None:
                self._buttons = []
                for button in buttons:
                    if button:
                        self._buttons.append(button)
        return self._buttons

    @property
    def conn(self):
        """Database connection for this controller."""
        if not hasattr(self, "_conn"):
            self._conn = db.connect()
        return self._conn

    @property
    def cursor(self):
        """Database cursor for this controller."""
        if not hasattr(self, "_cursor"):
            self._cursor = self.conn.cursor()
        return self._cursor

    @property
    def default_audience(self):
        """Let a subclass override the default for the audience picklist."""
        return None

    @property
    def doc_titles(self):
        """Cached lookup of CDR document titles by ID.

        By default, only the portion of the title column's value before
        the first semicolon is used. If "Inactive;" is at the front of
        the title string the second segment of the title is used instead
        (if it exists) and " (inactive)" is appended. To preserve the
        entire contents of the title column's values, set the class-level
        property `KEEP_COMPLETE_TITLES` to `True` in the derived class.
        """

        if not hasattr(self, "_doc_titles"):
            class DocTitles(collections.UserDict):
                def __init__(self, control):
                    self.__control = control
                    collections.UserDict.__init__(self)
                def __getitem__(self, key):
                    if key not in self.data:
                        query = self.__control.Query("document", "title")
                        query.where(query.Condition("id", key))
                        row = query.execute(self.__control.cursor).fetchone()
                        title = row.title.strip() if row else ""
                        if not self.__control.KEEP_COMPLETE_TITLES:
                            pieces = [p.strip() for p in row.title.split(";")]
                            title = pieces[0]
                            if title.lower() == "inactive" and len(pieces) > 1:
                                title = f"{pieces[1]} (inactive)"
                        self.data[key] = title or None
                    return self.data[key]
            self._doc_titles = DocTitles(self)
        return self._doc_titles

    @property
    def elapsed(self):
        """How long have we been running?"""
        return datetime.datetime.now() - self.started

    @property
    def fields(self):
        """CGI fields for the web form."""
        if not hasattr(self, "_fields"):
            self._fields = cgi.FieldStorage()
        return self._fields

    @property
    def footer(self):
        """Override to alter or suppress the default report footer."""

        if not hasattr(self, "_footer"):
            user = self.session.User(self.session, id=self.session.user_id)
            name = user.fullname or user.name
            today = datetime.date.today()
            generated = f"Report generated {today} by {name}"
            elapsed = HTMLPage.B.SPAN(str(self.elapsed), id="elapsed")
            args = generated, HTMLPage.B.BR(), "Elapsed: ", elapsed
            self._footer = HTMLPage.B.E("footer", HTMLPage.B.P(*args))
        return self._footer

    @property
    def form_page(self):
        """Create a form page.

        Cache the page so we don't end up populating one page and
        sending another one.
        """
        if not hasattr(self, "_form_page"):
            opts = {
                "action": self.script,
                "buttons": [HTMLPage.button(b) for b in self.buttons],
                "subtitle": self.subtitle,
                "session": self.session,
                "method": self.method,
            }
            self._form_page = HTMLPage(self.title, **opts)
        return self._form_page

    @property
    def format(self):
        """Either "html" (the default) or "excel"."""
        if not hasattr(self, "_format"):
            self._format = self.fields.getvalue("format")
            if not self._format:
                self._format = self.__opts.get("format") or self.FORMATS[0]
            if self._format not in self.FORMATS:
                self.bail("invalid report format")
        return self._format

    @property
    def logger(self):
        """Object for recording what we do."""
        if not hasattr(self, "_logger"):
            self._logger = self.__opts.get("logger")
            if self._logger is None:
                opts = dict(level=self.loglevel)
                self._logger = cdr.Logging.get_logger(self.LOGNAME, **opts)
        return self._logger

    @property
    def loglevel(self):
        """Override this to provide runtime control of logging."""
        return self.LOGLEVEL

    @property
    def method(self):
        """Allow override of form method."""
        return self.fields.getvalue("method") or self.METHOD

    @property
    def no_results(self):
        """Message to display if no result tables are returned."""
        return "No report results found."

    @property
    def report(self):
        """Create the `Reporter` object for this job."""

        if not hasattr(self, "_report"):
            tables = self.build_tables()
            buttons = []
            for button in (self.SUBMENU, self.ADMINMENU, self.LOG_OUT):
                if button:
                    buttons.append(HTMLPage.button(button))
            opts = {
                "banner": self.title or "",
                "footer": self.footer,
                "subtitle": self.subtitle,
                "no_results": self.no_results,
                "page_opts": {
                    "buttons": buttons,
                    "session": self.session,
                    "action": buttons and self.script or None,
                }
            }
            self._report = Reporter(self.title, tables, **opts)
        return self._report

    @property
    def request(self):
        """Name of clicked request button, if any."""
        if not hasattr(self, "_request"):
            self._request = getRequest(self.fields)
        return self._request

    @request.setter
    def request(self, value):
        """Support re-routing."""
        self._request = value

    @property
    def script(self):
        """Name of form submission handler."""
        if not hasattr(self, "_script"):
            self._script = self.__opts.get("script")
            if self._script is None:
                self._script = os.path.basename(sys.argv[0])
        return self._script

    @property
    def selection_method(self):
        """How does the user want to identify summaries for the report?"""

        if not hasattr(self, "_selection_method"):
            name = "selection_method"
            self._selection_method = self.fields.getvalue(name, "board")
            if self._selection_method not in self.SUMMARY_SELECTION_METHODS:
                self.bail()
        return self._selection_method

    @property
    def session(self):
        """Session object for this controller.

        Note: this is an object, not a string. For the session name,
        use `self.session.name` or `str(self.session)` or `f"{self.session}"`.

        No need to specify a tier here, as web CGI scripts are only
        intended to work on the local tier.
        """

        if not hasattr(self, "_session"):
            session = self.__opts.get("session")
            if not session:
                session = self.fields.getvalue(SESSION) or "guest"
            if isinstance(session, bytes):
                session = str(session, "ascii")
            if isinstance(session, str):
                self._session = Session(session)
            else:
                self._session = session
            if not isinstance(self._session, Session):
                raise Exception("Not a session object")
        return self._session

    @property
    def summary_selection_js(self):
        "Local JavaScript to manage sections of the form dynamically."

        return """\
function check_set(name, val) {
    var all_selector = "#" + name + "-all";
    var ind_selector = "#" + name + "-set .ind";
    if (val == "all") {
        if (jQuery(all_selector).prop("checked"))
            jQuery(ind_selector).prop("checked", false);
        else
            jQuery(all_selector).prop("checked", true);
    }
    else if (jQuery(ind_selector + ":checked").length > 0)
        jQuery(all_selector).prop("checked", false);
    else
        jQuery(all_selector).prop("checked", true);
}
function check_board(board) { check_set("board", board); }
function check_selection_method(method) {
    switch (method) {
        case 'id':
            jQuery('.by-board-block').hide();
            jQuery('.by-id-block').show();
            jQuery('.by-title-block').hide();
            break;
        case 'board':
            jQuery('.by-board-block').show();
            jQuery('.by-id-block').hide();
            jQuery('.by-title-block').hide();
            break;
        case 'title':
            jQuery('.by-board-block').hide();
            jQuery('.by-id-block').hide();
            jQuery('.by-title-block').show();
            break;
    }
}
jQuery(function() {
    var method = jQuery("input[name='selection_method']:checked").val();
    check_selection_method(method);
});"""

    @property
    def summary_titles(self):
        """Find the summaries that match the user's title fragment.

        Note that the user is responsible for adding any non-trailing
        SQL wildcards to the fragment string. If the title is longer
        than 60 characters, truncate with an ellipsis, but add a
        tooltip showing the whole title. We create a local class for
        the resulting list.

        ONLY WORKS IF YOU IMPLEMENT THE `self.fragment` PROPERTY!!!
        """

        if not hasattr(self, "_summary_titles"):
            class SummaryTitle:
                def __init__(self, doc_id, display, tooltip=None):
                    self.id = doc_id
                    self.display = display
                    self.tooltip = tooltip
            fragment = f"{self.fragment}%"
            query = self.Query("active_doc d", "d.id", "d.title")
            query.join("doc_type t", "t.id = d.doc_type")
            query.where("t.name = 'Summary'")
            query.where(query.Condition("d.title", fragment, "LIKE"))
            query.order("d.title")
            rows = query.execute(self.cursor).fetchall()
            self._summary_titles = []
            for doc_id, title in rows:
                if len(title) > 60:
                    short_title = title[:57] + "..."
                    summary = SummaryTitle(doc_id, short_title, title)
                else:
                    summary = SummaryTitle(doc_id, title)
                self._summary_titles.append(summary)
        return self._summary_titles

    @property
    def timestamp(self):
        """String used to distinguish multiple instances of named items."""
        return self.started.strftime("%Y%m%d%H%M%S")

    @property
    def started(self):
        """When did we start processing?"""
        return self.__started

    @property
    def subtitle(self):
        """String to be displayed under the main banner, if supplied."""
        return self.__opts.get("subtitle") or self.SUBTITLE

    @property
    def title(self):
        """Title to be used for the page."""
        return self.__opts.get("title") or self.TITLE or self.PAGE_TITLE

    @staticmethod
    def bail(message=TAMPERING, banner="CDR Web Interface", extra=None,
             logfile=None):
        """
        Send an error page to the browser with a specific banner and title.
        Pass:
            message - Display this.
            banner  - Optional changed line for page banner.
            extra   - Optional sequence of extra lines to append.
            logfile - Optional name of logfile to write to.
        Return:
            No return. Exits here.
        """

        opts = dict(banner=banner, subtitle="An error has occurred", scripts=[])
        page = HTMLPage("CDR Error", **opts)
        page.body.append(page.B.P(str(message), page.B.CLASS("error")))
        if extra:
            for arg in extra:
                page.body.append(page.B.P(str(arg), page.B.CLASS("error")))
        if logfile:
            if logfile.lower().endswith(".log"):
                logfile = logfile[:-4]
            logger = cdr.Logging.get_logger(logfile)
            logger.error("cdrcgi bailout: %s", message)
        page.send()


class FormFieldFactory:
    """Provide class methods for creating HTML form fields.

    Also has a factory method for creating a fieldset (with an optional
    legend) which isn't really a field, but it seemed to fit best here.

    These methods are inherited by the HTMLPage class below, so that
    users passed an object of that class have easy access to the
    methods without having to import anything extra.

    Here are the categories of fields supported:

        text fields
           - text_field()
           - password_field()
           - date_field()

        clickable fields
           - checkbox()
           - radio_button()

        other
           - file_field()
           - hidden_field()
           - select()

    Pro tip:
       If you don't specify a label for a field, the class will create
       one for you based on the field's value, by replacing underscores
       with spaces, and capitalizing each word. Of course, this is only
       useful for fields which have an initial value (as is always the
       case for radio buttons and checkboxes). For example:

       page.radio_button("sort", value="by_board")

       will result in a field whose label is "By Board".
    """

    EN_DASH = "\u2013"
    CLICKABLE = "checkbox", "radio"
    B = lxml.html.builder

    @classmethod
    def button(cls, label, **kwargs):
        """Create a button to be added to an HTMLPage object.

        Typically, one or more of these buttons will be passed as the
        buttons keyword argument to the HTMLPage constructor, to be
        displayed on the right side of the main banner, but it is also
        possible to place a button elsewhere on the page. It is also
        possible to override the default buttons for a form by overriding
        its page's `buttons` property.

        These are called "buttons" even though the HTML element used
        to create the widget is an "input" element with type of
        "submit" or "reset" (and even though, confusingly, there is
        another HTML element called "button," which is not being used
        here).

        Not to be confused with radio buttons. :-)

        Required positional argument:

            label
                string to be displayed on the button; also used by the
                form submission handler to determine which button was
                clicked

        Optional keyword arguments:

            button_type
                set to "reset" to override the default of "submit"

            onclick
                optional JavaScript to be invoked when the button
                is clicked

        Return:

            lxml object for an INPUT element

        """

        button = cls.B.INPUT(value=label, name="Request")
        button.set("type", kwargs.get("button_type", "submit"))
        onclick = kwargs.get("onclick")
        if onclick:
            button.set("onclick", onclick)
        return button

    @classmethod
    def checkbox(cls, group, **kwargs):
        """Create a checkbox field block with label.

        Required positional argument:

            group

                string identifying the set of checkboxes whose checked
                values will be returned to the form processor as a
                single sequence

        See the `__field()`, `__wrapper()`, and `__clickable()`
        methods for a description of the available optional keyword
        arguments.

        Example:

            fieldset = page.fieldset("Select Board(s)")
            for board in self.boards:
                opts = dict(value=board.id, label=board.name)
                fieldset.append(page.checkbox("board", **opts)
            page.append(fieldset)

        Return:

            lxml object for a wrapper element, enclosing an INPUT element
        """

        return cls.__clickable(group, "checkbox", **kwargs)

    @classmethod
    def date_field(cls, name, **kwargs):

        """Create a date field block with optional label.

        A date picker is displayed for ease of choosing a value for the
        field. The text widget is still editable directly.

        Required positional argument:

            name
                unique name of the field on the page; it is essential
                that the name be unique, even across forms (in the
                event that the caller adds a second form to the page)

        See the `__field()` and `__wrapper()` methods for a
        description of the available optional keyword arguments.

        Return:

            lxml object for a wrapper element, enclosing an INPUT element
        """

        wrapper = cls.text_field(name, **kwargs)
        field = wrapper.find("input")
        classes = field.get("class", "").split()
        if "CdrDateField" not in classes:
            classes.append("CdrDateField")
            field.set("class", " ".join(classes))
        return wrapper

    @classmethod
    def date_range(cls, name, **kwargs):
        """Date an inline pair of dates with a single label.

        Required positional argument:

            name
                unique name of the field on the page; it is essential
                that the name be unique, even across forms (in the
                event that the caller adds a second form to the page)

        Optional keyword arguments:

            start_date
                initial value for the first date field

            end_date
                initial value for the second date field

        See the `__field()` and `__wrapper()` methods for a
        description of other available optional keyword arguments.

        Return:

            lxml object for a wrapper element, enclosing two INPUT elements
            with a separating SPAN between
        """

        opts = dict(**kwargs, wrapper_classes="date-range")
        wrapper = cls.__wrapper(name, **opts)
        for which in ("start", "end"):
            opts["value"] = opts.get(f"{which}_date")
            field = cls.__field(f"{name}-{which}", "text", **opts)
            classes = field.get("class", "").split()
            if "CdrDateField" not in classes:
                classes.append("CdrDateField")
                field.set("class", " ".join(classes))
            wrapper.append(field)
            if which == "start":
                separator = cls.B.SPAN(cls.EN_DASH)
                separator.set("class", "date-range-sep")
                wrapper.append(separator)
        return wrapper

    @classmethod
    def fieldset(cls, legend=None, **opts):
        """Create an HTML fieldset element with an optional legend child.

        Optional keyword argument:

            legend

                string for optional legend to be displayed for the
                fieldset

            id

                unique ID for the element

        Return:

            lxml object for a FIELDSET element
        """

        fieldset = cls.B.FIELDSET()
        if legend:
            fieldset.append(cls.B.LEGEND(legend))
        if opts.get("id"):
            fieldset.set("id", opts.get("id"))
        return fieldset

    @classmethod
    def file_field(cls, name, **kwargs):
        """Create a file upload field block with optional label.

        The widget design for picking a field varies from browser to
        browser.

        Required positional argument:

            name
                unique name of the field on the page; it is essential
                that the name be unique, even across forms (in the
                event that the caller adds a second form to the page)

        See the `__field()` and `__wrapper()` methods for a
        description of the available optional keyword arguments.

        Return:

            lxml object for a wrapper element, enclosing an INPUT element
        """

        field = cls.__field(name, "file", **kwargs)
        if kwargs.get("multiple"):
            field.set("multiple")
        wrapper = cls.__wrapper(name, **kwargs)
        wrapper.append(field)
        return wrapper

    @classmethod
    def hidden_field(cls, name, value):
        if value is None:
            value = ""
        return cls.B.INPUT(name=name, value=str(value), type="hidden")

    @classmethod
    def password_field(cls, name, **kwargs):
        """Create a password field block with optional label.

        The value for the field is not displayed.

        Required positional argument:

            name
                unique name of the field on the page; it is essential
                that the name be unique, even across forms (in the
                event that the caller adds a second form to the page)

        See the `__field()` and `__wrapper()` methods for a
        description of the available optional keyword arguments.

        Return:

            lxml object for a wrapper element, enclosing an INPUT element
        """

        field = cls.__field(name, "password", **kwargs)
        wrapper = cls.__wrapper(name, **kwargs)
        wrapper.append(field)
        return wrapper

    @classmethod
    def radio_button(cls, group, **kwargs):
        """Create a wrapped radio button field with label.

        Required positional argument:

            group

                string identifying the set of radio buttons of which
                at most only one can be checked

        See the `__field()`, `__wrapper()`, and `__clickable()`
        methods for a description of the available optional keyword
        arguments.

        Example:

            choices = ("html", "Web Page"), ("excel", "Excel Workbook")
            fieldset = page.fieldset("Report Format")
            page.append(fieldset)
            for value, label in choices:
                opts = dict(label=label, value=value)
                if value == "html":
                    opts["checked"] = True
                fieldset.append(page.radio_button("format", **opts))

        Return:

            lxml object for a wrapper element, enclosing an INPUT element
        """

        return cls.__clickable(group, "radio", **kwargs)

    @classmethod
    def select(cls, name, **kwargs):
        """Create a wrapped picklist field with optional label.

        Required positional argument:

            name
                unique name of the field on the page; it is essential
                that the name be unique, even across forms (in the
                event that the caller adds a second form to the page)

        Optional keyword arguments:

            options
                sequence of choices

            size
                height of the select box (default 1; 4 if multiple)

            onchange
                javascript to execute when the selection changes

            multiple
                whether multiple selections are permitted

        See the `__field()` and `__wrapper()` methods for a description
        of additional available optional keyword arguments.

        Return:

            lxml object for a wrapper element, enclosing an SELECT element
        """

        # Create the widget and its wrapper element.
        wrapper = cls.__wrapper(name, **kwargs)
        field = cls.__field(name, "select", **kwargs)
        wrapper.append(field)

        # Add attributes unique to picklist fields.
        multiple = True if kwargs.get("multiple") else False
        if multiple:
            field.set("multiple")
        if kwargs.get("onchange"):
            field.set("onchange", kwargs["onchange"])
        if kwargs.get("size"):
            field.set("size", str(kwargs["size"]))

        # If we have any options, add them as children of the widget object.
        options = kwargs.get("options")
        if options:
            default = kwargs.get("default")
            if not isinstance(default, (list, tuple, set)):
                default = [default] if default else []
            if not multiple and len(default) > 1:
                error = "Multiple defaults specified for single picklist"
                raise Exception(error)
            if isinstance(options, dict):
                options = sorted(options.items(),
                                 key=lambda o:str(o[1]).lower())
            for option in options:
                if isinstance(option, (list, tuple)):
                    value, display = option
                else:
                    value = display = option
                if value is None:
                    value = ""
                option = cls.B.OPTION(str(display), value=str(value))
                if value in default:
                    option.set("selected")
                field.append(option)

        # Done.
        return wrapper

    @classmethod
    def textarea(cls, name, **kwargs):
        """Create a textarea field block with optional label.

        Required positional argument:

            name
                unique name of the field on the page; it is essential
                that the name be unique, even across forms (in the
                event that the caller adds a second form to the page)

        See the `__field()` and `__wrapper()` methods for a
        description of the available optional keyword arguments.

        Return:

            lxml object for a wrapper element, enclosing an INPUT element
        """

        field = cls.__field(name, "textarea", **kwargs)
        if "rows" in kwargs:
            field.set("rows", str(kwargs["rows"]))
        wrapper = cls.__wrapper(name, **kwargs)
        wrapper.append(field)
        return wrapper

    @classmethod
    def text_field(cls, name, **kwargs):
        """Create a text field block with optional label.

        Required positional argument:

            name
                unique name of the field on the page; it is essential
                that the name be unique, even across forms (in the
                event that the caller adds a second form to the page)

        See the `__field()` and `__wrapper()` methods for a
        description of the available optional keyword arguments.

        Return:

            lxml object for a wrapper element, enclosing an INPUT element
        """

        field = cls.__field(name, "text", **kwargs)
        wrapper = cls.__wrapper(name, **kwargs)
        wrapper.append(field)
        return wrapper

    @classmethod
    def __field(cls, name, field_type, **kwargs):
        """Build an HTML form field widget.

        Required positional arguments:

            name
                unique name of the field on the page (not just unique
                to the form); be careful also to avoid clashes with
                names generated for date range fields ({name}-start
                and {name}-end)

            field_type
                one of the following string values:
                   - text
                   - password
                   - file
                   - textarea
                   - select
                   - checkbox
                   - radio

        Optional keyword arguments:

            value
                initial value for the field (default: "")
                not used for "select" fields

            widget_id
                custom value for the field element's ID attribute
                (default: normalized name-value string for radio
                buttons and checkboxes, name for all other field
                types)

            classes
                if present, used as the 'class' attribute for the
                field element; may be passed as a list, tuple, or set,
                or as a string value with one or more space-delimited
                class names (default: no classes)

            tooltip
                if present, used as the 'title' attribute for the
                field element (default: no tooltip)

            readonly
                if True, the field is not editable, but it can get the
                UI focus, and it is still sent with the form submission
                (default: False); not used for 'select' fields

            disabled
                like readonly, but the form element does not receive
                focus, is skipped in tabbing navigation, and is not
                passed to the form processor

        Returns:

            lxml HTML element object
        """

        # Create the object for the field element.
        value = kwargs.get("value")
        if value is None:
            value = ""
        value = str(value)
        if field_type == "textarea":
            field = cls.B.TEXTAREA(value, name=name)
        elif field_type == "select":
            field = cls.B.SELECT(name=name)
        else:
            field = cls.B.INPUT(name=name, value=value)
            field.set("type", field_type)

        # Add the element's unique ID attribute.
        if field_type in cls.CLICKABLE:
            widget_id = kwargs.get("widget_id")
            if not widget_id:
                widget_id = f"{name}-{value}".replace(" ", "-").lower()
        else:
            widget_id = name
        field.set("id", widget_id)

        # Add the classes for the widget element.
        classes = cls.__classes(kwargs.get("classes"))
        if classes:
            field.set("class", " ".join(classes))

        # Tooltip for clickables goes on the label, not the widget.
        elif kwargs.get("tooltip"):
            field.set("title", kwargs["tooltip"])

        # Add the attributes which (almost) any field can use, and we're done.
        if kwargs.get("disabled"):
            field.set("disabled")
        if kwargs.get("readonly") and field_type != "select":
            field.set("readonly")
        return field

    @classmethod
    def __wrapper(cls, name, **kwargs):
        """Create an enclosing element for a form field

        Required positional argument:

            name
                unique name for the field

        Optional keyword arguments:

            clickable
                True if the field is a checkbox or radio button

            wrapper
                string value for the name of the element to use
                for the field's enclosing element (default: "div")

            label
                string for identify which field this is

            wrapper_id
                custom value for the enclosing element's ID attribute
                (default: None)

            wrapper_classes
                one or more classes to be applied to the div
                wrapper (as space-delimited string, or as list,
                tuple, or set) or None if no additional classes
                are to be applied; the "labeled-field" class
                will be added to the set of classes for fields
                labeled on the left

        Returns:

            lxml HTML element object
        """

        # Create the enclosing element.
        wrapper = cls.B.E(kwargs.get("wrapper") or "div")

        # Set the unique ID for the enclosing element.
        if kwargs.get("wrapper_id"):
            wrapper.set("id", kwargs["wrapper_id"])

        # Determine the classes to be applied to the wrapper element.
        classes = cls.__classes(kwargs.get("wrapper_classes"))
        label = kwargs.get("label", name.replace("_", " ").title())
        if label and not kwargs.get("clickable"):
            classes.add("labeled-field")
            widget_id = kwargs.get("widget_id") or name
            label = cls.B.LABEL(label, cls.B.FOR(widget_id))
            if kwargs.get("tooltip"):
                label.set("title", kwargs["tooltip"])
            wrapper.append(label)
        if classes:
            wrapper.set("class", " ".join(classes))

        # Done.
        return wrapper

    @classmethod
    def __clickable(cls, group, field_type, **kwargs):
        """Create a wrapped checkbox or radio button field.

        Required positional arguments:

            group
                string identifying a set of related clickable fields
                which will be returned as a sequence of checked
                values to the form processor

            field_type
                one of "checkbox" or "radio"

        See the `__field()` and `__wrapper()` methods for a
        description of the available optional keyword arguments.

        Return:

            lxml object for a wrapper element, enclosing an INPUT element
        """

        # Create the field and its wrapper.
        wrapper = cls.__wrapper(group, clickable=True, **kwargs)
        widget = cls.__field(group, field_type, **kwargs)
        widget.tail = " "

        # Add attributes unique to radio buttons and checkboxes.
        if kwargs.get("checked"):
            widget.set("checked")
        value = kwargs.get("value")
        if value is None:
            value = ""
        value = str(value)
        onclick = kwargs.get("onclick", f"check_{group}('{value}')")
        if onclick:
            widget.set("onclick", onclick.replace("-", "_"))

        # For these fields, the label follows the widget.
        label = kwargs.get("label", value.replace("_", " ").title())
        label = cls.B.LABEL(cls.B.FOR(widget.get("id")), label)
        label.set("class", "clickable")
        if kwargs.get("tooltip"):
            label.set("title", kwargs["tooltip"])
        wrapper.append(widget)
        wrapper.append(label)
        return wrapper

    @classmethod
    def __classes(cls, classes):
        """Package the classes to be assigned to an element as a set

        Required positional argument:

            classes
                one of the following:
                   - None
                   - a space-delimited string of class names
                   - a `tuple` of class names
                   - a `list` of class names
                   - a `set` of class names

        Returns:

            a `set` of zero or more class name strings
        """

        if classes is None:
            return set()
        if isinstance(classes, str):
            return set(classes.split())
        return set(classes)


class HTMLPage(FormFieldFactory):
    """Web page for the CDR system.

    Replacement for the old `Page` class, which built up a page as a
    sequence of strings. This class does it the way it should always
    have been done, by building an object tree, using the html.builder
    support in the lxml package.

    Sample usage:

        buttons = HTMLPage.button("Submit"), HTMLPage.button("Reports")
        page = HTMLPage("Advanced Search", subtitle="Drug", buttons=buttons)
        fieldset = page.fieldset("Search Criteria")
        fieldset.append(page.text_field("name", "Name"))
        fieldset.append(page.date_field("since", "Since"))
        page.form.append(fieldset)
        page.send()
    """

    VERSION = "201909071039"
    CDR_CSS = f"/stylesheets/cdr.css?v={VERSION}"
    APIS = "https://ajax.googleapis.com/ajax/libs"
    JQUERY = f"{APIS}/jquery/3.4.1/jquery.min.js"
    JQUERY_UI = f"{APIS}/jqueryui/1.12.1/jquery-ui.min.js"
    JQUERY_CSS = f"{APIS}/jqueryui/1.12.1/themes/smoothness/jquery-ui.css"
    STYLESHEETS = JQUERY_CSS, CDR_CSS
    SCRIPTS = JQUERY, JQUERY_UI
    PRIMARY_FORM_ID = "primary-form"
    STRING_OPTS = dict(pretty_print=True, doctype="<!DOCTYPE html>")
    CALENDAR_SCRIPT = "\n".join([
        "jQuery(function() {",
        "    jQuery('.CdrDateField').datepicker({",
        "        dateFormat: 'yy-mm-dd',",
        "        showOn: 'button',",
        "        buttonImageOnly: true,",
        "        buttonImage: '/images/calendar.png',",
        "        buttonText: 'Select date',",
        "        dayNamesMin: [ 'S', 'M', 'T', 'W', 'T', 'F', 'S' ]",
        "    });",
        "});"])

    def __init__(self, title, **kwargs):
        """Capture the initial settings for the page.

        Required positional argument:

            title
                string representing the page's title

        Optional keyword arguments:

            action
                form submission handler (default: URL which draws form)

            banner
                string for main banner (default: page title)

            body_id
                ID attribute for body element (default: "cdr-page")

            buttons
                sequence of objects created by FormFieldFactory.button()

            head_title
                title for head block (default: page title)

            icon
                favicon path (defaults: "/favicon.ico")

            method
                set to "get" to override default of "post")

            scripts
                urls for js to load (default: jQuery and jQueryUI URLs)

            session
                object representing the current CDR login context

            subtitle
                string for a smaller second banner

            stylesheets
                urls for CSS to load (default: CDR and jQueryUI CSS)
        """

        self.__title = title
        self.__opts = kwargs

    def tostring(self):
        """Return the serialized Unicode string for the page object."""
        opts = dict(self.STRING_OPTS, encoding="unicode")
        return lxml.html.tostring(self.html, **opts)

    def tobytes(self):
        """Return the serialized page as ASCII bytes.

        The Unicode characters contained on the page will have
        been encoded as HTML entities.
        """

        return lxml.html.tostring(self.html, **self.STRING_OPTS)

    def send(self):
        """Push the page back to the browser via the web server."""
        if self.body.get("class") == "admin-menu" and self.news:
            header = self.body.find("form/header")
            if header is not None:
                for name in reversed(sorted(self.news, key=str.lower)):
                    news = self.news[name]
                    p = self.B.P(news)
                    if "error" in name.lower() or "failure" in name.lower():
                        p.set("class", "failure news")
                    else:
                        p.set("class", "info news")
                    header.addnext(p)
        sendPage(self.tostring())

    def add_css(self, css):
        """Add style rules directly to the page."""
        self.head.append(self.B.STYLE(css))

    def add_script(self, script):
        """Add script code directly to the page."""
        self.head.append(self.B.SCRIPT(script))

    def add_output_options(self, default=None, onclick=None):
        """
        Allow the user to decide between HTML and Excel.
        """
        choices = ("html", "Web Page"), ("excel", "Excel Workbook")
        fieldset = self.fieldset("Report Format")
        fieldset.set("id", "report-format-block")
        self.form.append(fieldset)
        for value, label in choices:
            if value is None:
                value = ""
            opts = dict(label=label, value=value, onclick=onclick)
            if value == default:
                opts["checked"] = True
            fieldset.append(self.radio_button("format", **opts))
        return fieldset

    def add_session_field(self, session):
        """Add hidden session field if it isn't there already."""
        if self.form is not None:
            if not self.form.xpath(f"//input[@name='{SESSION}']"):
                self.form.append(self.hidden_field(SESSION, session))

    def menu_link(self, script, display, **params):
        """
        Add a list item containing a CDR admin menu link.
        """

        url = script
        if self.session and SESSION not in params:
            params[SESSION] = self.session
        if params:
            url = f"{url}?{urllib.parse.urlencode(params)}"
        return self.B.A(display, href=url)

    @property
    def action(self):
        """URL for the form submission handler."""
        if not hasattr(self, "_action"):
            self._action = self.__opts.get("action")
            if self._action is None:
                self._action = os.path.basename(sys.argv[0])
            if self._action == "flask":
                self._action = ""
        return self._action

    @property
    def banner_title(self):
        """The title to be displayed in the main banner for the page."""
        if not hasattr(self, "_banner_title"):
            self._banner_title = self.__opts.get("banner_title")
            if self._banner_title is None:
                self._banner_title = self.title
        return self._banner_title

    @property
    def body(self):
        """The body content element for the page.

        May or may not contain a form wrapper for the page's content,
        depending on whether any buttons were passed to the HTMLPage's
        constructor. Typically, the code creating an HTMLPage object
        will then append to this object, or the enclosed form object,
        or both.
        """

        if not hasattr(self, "_body"):
            self._body = self.B.BODY(id=self.body_id)
            if self.body_classes:
                self._body.set("class", " ".join(self.body_classes))
            banner = self.B.H1(self.banner_title)
            header = self.B.E("header", banner)
            if self.subtitle:
                header.append(self.B.H2(self.subtitle))
            if self.buttons:
                buttons = self.B.SPAN(*self.buttons)
                buttons.set("id", "header-buttons")
                banner.append(buttons)
                form = self.B.FORM(action=self.action, method=self.method)
                if self.session:
                    form.append(self.hidden_field(SESSION, self.session))
                form.set("id", self.PRIMARY_FORM_ID)
                self._body.append(form)
                form.append(header)
            else:
                self._body.append(header)
        return self._body

    @property
    def body_id(self):
        """ID attribute to be applied to the page's body element."""
        if not hasattr(self, "_body_id"):
            self._body_id = self.__opts.get("body_id")
            if not self._body_id:
                self._body_id = "cdr-page"
        return self._body_id

    @property
    def body_classes(self):
        if not hasattr(self, "_body_classes"):
            classes = self.__opts.get("body_classes")
            if classes:
                if isinstance(classes, str):
                    classes = classes.strip().split()
                self._body_classes = set(classes)
            else:
                self._body_classes = set()
        return self._body_classes

    @property
    def buttons(self):
        """The buttons to appear on the right side of the main banner."""
        return self.__opts.get("buttons")

    @property
    def form(self):
        """The body's <form> element.

        Only present if one or more buttons passed to the constructor,
        or if the caller has added its own form(s). If the page has
        more than one form, this only finds the first one (and it won't
        even find that, if it was attached by the caller inside a
        wrapper other than `body`). In these more complicated cases,
        it will be the caller's responsibility to keep track of the
        added forms.
        """

        return self.body.find("form")

    @property
    def head(self):
        """Assemble the head block for the HTML page."""
        if not hasattr(self, "_head"):
            self._head = self.B.HEAD(
                self.B.META(charset="utf-8"),
                self.B.TITLE(self.head_title),
                self.B.LINK(href="/favicon.ico", rel="icon")
            )
            for sheet in self.stylesheets:
                self._head.append(self.B.LINK(href=sheet, rel="stylesheet"))
            for script in self.scripts:
                self._head.append(self.B.SCRIPT(src=script))
        return self._head

    @property
    def header(self):
        """The <header> element at the top of the body."""
        header = self.body.find("form/header")
        if header is not None:
            return header
        return self.body.find("header")

    @property
    def head_title(self):
        """The title to be inserted into the head block of the page."""
        if not hasattr(self, "_head_title"):
            self._head_title = self.__opts.get("head_title")
            if self._head_title is None:
                self._head_title = self.title
        return self._head_title

    @property
    def html(self):
        """Top-level object for the page.

        Slips in the calendar JavaScript if there are any date fields.
        """

        if not hasattr(self, "_html"):
            if not hasattr(self, "_has_calendar_js"):
                self._has_calendar_js = False
            if not self._has_calendar_js:
                if self.body.xpath("//*[contains(@class, 'CdrDateField')]"):
                    self.body.append(self.B.SCRIPT(self.CALENDAR_SCRIPT))
                    self._has_calendar_js = True
            self._html = self.B.HTML(self.head, self.body)
        return self._html

    @property
    def title(self):
        """Return the title string for the page.

        Used by default in the head block and in the main banner, but
        each can be overridden individually using keyword arguments
        passed to the constructor.
        """

        return self.__title or ""

    @property
    def method(self):
        """CGI verb to be used for form submission."""
        return self.__opts.get("method", "post")

    @property
    def news(self):
        """Information to be displayed at the top of the menu pages."""
        if not hasattr(self, "_news"):
            self._news = cdr.getControlGroup("news")
        return self._news

    @property
    def scripts(self):
        """Client-side scripts to be loaded for the page."""
        if not hasattr(self, "_scripts"):
            self._scripts = self.__opts.get("scripts")
            if self._scripts is None:
                self._scripts = self.SCRIPTS
        return self._scripts

    @property
    def session(self):
        """CDR login context for this page."""
        if not hasattr(self, "_session"):
            self._session = self.__opts.get("session", "guest")
            if isinstance(self._session, str):
                self._session = Session(self._session)
        return self._session

    @property
    def stylesheets(self):
        """CSS rules to be loaded for the page."""
        if not hasattr(self, "_stylesheets"):
            self._stylesheets = self.__opts.get("stylesheets")
            if self._stylesheets is None:
                self._stylesheets = self.STYLESHEETS
        return self._stylesheets

    @property
    def subtitle(self):
        """Optional title to be displayed in a second, smaller banner."""
        if not hasattr(self, "_subtitle"):
            self._subtitle = self.__opts.get("subtitle")
        return self._subtitle


class Reporter:
    """Create web-based or Excel workbook reports.

    This replaces the older `Report` class.

    Uses the HTMLPage class for HTML report output, avoiding the ugly
    and error-prone approach of creating web pages by direct string
    juggling. When we have converted all of the existing reports to
    use this new class, move the inherited functions in here, rename
    `Reporter` to `Report` (here and for the users of this new class),
    and delete the old class.

    Example usage:

        R = cdrcgi.Reporter
        columns = (
            R.Column("Type ID", width="75px"),
            R.Column("Type Name", width="300px"),
        )
        query = db.Query("doc_type", "id", "name").order("name")
        rows = query.execute().fetchall()
        table = R.Table(rows, columns=columns, caption="Document Types")
        report = R("Simple Report", table)
        report.send("html")
    """

    def __init__(self, title, tables, **opts):
        """Capture the values for the report."""
        self.__title = title
        self.__tables = tables
        self.__opts = opts

    def send(self, format="html"):
        """Send the web page or Excel workbook to the browser."""
        if format == "excel":
            self.workbook.send()
            if self.debug:
                self.workbook.save(cdr.TMP)
        else:
            self.page.send()
        sys.exit(0)

    @property
    def banner(self):
        """Banner for the top of an HTML report (unused for Excel output)."""
        return self.__opts.get("banner")

    @property
    def css(self):
        """Sequence of string for <style> elements on HTML reports."""
        if not hasattr(self, "_css"):
            self._css = self.__opts.get("css")
            if isinstance(self._css, str):
                self._css = [self._css]
            elif not self._css:
                self._css = []
        return self._css

    @property
    def debug(self):
        """Boolean; if True, save the workbook to the file system."""
        return self.__opts.get("debug")

    @property
    def elapsed(self):
        """Optional length of time the report took to generate.

        Will be converted to a string at render time.
        """

        return self.__opts.get("elapsed")

    @property
    def footer(self):
        """Optional footer to appear on the HTML report."""
        return self.__opts.get("footer")

    @property
    def no_results(self):
        """What to say if no result tables are found."""

        if not hasattr(self, "_no_results"):
            if "no_results" in self.__opts:
                self._no_results = self.__opts.get("no_results")
            else:
                self._no_results = "No report results found."
        return self._no_results

    @property
    def page(self):
        """HTML version of report."""
        if not hasattr(self, "_page"):
            opts = {
                "banner": self.banner,
                "subtitle": self.subtitle,
                "body_classes": "report",
                "styles": self.css,
            }
            opts.update(self.page_opts)
            self._page = HTMLPage(self.title, **opts)
            if self.css:
                self._page.add_css(self.css)
            if not self.tables and self.no_results:
                no_results = self._page.B.P(self.no_results)
                no_results.set("class", "no-results")
                self._page.body.append(no_results)
            for table in self.tables:
                if table.node is not None:
                    self._page.body.append(table.node)
            if self.footer is not None:
                self._page.body.append(self.footer)
            if self.elapsed:
                footnote = self._page.B.P(f"elapsed: {self.elapsed}")
                footnote.set("class", "footnote")
                self._page.body.append(footnote)
        return self._page

    @property
    def page_opts(self):
        """Dictionary of options passed to HTMLPage class's constructor."""
        return self.__opts.get("page_opts") or {}

    @property
    def subtitle(self):
        """String for display under the primary banner of the web report.

        Not used for Excel reports.
        """

        return self.__opts.get("subtitle")

    @property
    def tables(self):
        """Sequence of tables to be included in the report.

        A single table will be converted to a list.
        """
        if not hasattr(self, "_tables"):
            self._tables = self.__tables
            if isinstance(self._tables, Reporter.Table):
                self._tables = [self._tables]
        return self._tables

    @property
    def title(self):
        """Title of the report.

        Used for the workbook filename or the /html/head/title element.
        """

        return self.__title

    @property
    def workbook(self):
        """Wrapper for Excel workbook."""
        if not hasattr(self, "_workbook"):
            opts = dict(stamp=True)
            if self.wrap is not None:
                opts["wrap"] = self.wrap
            self._workbook = Excel(self.title, stamp=True, wrap=self.wrap)
            for table in self.tables:
                table.add_worksheet(self._workbook)
        return self._workbook

    @property
    def wrap(self):
        return self.__opts.get("wrap", True)


    class Cell:
        """Data for one cell in a report table."""

        B = lxml.html.builder

        def __init__(self, *values, **opts):
            """Capture the information needed to show this cell on the report.
            """

            self.__vals = values
            self.__opts = opts

        def write(self, book, rownum, colnum, columns):
            """Add this cell's data to the worksheet.

            Only used for the Excel flavor of the report.

            Pass:
                book - `Excel` object
                rownum - integer position for the row we're on
                colnum - starting column for this cell's data
                columns - sequence of `Reporter.Column` objects for the table

            Return:
                integer position for the next cell to be written
            """

            # May have to skip columns if there were previous rowspans.
            while colnum <= len(columns):
                if columns[colnum-1].skip > 0:
                    columns[colnum-1].skip -= 1
                    colnum += 1
                else:
                    break
            if colnum > len(columns):
                raise Exception(f"too many cells for row {rownum:d}")

            # Assemble the values to be written
            if len(self.values) == 1:
                values = self.values[0]
            else:
                values = "\n".join([str(value) for value in self.values])

            # Determine the styles to be applied.
            styles = self.sheet_styles
            if self.right:
                styles["alignment"] = book.right
            elif self.center:
                styles["alignment"] = book.center
            elif "alignment" not in styles:
                styles["alignment"] = book.left
            if self.href:
                values = f'=HYPERLINK("{self.href}", "{values}")'
                styles["font"] = book.hyperlink
            elif self.bold:
                styles["font"] = book.bold

            # Handle the case of writing to more than one cell.
            nextcol = colnum + 1
            if self.colspan or self.rowspan:
                colspan = self.colspan or 1
                rowspan = self.rowspan or 1
                r1, c1 = r2, c2 = rownum, colnum
                if colspan > 1:
                    c2 += colspan - 1
                    if c2 > len(columns):
                        msg = "not enough room for colspan on row {rownum:d}"
                        raise Exception(msg)
                if rowspan > 1:
                    extra_rows = rowspan - 1
                    r2 += extra_rows
                    while colnum <= c2:
                        if columns[colnum-1].skip > 0:
                            msg = "overlapping rowspan at row {rownum:d}"
                            raise Exception(msg)
                        columns[colnum-1].skip = extra_rows
                        colnum += 1
                nextcol = c2 + 1
                book.merge(r1, c1, r2, c2)
                colnum = c1
            cell = book.write(rownum, colnum, values, styles)
            # This resulted in horrible performance. Use formula instead.
            #if self.href:
            #    cell.hyperlink = self.href
            return nextcol

        @property
        def bold(self):
            """Boolean indicating whether the cell values should be bolded."""
            return self.__opts.get("bold")

        @property
        def center(self):
            """Boolean indicating whether to center the cell's content."""
            return self.__opts.get("center")

        @property
        def classes(self):
            """Sequence of class names for this cell."""
            if not hasattr(self, "_classes"):
                classes = self.__opts.get("classes")
                if not classes:
                    self._classes = set()
                elif isinstance(classes, set):
                    self._classes = classes
                elif isinstance(classes, str):
                    self._classes = {classes}
                elif isinstance(classes, (tuple, list)):
                    self._classes = set(classes)
                else:
                    message = "unexpected type {} for Cell classes: {}"
                    args = type(classes), repr(classes)
                    raise Exception(message.format(args))
                if self.bold and not self.href:
                    self._classes.add("strong")
                if self.center:
                    self._classes.add("center")
                elif self.right:
                    self._classes.add("right")
                if self.middle:
                    self._classes.add("middle")
            return self._classes

        @property
        def colspan(self):
            """How many columns does this cell span horizontally?

            Default is 1.
            """

            if not hasattr(self, "_colspan"):
                self._colspan = self.__opts.get("colspan")
                if self._colspan:
                    self._colspan = int(self._colspan)
            return self._colspan

        @property
        def href(self):
            """URL value for link."""
            return self.__opts.get("href")

        @property
        def middle(self):
            """Override vertical alignment."""
            return self.__opts.get("middle")

        @property
        def right(self):
            """Boolean indicating whether the values should be right aligned.
            """

            return self.__opts.get("right")

        @property
        def rowspan(self):
            """How many rows does this cell span vertically?

            Default is 1.
            """

            if not hasattr(self, "_rowspan"):
                self._rowspan = self.__opts.get("rowspan")
                if self._rowspan:
                    self._rowspan = int(self._rowspan)
            return self._rowspan

        @property
        def sheet_styles(self):
            """Optional dictionary of style attributes for the Excel report."""
            if not hasattr(self, "_sheet_styles"):
                self._sheet_styles = self.__opts.get("sheet_styles") or {}
            return self._sheet_styles

        @property
        def target(self):
            """Target for links.

            Only used for web output, when the `href` option has been set.
            """

            return self.__opts.get("target")

        @property
        def td(self):
            """HTML node for this cell in a web report."""
            if not hasattr(self, "_td"):
                self._td = container = self.B.TD()
                if self.href:
                    container = self.B.A(href=self.href)
                    if self.target:
                        container.set("target", self.target)
                    if self.bold:
                        container.set("class", "strong")
                    self._td.append(container)
                if len(self.values) == 1:
                    if isinstance(self.values[0], lxml.html.HtmlElement):
                        container.append(self.values[0])
                    else:
                        container.text = str(self.values[0])
                else:
                    values = list(self.values)
                    container.append(self.B.SPAN(str(values.pop(0))))
                    while values:
                        container.append(self.B.BR())
                        container.append(self.B.SPAN(str(values.pop(0))))
                if self.colspan:
                    self._td.set("colspan", str(self.colspan))
                if self.rowspan:
                    self._td.set("rowspan", str(self.rowspan))
                if self.classes:
                    self._td.set("class", " ".join(self.classes))
                if self.title:
                    self._td.set("title", self.title)
            return self._td

        @property
        def title(self):
            """Optional string to be shown in popup when hovering."""
            if not hasattr(self, "_title"):
                if "title" in self.__opts:
                    self._title = self.__opts["title"]
                elif "tooltip" in self.__opts:
                    self._title = self.__opts["tooltip"]
                else:
                    self._title = None
            return self._title

        @property
        def tooltip(self):
            """Alias for the title property."""
            return self.title

        @property
        def values(self):
            """Collect the values for the cell into a sequence."""
            if not hasattr(self, "_values"):
                self._values = []
                for value in self.__vals:
                    if isinstance(value, (list, tuple)):
                        self._values += list(value)
                    elif value is not None:
                        self._values.append(value)
                if not self._values:
                    self._values = [""]
            return self._values


    class Column:
        """Header and properties for one column in a report table."""

        def __init__(self, name, **opts):
            """Save what we need to render this column in the report."""
            self.__name = name
            self.__opts = opts

        @property
        def classes(self):
            """Optional classes for the th element (HTML only)."""
            return self.__opts.get("classes")

        @property
        def colspan(self):
            """How many columns does this header need to cover?"""
            return self.__opts.get("colspan")

        @property
        def id(self):
            """Optional unique ID for the th element (HTML only)."""
            return self.__opts.get("id")

        @property
        def name(self):
            """What we display at the top of the column."""
            return self.__name

        @property
        def skip(self):
            """Keep track of rows to be skipped when rowspan is set."""
            if not hasattr(self, "_skip"):
                self._skip = 0
            return self._skip

        @skip.setter
        def skip(self, other):
            """Let table rendering modify this value."""
            self._skip = other

        @property
        def tooltip(self):
            """Popup string for help when the user hovers over the column."""
            return self.__opts.get("tooltip")

        @property
        def style(self):
            """HTML style attribute for column element on web page."""
            if not hasattr(self, "_style"):
                self._style = None
                style = self.__opts.get("style") or ""
                rules = [r for r in style.rstrip(";").split(";") if r]
                if self.width:
                    rules.append(f"min-width: {self.width}")
                if rules:
                    self._style = ";".join(rules)
            return self._style

        @property
        def width(self):
            """Minimum width of column (e.g., '40px')."""
            return self.__opts.get("width")


    class Table:
        """Grid of rows and columns for the report.

        A report can have more than one table.
        """

        B = lxml.html.builder

        def __init__(self, rows, **opts):
            """Capture the information we need to render the table."""
            self.__rows = rows
            self.__opts = opts

        def add_worksheet(self, book):
            """Create an Excel worksheet and add it to the workbook.

            Pass:
                book
                    `Excel` object
            """

            # Create a new worksheet.
            self.sheet = book.add_sheet(self.sheet_name)
            self.sheet.sheet_format.defaultRowHeight = 200
            if self.freeze_panes:
                self.sheet.freeze_panes = self.freeze_panes

            # Add the rows for the caption strings.
            rownum = 1
            styles = dict(alignment=book.center, font=book.bold)
            for caption in self.caption:
                book.merge(rownum, 1, rownum, len(self.columns))
                book.write(rownum, 1, caption, styles)
                rownum += 1
            if self.caption:
                book.merge(rownum, 1, rownum, len(self.columns))
                rownum += 1

            # Show the report date between caption and headers if requested.
            if self.show_report_date:
                report_date = f"Report date: {datetime.date.today()}"
                book.merge(rownum, 1, rownum, len(self.columns))
                book.write(rownum, 1, report_date, dict(font=book.bold))
                rownum += 1
                book.merge(rownum, 1, rownum, len(self.columns))
                rownum += 1

            # Set the column headers and widths.
            colnum = 1
            styles["alignment"] = book.center_middle
            for column in self.columns:
                if column.width:
                    width = book.pixels_to_chars(column.width)
                    book.set_width(colnum, width)
                book.write(rownum, colnum, column.name, styles)
                colnum += 1

            # Add each of the data rows to the worksheet.
            for row in self.rows:
                rownum += 1
                colnum = 1
                for cell in row:
                    if not isinstance(cell, Reporter.Cell):
                        cell = Reporter.Cell(cell)
                    colnum = cell.write(book, rownum, colnum, self.columns)

        def debug(self, message, *args):
            """If we have a logger, use it for debugging."""

            if self.logger:
                self.logger.debug(message, *args)

        @property
        def caption(self):
            """Sequence of strings to be displayed for the table's caption.

            If more than one string, each will be rendered on a separate line.
            """

            if not hasattr(self, "_caption"):
                self._caption = self.__opts.get("caption")
                if not self._caption:
                    self._caption = []
                elif isinstance(self._caption, str):
                    self._caption = [self._caption]
            return self._caption

        @property
        def classes(self):
            """Optional classes for the table element (HTML only)."""
            return self.__opts.get("classes")

        @property
        def cols(self):
            """Alias for `columns` property."""
            return self.columns

        @property
        def columns(self):
            """Sequence of `Reporter.Column` objects for this table."""
            if not hasattr(self, "_columns"):
                self._columns = []
                columns = self.__opts.get("columns") or self.__opts.get("cols")
                if columns:
                    self._columns = []
                    for column in columns:
                        if isinstance(column, str):
                            column = Reporter.Column(column)
                        self._columns.append(column)
            return self._columns

        @property
        def freeze_panes(self):
            """Optional cell marking row/col freezing (Excel only)."""
            return self.__opts.get("freeze_panes")

        @property
        def id(self):
            """Optional id attribute for the table element (HTML only)."""
            return self.__opts.get("id")

        @property
        def logger(self):
            """Access to logging."""
            return self.__opts.get("logger")

        @property
        def node(self):
            """HTML object for table."""
            if not hasattr(self, "_node"):

                # Hold off until we know we have child nodes for the table.
                self._node = None
                children = []

                # Add the caption strings to the table if we have any.
                caption = list(self.caption)
                if self.show_report_date:
                    caption.append("")
                    caption.append(f"Report date: {datetime.date.today()}")
                if caption:
                    nodes = [self.B.SPAN(caption[0])]
                    for line in caption[1:]:
                        nodes.append(self.B.BR())
                        nodes.append(self.B.SPAN(line))
                    children.append(self.B.CAPTION(*nodes))

                # Add the column headers if they have been provided.
                if self.columns:
                    tr = self.B.TR()
                    for column in self.columns:
                        th = self.B.TH(column.name)
                        if column.style:
                            th.set("style", column.style)
                        if column.id:
                            th.set("id", column.ids)
                        if column.tooltip:
                            th.set("title", column.tooltip)
                        if column.classes:
                            if isinstance(column.classes, str):
                                th.set("class", column.classes)
                            else:
                                th.set("class", " ".join(column.classes))
                        if column.colspan:
                            th.set("colspan", str(column.colspan))
                        tr.append(th)
                    children.append(self.B.THEAD(tr))

                # Only create the <tbody> element if there are data rows.
                if self.rows:
                    tbody = self.B.TBODY()
                    for row in self.rows:
                        tr = self.B.TR()
                        self.debug("row has %d cells", len(row))
                        for cell in row:
                            if not isinstance(cell, Reporter.Cell):
                                cell = Reporter.Cell(cell)
                            self.debug("cell values: %s", cell.values)
                            self.debug("td: %s", cell.td)
                            tr.append(cell.td)
                        self.debug("tr: %s", lxml.html.tostring(tr))
                        tbody.append(tr)
                    children.append(tbody)

                # Create the table element if we found any child nodes.
                if children:
                    self._node = self.B.TABLE(*children)
                    if self.id:
                        self._node.set("id", self.id)
                    if self.classes:
                        if isinstance(self.classes, str):
                            self._node.set("class", self.classes)
                        else:
                            self._node.set("class", " ".join(self.classes))
            return self._node

        @property
        def rows(self):
            """Sequence of data cells displayed by the table."""
            return self.__rows

        @property
        def sheet_name(self):
            """Optional name of the sheet.

            Not used for HTML reports. Defaults to "SheetN" where
            N is the number of existing sheets plus 1.
            """

            return self.__opts.get("sheet_name")

        @property
        def show_report_date(self):
            """If True, add the report date between the caption and headers."""
            return True if self.__opts.get("show_report_date") else False


class Excel:
    """Build workbooks using OpenPyXl

    See https://openpyxl.readthedocs.io/en/stable/

    We have created most of our Excel reports in the past using
    the pre-2007 formats. This package uses the current, more
    capable Excel format, and is the recommended package for
    generating Excel workbooks in Python.

    In rare cases it will be necessary to use the XlsxWriter
    package (https://xlsxwriter.readthedocs.io). See, for example,
    the MediaCaptionContent.py web admin report. One drawback of
    the XlsxWriter package is that it cannot read Excel workbooks,
    only create them.
    TODO: document the limitations of the OpenPyXl package which
    require the use of XlsxWriter.

    Note that an object of this class has properties for commonly
    used styles. It caches these properties, rather that creating
    a new style object for each call, so be careful that you not
    alter the returned property unless you are sure that you will
    want _all_ uses of that style property to have that change.
    """

    MIME_SUBTYPE = "vnd.openxmlformats-officedocument.spreadsheetml.sheet"
    MIME_TYPE = f"application/{MIME_SUBTYPE}"
    from openpyxl.utils import get_column_letter

    def __init__(self, title=None, **opts):
        self.__title = title
        self.__opts = opts

    def add_sheet(self, name=None):
        if not name:
            name = f"Sheet{len(self.book.worksheets)+1}"
        sheet = self.book.create_sheet(title=name)
        sheet.print_options.gridLines = True
        return sheet

    def merge(self, start_row, start_column, end_row, end_column):
        self.sheet.merge_cells(
            start_row=start_row,
            start_column=start_column,
            end_row=end_row,
            end_column=end_column
        )

    def save(self, directory=None):
        path = self.filename
        if directory:
            path = f"{directory}/{path}"
        with open(path, "wb") as fp:
            self.book.save(fp)

    def send(self):
        headers = (
            f"Content-type: {self.book.mime_type}",
            f"Content-disposition: attachment; filename={self.filename}"
        )
        headers = "\r\n".join(headers) + "\r\n\r\n"
        sys.stdout.buffer.write(headers.encode("utf-8"))
        self.book.save(sys.stdout.buffer)

    def set_width(self, col, width):
        letter = Excel.get_column_letter(col)
        self.sheet.column_dimensions[letter].width = width

    def write(self, row, column, value, styles=None):
        cell = self.sheet.cell(row=row, column=column, value=value)
        if styles:
            for name in styles:
                setattr(cell, name, styles[name])
        return cell

    @property
    def hyperlink(self):
        """Font styling we want for links."""
        if not hasattr(self, "_hyperlink"):
            opts = dict(color = "000000FF", underline="single")
            self._hyperlink = openpyxl.styles.Font(**opts)
        return self._hyperlink

    @property
    def bold(self):
        if not hasattr(self, "_bold"):
            self._bold = openpyxl.styles.Font(bold=True)
        return self._bold

    @property
    def center(self):
        if not hasattr(self, "_center"):
            opts = dict(horizontal="center", vertical="top",
                        wrap_text=self.wrap)
            self._center = openpyxl.styles.Alignment(**opts)
        return self._center

    @property
    def right(self):
        if not hasattr(self, "_right"):
            opts = dict(horizontal="right", vertical="top",
                        wrap_text=self.wrap)
            self._right = openpyxl.styles.Alignment(**opts)
        return self._right

    @property
    def left(self):
        if not hasattr(self, "_left"):
            opts = dict(horizontal="left", vertical="top", wrap_text=self.wrap)
            self._left = openpyxl.styles.Alignment(**opts)
        return self._left

    @property
    def center_middle(self):
        """Center both horizontally and vertically."""
        if not hasattr(self, "_center_middle"):
            opts = dict(horizontal="center", vertical="center",
                        wrap_text=self.wrap)
            self._center_middle = openpyxl.styles.Alignment(**opts)
        return self._center_middle

    @property
    def book(self):
        """Create a workbook with no sheets."""
        if not hasattr(self, "_book"):
            self._book = openpyxl.Workbook()
            if not self.__opts.get("keep_initial_sheet"):
                for sheet in self._book.worksheets:
                    self._book.remove(sheet)
        return self._book

    @property
    def sheet(self):
        """The most recently added sheet if any, else None."""
        return self.book.worksheets[-1] if self.book.worksheets else None

    @property
    def title(self):
        return self.__title

    @property
    def filename(self):
        if not hasattr(self, "_filename"):
            stamp = ""
            if self.__opts.get("stamp") or not self.title:
                stamp = datetime.datetime.now().strftime("-%Y%m%d%H%M%S")
            if self.title:
                title = re.sub(r"\W", "_", self.title)
            else:
                title = "workbook"
            self._filename = f"{title}{stamp}.xlsx"
        return self._filename

    @property
    def wrap(self):
        return self.__opts.get("wrap")

    @classmethod
    def pixels_to_chars(cls, pixels):
        """Convert pixels to characters for column widths, etc.

        See https://docs.microsoft.com/en-us/previous-versions/office\
        /developer/office-2010/cc802410(v=office.14)

        In particular:
          "To translate from pixels to character width, use this calculation:
            =Truncate(({pixels}-5)/{Maximum Digit Width} * 100+0.5)/100

        The following code gets the maximum digit width for the default
        font in a new Excel installation:

        >>> from PIL import ImageFont
        >>> font = ImageFont.truetype('d:/Windows/Fonts/calibri.ttf', 11)
        >>> font.getsize("0")
        (6, 9)

        In other words, 6 pixels wide.

        To go back from chars to pixels:
            pixels = int(round(6 * ((100 * chars - .5) / 100) + 5))
        """
        max_digit_width = 6
        pixels = int(re.sub("[^0-9]+", "", pixels))
        return int((pixels - 5) / max_digit_width * 100 + 0.5) / 100


class AdvancedSearch(FormFieldFactory):
    """Search for CDR documents of a specific type.

    It is the responsibility of the derived classes to:
       1. populate the search_fields member
       2. populate the query_fields member
       3. assign the class-level DOCTYPE value
       4. assign the class-level FILTER value (if needed)
       5. override customize_form() if appropriate
       6. override customize_report() if appropriate

    Public methods:

        run()
            top-level processing entry point
    """

    INCLUDE_ROWS = True
    FILTER = None
    DBQuery = db.Query

    def __init__(self):
        self.match_all = True if self.fields.getvalue("match_all") else False
        self.session = Session(getSession(self.fields) or "guest")
        self.request = self.fields.getvalue("Request")
        self.search_fields = []

    @property
    def fields(self):
        """Named values from the CGI form."""
        if not hasattr(self, "_fields"):
            self._fields = cgi.FieldStorage()
        return self._fields

    def run(self):
        try:
            if self.request == "Search":
                self.show_report()
            else:
                self.show_form()
        except Exception as e:
            try:
                message = "AdvancedSearch.run(request=%r)"
                self.session.logger.exception(message, self.request)
            except:
                bail(f"Unable to log exception {e}")
            bail(f"AdvancedSearch.run() failure: {e}")

    def show_form(self, subtitle=None, error=None):
        args = self.session.name, subtitle or self.SUBTITLE, self.search_fields
        page = self.Form(*args, error=error)
        self.customize_form(page)
        sendPage(page.tostring())

    def show_report(self):
        args = self.query_fields, self.DOCTYPE, self.match_all
        self.query = query = self.Query(*args)
        self.rows = rows =query.execute(self.session.cursor).fetchall()
        connector = " and " if self.match_all else " or "
        strings = connector.join([repr(c) for c in query.criteria])
        subtitle = self.SUBTITLE
        opts = dict(search_strings=strings, count=len(rows), subtitle=subtitle)
        if self.INCLUDE_ROWS:
            opts["rows"] = rows
        if self.FILTER:
            opts["filter"] = self.FILTER
        page = self.ResultsPage(self.DOCTYPE, **opts)
        self.customize_report(page)
        sendPage(page.tostring())

    def customize_form(self, page):
        """Override in derived class if default behavior isn't enough."""

    def customize_report(self, page):
        """Override in derived class if default behavior isn't enough."""

    def values_for_paths(self, paths):
        query = db.Query("query_term", "value").unique().order("value")
        query.where(query.Condition("path", paths, "IN"))
        rows = query.execute(self.session.cursor).fetchall()
        return [row.value for row in rows if row.value.strip()]

    @property
    def valid_values(self):
        if not hasattr(self, "_valid_values"):
            doctype = cdr.getDoctype("guest", self.DOCTYPE)
            self._valid_values = dict(doctype.vvLists)
        return self._valid_values

    @property
    def countries(self):
        query = db.Query("document d", "d.id", "d.title")
        query.join("doc_type t", "t.id = d.doc_type")
        query.where("t.name = 'Country'")
        query.order("d.title")
        rows = query.execute(self.session.cursor).fetchall()
        return [(f"CDR{row.id:010d}", row.title) for row in rows]

    @property
    def states(self):
        fields = "s.id AS i", "s.title AS s", "c.title as c"
        query = db.Query("document s", *fields)
        query.join("query_term r", "r.doc_id = s.id")
        query.join("document c", "c.id = r.int_val")
        query.where("r.path = '/PoliticalSubUnit/Country/@cdr:ref'")
        query.order("s.title", "c.title")
        rows = query.execute(self.session.cursor).fetchall()
        return [(f"CDR{row.i:010d}", f"{row.s} [{row.c}]") for row in rows]

    @property
    def statuses(self):
        """Valid active_status value for non-deleted CDR documents."""
        return [("A", "Active"), ("I", "Inactive")]


    class Form(HTMLPage):
        BUTTONS = (
            HTMLPage.button("Search"),
            HTMLPage.button("Clear", button_type="reset"),
        )
        TITLE = "CDR Advanced Search"
        MATCH_ALL_HELP = "If unchecked any field match will succeed."
        def __init__(self, session, subtitle, fields, **kwargs):
            if "buttons" not in kwargs:
                buttons = self.BUTTONS
            kwargs = dict(
                **kwargs,
                buttons=buttons,
                session=session,
                subtitle=subtitle,
                method="get",
            )
            HTMLPage.__init__(self, self.TITLE, **kwargs)
            self.add_session_field(session or "guest")
            fieldset = self.fieldset("Search Fields")
            if kwargs.get("error"):
                classes = self.B.CLASS("error center")
                self.form.append(self.B.P(kwargs["error"], classes))
            self.form.append(fieldset)
            for field in fields:
                fieldset.append(field)
            if len(fields) > 1:
                fieldset = self.fieldset("Options")
                self.form.append(fieldset)
                opts = dict(
                    label="Match All Criteria",
                    checked=True,
                    value="yes",
                    tooltip=self.MATCH_ALL_HELP,
                )
                fieldset.append(self.checkbox("match_all", **opts))
            else:
                self.form.append(self.hidden_field("match_all", "yes"))


    class ResultsPage(HTMLPage):

        TITLE = "CDR Advanced Search Results"
        PERSONS_ORGS_GLOSSARY = (
            "Person",
            "Organization",
            "GlossaryTermConcept",
            "GlossaryTermName",
        )

        def __init__(self, doctype, **kwargs):

            # Let the base class get us started.
            subtitle = kwargs.get("subtitle", doctype)
            opts = dict(body_id="advanced-search-results", subtitle=subtitle)
            HTMLPage.__init__(self, self.TITLE, **opts)

            # Add a summary line if we have the requisite information.
            strings = kwargs.get("search_strings")
            count = kwargs.get("count")
            if strings and isinstance(count, int):
                if count == 1:
                    summary = f"1 document matches {strings}"
                else:
                    summary = f"{count:d} documents match {strings}"
                self.body.append(self.B.P(summary))

            # Start the table for the results.
            table = self.B.TABLE()
            self.body.append(table)

            # If we have the rows for the results set, add them to the table.
            rows = kwargs.get("rows")
            if rows:
                for i, row in enumerate(rows):
                    cdr_id = cdr.normalize(row[0])
                    title = row[1].replace(";", "; ")

                    # Create the link.
                    base = f"{BASE}/QcReport.py"
                    if doctype in self.PERSONS_ORGS_GLOSSARY:
                        url = f"{base}?DocId={cdr_id}"
                    elif doctype == "Summary":
                        url = f"{base}?DocId={cdr_id}&ReportType=nm"
                    else:
                        base = f"{BASE}/Filter.py"
                        filtre = kwargs.get("filter")
                        url = f"{base}?DocId={cdr_id}&Filter={filtre}"
                    url += f"&{SESSION}={self.session}"
                    link = self.B.A(cdr_id, href=url)

                    # Assemble the table row and attach it.
                    tr = self.B.TR(self.B.CLASS("row-item"))
                    tr.append(self.B.TD(f"{i+1}.", self.B.CLASS("row-number")))
                    tr.append(self.B.TD(link, self.B.CLASS("doc-link")))
                    tr.append(self.B.TD(title, self.B.CLASS("doc-title")))
                    table.append(tr)


    class QueryField:
        """Information used to plug one field into the search query."""

        def __init__(self, var, selectors):
            """Capture the value and the paths used to look for it.

            Pass:
                var - the CGI variable's value for this search field
                selectors - list of paths to look for in the query_term table

            In some cases `selectors` is a single string, in which case it
            is the name of a column in the `document` table.
            """
            self.var = var
            self.selectors = selectors

    class Query:
        """Builds a database query for an advanced search.

        Attributes:
            fields - information on the search fields from the form
            doctype - string identifying which documents we're looking for
            match_all - if False then OR the field conditions together
            criteria - list of all the field values being search for,
                       so we can remind the user what she put on the form
            query - db.Query object built on demand
        """

        def __init__(self, fields, doctype, match_all=True):
            """Capture what we'll need to build the SQL query.

            Pass:
                fields - sequence of `QueryField` objects
                doctype - string for the CDR document type (e.g., 'Summary')
                match_all - if set to `False` then _any_ field match works
            """

            self.fields = fields
            self.match_all = match_all
            self.doctype = doctype
            self.criteria = []

        def execute(self, cursor=None):
            """Convenience method to make the code cleaner."""
            return self.query.execute(cursor)

        @property
        def query(self):
            """Build the query for the search if it's not already cached."""

            if not hasattr(self, "_query"):

                # Set up some convenience aliases.
                Query = db.Query
                Condition = Query.Condition
                Or = Query.Or

                # Create a query object.
                columns = f"d.id", "d.title" #, f"'{self.doctype}' AS doctype"
                self._query = Query("document d", *columns).order("d.title")

                # Add the conditions: one for each field with a value.
                conditions = []
                have_doctype = False
                for field in self.fields:

                    # See if we got a value for this field.
                    value = field.var.strip() if field.var else None
                    if not value:
                        continue

                    # Remember the criterion for display to the user.
                    self.criteria.append(value)

                    # If 'selectors' is a string, it's a column in `document`.
                    value_op = getQueryOp(value)
                    if isinstance(field.selectors, str):
                        column = f"d.{field.selectors}"
                        conditions.append(Condition(column, value, value_op))
                        continue

                    # Build up a subquery for the field.
                    have_doctype = True
                    subquery = Query("query_term", "doc_id").unique()

                    # These are ORd together.
                    selector_conditions = []
                    for path in field.selectors:
                        path_op = "LIKE" if "%" in path else "="

                        # Simple case: test for a string stored in the doc.
                        if not path.endswith("/@cdr:ref[int_val]"):
                            path_test = Condition("path", path, path_op)
                            value_test = Condition("value", value, value_op)
                            selector_conditions.append((path_test, value_test))

                        # Trickier case: find values in linked documents.
                        else:
                            path = path.replace("[int_val]", "")
                            title_query = Query("document", "id").unique()
                            args = "title", value, value_op
                            title_query.where(Condition(*args))
                            args = "int_val", title_query, "IN"
                            title_test = Condition(*args)
                            path_test = Condition("path", path, path_op)
                            selector_conditions.append((path_test, title_test))

                    # Add the conditions for this field's selectors to the mix.
                    subquery.where(Or(*selector_conditions))
                    conditions.append(Condition("d.id", subquery, "IN"))

                # Sanity check.
                if not conditions:
                    raise Exception("No search conditions specified")

                # If all the selectors are from the document table (or,
                # view, to be precise), then we still need to narrow
                # the search to this document type.
                if not have_doctype:
                    self._query.join("doc_type t", "t.id = d.doc_type")
                    self._query.where(Condition("t.name", self.doctype))
                # Plug the top-level conditions into the query.
                if self.match_all:
                    for condition in conditions:
                        self._query.where(condition)
                else:
                    self._query.where(Or(*conditions))

            # All the fields with values have been folded into the query.
            #bail(str(self._query))
            return self._query


#----------------------------------------------------------------------#
#          LEGACY CLASSES -- USE THE REPLACEMENTS ABOVE INSTEAD        #
#----------------------------------------------------------------------#

NEWLINE  = "@@@NEWLINE-PLACEHOLDER@@@"
BR       = "@@@BR-PLACEHOLDER@@@"
bail     = Controller.bail

class Control:
    """
    Base class for top-level controller for a CGI script, which
    puts up a request form (typically for a report), collects
    the options specified by the user, and fulfills the request.

    The call tree for this class looks something like this:

      __init__() [implied invokation when the object is created]
      run() [invoked explicitly by the writer of the CGI script]
        logout() [if the button for logging out is clicked]
        navigate_to() [if a button to navigate elsewhere is clicked]
        show_report() [if Submit is clicked]
          build_tables()
          set_report_options()
          report.send()
        show_form() [if not button was clicked]
           set_form_options()
           populate_form()
           form.send()
    """
    PAGE_TITLE = "CDR Administration"
    REPORTS_MENU = SUBMENU = "Reports Menu"
    ADMINMENU = MAINMENU
    DEVMENU  = DEVTOP
    SUBMIT = "Submit"
    LOG_OUT = "Log Out"
    FORMATS = "html", "excel"
    BOARD_NAME = "/Organization/OrganizationNameInformation/OfficialName/Name"
    AUDIENCES = "Health Professional", "Patient"
    LANGUAGES = "English", "Spanish"
    SUMMARY_SELECTION_METHODS = "id", "title", "board"
    LOGNAME = "reports"
    LOGLEVEL = "INFO"

    def __init__(self, title=None, subtitle=None, **opts):
        """
        Sets up a skeletal controller; derived class fleshes it out,
        including fetching and validating options for the specific
        report.
        """
        self.started = datetime.datetime.now()
        self.title = title or ""
        self.logger = self.get_logger()
        self.subtitle = subtitle
        self.opts = opts
        if self.opts.get("open_cursor", True):
            self.cursor = db.connect(user="CdrGuest").cursor()
        else:
            self.cursor = None
        self.fields = cgi.FieldStorage()
        self.session = getSession(self.fields, cursor=self.cursor)
        self.request = getRequest(self.fields)
        self.format = self.fields.getvalue("format", self.FORMATS[0])
        self.buttons = []
        for button in (self.SUBMIT, self.SUBMENU, self.ADMINMENU, self.LOG_OUT):
            if button:
                self.buttons.append(button)
        self.script = os.path.basename(sys.argv[0])
    def run(self):
        "Derived class overrides this method if there are custom actions."
        try:
            if self.request == self.ADMINMENU:
                navigateTo("Admin.py", self.session)
            elif self.request == self.REPORTS_MENU:
                navigateTo("Reports.py", self.session)
            elif self.request == self.DEVMENU:
                navigateTo("DevSA.py", self.session)
            elif self.request == self.LOG_OUT:
                logout(self.session)
            elif self.request == self.SUBMIT:
                self.show_report()
            else:
                self.show_form()
        except Exception as e:
            bail(str(e))
    def show_report(self):
        "Override this method if you have a non-tabular report."
        tables = self.build_tables()
        buttons = []
        for button in (self.SUBMENU, self.ADMINMENU, self.LOG_OUT):
            if button:
                buttons.append(button)
        opts = {
            "banner": self.title or "",
            "subtitle": self.subtitle,
            "page_opts": {
                "buttons": buttons,
                "action": self.script,
                "session": self.session,
                "action": buttons and self.script or None
            }
        }
        opts = self.set_report_options(opts)
        report = Report(self.PAGE_TITLE or "", tables, **opts)
        report.send(self.format)
    def show_form(self):
        opts = {
            "buttons": self.buttons,
            "action": self.script,
            "subtitle": self.title,
            "session": self.session
        }
        opts = self.set_form_options(opts)
        form = Page(self.PAGE_TITLE or "", **opts)
        form.add_script("""\
function check_set(name, val) {
    var all_selector = "#" + name + "-all";
    var ind_selector = "#" + name + "-set .ind";
    if (val == "all") {
        if (jQuery(all_selector).prop("checked"))
            jQuery(ind_selector).prop("checked", false);
        else
            jQuery(all_selector).prop("checked", true);
    }
    else if (jQuery(ind_selector + ":checked").length > 0)
        jQuery(all_selector).prop("checked", false);
    else
        jQuery(all_selector).prop("checked", true);
}""")
        self.populate_form(form)
        form.send()
    def build_tables(self):
        "Stub, to be overridden by real controllers."
        return []
    def set_report_options(self, opts):
        "Stub, to be overridden by real controllers."
        return opts
    def set_form_options(self, opts):
        "Stub, to be overridden by real controllers."
        return opts
    def populate_form(self, form):
        "Stub, to be overridden by real controllers."
        pass

    def get_logger(self):
        """
        Create a logging object.

        Separated out so derived classes can completely customize this.
        """

        logger = cdr.Logging.get_logger(self.LOGNAME, level=self.LOGLEVEL)
        if self.title:
            logger.info("started %s", self.title)
        return logger

    def get_unicode_parameter(self, name):
        """
        Get the Unicode value for a CGI parameter

        Pass:
          name - string for the name of the CGI parameter

        Return:
          Unicode value for the parameter (u"" for None)
        """

        value = self.fields.getvalue(name)
        if value is None:
            return ""
        if isinstance(value, bytes):
            return str(value, "utf-8")
        return str(value)

    @staticmethod
    def get_referer():
        "Find out which page called us."
        return os.environ.get("HTTP_REFERER")

    def get_boards(self):
        """
        Assemble a dictionary of the PDQ board names, indexed by
        CDR Organization document ID. Trim the names to their
        short forms, pruning away the "PDQ" prefix and the
        "Editorial Board" suffix.
        """

        return self.get_pdq_editorial_boards(self.cursor)

    @classmethod
    def get_pdq_editorial_boards(cls, cursor=None):
        """
        Expose this functionality to non-CGI code.
        """

        query = db.Query("query_term n", "n.doc_id", "n.value")
        query.join("query_term t", "t.doc_id = n.doc_id")
        query.join("active_doc a", "a.id = n.doc_id")
        query.where("t.path = '/Organization/OrganizationType'")
        query.where("n.path = '%s'" % cls.BOARD_NAME)
        query.where("t.value = 'PDQ Editorial Board'")
        rows = query.execute(cursor).fetchall()
        boards = {}
        prefix, suffix = "PDQ ", " Editorial Board"
        for org_id, name in rows:
            if name.startswith(prefix):
                name = name[len(prefix):]
            if name.endswith(suffix):
                name = name[:-len(suffix)]
            boards[org_id] = name
        return boards

    def add_summary_selection_fields(self, form, **opts):
        """
        Display the fields used to specify which summaries should be
        selected for a report, using one of several methods:

            * by summary document ID
            * by summary title
            * by summary board

        There are two branches taken by this method. If the user has
        elected to select a summary by summary title, and the summary
        title fragment matches more than one summary, then a follow-up
        page is presented on which the user selects one of the summaries
        and re-submits the report request. Otherwise, the user is shown
        options for choosing a selection method, which in turn displays
        the fields appropriate to that method dynamically. We also add
        JavaScript functions to handle the dynamic control of field display.
        If the 'boards' attribute exists, it is used for the board check
        boxes. Otherwise, we call our get_boards() method ourselves.

        Pass:
            form     - Page object on which to show the fields
            titles   - an optional array of SummaryTitle objects
            audience - if False, omit Audience buttons (default is True)
            language - if False, omit Language buttons (default is True)
            id-label - optional string for the CDR ID field (defaults
                       to "CDR ID" but can be overridden, for example,
                       to say "CDR ID(s)" if multiple IDs are accepted)
            id-tip   - optional string for the CDR ID field for popup
                       help (e.g., "separate multiple IDs by spaces")

        Return:
            nothing (the form object is populated as a side effect)
        """

        #--------------------------------------------------------------
        # Show the second stage in a cascading sequence of the form if we
        # have invoked this method directly from build_tables(). Widen
        # the form to accomodate the length of the title substrings
        # we're showing.
        #--------------------------------------------------------------
        titles = opts.get("titles")
        if titles:
            form.add_css("fieldset { width: 600px; }")
            form.add_hidden_field("method", "id")
            form.add("<fieldset>")
            form.add(form.B.LEGEND("Choose Summary"))
            for t in titles:
                form.add_radio("cdr-id", t.display, t.id, tooltip=t.tooltip)
            form.add("</fieldset>")
            self.new_tab_on_submit(form)

        else:
            # Fields for the original form.
            form.add("<fieldset>")
            form.add(form.B.LEGEND("Selection Method"))
            form.add_radio("method", "By PDQ Board", "board", checked=True)
            form.add_radio("method", "By CDR ID", "id")
            form.add_radio("method", "By Summary Title", "title")
            form.add("</fieldset>")
            self.add_board_fieldset(form)
            if opts.get("audience", True):
                self.add_audience_fieldset(form)
            if opts.get("language", True):
                self.add_language_fieldset(form)
            form.add("<fieldset class='by-id-block'>")
            form.add(form.B.LEGEND("Summary Document ID"))
            form.add_text_field("cdr-id", opts.get("id-label", "CDR ID"),
                                tooltip=opts.get("id-tip"))
            form.add("</fieldset>")
            form.add("<fieldset class='by-title-block'>")
            form.add(form.B.LEGEND("Summary Title"))
            form.add_text_field("title", "Title",
                                tooltip="Use wildcard (%) as appropriate.")
            form.add("</fieldset>")
            form.add_script(self.get_script_for_summary_selection_form())

    def add_board_fieldset(self, form):
        classes = ["by-board-block"]
        form.add("<fieldset class='%s' id='board-set'>" % " ".join(classes))
        form.add(form.B.LEGEND("Board"))
        form.add_checkbox("board", "All Boards", "all", checked=True)
        boards = getattr(self, "boards")
        if not boards:
            boards = self.get_boards()
        for board_id in sorted(boards, key=boards.get):
            form.add_checkbox("board", boards.get(board_id),
                              board_id, widget_classes="ind")
        form.add("</fieldset>")

    def add_audience_fieldset(self, form, include_any=False):
        form.add("<fieldset id='audience-block' class='by-board-block'>")
        form.add(form.B.LEGEND("Audience"))
        checked = True
        if include_any:
            form.add_radio("audience", "Any", "", checked=True)
            checked = False
        for value in self.AUDIENCES:
            form.add_radio("audience", value, value, checked=checked)
            checked = False
        form.add("</fieldset>")

    def add_language_fieldset(self, form, include_any=False):
        form.add("<fieldset id='language-block' class='by-board-block'>")
        form.add(form.B.LEGEND("Language"))
        checked = True
        if include_any:
            form.add_radio("language", "Any", "", checked=True)
            checked = False
        for value in self.LANGUAGES:
            form.add_radio("language", value, value, checked=checked)
            checked = False
        form.add("</fieldset>")

    def validate_audience(self):
        if self.audience and self.audience not in self.AUDIENCES:
            bail()

    def validate_language(self):
        if self.language and self.language not in self.LANGUAGES:
            bail()

    def validate_boards(self):
        if not self.board or "all" in self.board:
            self.board = ["all"]
        else:
            boards = []
            for board in self.board:
                try:
                    board = int(board)
                except:
                    bail()
                if board not in self.boards:
                    bail()
                boards.append(board)
            self.board = boards

    def validate_selection_method(self):
        if self.selection_method not in self.SUMMARY_SELECTION_METHODS:
            bail()

    def get_cdr_ref_int(self, node):
        """
        Extract and return integer from cdr:ref attribute on passed node.
        Return None if valid ID not found.
        """

        cdr_ref = node.get("{cips.nci.nih.gov/cdr}ref")
        if not cdr_ref:
            return None
        integer = re.sub("[^\\d]+", "", cdr_ref)
        return integer and int(integer) or None

    def get_doc_title(self, doc_id):
        """
        Fetch the title column from the all_docs table for a CDR document.
        """

        query = db.Query("document", "title")
        query.where(query.Condition("id", doc_id))
        row = query.execute(self.cursor).fetchone()
        return row and row[0] or ""

    def get_int_cdr_id(self, value):
        """
        Convert CDR ID to integer. Exit with an error message on failure.
        """

        if value:
            try:
                return cdr.exNormalize(value)[1]
            except:
                bail("Invalid format for CDR ID")
        return None

    def get_parsed_doc_xml(self, doc_id, doc_version=None):
        """
        Fetch the xml for a CDR document and return the root element object.
        """

        xml = self.get_doc_xml(doc_id, doc_version)
        return etree.fromstring(xml.encode("utf-8"))

    def get_doc_xml(self, doc_id, doc_version=None):
        """
        Fetch the XML for a CDR document. Caller handles exceptions.
        Return value is Unicode. Encode if necessary.
        """

        if doc_version:
            query = db.Query("doc_version", "xml")
            query.where(query.Condition("num", doc_version))
        else:
            query = db.Query("document", "xml")
        query.where(query.Condition("id", doc_id))
        return query.execute(self.cursor).fetchone()[0]

    def summaries_for_title(self, fragment):
        """
        Find the summaries that match the user's title fragment. Note
        that the user is responsible for adding any non-trailing SQL
        wildcards to the fragment string. If the title is longer than
        60 characters, truncate with an ellipsis, but add a tooltip
        showing the whole title. We create a local class for the
        resulting list.
        """

        class SummaryTitle:
            def __init__(self, doc_id, display, tooltip=None):
                self.id = doc_id
                self.display = display
                self.tooltip = tooltip

        if isinstance(fragment, bytes):
            fragment = str(fragment, "utf-8")
        query = db.Query("active_doc d", "d.id", "d.title")
        query.join("doc_type t", "t.id = d.doc_type")
        query.where("t.name = 'Summary'")
        query.where(query.Condition("d.title", fragment + "%", "LIKE"))
        query.order("d.title")
        rows = query.execute(self.cursor).fetchall()
        summaries = []
        for doc_id, title in rows:
            if len(title) > 60:
                short_title = title[:57] + "..."
                summary = SummaryTitle(doc_id, short_title, title)
            else:
                summary = SummaryTitle(doc_id, title)
            summaries.append(summary)
        return summaries

    class UploadedFile:
        """
        Information about a file upload field's values (including bytes).

        The caller is responsible for ensuring that standard input is
        set to binary mode when running on Windows.

        Instance values:
            filename - the name of the uploaded file
            bytes - the binary content of the file
        """

        def __init__(self, control, field_name):
            """
            Collect and return the bytes for a file field.

            Pass:
            field_name - name of the file upload field
            """

            control.logger.info("loading file upload field %r", field_name)
            self.filename = self.bytes = None
            if field_name not in control.fields:
                return
            val = control.fields[field_name]
            if val.file:
                self.filename = val.filename
                control.logger.info("file name is %r", self.filename)
                bytes = []
                more_bytes = val.file.read()
                while more_bytes:
                    control.logger.info("read %d bytes from file",
                                        len(more_bytes))
                    bytes.append(more_bytes)
                    more_bytes = val.file.read()
            else:
                bytes = [val.value]
            self.bytes = "".join(bytes)
            control.logger.info("total bytes for file: %d", len(self.bytes))


    def new_tab_on_submit(self, form):
        """
        Take over the onclick event for the Submit button in order to
        show the report in a new tab. This avoids the problem of the
        request to resubmit a form unnecessarily when navigating back
        to the base report request form through an intermediate form
        (such as the one to choose from multiple matching titles).

        Pass:
            form - reference to the form object to which the script is added
        """

        form.add_script("""\
jQuery("input[value='Submit']").click(function(e) {
    var parms = jQuery("form").serialize();
    if (!/Request=Submit/.test(parms)) {
        if (parms)
            parms += "&";
        parms += "Request=Submit";
    }
    var url = "%s?" + parms;
    window.open(url, "_blank");
    e.preventDefault();
});""" % self.script)

    @staticmethod
    def toggle_display(function_name, show_value, class_name):
        """
        Create JavaScript function to show or hide elements.

        Pass:
            function_name  - name of the JavaScript function to create
            show_value     - controlling element's value causing show
            class_name     - class of which the controlled blocks are members
        Return:
            source code for JavaScript function
        """

        return """\
function %s(value) {
    if (value == "%s")
        jQuery(".%s").show();
    else
        jQuery(".%s").hide();
}""" % (function_name, show_value, class_name, class_name)

    def get_script_for_summary_selection_form(self):
        " Local JavaScript to manage sections of the form dynamically."
        return """\
function check_board(board) { check_set("board", board); }
function check_method(method) {
    switch (method) {
        case 'id':
            jQuery('.by-board-block').hide();
            jQuery('.by-id-block').show();
            jQuery('.by-title-block').hide();
            break;
        case 'board':
            jQuery('.by-board-block').show();
            jQuery('.by-id-block').hide();
            jQuery('.by-title-block').hide();
            break;
        case 'title':
            jQuery('.by-board-block').hide();
            jQuery('.by-id-block').hide();
            jQuery('.by-title-block').show();
            break;
    }
}
jQuery(function() {
    check_method(jQuery("input[name='method']:checked").val());
});"""

xlwt.add_palette_colour("hdrbg", 0x21)

class ExcelStyles:
    """
    Styles for an Excel workbook.

    Used for rewriting legacy reports originally using ExcelWriter,
    in cases where we don't want to spend the time on a more extensive
    rewrite of the report to use the Page/Report/Control classes below.

    Example usage:

    import cdrcgi
    styles = cdrcgi.ExcelStyles()
    sheet = styles.add_sheet("Sample Report")
    sheet.set_panes_frozen(True)
    sheet.set_horz_split_pos(1)
    widths = 10, 60, 100
    labels = "CDR ID", "Title", "Comments"
    assert(len(widths) == len(labels))
    for i, chars in enumerate(widths):
        sheet.col(i).width = styles.chars_to_width(chars)
    sheet.write_merge(0, 0, 0, len(labels) - 1, "Sample Report", styles.banner)
    for i, label in enumerate(labels):
        sheet.write(1, i, label, styles.header)
    data = (
        (500000, "Title of the first document", "A comment on the document"),
        (600000, "Title for the next document", "Another comment"),
        (700000, "Last document title", "Final comment")
    )
    for i, values in enumerate(data):
        for col, value in enumerate(values):
            sheet.write(i + 2, col, value, styles.left)
    styles.book.save("sample-report.xls")
    """

    BORDERS = "borders: top hair, bottom hair, left hair, right hair"
    LEFT = "align: wrap True, horiz left, vert top"
    RIGHT = "align: wrap True, horiz right, vert top"
    CENTER = "align: wrap True, horiz center, vert center"
    CENTER_TOP = "align: wrap True, horiz center, vert top"
    RED = "font: color red"
    BLUE = "font: color blue"
    MAROON = "font: color dark_red"
    TEAL = "font: color teal"
    PURPLE = "font: color violet"
    WHITE = "font: color white"
    HYPERLINK = "font: color blue, underline single"
    TWIPS = 20

    def __init__(self):
        """
        Create some commonly useful style objects.
        """

        self.book = xlwt.Workbook(encoding="UTF-8")
        self.left = self.style(self.LEFT)
        self.right = self.style(self.RIGHT)
        self.center = self.style(self.CENTER_TOP)
        self.banner = self.style(self.CENTER, self.bold_font(12))
        self.header = self.style(self.CENTER, self.bold_font())
        self.bold = self.style(self.bold_font())
        self.red = self.style(self.RED)
        self.blue = self.style(self.BLUE)
        self.teal = self.style(self.TEAL)
        self.purple = self.style(self.PURPLE)
        self.maroon = self.style(self.MAROON)
        self.white = self.style(self.WHITE)
        self.error = self.style(self.bold_font(color="red"))
        self.url = self.style(self.HYPERLINK, self.LEFT)

    def add_sheet(self, name, **opts):
        """
        Create a new worksheet, possibly with frozen rows or columns.

        Pass:
            name         - required string for the worksheet's name
            frozen_rows  - optional keyword argument for the number of rows
                           which should always be visible at the top
            frozen_cols  - optional keyword argument for the number of
                           columns which should always be visible at the
                           left
            cell_overwrite_ok - optional keyword argument to override the
                           default behavior which prevents the code from
                           writing to the same cell more than once (see
                           http://stackoverflow.com/questions/41770461).
        Return:
            new object representing a new Excel worksheet
        """

        cell_overwrite_ok = opts.get("cell_overwrite_ok") and True or False
        sheet = self.book.add_sheet(name, cell_overwrite_ok)
        sheet.print_grid = True
        frozen_rows = opts.get("frozen_rows")
        frozen_cols = opts.get("frozen_cols")
        if frozen_rows:
            sheet.panes_frozen = True
            sheet.horz_split_pos = int(frozen_rows)
        if frozen_cols:
            sheet.panes_frozen = True
            sheet.vert_split_pos = int(frozen_cols)
        return sheet

    def write_headers(self, sheet, labels, widths=None, **opts):
        """
        Write the header labels for each of the columns in an Excel
        worksheet, and (optionally) set the column widths.

        Pass:
            sheet - reference to an xlwt.Worksheet object
            labels - sequence of strings to write for the header labels
            widths - optional column widths, in number of characters

        Named options:
            blank - integer representing the number blank rows to leave
                    immediately above the row for the column labels
            banners - sequence of strings to be written to the top of the
                      sheet; the first string is written in a larger font

        Return:
            row number for the first data column
        """

        if widths:
            assert(len(labels) == len(widths))
            self.set_widths(sheet, widths)
        row = 0
        banners = opts.get("banners")
        if banners:
            style = self.banner
            for banner in banners:
                sheet.write_merge(row, row, 0, len(labels) - 1, banner, style)
                sytle = self.header
                row += 1
        row += opts.get("blank", 0)
        for i, label in enumerate(labels):
            sheet.write(row, i, label, self.header)
        return row + 1

    @staticmethod
    def link(url, label):
        """
        Wrap the value inside a hyperlink to the specified url.
        """

        return xlwt.Formula('HYPERLINK("%s";"%s")' % (url, label))

    def adjust_color(self, name, rgb):
        """
        Modify the values for a color in the book's palette.

        Pass:
            name  - name of a color in the xlwt color map
            rgb   - six-digit hex string for the new color values
        """

        color_slot = xlwt.Style.colour_map.get(name)
        if not color_slot:
            raise Exception("set_color(): invalid color name %s" % repr(name))
        try:
            r, g, b = int(rgb[:2], 16), int(rgb[2:4], 16), int(rgb[4:6], 16)
        except:
            raise Exception("set_color(): bad argument %s" % repr(rgb))
        self.book.set_colour_RGB(color_slot, r, g, b)

    @classmethod
    def bold_font(cls, point_size=10, color=None):
        """
        Return the string for a bold font of a certain size.

        The string will be passed (possibly as part of a larger string)
        to the xlwt.easyxf() function by our own style() method to create
        an xlwt.XFStyle object.
        """

        font = "font: bold True, height %d" % (cls.TWIPS * point_size)
        if color:
            font += ", color %s" % color
        return font

    @classmethod
    def set_size(cls, style, point_size=10):
        """
        Modify the font size for an existing xlwt XFStyle object.

        Pass:
            style      - reference to an xlwt.XFStyle object
            point_size - new size specified in (possibly fractional) points,
                         which will be converted to twips, which the xlwt
                         package expects
        """

        style.font.height = int(point_size * cls.TWIPS)

    @classmethod
    def set_row_height(cls, row, point_size):
        """
        Adjust the height of a row in an Excel worksheet.

        Pass:
            row        - reference to an xlwt.Row object
            point_size - new height specified in (possibly fractional) points,
                         which will be converted to twips, which the xlwt
                         package expects

        """

        row.height_mismatch = True
        row.height = cls.TWIPS * point_size

    @staticmethod
    def style(*definitions):
        """
        Contruct an xlwt.XFStyle object for formatting cell content.

        Pass:
            one anonymous argument for each style group which needs
            to be modified; the arguments are strings in the format:

                group_name: attribute-name attribute-value[, ...]

            The style groups include:
                * font
                * align[ment]
                * border[s]
                * pattern
                * protection

            See http://xlwt.readthedocs.io/en/latest/api.html and the
            xlwt source code (including examples) for more information.
        """

        return xlwt.easyxf("; ".join(definitions))

    @staticmethod
    def font(spec):
        """
        Construct an xlwt.Font object for formatting cell content segments.

        This is used to support calling sheet.write_rich_text(). For example:

            styles = cdrcgi.ExcelStyles()
            sheet = styles.add_sheet("Rich Text")
            sheet.col(0).width = 5000
            name = "atezolizumab"
            comment = "For NCT01375842; No info available at this time"
            font = styles.font("italic true, color green")
            segments = (name + "\n", (comment, font))
            sheet.write_rich_text(0, 0, segments, styles.left)

            See http://xlwt.readthedocs.io/en/latest/api.html and the
            xlwt source code (including examples) for more information.

            Pass:
                spec - string providing desired attribute values in the form
                       name value[, name value[, ...]
            Return:
                a new xlwt.Font object
        """

        return xlwt.easyfont(spec)

    @staticmethod
    def set_color(style, color="black"):
        """
        Change the foreground color for an xlwt.Style object.

        Pass:
            style - reference to an xlwt.Style object
            color - string identifying one of the colors which can
                    be used in the workbook's cells; examples of
                    valid names include red, dark_red, blue, teal,
                    violet, white, black. For the complete list,
                    refer to the source code in:
                       $PYTHON/lib/site-packages/xlwt/Styles.py
        """

        try:
            style.font.colour_index = xlwt.Style.colour_map[color]
        except:
            raise Exception("set_color(): invalid color %s" % repr(color))

    @staticmethod
    def set_background(style, color="white"):
        """
        Change the foreground color for an xlwt.Style object.

        Pass:
            style - reference to an xlwt.Style object
            color - string identifying one of the colors which can
                    be used in the workbook's cells; examples of
                    valid names include red, dark_red, blue, teal,
                    violet, white, black. For the complete list,
                    refer to the source code in:
                       $PYTHON/lib/site-packages/xlwt/Styles.py
        """

        style.pattern.pattern = xlwt.Pattern.SOLID_PATTERN
        try:
            style.pattern.pattern_fore_colour = xlwt.Style.colour_map[color]
        except:
            raise Exception("set_background(): invalid color %s" % repr(color))

    @staticmethod
    def link(url, label):
        """
        Prepare a value for display as a hyperling in a worksheet cell.

        Pass:
            url   - HTTP[S] address for the link
            label - the value to be displayed for the cell
        """

        return xlwt.Formula('HYPERLINK("%s";"%s")' % (url, label))

    @staticmethod
    def clone(style):
        """
        Create a copy of an xlwt.Style object, which can be altered
        (for example, by calling Style.set_color(), or Style.set_background()).
        """

        return copy.deepcopy(style)

    @staticmethod
    def lookup_color(name):
        """
        Find the color code used internally by Excel for the named color.
        """

        return xlwt.Style.colour_map.get(name)

    @staticmethod
    def points_to_width(points):
        """
        Convert column width from points to BIFF COLINFO units.

        Unfortunately, Microsoft has used a number of different ways over
        the years to represent Excel column widths. The xlwt package uses
        the original BIFF units, which (according to Microsoft's docs) is
        "[w]idth of the columns in 1/256 of the width of the zero character,
        using default font (first FONT record in the file)." Many of our
        older reports were written when XML Excel files were Microsoft's
        format du jour, which measured the column width in points. This
        conversion is an approximation, because the scaling of column
        widths as they increase isn't strictly proportional. But it's close
        enough.
        """

        return int(points * 49)

    @staticmethod
    def chars_to_width(chars):
        """
        Convert width in number of characters to internal BIFF units.
        Determined by experimentation, as I didn't find this formula
        documented anywhere. My guess is that the extra 200 represents
        cell padding.
        """

        return int(chars * 256 + 200)

    @classmethod
    def set_width(cls, sheet, col, chars):
        """
        Set the width of a column in an Excel worksheet.

        Pass:
            sheet - reference to an xlwt.Worksheet object
            col - integer identifying the worksheet column (based 0)
            chars - amount of horizontal space for the column in units
                    of the width of the zero character ("0") using the
                    default font (the first FONT record in the file);
                    see chars_to_width() above
        """

        sheet.col(col).width = cls.chars_to_width(chars)

    @classmethod
    def set_widths(cls, sheet, widths):
        """
        Set the column widths for an Excel worksheet.
        """

        for i, chars in enumerate(widths):
            cls.set_width(sheet, i, chars)

class Page:
    """
    Object used to build a web page.

    Sample usage:

        form = cdrcgi.Page('Simple Report', subtitle='Documents by Title',
                           buttons=('Submit', 'Admin'),
                           action='simple-report.py')
        form.add('<fieldset>')
        form.add(cdrcgi.Page.B.LEGEND('Select Documents By Title'))
        form.add_text_field('title', 'Containing')
        form.add('</fieldset>')
        form.add_output_options()
        form.send()
    """

    INDENT = "  "
    CDN = "https://ajax.googleapis.com/ajax/libs/"
    JS = (
        CDN + "jquery/2.1.4/jquery.min.js",
        CDN + "jqueryui/1.11.4/jquery-ui.min.js"
    )
    STYLESHEETS = (
        CDN + "jqueryui/1.11.4/themes/smoothness/jquery-ui.css",
        CDRCSS
    )
    B = lxml.html.builder

    def __init__(self, title, **kwargs):
        """
        Initializes an object for generating an HTML web page.

        Required argument:

            title -       used in the 'title' element of the 'head' block;
                          also used as the page banner unless overridden

        Optional keyword arguments:

            banner -      overrides the value of the title argument for
                          display of the page's top h1 banner; set to
                          None to eliminate the banner
            subtitle -    displayed below the banner
            method -      'post' or 'get'; defaults to 'post'
            buttons -     values for buttons rendered to the right of the
                          banner; must be an array; empty by default
            action -      handler for HTML form, which will be automatically
                          opened unless no action is provided
            icon -        favicon path; defaults to '/favicon.ico'
            js -          array of external Javascript files to be included;
                          defaults to scripts for jquery and the CDR calendar
                          widget
            stylesheets - array of paths to external CSS files to be loaded;
                          defaults to the standard cdr and CdrCalendar sheets
            session -     string identifying the CDR login session; can be
                          used to override the current session, which will
                          be retrieved from the CGI variables if you don't
                          specify a replacement string here; the session
                          string will be added as a hidden varilable if
                          the 'action' paramater is set
            body_classes -e.g., 'report'
            enctype -     (for form element; e.g., "multipart/form-data")
        """
        self._finished = self._have_date_field = False
        self._html = []
        self._script = []
        self._css = []
        self._level = 0
        self._title = title
        self._banner = kwargs.get("banner", title)
        self._subtitle = kwargs.get("subtitle")
        self._method = kwargs.get("method", "post")
        self._buttons = kwargs.get("buttons")
        self._action = kwargs.get("action")
        self._icon = kwargs.get("icon", "/favicon.ico")
        self._js = kwargs.get("js", Page.JS)
        self._stylesheets = kwargs.get("stylesheets", Page.STYLESHEETS)
        self._session = kwargs.get("session")
        self._body_classes = kwargs.get("body_classes")
        self._enctype = kwargs.get("enctype")
        self._start()

    def get_action(self): return self._action

    def add(self, line, post_indent=True):
        """
        Add a line to the HTML for the page.

        Passed:

            line -        either a string or an object created by the
                          lxml.html.builder module
            post_indent - optional argument which can be used to suppress
                          the default indenting logic
        """

        if isinstance(line, bytes):
            line = str(line, "utf-8")
        if not isinstance(line, str):
            line = lxml.html.tostring(line, encoding="unicode")
            post_indent = False
        elif line.startswith("</"):
            if self._level > 0:
                self._level -= 1
            post_indent = False
        self._html.append(Page._indent(self._level, line))
        if post_indent:
            self._level += 1

    def add_checkbox(self, group, label, value, **kwargs):
        """
        Add a labeled form checkbox to the HTML for the page.

        Required positional arguments:

            group        used for the name of the input element
            label        string to identify the checkbox to the user
            value        used as the 'value' attribute of the element

        Optional keyword arguments:

            widget_id       override the default id, which is normally formed
                            by concatenating the group and value arguments,
                            separated by a hyphen, and then lowercasing the
                            result
            widget_classes  if present, used as the 'class' attribute for
                            the input element
            wrapper         defaults to 'div' which keeps the field on
                            a separate line from the other checkboxes;
                            set to None to have no wrapper
            wrapper_classes if present, used as the 'class' attribute for
                            the wrapper element
            tooltip         if present, used as the 'title' attribute for
                            the label element
            checked         if set to True will cause the checkbox to be
                            checked by default
            onclick         Javascript to be invoked when the checkbox is
                            clicked; defaults to check_GROUP('VALUE') where
                            GROUP is the value of the group argument and
                            VALUE is the value of the value argument
        """
        self._add_checkbox_or_radio_button("checkbox", group, label, value,
                                           **kwargs)

    def add_radio(self, group, label, value, **kwargs):
        """
        Add a labeled form radio button to the HTML for the page.

        Required positional arguments:

            group        used for the name of the input element
            label        string to identify the radio button to the user
            value        used as the 'value' attribute of the element

        Optional keyword arguments:

            widget_id       override the default id, which is normally formed
                            by concatenating the group and value arguments,
                            separated by a hyphen, and then lowercasing the
                            result
            widget_classes  if present, used as the 'class' attribute for
                            the input element
            wrapper         defaults to 'div' which keeps the field on
                            a separate line from the other radio buttons;
                            set to None to have no wrapper
            wrapper_classes if present, used as the 'class' attribute for
                            the wrapper element
            tooltip         if present, used as the 'title' attribute for
                            the label element
            checked         if set to True will cause the radio button to be
                            selected by default
            onclick         Javascript to be invoked when the button is
                            clicked; defaults to check_GROUP('VALUE') where
                            GROUP is the value of the group argument and
                            VALUE is the value of the value argument
        """
        self._add_checkbox_or_radio_button("radio", group, label, value,
                                           **kwargs)
    def add_select(self, name, label, options, default=None, **kwargs):
        """
        Add a picklist field.

        Required positional arguments:

            name            the name of the field, also used as the field's ID
            label           string used to identify the field to the user
            options         sequence of choices for the picklist; each member
                            of the sequence can be a sequence of value and
                            display, in order to have a different string
                            displayed for the choice than the value returned
                            when that choice is selected, or a single value,
                            in which case the value returned will be the same
                            as the string displayed for the option

        Optional positional argument:

            default         choice(s) which should be selected when the form
                            is initially displayed; can be a sequence of
                            values if the 'multiple' argument is passed as
                            True; values for default are matched against
                            the values for the choices, not against the
                            display strings for those choices

        Optional keyword arguments

            multiple        if passed and set to True, allows the user to
                            select multiple choices from the picklist
            onchange        Javascript to be invoked when the users changes
                            the choice(s) from the picklist
            classes         string to be set as the 'class' attribute of
                            the select element
            wrapper_classes classes to be added to the div wrapper
            tooltip         if present, used as the 'title' attribute for
                            the select element
        """
        wrapper_classes = kwargs.get("wrapper_classes")
        if wrapper_classes:
            if isinstance(wrapper_classes, str):
                wrapper_classes = wrapper_classes.split()
        else:
            wrapper_classes = []
        if "labeled-field" not in wrapper_classes:
            wrapper_classes.append("labeled-field")
        self.add('<div class="%s">' % " ".join(wrapper_classes))
        label = Page.B.LABEL(Page.B.FOR(name), label)
        #if tooltip:
        #    label.set("title", tooltip)
        self.add(label)
        open_tag = '<select name="%s" id="%s"' % (name, name)
        classes = kwargs.get("classes") or kwargs.get("class_")
        if classes:
            if type(classes) in (list, tuple, set):
                classes = " ".join(classes)
            open_tag += ' class="%s"' % classes
        if type(default) not in (list, tuple):
            default = default and [default] or []
        if kwargs.get("multiple"):
            open_tag += " multiple"
        elif len(default) > 1:
            raise Exception("Multiple defaults specified for single picklist")
        tooltip = kwargs.get("tooltip")
        if tooltip:
            open_tag += ' title="%s"' % tooltip.replace('"', "&quot;")
        onchange = kwargs.get("onchange")
        if onchange:
            open_tag += ' onchange="%s"' % onchange.replace('"', "&quot;")
        self.add(open_tag + ">")
        for option in options:
            try:
                value, display = option
            except:
                value = display = option
            o = Page.B.OPTION(display, value=str(value))
            if value in default:
                o.set("selected", "selected")
            self.add(o)
        self.add("</select>")
        self.add("</div>")

    def add_text_field(self, name, label, **kwargs):
        """
        Add a labeled text input field to an HTML form.

        Required positional arguments:

            name            used as the 'name' attribute for the input
                            element
            label           used for the accompanying label value's content

        Optional keywork arguments:

            value           default value for field
            classes         if present, used as the 'class' attribute for
                            the input element.  May include multiple space
                            separated class names.
            wrapper_classes classes to be added to the div wrapper
            password        if True, value is not displayed
            upload          if True, type is set to "file"
            tooltip         if present, used as the 'title' attribute for
                            the input element
        """
        wrapper_classes = kwargs.get("wrapper_classes")
        if wrapper_classes:
            if isinstance(wrapper_classes, str):
                wrapper_classes = wrapper_classes.split()
        else:
            wrapper_classes = []
        if "labeled-field" not in wrapper_classes:
            wrapper_classes.append("labeled-field")
        self.add('<div class="%s">' % " ".join(wrapper_classes))
        self.add(Page.B.LABEL(Page.B.FOR(name), label))
        field = Page.B.INPUT(id=name, name=name)
        tooltip = kwargs.get("tooltip")
        if tooltip:
            field.set("title", tooltip)
        classes = kwargs.get("classes") or kwargs.get("class_")
        classes = classes or kwargs.get("class")
        if classes:
            if type(classes) in (list, tuple, set):
                classes = " ".join(classes)
            field.set("class", classes)
        if "value" in kwargs:
            field.set("value", str(kwargs["value"]))
        if kwargs.get("password"):
            field.set("type", "password")
        if kwargs.get("upload"):
            field.set("type", "file")
        if kwargs.get("readonly"):
            field.set("readonly", "readonly")
        if kwargs.get("disabled"):
            field.set("disabled", "disabled")
        self.add(field)
        self.add("</div>")

    def add_textarea_field(self, name, label, **kwargs):
        """
        Add a labeled text input field to an HTML form.

        Required positional arguments:

            name            used as the 'name' attribute for the input
                            element
            label           used for the accompanying label value's content

        Optional keywork arguments:

            classes         if present, used as the 'class' attribute for
                            the input element.  May include multiple space
                            separated class names.
            wrapper_classes classes to be added to the div wrapper
            value           initial value for the field (optional)
            tooltip         if present, used as the 'title' attribute for
                            the textarea element
        """
        wrapper_classes = kwargs.get("wrapper_classes")
        if wrapper_classes:
            if isinstance(wrapper_classes, str):
                wrapper_classes = wrapper_classes.split()
        else:
            wrapper_classes = []
        if "labeled-field" not in wrapper_classes:
            wrapper_classes.append("labeled-field")
        self.add('<div class="%s">' % " ".join(wrapper_classes))
        label = Page.B.LABEL(Page.B.FOR(name), label)
        self.add(label)
        value = str(kwargs.get("value", ""))
        field = Page.B.TEXTAREA(value, id=name, name=name)
        tooltip = kwargs.get("tooltip")
        if tooltip:
            field.set("title", tooltip)
        classes = kwargs.get("classes") or kwargs.get("class_")
        classes = classes or kwargs.get("class")
        if classes:
            if type(classes) in (list, tuple, set):
                classes = " ".join(classes)
            field.set("class", classes)
        self.add(field)
        self.add("</div>")

    def add_date_field(self, name, label, **kwargs):
        """
        Add a labeled date input field to an HTML form.

        See documentation for the add_text_field() method, which takes
        the same arguments.
        """
        kwargs["classes"] = "CdrDateField"
        self.add_text_field(name, label, **kwargs)
        self._have_date_field = True

    def add_menu_link(self, script, display, session=None, **kwargs):
        """
        Add a list item containing a CDR admin menu link.
        """
        url = script
        if session:
            kwargs[SESSION] = session
        if kwargs:
            url = "%s?%s" % (url, urllib.parse.urlencode(kwargs))
        link = Page.B.A(display, href=url)
        self.add(Page.B.LI(link))

    def add_hidden_field(self, name, value):
        "Utility method to insert a hidden CGI field."
        value = str(value) if value else ""
        self.add(Page.B.INPUT(name=name, value=value, type="hidden"))

    def add_script(self, script):
        """
        Add a block of Javascript code.

        The code will be inserted into the HTML page immediately before
        the closing 'body' tag.

        Required positional argument:

            script          block of Javascript, everything that would go
                            between <script> and </script>.  Do not include
                            the "script" tags.
        """
        self._script.append(script.rstrip() + "\n")

    def add_css(self, script):
        """
        Add a block of CSS rules.

        The code will be inserted into the HTML page immediately before
        the closing 'body' tag, along with any javascript code blocks.

        Required positional argument:

            script          block of css rules, everything that would go
                            between <style> and </style>.  Do not include
                            the "style" tags.
        """
        self._css.append(script.rstrip() + "\n")

    def add_output_options(self, default=None, onclick=None):
        """
        Allow the user to decide between HTML and Excel.
        """
        h_checked = default == "html"
        e_checked = default == "excel"
        self.add("<fieldset id='report-format-block'>")
        self.add(self.B.LEGEND("Report Format"))
        self.add_radio("format", "Web Page", "html", checked=h_checked,
                       onclick=onclick)
        self.add_radio("format", "Excel Workbook", "excel", checked=e_checked,
                       onclick=onclick)
        self.add("</fieldset>")

    def send(self):
        """
        Returns the HTML page to the web server for delivery to the browser.
        """

        self._finish()
        sendPage("".join(self._html))

    @classmethod
    def _indent(class_, level, block):
        """
        Add indenting to a block of lines.

        This turns out not to have been such a good idea. The intention
        was to make the generated HTML source more readable by a human,
        giving a visual indication of the nested hierarchy of the elements.
        However, it introduced undesirable and unanticipated side effects,
        the most notable of which was that multiline values for textarea
        form fields was garbled. The original solution for that problem
        was to have the caller replace the newlines in the value with a
        unique placeholder, which would prevent the garbling, and this
        method swaps back in the newlines at the last minute. For right
        now we're retaining that swap so that older scripts using that
        technique will still work correctly, but we are no longer doing
        any indenting behind the curtain.

        indent = class_.INDENT * level
        if not "\n" in block:
            result = u"%s%s\n" % (indent, block)
        else:
            lines = block.splitlines()
            result = u"".join([u"%s%s\n" % (indent, line) for line in lines])
        return result.replace(NEWLINE, "\n").replace(BR, "<br>")
        """

        return block.replace(NEWLINE, "\n").replace(BR, "<br>") + "\n"

    def _add_checkbox_or_radio_button(self, widget, group, label, value,
                                     **kwargs):
        """
        Internal helper method for add_radio() and add_checkbox()

        See documentation for those methods.
        """
        default_widget_id = ("%s-%s" % (group, value)).replace(" ", "-")
        widget_id = kwargs.get("widget_id", default_widget_id.lower())
        widget_classes = kwargs.get("widget_classes")
        wrapper = kwargs.get("wrapper", "div")
        wrapper_id = kwargs.get("wrapper_id")
        wrapper_classes = kwargs.get("wrapper_classes")
        tooltip = kwargs.get("tooltip")
        checked = kwargs.get("checked") and True or False
        onclick = kwargs.get("onclick", "check_%s('%s')" % (group, value))
        if wrapper:
            tag = "<%s" % wrapper
            if wrapper_id:
                tag += ' id="%s"' % wrapper_id
            if wrapper_classes:
                if type(wrapper_classes) in (list, tuple, set):
                    wrapper_classes = " ".join(wrapper_classes)
                tag += ' class="%s"' % wrapper_classes
            self.add(tag + ">")
        if not isinstance(value, str):
            value = str(value)
        field = Page.B.INPUT(
            id=widget_id,
            type=widget,
            name=group,
            value=value,
        )
        if checked:
            field.set("checked", "checked")
        if onclick:
            field.set("onclick", onclick.replace("-", "_"))
        if widget_classes:
            if type(widget_classes) in (list, tuple, set):
                widget_classes = " ".join(widget_classes)
            field.set("class", widget_classes)
        self.add(field)
        label = Page.B.LABEL(Page.B.FOR(widget_id), label,
                             Page.B.CLASS("clickable"))
        if tooltip:
            label.set("title", tooltip)
        self.add(label)
        if wrapper:
            self.add("</%s>" % wrapper)

    def _start(self):
        """
        Helper method for __init__().
        """
        self.add("<!DOCTYPE html>", False)
        self.add("<html>")
        self.add("<head>")
        self.add(Page.B.META(charset="utf-8"))
        self.add(Page.B.TITLE(self._title))
        if self._icon:
            self.add(Page.B.LINK(rel="icon", href="/favicon.ico"))
        for sheet in self._stylesheets:
            self.add(Page.B.LINK(rel="stylesheet", href=sheet))
        for js in self._js:
            self.add(Page.B.SCRIPT(src=js))
        self.add("</head>")
        if self._body_classes:
            body_classes = self._body_classes
            if type(body_classes) in (list, tuple, set):
                body_classes = " ".join(body_classes)
            self.add('<body class="%s">' % body_classes)
        else:
            self.add("<body>")
        if self._action and self._buttons:
            prefix = "/cgi-bin/cdr/"
            if "/" in self._action:
                prefix=""
            enctype = ""
            if self._enctype:
                enctype = " enctype=\"%s\"" % self._enctype
            self.add("""<form action="%s%s" method="%s"%s>""" %
                     (prefix, self._action, self._method, enctype))
            self.add("<header>")
            self.add("<h1>%s" % self._banner)
            self.add("<span>")
            for b in self._buttons:
                self.add(Page.B.INPUT(name="Request", value=b, type="submit"))
            self.add("</span>")
            self.add("</h1>")
        else:
            self.add("<header>")
            if self._banner:
                self.add(Page.B.H1(self._banner))
        if self._subtitle:
            self.add(Page.B.H2(self._subtitle))
        self.add("</header>")
        if self._action:
            if not self._buttons:
                self.add("""<form action="/cgi-bin/cdr/%s" method="%s">""" %
                         (self._action, self._method))
            if self._session:
                self.add_hidden_field(SESSION, self._session)

    def _finish(self):
        """
        Helper function called by the send() method.
        """
        if not self._finished:
            if self._have_date_field:
                self.add_script("""\
jQuery(function() {
    jQuery('.CdrDateField').datepicker({
        dateFormat: 'yy-mm-dd',
        showOn: 'button',
        buttonImageOnly: true,
        buttonImage: "/images/calendar.png",
        buttonText: "Select date",
        dayNamesMin: [ "S", "M", "T", "W", "T", "F", "S" ]
    });
});""")
            if self._action:
                self.add("</form>")
            if self._css:
                self.add("<style>")
                self._html += self._css
                self.add("</style>")
            if self._script:
                self.add("<script>")
                self._html += self._script
                self.add("</script>")
            self.add("</body>")
            self.add("</html>")
            self._finished = True

    @staticmethod
    def test():
        """
        Quick check to see if anything obvious got broken by changes to the
        Page class.
        """
        P = Page
        page = P("Test", banner="A Banner", subtitle="A Subtitle",
                 buttons=("Manny", "Moe", "Jack"), action="dummy.py",
                 session="guest", body_classes="custom-form")
        page.add("<fieldset>")
        page.add(P.B.LEGEND("Checkboxes"))
        page.add_checkbox("cb", "Checkbox 1", "1", onclick=None)
        page.add_checkbox("cb", "Checkbox 2", "2", onclick=None)
        page.add_checkbox("cb", "Checkbox 3", "3", onclick=None)
        page.add("</fieldset>")
        page.add("<fieldset>")
        page.add(P.B.LEGEND("Radio Buttons"))
        page.add_radio("ra", "Button 1", "1")
        page.add_radio("ra", "Button 2", "2")
        page.add_radio("ra", "Button 3", "3")
        page.add("</fieldset>")
        page.add("<fieldset>")
        page.add(P.B.LEGEND("Picklist"))
        page.add_select("se", "Names",
                        ("Larry", "Moe", "Curly", "Tristan", "Brunnhilde"),
                        ("Moe", "Tristan"), multiple=True)
        page.add("</fieldset>")
        page.add("<fieldset>")
        page.add(P.B.LEGEND("Text Fields"))
        page.add_text_field("addr", "Address")
        page.add_text_field("phone", "Phone")
        page.add_text_field("email", "Email")
        page.add("</fieldset>")
        page.add("<fieldset>")
        page.add(P.B.LEGEND("Date Fields"))
        page.add_date_field("start", "Start")
        page.add_date_field("end", "End", value=str(datetime.date.today()))
        page.add("</fieldset>")
        page.add_output_options("html")
        page.add_script("""\
function check_ra(val) {
    alert('You checked ' + val + '!');
}""")
        page.add_css("header h1 { background-color: blue; }")
        page._finish()
        print("".join(page._html))

class Report:
    """
    CDR Report which can be rendered as an HTML page or as an Excel workbook.

    Example usage:

        R = cdrcgi.Report
        cursor = db.connect(user='CdrGuest').cursor()
        cursor.execute('''\
            SELECT id, name
              FROM doc_type
          ORDER BY name''')
        columns = (
            R.Column('Type ID', width='75px'),
            R.Column('Type Name', width='300px')
        )
        rows = cursor.fetchall()
        table = R.Table(columns, rows, caption='Document Types')
        report = R('Simple Report', [table])
        report.send('html')
    """

    def __init__(self, title, tables, **options):
        """
        Starts a new CDR report.

        Required parameters:

            title -       title of the report; used as the 'title' element
                          of the head block for HTML output; used to create
                          the Excel workbook filename for 'excel' output
            tables -      array of Table objects; these will be rendered as
                          separate spreadsheets for 'excel' output, or as
                          separate HTML table blocks for web page output

        Optional parameters:

            banner -      banner for the top of a web page; unused for
                          Excel output
            subtitle -    displayed underneath the banner on a web page;
                          not used for Excel output
            page_opts -   dictionary of options to be passed to Page
                          constructor for HTML output (for example, to
                          get navigation buttons on the report page's
                          banner); for more extensive modifications to
                          the page, derive your own class from the
                          Report class and override _create_html_page()
            css -         optional string or sequence of strings to be
                          added to the report page if HTML
        """
        self._title = title
        self._tables = tables
        self._options = options
        if not isinstance(tables, (list, tuple)):
            self._tables = [tables]

    def send(self, fmt="html"):
        """
        Send the report to the web server to be returned to the user's browser

        Passed:

            fmt -         'html' or 'excel'; defaults to 'html'
        """
        if fmt == "html":
            self._send_html()
        else:
            self._send_excel_workbook()

    def _create_html_page(self, **opts):
        """
        Separated out from _send_html() so it can be overridden.
        """
        return Page(self._title, **opts)

    def _send_html(self):
        """
        Internal helper method for Report.send()
        """
        opts = {
            "banner": self._options.get("banner"),
            "subtitle": self._options.get("subtitle"),
            "stylesheets": [CDRCSS],
            "body_classes": "report",
            "js": []
        }
        opts.update(self._options.get("page_opts") or {})
        page = self._create_html_page(**opts)
        css = self._options.get("css")
        if css:
            if isinstance(css, str):
                css = [css]
            for c in css:
                page.add_css(c)
        B = page.B
        for table in self._tables:
            if table._html_callback_pre:
                table._html_callback_pre(table, page)
            page.add('<table class="report">')
            if table._caption or table._show_report_date:
                if not table._caption:
                    lines = []
                if type(table._caption) in (list, tuple):
                    lines = list(table._caption)
                else:
                    lines = [table._caption]
                if table._show_report_date:
                    today = datetime.date.today()
                    lines += ["", "Report date: {}".format(today), ""]
                line = lines.pop(0)
                caption = B.CAPTION(line)
                while lines:
                    line = lines.pop(0)
                    br = B.BR()
                    br.tail = line
                    caption.append(br)
                page.add(caption)
            page.add("<thead>")
            page.add("<tr>")
            for column in table._columns:
                cell = B.TH(column._name)
                for opt in column._options:
                    if opt == "width":
                        width = column._options["width"]
                        cell.set("style", "min-width:%s;" % width)
                    else:
                        cell.set(opt, column._options[opt])
                page.add(cell)
            page.add("</tr>")
            page.add("</thead>")
            if table._rows:
                if table._stripe:
                    prev_rowspan = 0
                    class_ = None
                page.add("<tbody>")
                for row in table._rows:
                    if table._stripe:
                        if not prev_rowspan:
                            class_ = class_ == "odd" and "even" or "odd"
                        else:
                            prev_rowspan -= 1
                        page.add('<tr class="%s">' % class_)
                    else:
                        page.add("<tr>")
                    for cell in row:
                        if isinstance(cell, self.Cell):
                            td = cell.to_td()
                            if table._stripe and cell._rowspan:
                                extra_rows = int(cell._rowspan) - 1
                                if extra_rows > prev_rowspan:
                                    prev_rowspan = extra_rows
                        else:
                            td = B.TD()
                            self.Cell.set_values(td, cell)
                        page.add(td)
                    page.add("</tr>")
                page.add("</tbody>")
            page.add("</table>")
            if table._html_callback_post:
                table._html_callback_post(table, page)
        elapsed = self._options.get("elapsed")
        if elapsed:
            page.add(page.B.P("elapsed: %s" % elapsed,
                              page.B.CLASS("footnote")))
        page.send()

    @staticmethod
    def xf(**opts):
        """
        By default cells are top aligned vertically, with horizontal
        alignment determined by the type of data (right for integers,
        left for everything else), without bold font, and with word
        wrapping turned on. This function wraps the style creation
        functionality so that report writers can create a custom style
        object to override the default styling by passing the object
        as the value of the sheet_style optional argument for the
        Cell class constructor. If the options here don't support
        the styling you want, you can adjust the properties of the
        returned object yourself.

        https://github.com/python-excel/xlwt/blob/master/xlwt/Formatting.py

        Optional arguments:

            header - if True, apply bold font with white text against
                     a colored background centered in the cell; used
                     for column headers
            banner - same as header, with increased height (for first row)
            wrap   - if set to False, turns off word wrapping
            horiz  - one of:
                       "left"
                       "center"
                       "right"
                       "general" (the default)
            vert    - one of:
                       "top" (the default)
                       "center"
                       "bottom"
            easyxf  - if set, overrides the default string passed to
                      xlwt.easyxf(), creating the object to which the
                      other options will be applied
        """
        horz = {
            "left": xlwt.Alignment.HORZ_LEFT,
            "center": xlwt.Alignment.HORZ_CENTER,
            "right": xlwt.Alignment.HORZ_RIGHT,
            "general": xlwt.Alignment.HORZ_GENERAL
        }
        vert = {
            "top": xlwt.Alignment.VERT_TOP,
            "center": xlwt.Alignment.VERT_CENTER,
            "bottom": xlwt.Alignment.VERT_BOTTOM
        }
        wrap = {
            True: xlwt.Alignment.WRAP_AT_RIGHT,
            False: xlwt.Alignment.NOT_WRAP_AT_RIGHT
        }
        settings = {
            "align": "wrap True, vert top"
        }
        if opts.get("header") or opts.get("banner"):
            font = ["colour black", "bold True"]
            if opts.get("banner"):
                font.append("height 240")
            settings["font"] = ", ".join(font)
            settings["align"] = "wrap True, vert centre, horiz centre"
        default = ";".join(["%s: %s" % (k, settings[k]) for k in settings])
        style = xlwt.easyxf(opts.get("easyxf", default))
        if "wrap" in opts:
            style.alignment.wrap = wrap.get(opts["wrap"],
                                            xlwt.Alignment.WRAP_AT_RIGHT)
        if "horiz" in opts:
            style.alignment.horz = horz.get(opts["horiz"],
                                            xlwt.Alignment.HORZ_GENERAL)
        if "vert" in opts:
            style.alignment.vert = vert.get(opts["vert"],
                                            xlwt.Alignment.VERT_TOP)
        if "bold" in opts:
            style.font.bold = opts["bold"] and True or False
        return style

    def _send_excel_workbook(self):
        """
        Internal helper method for Report.send()
        """
        book = xlwt.Workbook(encoding="UTF-8")
        book.set_colour_RGB(0x21, 153, 52, 102) #993366
        self._data_style = self.xf()
        self._header_style = self.xf(header=True)
        self._banner_style = self.xf(banner=True)
        self._bold_data_style = self.xf(bold=True)
        self._right_data_style = self.xf(horiz="right")
        self._center_data_style = self.xf(horiz="center")
        count = 1
        for table in self._tables:
            self._add_worksheet(book, table, count)
            count += 1
        #import msvcrt
        now = datetime.datetime.now().strftime("%Y%m%d%H%M%S")
        book_name = re.sub(r"\W", "_", self._title)
        book_name += f"-{now}s.xls"
        #msvcrt.setmode(sys.stdout.fileno(), os.O_BINARY)
        headers = (
            "Content-type: application/vnd.ms-excel",
            f"Content-disposition: attachment; filename={book_name}"
        )
        headers = "\r\n".join(headers) + "\r\n\r\n"
        sys.stdout.buffer.write(headers.encode("utf-8"))
        book.save(sys.stdout.buffer)
        if self._options.get("debug"):
            name = "d:/tmp/%s" % book_name
            fp = open(name, "wb")
            book.save(fp)
            fp.close()
        sys.exit(0)

    def _add_worksheet(self, book, table, count):
        """
        Internal help method called by Report._send_excel_workbook
        """
        name = table._options.get("sheet_name", "Sheet%d" % count)
        sheet = book.add_sheet(name)
        sheet.print_grid = True
        row_number = 0
        last_col = len(table._columns) - 1
        for col_number, column in enumerate(table._columns):
            width = Report._get_excel_width(column)
            if width:
                sheet.col(col_number).width = width
        if table._caption:
            if type(table._caption) in (list, tuple):
                captions = table._caption
            else:
                captions = [table._caption]
            for caption in captions:
                sheet.write_merge(row_number, row_number, 0, last_col, caption,
                                  self._banner_style)
                row_number += 1
        if table._show_report_date:
            sheet.write_merge(row_number, row_number, 0, last_col, "")
            row_number += 1
            sheet.write_merge(row_number, row_number, 0, last_col,
                              "Report date: {}".format(datetime.date.today()),
                              self._bold_data_style)
            row_number += 1
            sheet.write_merge(row_number, row_number, 0, last_col, "")
            row_number += 1
        for col_number, column in enumerate(table._columns):
            sheet.write(row_number, col_number, column._name,
                        self._header_style)
        for row in table._rows:
            row_number += 1
            col_number = 0
            for cell in row:

                # Determine coordinates where cell should be written.
                while col_number < len(table._columns):
                    if table._columns[col_number]._skip > 0:
                        table._columns[col_number]._skip -= 1
                        col_number += 1
                    else:
                        break
                if col_number >= len(table._columns):
                    raise Exception("too many cells for row %d" % row_number)

                # Assemble the values to be written
                values = Report.Cell._get_values(cell, True)
                if not isinstance(values, int):
                    values = "\n".join(values)
                style = self._data_style
                if isinstance(cell, self.Cell):
                    if cell._sheet_style:
                        style = cell._sheet_style
                    elif cell._bold:
                        style = self._bold_data_style
                    elif cell._right:
                        style = self._right_data_style
                    elif cell._center:
                        style = self._center_data_style
                    if cell._href:
                        vals = str(values).replace('"', '""')
                        formula = 'HYPERLINK("%s";"%s")' % (cell._href, vals)
                        values = xlwt.Formula(formula)
                    if cell._colspan or cell._rowspan:
                        colspan = cell._colspan and int(cell._colspan) or 1
                        rowspan = cell._rowspan and int(cell._rowspan) or 1
                        r1, c1 = r2, c2 = row_number, col_number
                        if colspan > 1:
                            c2 += colspan - 1
                            if c2 >= len(table._columns):
                                raise Exception("not enough room for colspan "
                                                "on row %d" % row_number)
                        if rowspan > 1:
                            extra_rows = rowspan - 1
                            r2 += extra_rows
                            while col_number <= c2:
                                if table._columns[col_number]._skip > 0:
                                    raise Exception("overlapping rowspan "
                                                    "detected at row %d" %
                                                    row_number)
                                table._columns[col_number]._skip = extra_rows
                                col_number += 1
                        sheet.write_merge(r1, r2, c1, c2, values, style)
                        col_number = c2 + 1
                        continue
                sheet.write(row_number, col_number, values, style)
                col_number += 1

    @staticmethod
    def _get_excel_width(column):
        """
        Convert CSS cell width to width units used by the xlwt module

        Passed:

            column -     instance of the Column class

        The width option of the Column object can be given as 'NN.NNin' or
        'NN.NNpx' where 'NN' is any number including of decimal digits.
        The decimal point is optional, and there don't have to be digits
        on both sides of the decimal point, but has to be at least one
        digit somewhere in the string.  Percentages aren't supported (yet).

        The formula for converting pixels to xlwt units is derived from
        the xlwt module's Column.width_in_pixels() method (see
        https://github.com/python-excel/xlwt/blob/master/xlwt/Column.py)

        The xlwt units represent 1/256 of the width of the character '0'
        in the workbook's default font.

        See http://office.microsoft.com/en-us/excel-help\
        /measurement-units-and-rulers-in-excel-HP001151724.aspx
        for the formula used to convert inches to pixels.

        """

        width = column._options.get("width")
        if width:
            if width.endswith("in"):
                inches = float(width[:-2])
                pixels = 96 * inches
            else:
                pixels = float(re.sub(r"[^\d.]+", "", width))
            return int((pixels - .446) / .0272)
        return None

    @classmethod
    def test(cls):
        """
        Very crude little test to check for any obvious breakage.

        Run like this from the command line:

         python -c "import cdrcgi;cdrcgi.Report.test()" | sed -n /DOCTYPE/,$p

        Wouldn't hurt to add more testing to this method as we get time.
        """
        R = cls
        cursor = db.connect(user="CdrGuest").cursor()
        cursor.execute("""\
            SELECT id, name
              FROM doc_type
             WHERE active = 'Y'
               AND name > ''
          ORDER BY name""")
        columns = (
            R.Column("Type ID", width="75px"),
            R.Column("Type Name", width="300px")
        )
        rows = cursor.fetchall()
        table = R.Table(columns, rows, caption="Document Types")
        report = R("Simple Report", table)
        report.send("html")

    class Column:
        """
        Information about a column in a tabular report.
        """
        def __init__(self, name, **options):
            self._name = name
            self._options = options
            self._skip = 0
        def set_name(self, name):
            self._name = name

    class Table:
        """
        One of (possibly) multiple tables in a CDR report
        """
        def __init__(self, columns, rows, **options):
            """
            Table constructor.

            Required arguments:
                columns - Array of Column objects, one for each column.

                rows    - Array of arrays of values for the cells.  Each of
                          the sub-arrays has one value for one cell.
                          Values can be string or numeric.  (Or formula?)

            Optional keyword arguments:

                caption - Caption centered at the top of the table, full
                          width.  If the caption is an array, each element of
                          the array will be centered on a new line.

                html_callback_pre
                        - Reference to a function to be called before
                          processing the table by send("html").  Note that
                          function outputs may or may appear before the
                          table in the output HTML.

                          The function is not called when output is Excel.

                html_callback_post
                        - Reference to a function to be called after
                          processing the table by send("html").

                user_data
                        - Store whatever the value is for return later if
                          and when Table.user_data() is called.  Value can
                          be anything - string, object, array, dictionary,
                          whatever.

                stripe  - Use odd / even background coloring for rows.
                          Default=True.

                show_report_date
                        - If true, add "Report date: yyyy-mm-dd" line
            """
            if not columns:
                raise Exception("no columns specified for table")
            if type(columns) not in (list, tuple):
                raise Exception("table columns must be a sequence")
            self._columns = columns
            self._rows = rows
            self._options = options
            self._caption = options.get("caption")
            self._html_callback_pre = options.get("html_callback_pre")
            self._html_callback_post = options.get("html_callback_post")
            self._user_data = options.get("user_data")
            # Note None != False, hence True is default
            self._stripe = options.get("stripe") != False
            self._show_report_date = options.get("show_report_date")

        def options(self):
            """
            Accessor object for the table's options.
            """
            return self._options

        def user_data(self):
            """
            Accessor object for data stored by the caller.
            """
            return self._user_data

    class Cell:
        """
        Single cell of data for a table in a CDR report.

        The cell can optionally span multiple rows and/or columns
        (using the optional 'colspan' or 'rowspan' arguments
        to the constructor.

        The value can be a single string or number or an array of
        strings or numbers.  Each value in an array of values will
        be displayed on a separate line of the cell.
        """

        B = lxml.html.builder

        def __init__(self, value, **options):
            self._value = value
            self._options = options
            self._colspan = options.get("colspan")
            self._rowspan = options.get("rowspan")
            self._href = options.get("href")
            self._target = options.get("target")
            self._bold = options.get("bold")
            self._right = options.get("right")
            self._center = options.get("center")
            self._sheet_style = options.get("sheet_style")
            self._callback = options.get("callback")
            self._title = options.get("title")
            classes = options.get("classes")
            if not classes:
                self._classes = []
            elif isinstance(classes, str):
                self._classes = classes.split()
            elif isinstance(classes, (set, tuple)):
                self._classes = list(classes)
            elif isinstance(classes, list):
                self._classes = classes
            else:
                raise Exception("unexpected type %s for Cell classes: %s" %
                                (type(classes), repr(classes)))

        def values(self):
            return self._value

        def options(self):
            return self._options

        def to_td(self):
            if self._callback:
                td = self._callback(self, "html")
                if td is not None:
                    return td
            td = self.B.TD()
            if self._href:
                element = self.B.A(href=self._href)
                if self._target:
                    element.set("target", self._target)
                if self._bold:
                    element.set("class", "strong")
            else:
                element = td
                if self._bold and "strong" not in self._classes:
                    self._classes.append("strong")
            self.set_values(element, self)
            if self._colspan:
                td.set("colspan", str(self._colspan))
            if self._rowspan:
                td.set("rowspan", str(self._rowspan))
            if self._center:
                if "center" not in self._classes:
                    self._classes.append("center")
            elif self._right:
                if "right" not in self._classes:
                    self._classes.append("right")
            if self._classes:
                td.set("class", " ".join(self._classes))
            if self._title:
                td.set("title", self._title)
            if element is not td:
                td.append(element)
            return td

        @classmethod
        def set_values(cls, element, cell):
            values = cls._get_values(cell)
            value = values.pop(0)
            element.text = value
            while values:
                value = values.pop(0)
                br = cls.B.BR()
                br.tail = value
                element.append(br)

        @classmethod
        def _get_values(cls, cell, preserve_int=False):
            """
            Returns the values for a table cell as an array

            Passed:

                cell -       either a string or number, an array of strings
                             and/or numbers, or a Report.Cell object (whose
                             _value member may in turn be a string, a number,
                             or an array of strings and/or numbers)
            """
            if isinstance(cell, cls):
                values = cell._value
            else:
                values = cell
            if isinstance(values, int) and preserve_int:
                return values
            if values is None:
                return [""]
            if type(values) not in (list, tuple):
                return [str(values)]
            elif not values:
                return [""]
            return [str(v) for v in values]


#----------------------------------------------------------------------#
# LEGACY GLOBAL FUNCTIONS -- USE NEW CDRAPI MODULES INSTEAD IF YOU CAN #
#----------------------------------------------------------------------#


def header(title, banner, subtitle, *args, **kwargs):
    """Create the top portion of a serialized HTML form document.

    This is an ancient function, and was how we created CDR web pages
    back around the turn of the century. It needs to be retired, but
    doing so will involve rewrites of around 40 scripts. The worst
    of the function's numerous flaws was that it involved building
    up HTML using string manipulation, mixing byte strings and unicode
    strings, and throwing in some regular expression parsing for fun.
    At least we have replaced strings splicing with a real HTML
    object builder (from the lxml package). All strings must be real
    `str` objects, not `bytes` or `bytearray` objects.

    The existing code calling this function never went beyond passing
    the first two of the keyword arguments as positional arguments,
    so we catch those ("script" and "buttons") either way. The old
    numBreaks, bkgd, and formExtra keyword arguments are now ignored.

    Required positional arguments:

        title
            string used in the html/head/title

        banner
            string displayed in the header element's h1 child

        subtitle
            string displayed in the header element's h2 child

    Optional keyword arguments:

        script
            file name for the script handling the form submission
            (can also be passed as the 4th positional argument)

        buttons
            sequence of strings used for buttons placed on the left
            side of the banner (can also be passed as the 5th positional
            argument)

        method
            set to "get" to override the default ("post")

        stylesheet
            optional string containing serialized HTML fragments
            to be placed at the end of html/head

    Returns:
        serialized HTML string with the closing tags for the form,
        body, and html stripped off
    """

    # Assemble the optional keyword arguments.
    script = args[0] if args else kwargs.get("script")
    button_labels = args[1] if len(args) > 1 else kwargs.get("buttons")
    method = kwargs.get("method") or "post"
    head_extra = kwargs.get("stylesheet")

    # Create the skeleton for the page (caller will add its own session).
    opts = dict(
        banner=banner,
        subtitle=subtitle,
        method=method,
        body_id="legacy-page",
        session=None,
        stylesheets=["/stylesheets/dataform.css"],
        scripts=[],
    )
    if script is not None:
        opts["action"] = f"/cgi-bin/cdr/{script}"
    if button_labels:
        buttons = []
        for label in button_labels:
            if label == "Load":
                buttons.append(HTMLPage.button(button))
            buttons.append(HTMLPage.button(label))
        opts["buttons"] = buttons
    page = HTMLPage(title, **opts)

    # Fold in any additional html/head child elements.
    if head_extra:
        for fragment in lxml.html.fragments_fromstring(head_extra):
            page.head.append(fragment)

    # Let the caller close out the form/body/html elements.
    html = page.tostring()
    if "</form>" in html:
        return "".join(html.split("</form>")[:-1])
    return "".join(html.split("</body>")[:-1])


#----------------------------------------------------------------------
# Scrubber
#----------------------------------------------------------------------
def scrubStr(chkStr, charset="[^A-Za-z0-9 -]", bailout=True,
             msg="Invalid content in data"):
    """
    Ensure that only legal characters appear in the passed string.

    Pass:
        chkStr  - String to scrub.
        charset - Allowed characters in the string.  The default set is
                  suitable for Request, Session, and possibly other strings.
        bailout - True = bail(msg) if any unallowed chars found.
        msg     - Default bail msg, purposely vague to avoid giving
                  clues to hackers.

    Return:
        If scrubbed string is unchanged or bailout == False:
            Return scrubbed string.
        Returns passed string if it's None or ''
    """
    if not chkStr:
        return chkStr

    scrub = re.sub(charset, "", chkStr)
    if scrub != chkStr and bailout:
        bail(msg)

    return scrub

#----------------------------------------------------------------------
# Get a session ID based on current form field values.
# Can't use this funtion to log into the CDR any more (OCECDR-3849).
# Validate the returned value.
#----------------------------------------------------------------------
def getSession(fields, **opts):
    try:
        session = fields.getvalue(SESSION, None)
    except:
        return "guest"

    # Bail if required session is missing. I'm tempted to make required
    # the default.
    if not session:
        if opts.get("required"):
            bail("Session missing")
        else:
            return "guest"

    # Make sure it's an active session.
    query = db.Query("session", "id")
    query.where(query.Condition("name", session))
    query.where("ended IS NULL")
    try:
        rows = query.execute(opts.get("cursor")).fetchall()
    except:
        # Looks like there's a bug in ADODB, triggered when a query
        # parameter is longer than the target column's definition.
        bail("Invalid session ID")
    if not rows:
        bail("Session not active")

    return session

#----------------------------------------------------------------------
# Get the name of the submitted request. Scrub the returned value.
#----------------------------------------------------------------------
def getRequest(fields):
    return scrubStr(fields.getvalue(REQUEST, None))

#----------------------------------------------------------------------
# Send an HTML page back to the client.
# If the parms parameter gets passed we need to redirect the output
# and run the QCforWord.py script to properly convert the HTML output
# to Word.
#----------------------------------------------------------------------
def sendPage(page, textType='html', parms='', docId='', docType='', docVer=''):
    """
    Send a completed page of text to stdout, assumed to be piped by a
    webserver to a web browser.

    Note that _all_ the values passed in are Unicode strings. Any
    encoding happens on the way out the door.

    Pass:
        page     - Text to send, assumed to be a unicode string (not bytes).
        textType - HTTP Content-type, assumed to be html.
        parms    - RowID storing all parameters if report needs to
                   be converted to Word, usually an empty string.
        docType  - if parms is supplied the document type is needed
                   to properly redirect the output, usually an empty string.

    Return:
        No return.  After writing to the browser, the process exits.
    """

    # Handle redirect.
    if parms:
        url = f"https://{WEBSERVER}{BASE}/QCforWord.py"
        args = docId, docType, docVer, parms
        parms = "DocId={}&DocType={}&DocVersion={}&{}".format(*args)
        print(f"Location: {url}?{parms}\n")
        sys.exit(0)
    else:
        Controller.send_page(page, textType)

#----------------------------------------------------------------------
# Log out of the CDR session and put up a new login screen.
#----------------------------------------------------------------------
def logout(session):

    # Make sure we have a session to log out of.
    if not session: bail('No session found.')

    # Perform the logout.
    message = "Session Logged Out Successfully"
    try:
        cdr.logout(session)
    except Exception as e:
        message = str(e)

    # Display a page with a link to log back in.
    title   = "CDR Administration"
    buttons = ["Log In"]
    action="/cgi-bin/secure/admin.py"
    page = Page(title, subtitle=message, buttons=buttons, action=action)
    page.add(Page.B.P("Thanks for spending quality time with the CDR!"))
    page.send()

#----------------------------------------------------------------------
# Navigate to menu location or publish preview.
#----------------------------------------------------------------------
def navigateTo(where, session, **params):
    Controller.navigate_to(where, session, **params)

#----------------------------------------------------------------------
# Determine whether query contains unescaped wildcards.
#----------------------------------------------------------------------
def getQueryOp(query):
    escaped = 0
    for char in query:
        if char == '\\':
            escaped = not escaped
        elif not escaped and char in "_%": return "LIKE"
    return "="

#----------------------------------------------------------------------
# Escape single quotes in string.
#----------------------------------------------------------------------
def getQueryVal(val):
    return val.replace("'", "''")

#----------------------------------------------------------------------
# Helper function to reduce SQL injection possibilities in input
#----------------------------------------------------------------------
def sanitize(formStr, dType='str', maxLen=None, noSemis=True,
             quoteQuotes=True, noDashDash=True, excp=False):
    """
    Validate and/or sanitize a string to try to prevent SQL injection
    attacks using SQL inserted into formData.

    Pass:
        formStr      - String received from a form.
        dType        - Expected data type, one of:
                        'str'     = string, i.e., any data entry okay
                        'int'     = integer
                        'date'    = ISO date format YYYY-MM-DD
                        'datetime'= ISO datetime YYYY-MM-DD HH:MM:SS
                        'cdrID'   = One of our IDs with optional frag ID
                                    All forms are okay, including plain int.
        maxLen       - Max allowed string length.
        noSemis      - True = remove semicolons.
        quoteQuotes  - True = double single quotes, i.e., ' -> ''
        noDashDash   - True = convert runs of '-' to single '-'
        excp         - True = raise ValueError with a specific message.

    Return:
        Possibly modified string.
        If validation fails, return None unless excp=True.
          [Note: raising an exception will break some existing code, but
           returning None will be safe and most existing code will behave
           as if there were no user input.]
    """
    newStr = formStr

    # Data type checking
    if dType != 'str':
        if dType == 'int':
            try:
                int(newStr)
            except ValueError as info:
                if excp:
                    raise
                return None
        elif dType == 'date':
            try:
                time.strptime(newStr, '%Y-%m-%d')
            except ValueError:
                if excp:
                    raise
                return None
        elif dType == 'datetime':
            try:
                time.strptime(newStr, '%Y-%m-%d %H:%M:%S')
            except ValueError:
                try:
                    time.strptime(newStr, '%Y-%m-%d %H:%M')
                except ValueError:
                    try:
                        time.strptime(newStr, '%Y-%m-%d %H')
                    except ValueError:
                        try:
                            time.strptime(newStr, '%Y-%m-%d')
                        except ValueError:
                            if excp:
                                raise
                            return None
        elif dType == 'cdrID':
            try:
                cdr.exNormalize(newStr)
            except Exception as e:
                if excp:
                    raise ValueError(e)
                return None

    # Maximum string length
    if maxLen:
        if len(newStr) > maxLen:
            if excp:
                raise ValueError("Max value length exceeded.")
            return None

    # Semicolons forbidden
    # This version just strips them out
    # XXX Is that safe?
    if noSemis:
        newStr = newStr.replace(";", "")

    # Double single quotation marks
    if quoteQuotes:
        newStr = newStr.replace("'", "''")

    # Convert any substring of 2 or more dashes (SQL comment) to single dash
    if noDashDash:
        while True:
            pos = newStr.find("--")
            if pos >= 0:
                newStr = newStr[:pos] + newStr[pos+1:]
            else:
                break

    # Return (possibly modified) string
    return newStr

#----------------------------------------------------------------------
# Get the full user name for a given session.
#----------------------------------------------------------------------
def getFullUserName(session, conn):
    try:
        cursor = conn.cursor()
        cursor.execute("""\
                SELECT fullname
                  FROM usr
                  JOIN session
                    ON session.usr = usr.id
                 WHERE session.name = ?""", session)
        name = cursor.fetchone()[0]
    except:
        bail("Unable to find current user name")
    return name


#----------------------------------------------------------------------
# Determine whether a parameter is a valid ISO date.
#----------------------------------------------------------------------
def is_date(date):
    return re.match(r"^\d\d\d\d-\d\d-\d\d$", str(date)) and True or False

def _valParmHelper(val, bailout=True, reveal=True, msg=None):
    """
    If validation has failed for a value, this helper function handles
    bailouts and messages.

    Pass:
        val     - value which failed validation
        bailout - True = Invoke cdrcgi.bail()
        reveal  - False = hide the value that failed, else show it in the
                  bail.  Only meaningful if bailout == True.
        msg     - Custom message to display on error.  Else use defaults.
                  Only meaningful if bailout == True.

    Return:
        False, or no return if bailing out.
    """
    if bailout:
        if reveal:
            if not msg:
                msg = 'Invalid parameter value received: "%s"' % val
        else:
            if not msg:
                msg = 'Invalid parameter value received'
        bail(msg)

    # Caller just wants pass/fail, no bail
    return False

# Default values for valParm functions
# To change defaults for all valParm calls in a single script:
# In the script that imports cdrcgi:
#    cdrcgi.BAILOUT_DEFAULT = False # A new value for all valParm calls
# Other scripts are unaffected
BAILOUT_DEFAULT = True # True = bail() if validation faile
REVEAL_DEFAULT  = True # True = Reveal invalid values to the user

# Some common, frequently used, pre-tested, validation patterns
VP_UNSIGNED_INT = r'^\d+$'
VP_SIGNED_INT   = r'^(-|\+)?\d+$'
VP_PADDED_INT   = r'^\s*\d+\s*$'
VP_SIGNED_PAD   = r'^\s*(-|\+)?\d+\s*$'
VP_US_ZIPCODE   = r'^\s*\d{5}(-\d{4})?\s*$'
VP_DATETIME     = r"^\d{4}-\d{2}-\d{2}( \d{2}:\d{2}(:\d{2})?)?$"

def valParmVal(val, **opts):
    """
    Validate a value, typically a CGI parameter, against a list
    of valid values or a regular expression.

    This should not be too expensive in the intended cases of validating
    parameters passed in from CGI forms.

    Note that regex and valList are both optional. If both are empty,
    and emptyOK is False, then the value(s) will always fail validation.

    Pass:
        val     - Value(s) to validate.
                  May be a single value, a list of values, or even a list
                  of lists of values.
        valList - List of valid values. If only a single value is
                  acceptable, it can be passed either as a string,
                  or as a sequence containing a single string.
        regex   - Regular expression for testing validity.
                  Use $ at the end of the expression, unless you
                  want the value(s) accepted if the beginning of
                  each value matches the expression
        iCase   - if True, ignore case when testing the value(s)
        emptyOK - If True, empty/missing values are accepted as valid.
                  If val is a sequence this option is applied to the
                  individual values in the sequence, not to the sequence
                  itself. In other words, if an empty sequence is
                  passed for val, True is always returned. If that's
                  not appropriate, use a separate test of your sequence
                  to ensure that it has at least one element.

        See _valParmHelper for the rest.

    Return:
        True = passed, False = failed, or no return if bailout.
    """
    if isinstance(val, (list, set, tuple)):
        return all([valParmVal(v, **opts) for v in val])
    if not val and (opts.get("emptyOK") or opts.get("empty_ok")):
        return True

    # Get the other optional parameters.
    regex = opts.get("regex")
    bailout = opts.get("bailout", BAILOUT_DEFAULT) and True or False
    reveal = opts.get("reveal", REVEAL_DEFAULT) and True or False
    icase = (opts.get("icase") or opts.get("iCase")) and True or False
    msg = opts.get("msg")

    # Validate the value against a regular expression
    if regex:
        flags = icase and re.IGNORECASE or 0
        if re.match(regex, val, flags):
            return True
    else:
        values = opts.get("valList") or opts.get("val_list") or []
        if isinstance(values, str):
            values = [values]
        cval = val
        if icase:
            values = [v.lower() for v in values]
            cval = val.lower()
        if cval in values:
            return True
        with open(f"{cdr.TMP}/val-parm-val.log", "a", encoding="utf-8") as fp:
            fp.write("cval=%s values=%s\n" % (cval, values))
    return _valParmHelper(val, bailout, reveal, msg)

def valParmEmail(val, **opts):
    """
    Validate an email format.

    This uses the Python email.utils.parseaddr() function to break the address
    into name and email parts, then does ultra simple additional validation.

    It is a very permissive function.  It will not catch every error, but
    should allow all valid addresses through.  Accepts anything with:

        text1@text2 - where text2 includes at least one '.'.

    Pass:
        val = Email address to validate
        See _valParmHelper for the rest.

    Return:
        Email portion of the string, e.g., for '"Joe Blow" <joe@mail.us>'
        returns 'joe@mail.us'.  Test for "not False"
        Note: email address may still be wrong and even absurdly wrong.

        False = Failed format checking
                Note: email address may still be
    """

    # Allow missing val if so requested.
    if not val and (opts.get("empty_ok") or opts.get("emptyOK")):
        return True

    # Get the optional paramaters
    bailout = opts.get("bailout", BAILOUT_DEFAULT) and True or False
    reveal = opts.get("reveal", REVEAL_DEFAULT) and True or False
    msg = opts.get("msg")

    # Parse out the parts
    (name, email) = parse_email_address(val)

    # If it's completely screwed up
    if not email:
        return _valParmHelper(val, bailout, reveal, msg)

    # Simple validation, may improve it later, but full RFC requires
    # an incredible thousand character regex.
    match = Controller.EMAIL_PATTERN.search(email)
    if match:
        return email

    return _valParmHelper(val, bailout, reveal, msg)

def valParmDate(val, fmt="ISOdate", **opts):
    """
    Uses time.strptime for validation via cdr.strptime().  This will validate
    semantics as well as format.

    Pass:
        val    - Date as a string.
        fmt    - Standard python datetime format, or 'ISOdate'.
                 Default is ISOdate, no time, e.g., '2015-07-04'.
                 Invalid date format can also cause False to be returned.
        See _valParmHelper for the rest.

    Return:
        True = passed, False = failed, or no return if bail.
    """

    # Allow missing value if so requested.
    if not val and (opts.get("empty_ok") or opts.get("emptyOK")):
        return True

    # Get the optional paramaters
    fmt = opts.get("fmt", "ISOdate")
    bailout = opts.get("bailout", BAILOUT_DEFAULT) and True or False
    reveal = opts.get("reveal", REVEAL_DEFAULT) and True or False
    msg = opts.get("msg")

    # strptime accepts a non-ISO format, e.g., 2015-7-4 instead of 2015-07-04
    # Don't allow that if ISO is requested
    if fmt == 'ISOdate':
        if not is_date(val):
            return _valParmHelper(val, bailout, reveal, msg)

        # Pattern passed, set format for strptime value check
        fmt = '%Y-%m-%d'

    # Check the numbers against actual dates
    try:
        if cdr.strptime(val, fmt) is not None:
            return True
    except ValueError:
        pass

    # Special case for handling datetime that might have optional
    # milliseconds or microseconds attached
    try:
        fmt2 = fmt + ".%f"
        if cdr.strptime(val, fmt2) is not None:
            return True
    except ValueError:
        pass

    # Date check failed
    return _valParmHelper(val, bailout, reveal, msg)
