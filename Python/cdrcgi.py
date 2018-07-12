#----------------------------------------------------------------------
# Common routines for creating CDR web forms.
#
# BZIssue::1000
# BZIssue::1335
# BZIssue::1531
# BZIssue::1876
# BZIssue::1980
# BZIssue::2753
# BZIssue::3132
# BZIssue::3923
# BZIssue::4205
# BZIssue::4381
# BZIssue::4653 CTRO Access to CDR Admin
# Fixed some security weaknesses and enhanced Page and Report classes.
# JIRA::OCECDR-4170 - add new ExcelStyles class
#----------------------------------------------------------------------

#----------------------------------------------------------------------
# Import external modules needed.
#----------------------------------------------------------------------
import cdr
import cdrdb
import cgi
import copy
import datetime
import lxml.etree as etree
import lxml.html
import lxml.html.builder
import os
import re
import sys
import textwrap
import time
import urllib
import xlwt
import xml.sax.saxutils

#----------------------------------------------------------------------
# Do this once, right after loading the module. Used in Report class.
#----------------------------------------------------------------------
xlwt.add_palette_colour("hdrbg", 0x21)

#----------------------------------------------------------------------
# Get some help tracking down CGI problems.
#----------------------------------------------------------------------
import cgitb
cgitb.enable(display = cdr.isDevHost(), logdir = cdr.DEFAULT_LOGDIR)

#----------------------------------------------------------------------
# Now that we're migrating to CBIIT hosting, we'll need a more flexible
# method for keeping track of web server names.  If we're being called
# in the context of a web request, get the name of the server handling
# that request.  Otherwise, fall back on the name we use for the CDR
# server.
#----------------------------------------------------------------------
def _getWebServerName():
    try:
        name = os.environ["SERVER_NAME"]
        if name:
            return name
    except:
        pass
    return cdr.getHostName()[1]

#----------------------------------------------------------------------
# Create some useful constants.  Some of these are no longer necessarily
# useful (for example, ISPLAIN, or THISHOST) but are retained in case
# older code relies on them.
#----------------------------------------------------------------------
VERSION = "201603211524"
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
FORMBG   = '/images/back.jpg'
BASE     = '/cgi-bin/cdr'
MAINMENU = 'Admin Menu'
WEBSERVER= _getWebServerName()
SPLTNAME = WEBSERVER.lower().split(".")
THISHOST = SPLTNAME[0]
ISPLAIN  = "." not in THISHOST
DOMAIN   = "." + ".".join(SPLTNAME[1:])
DAY_ONE  = cdr.URDATE
NEWLINE  = "@@@NEWLINE-PLACEHOLDER@@@"
BR       = "@@@BR-PLACEHOLDER@@@"
HEADER   = u"""\
<!DOCTYPE html>
<HTML>
 <HEAD>
  <TITLE>%s</TITLE>
  <meta http-equiv='Content-Type' content='text/html;charset=utf-8'>
  <link rel='shortcut icon' href='/favicon.ico'>
  <LINK TYPE='text/css' REL='STYLESHEET' HREF='/stylesheets/dataform.css'>
  <style type='text/css'>
    body         { background-color: #%s; }
    *.banner     { background-color: silver;
                   background-image: url(/images/nav1.jpg); }
    *.DTDerror   { color: red;
                   font-weight: bold; }
    *.DTDwarning { color: green; }
    TD.ttext     { color: black; }
    TD.tlabel    { font-weight: bold;
                   text-align: right;
                   white-space: nowrap;
                   vertical-align: top; }
    TH.theader   { font-weight: bold;
                   font-size: small;
                   text-align: left;
                   white-space: nowrap;
                   vertical-align: top; }
  </style>
  %s
 </HEAD>
 <BODY>
  <FORM ACTION='/cgi-bin/cdr/%s' METHOD='%s'%s>
   <TABLE WIDTH='100%%' CELLSPACING='0' CELLPADDING='0' BORDER='0'>
    <TR>
     <TH class='banner' NOWRAP ALIGN='left'>
      <span style='color: white; font-size: xx-large;'>&nbsp;%s</span>
     </TH>
"""
RPTHEADER   = """\
<!DOCTYPE HTML PUBLIC '-//W3C//DTD HTML 4.01 Transitional//EN'
                      'http://www.w3.org/TR/html4/loose.dtd'>
<HTML>
 <HEAD>
  <TITLE>%s</TITLE>
  <meta http-equiv='Content-Type' content='text/html;charset=utf-8'>
  <link rel='shortcut icon' href='/favicon.ico'>
  <LINK TYPE='text/css' REL='STYLESHEET' HREF='/stylesheets/dataform.css'>
  <style type='text/css'>
    body         { font-family: Arial;
                   background-color: #%s; }
    *.banner     { background-color: silver;
                   background-image: url(/images/nav1.jpg); }
    *.DTDerror   { color: red;
                   font-weight: bold; }
    *.DTDwarning { color: green; }
    tr.odd        { background-color: #F7F7F7; }
    tr.even       { background-color: #DFDFDF; }
    th           { font-size: 12pt;
                   font-weight: bold;
                   text-align: center;
                   background-color: #ADADAD; }
  </style>
%s
 </HEAD>
"""
B_CELL = """\
     <TD class='banner'
         VALIGN='middle'
         ALIGN='right'
         WIDTH='100%'>
"""
BUTTON = """\
      <INPUT TYPE='submit' NAME='%s' VALUE='%s'>&nbsp;
"""
SUBBANNER = """\
    </TR>
    <TR>
     <TD BGCOLOR='#FFFFCC' COLSPAN='3'>
      <span style='color: navy; font-size: small;'>&nbsp;%s<BR></span>
     </TD>
    </TR>
   </TABLE>
"""

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

        return xlwt.Formula(u'HYPERLINK("%s";"%s")' % (url, label))

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

        return xlwt.Formula(u'HYPERLINK("%s";"%s")' % (url, label))

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

    INDENT = u"  "
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
        if not isinstance(line, basestring):
            line = lxml.html.tostring(line)
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
            if isinstance(wrapper_classes, basestring):
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
            if type(option) in (list, tuple):
                value, display = option
            else:
                value = display = option
            o = Page.B.OPTION(display, value=unicode(value))
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
            if isinstance(wrapper_classes, basestring):
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
            field.set("value", unicode(kwargs["value"]))
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
            if isinstance(wrapper_classes, basestring):
                wrapper_classes = wrapper_classes.split()
        else:
            wrapper_classes = []
        if "labeled-field" not in wrapper_classes:
            wrapper_classes.append("labeled-field")
        self.add('<div class="%s">' % " ".join(wrapper_classes))
        label = Page.B.LABEL(Page.B.FOR(name), label)
        self.add(label)
        value = unicode(kwargs.get("value", ""))
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
            url = "%s?%s" % (url, urllib.urlencode(kwargs))
        link = Page.B.A(display, href=url)
        self.add(Page.B.LI(link))

    def add_hidden_field(self, name, value):
        "Utility method to insert a hidden CGI field."
        if not value:
            value = u""
        if not isinstance(value, basestring):
            value = str(value)
        value = value and str(value) or ""
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
        sendPage(u"".join(self._html))

    @classmethod
    def _indent(class_, level, block):
        """
        Add indenting to a block of lines.
        """
        indent = class_.INDENT * level
        if not "\n" in block:
            result = u"%s%s\n" % (indent, block)
        else:
            lines = block.splitlines()
            result = u"".join([u"%s%s\n" % (indent, line) for line in lines])
        return result.replace(NEWLINE, "\n").replace(BR, "<br>")

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
        if not isinstance(value, basestring):
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
        print "".join(page._html)

class Report:
    """
    CDR Report which can be rendered as an HTML page or as an Excel workbook.

    Example usage:

        R = cdrcgi.Report
        cursor = cdrdb.connect('CdrGuest').cursor()
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
        if isinstance(tables, Report.Table):
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
            if isinstance(css, basestring):
                css = [css]
            for c in css:
                page.add_css(c)
        B = page.B
        for table in self._tables:
            if table._html_callback_pre:
                table._html_callback_pre(table, page)
            page.add('<table class="report">')
            if table._caption:
                if type(table._caption) in (list, tuple):
                    lines = list(table._caption)
                else:
                    lines = [table._caption]
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
            font = ["colour white", "bold True"]
            if opts.get("banner"):
                font.append("height 240")
            settings["pattern"] = "pattern solid, fore_colour hdrbg"
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
        import msvcrt
        now = datetime.datetime.now().strftime("%Y%m%d%H%M%S")
        book_name = re.sub(r"\W", "_", self._title)
        book_name += "-%s.xls" % now
        msvcrt.setmode(sys.stdout.fileno(), os.O_BINARY)
        sys.stdout.write("Content-type: application/vnd.ms-excel\r\n")
        sys.stdout.write("Content-disposition: attachment; ")
        sys.stdout.write("filename=%s\r\n\r\n" % book_name)
        book.save(sys.stdout)
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
                sheet.write_merge(row_number, row_number,
                                  0, len(table._columns) - 1,
                                  caption, self._banner_style)
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
                if type(values) is not int:
                    values = u"\n".join(values)
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
                        vals = unicode(values).replace(u'"', u'""')
                        formula = u'HYPERLINK("%s";"%s")' % (cell._href, vals)
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

    @staticmethod
    def test():
        """
        Very crude little test to check for any obvious breakage.

        Run like this from the command line:

         python -c "import cdrcgi;cdrcgi.Report.test()" | sed -n /DOCTYPE/,$p

        Wouldn't hurt to add more testing to this method as we get time.
        """
        R = Report
        cursor = cdrdb.connect("CdrGuest").cursor()
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
        report = R("Simple Report", [table])
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
            elif isinstance(classes, basestring):
                self._classes = classes.split()
            elif type(classes) in (set, tuple):
                self._classes = list(classes)
            elif type(classes) is list:
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
            td = Page.B.TD()
            if self._href:
                element = Page.B.A(href=self._href)
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

        @staticmethod
        def set_values(element, cell):
            values = Report.Cell._get_values(cell)
            value = values.pop(0)
            element.text = value
            while values:
                value = values.pop(0)
                br = Page.B.BR()
                br.tail = value
                element.append(br)

        @staticmethod
        def _get_values(cell, preserve_int=False):
            """
            Returns the values for a table cell as an array

            Passed:

                cell -       either a string or number, an array of strings
                             and/or numbers, or a Report.Cell object (whose
                             _value member may in turn be a string, a number,
                             or an array of strings and/or numbers)
            """
            if isinstance(cell, Report.Cell):
                values = cell._value
            else:
                values = cell
            if type(values) is int and preserve_int:
                return values
            if type(values) not in (list, tuple):
                return [unicode(values)]
            elif not values:
                return [""]
            return [unicode(v) for v in values]

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
    SUBMIT = "Submit"
    LOG_OUT = "Log Out"
    FORMATS = ("html", "excel")
    BOARD_NAME = "/Organization/OrganizationNameInformation/OfficialName/Name"
    AUDIENCES = ("Health Professional", "Patient")
    LANGUAGES = ("English", "Spanish")
    SUMMARY_SELECTION_METHODS = ("id", "title", "board")
    LOGNAME = "reports"
    LOGLEVEL = "info"

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
            self.cursor = cdrdb.connect("CdrGuest").cursor()
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
            if self.request == MAINMENU:
                navigateTo("Admin.py", self.session)
            elif self.request == self.REPORTS_MENU:
                navigateTo("Reports.py", self.session)
            elif self.request == self.LOG_OUT:
                logout(self.session)
            elif self.request == self.SUBMIT:
                self.show_report()
            else:
                self.show_form()
        except Exception, e:
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

        query = cdrdb.Query("query_term n", "n.doc_id", "n.value")
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

        query = cdrdb.Query("document", "title")
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
                cdrcgi.bail("Invalid format for CDR ID")
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
            query = cdrdb.Query("doc_version", "xml")
            query.where(query.Condition("num", doc_version))
        else:
            query = cdrdb.Query("document", "xml")
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

        fragment = unicode(fragment, "utf-8")
        query = cdrdb.Query("active_doc d", "d.id", "d.title")
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
});""" % self.script);

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

#----------------------------------------------------------------------
# Display the header for a CDR web form.
#----------------------------------------------------------------------
def header(title, banner, subBanner, script = '', buttons = None,
           bkgd = 'DFDFDF', numBreaks = 2, method = 'POST', stylesheet='',
           formExtra = ''):
    html = HEADER % (title, bkgd, stylesheet, script,
                     method, formExtra, banner)
    if buttons:
        html = html + B_CELL
        for button in buttons:
            if button == "Load":
                html = html + "      <INPUT NAME='DocId' SIZE='14'>&nbsp;\n"
            html = html + BUTTON % (REQUEST, button)
        html = html + "     </TD>\n"
    html = html + SUBBANNER % subBanner
    return html + numBreaks * "   <BR>\n"

#----------------------------------------------------------------------
# Display the header for a CDR web report (no banner or buttons).
# By default the background is white for reports.
#----------------------------------------------------------------------
def rptHeader(title, bkgd = 'FFFFFF', stylesheet=''):
    html = RPTHEADER % (title, bkgd, stylesheet)
    return html

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
    query = cdrdb.Query("session", "id")
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
def sendPage(page, textType = 'html', parms='', docId='', docType='',
             docVer=''):
    """
    Send a completed page of text to stdout, assumed to be piped by a
    webserver to a web browser.

    Pass:
        page     - Text to send, assumed to be 16 bit unicode.
        textType - HTTP Content-type, assumed to be html.
        parms    - RowID storing all parameters if report needs to
                   be converted to Word, usually an empty string.
        docType  - if parms is supplied the document type is needed
                   to properly redirect the output, usually an empty string.

    Return:
        No return.  After writing to the browser, the process exits.
    """
    if parms == '':
        redirect = ''
    else:
        redirect = 'Location: http://%s%s/QCforWord.py?DocId=%s&DocType=%s&DocVersion=%s&%s\n' % (WEBSERVER, BASE, docId, docType, docVer, parms)
    print """\
%sContent-type: text/%s

%s""" % (redirect, textType, unicodeToLatin1(page))
    sys.exit(0)

#----------------------------------------------------------------------
# Emit an HTML page containing an error message and exit.
#----------------------------------------------------------------------
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
    page = Page("CDR Error", banner=banner, subtitle="An error has occurred",
                js=[], stylesheets=["/stylesheets/cdr.css"])
    page.add(Page.B.P(message, Page.B.CLASS("error")))
    if extra:
        for arg in extra:
            page.add(Page.B.P(arg, Page.B.CLASS("error")))
    if logfile:
        cdr.logwrite ("cdrcgi bailout:\n %s" % message, logfile)
    page.send()

#----------------------------------------------------------------------
# Encode XML for transfer to the CDR Server using utf-8.
#----------------------------------------------------------------------
def encode(xml): return unicode(xml, 'latin-1').encode('utf-8')

#----------------------------------------------------------------------
# Convert CDR Server's XML from utf-8 to latin-1 encoding.
#----------------------------------------------------------------------
decodePattern = re.compile(u"([\u0080-\uffff])")
def decode(xml):
    # Take input in utf-8:
    #   Convert it to unicode.
    #   Replace all chars above 127 with character entitites.
    #   Convert from unicode back to 8 bit chars, ascii is fine since
    #     anything above ascii is entity encoded.
    return re.sub(decodePattern,
                  lambda match: u"&#x%X;" % ord(match.group(0)[0]),
                  unicode(xml, 'utf-8')).encode('ascii')

def unicodeToLatin1(s):
    # Same as above, but with unicode input instead of utf-8
    # The unfortunate name is a historical artifact of our using 'latin-1'
    #   as the final encoding, but it's really just 7 bit ascii.
    # If something sends other than unicode, let's track who did it
    if type(s) != unicode:
        cdr.logwrite("cdrcgi.unicodeToLatin1 got non-unicode string.  "
                     "Stack trace follows", stackTrace=True)
    return re.sub(decodePattern,
                  lambda match: u"&#x%X;" % ord(match.group(0)[0]),
                  s).encode('ascii')

def unicodeToJavaScriptCompatible(s):
    # Same thing but with 4 char unicode syntax for Javascript
    return re.sub(decodePattern,
                  lambda match: u"\\u%04X" % ord(match.group(0)[0]),
                  s).encode('ascii')

#----------------------------------------------------------------------
# Log out of the CDR session and put up a new login screen.
#----------------------------------------------------------------------
def logout(session):

    # Make sure we have a session to log out of.
    if not session: bail('No session found.')

    # Perform the logout.
    error = cdr.logout(session)
    message = error or "Session Logged Out Successfully"

    # Display a page with a link to log back in.
    title   = "CDR Administration"
    buttons = ["Log In"]
    action="/cgi-bin/secure/admin.py"
    page = Page(title, subtitle=message, buttons=buttons, action=action)
    page.add(Page.B.P("Thanks for spending quality time with the CDR!"))
    page.send()

#----------------------------------------------------------------------
# Display the CDR Administation Main Menu.
#----------------------------------------------------------------------
def mainMenu(session, news=None):

    try:
        name = cdr.idSessionUser(session, session)[0]
        user = cdr.getUser(session, name)
    except:
        user = ""
    if isinstance(user, basestring):
        bail("Missing or expired session")
    menus = (
        ("Board Manager Menu Users", "BoardManagers.py", "OCC Board Managers"),
        ("CIAT/OCCM Staff Menu Users", "CiatCipsStaff.py", "CIAT/OCC Staff"),
        ("Developer/SysAdmin Menu Users", "DevSA.py",
         "Developers/System Administrators")
    )
    available = []
    for group, script, label in menus:
        if group in user.groups:
            available.append((script, label))
    if available:
        available.append(("Logout.py", "Log Out"))
    else:
        available = [("GuestUsers.py", "Guest User")]

    # If user is only in one of the groups above (with 'Logout' added),
    # make the menu for that group the landing page and jump directly to
    # it. The navigateTo() call doesn't return. But don't bypass this
    # page if there's news to show.
    if len(available) < 3 and not news:
        navigateTo(available[0][0], session)

    opts = { "subtitle": "Main Menu", "body_classes": "admin-menu" }
    page = Page("CDR Administration", **opts)
    section  = "Main Menu"
    if news:
        style = "color: green; font-weight: bold; font-style: italic;"
        page.add(page.B.P(news, style=style))
    page.add("<ol>")
    for script, label in available:
         page.add_menu_link(script, label, session)
    page.add("</ol>")
    page.send()

#----------------------------------------------------------------------
# Navigate to menu location or publish preview.
#----------------------------------------------------------------------
def navigateTo(where, session, **params):
    url = "https://%s%s/%s?%s=%s" % (WEBSERVER,
                                     BASE,
                                     where,
                                     SESSION,
                                     session)

    # Concatenate additional Parameters to URL for PublishPreview
    # -----------------------------------------------------------
    for param in params.keys():
        url += "&%s=%s" % (cgi.escape(param), cgi.escape(params[param]))

    print "Location:%s\n\n" % (url)
    sys.exit(0)


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
            except ValueError, info:
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
            except cdr.Exception, info:
                if excp:
                    raise ValueError(info)
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
# Query components.
#----------------------------------------------------------------------
class SearchField:
    def __init__(self, var, selectors):
        self.var       = var
        self.selectors = selectors

#----------------------------------------------------------------------
# Generate picklist for miscellaneous document types.
#----------------------------------------------------------------------
def miscTypesList(conn, fName):
    path = '/MiscellaneousDocument/MiscellaneousDocumentMetadata' \
           '/MiscellaneousDocumentType'
    try:
        cursor = conn.cursor()
        query  = """\
SELECT DISTINCT value
           FROM query_term
          WHERE path = '%s'
       ORDER BY value""" % path
        cursor.execute(query)
        rows = cursor.fetchall()
        cursor.close()
        cursor = None
    except cdrdb.Error, info:
        bail('Failure retrieving misc type list from CDR: %s' % info[1][0])
    html = """\
      <SELECT NAME='%s'>
       <OPTION VALUE='' SELECTED>&nbsp;</OPTION>
""" % fName
    for row in rows:
        html += """\
       <OPTION VALUE='%s'>%s &nbsp;</OPTION>
""" % (row[0], row[0])
    html += """\
      </SELECT>
"""
    return html

#----------------------------------------------------------------------
# Generate picklist for document publication status valid values.
#----------------------------------------------------------------------
def pubStatusList(conn, fName):
    return """\
      <SELECT NAME='%s'>
       <OPTION VALUE='' SELECTED>&nbsp;</OPTION>
       <OPTION VALUE='A'>Ready For Publication &nbsp;</OPTION>
       <OPTION VALUE='I'>Not Ready For Publication &nbsp;</OPTION>
      </SELECT>
""" % fName

#----------------------------------------------------------------------
# Generate picklist for countries.  See generateHtmlPicklist() comments.
#----------------------------------------------------------------------
def countryList(conn, fName, valCol=-1):
    query  = """\
  SELECT d.id, d.title
    FROM document d
    JOIN doc_type t
      ON t.id = d.doc_type
   WHERE t.name = 'Country'
ORDER BY d.title
"""
    pattern = "<option value='CDR%010d'>%s &nbsp;</option>"
    return generateHtmlPicklist(conn, fName, query, pattern,
                                valCol=valCol, valPat='CDR%010d')

#----------------------------------------------------------------------
# Generate picklist for states.
#----------------------------------------------------------------------
def stateList(conn, fName, valCol=-1):
    query  = """\
SELECT DISTINCT s.id,
                s.title,
                c.title
           FROM document s
           JOIN query_term clink
             ON clink.doc_id = s.id
           JOIN document c
             ON clink.int_val = c.id
          WHERE clink.path = '/PoliticalSubUnit/Country/@cdr:ref'
       ORDER BY 2, 3"""
    pattern = "<option value='CDR%010d'>%s [%s]&nbsp;</option>"
    return generateHtmlPicklist(conn, fName, query, pattern,
                                valCol=valCol, valPat='CDR%010d')

#----------------------------------------------------------------------
# Generate picklist for GlossaryAudience.
#----------------------------------------------------------------------
def glossaryAudienceList(conn, fName, valCol=-1):
    defaultOpt = "<option value='' selected>Select an audience...</option>\n"
    query  = """\
SELECT DISTINCT value, value
           FROM query_term
          WHERE path IN ('/GlossaryTermConcept/TermDefinition/Audience',
               '/GlossaryTermConcept/TranslatedTermDefinition/Audience')
       ORDER BY 1"""
    pattern = "<option value='%s'>%s&nbsp;</option>"
    return generateHtmlPicklist(conn, fName, query, pattern,
                    firstOpt=defaultOpt, valCol=valCol, valPat='CDR%010d')

#----------------------------------------------------------------------
# Generate picklist for GlossaryTermStatus.
#----------------------------------------------------------------------
def glossaryTermStatusList(conn, fName,
                           path='/GlossaryTermName/TermNameStatus', valCol=-1):
    defaultOpt = "<option value='' selected>Select a status...</option>\n"
    query  = """\
SELECT DISTINCT value, value
           FROM query_term
          WHERE path = '%s'
       ORDER BY 1""" % path
    pattern = "<option value='%s'>%s&nbsp;</option>"
    return generateHtmlPicklist(conn, fName, query, pattern,
                                firstOpt=defaultOpt, valCol=valCol)


#----------------------------------------------------------------------
# Generate picklist for GlossaryTermStatus.
#----------------------------------------------------------------------
def glossaryTermDictionaryList(conn, fName, valCol=-1):
    defaultOpt = "<option value='' selected>Select a dictionary...</option>\n"
    query  = """\
SELECT DISTINCT value, value
           FROM query_term
          WHERE path IN ('/GlossaryTermConcept/TermDefinition/Dictionary',
               '/GlossaryTermConcept/TranslatedTermDefinition/Dictionary')
       ORDER BY 1"""
    pattern = "<option value='%s'>%s&nbsp;</option>"
    return generateHtmlPicklist(conn, fName, query, pattern,
                                firstOpt=defaultOpt, valCol=valCol)


#----------------------------------------------------------------------
# Generate picklist for OrganizationType
#----------------------------------------------------------------------
def organizationTypeList(conn, fName, valCol=-1):
    query  = """\
  SELECT DISTINCT value, value
    FROM query_term
   WHERE path = '/Organization/OrganizationType'
     AND value IS NOT NULL
     AND value <> ''
ORDER BY 1"""
    pattern = "<option value='%s'>%s &nbsp;</option>"
    return generateHtmlPicklist(conn, fName, query, pattern, valCol=valCol)

#----------------------------------------------------------------------
# Generic HTML picklist generator.
#
# Note: this only works if the query generates exactly as many
# columns for each row as are needed by the % conversion placeholders
# in the pattern argument, and in the correct order.
#
# For example invocations, see stateList and countryList above.
#
# Similarly, if valCol >= 0, there must be an actual column corresponding
# to the valCol number.  See comments below.
#----------------------------------------------------------------------
def generateHtmlPicklist(conn, fieldName, query, pattern, selAttrs=None,
                         firstOpt=None, lastOpt=None, valCol=-1, valPat=None):
    """
    Generate one of two outputs:

     1) A string of HTML that implements a option list (picklist) for a
        <select> element in an HTML form field, or
     2) A list of values that the user might have picked from in order to
        validate that nothing untoward happened betwixt list selection
        and selection processing.

    The goal is to enable a program to use identical software to create a
    picklist and to validate a selection from that list in case of bugs or
    hacking (or AppScan) attempts to upset it.

    Pass:
        conn      - Database connection.
        fieldName - Name of the entire selection field, <select name=...
        query     - Database query to generate options.
        pattern   - Interpolation pattern for items in each row of query result
                      e.g.: "<option value='%s'>%s&nbsp;</option>"
        selAttrs  - Optional attributes to add to the select line
                      e.g.: "multiple='1' size='5'"
        firstOpt  - Optional line(s) at the beginning of the option list
                      e.g.: "<option value='any' selected='1'>Any</option>\n"
        lastOpt   - Optional line(s) at the end of the option list
                      e.g.: "<option value='all'>All</option>\n"
        valCol    - If valCol > -1, this call is not to generate a picklist,
                    but to generate a validation list for checking the
                    picklist.  valCol is the column number of the query rows.
                      e.g.: query = SELECT id, name FROM ...
                            if valCol == 0, return a list of ids.
                            if valCol == 1, return a list of names.
                    Bad things may happen if valCol >= the number of actual
                    columns selected in the query.
        valPat    - If not None, impose this string intepolation pattern on
                    each returned valid value
                      e.g., valPat='CDR%010d'

    Return:
        if valCol < 0:  Return a string of HTML containing a select option
                        list from which a user can pick a value.
        if valCol >= 0: Return a list of the values which may be picked.
    """
    # Select rows from the database
    try:
        cursor = conn.cursor()
        cursor.execute(query, timeout=300)
        rows = cursor.fetchall()
        cursor.close()
        cursor = None
    except cdrdb.Error, info:
        bail('Failure retrieving %s list from CDR: %s' % (fieldName,
                                                          info[1][0]))

    # For validation, we don't need HTML, just a list of valid values
    if valCol >= 0:
        if valPat:
            return [valPat % row[valCol] for row in rows]
        return [row[valCol] for row in rows]

    # Else generate a picklist
    html = "  <select name='%s'" % fieldName

    # Add any requested attributes to the select
    if selAttrs:
        html += " " + selAttrs
    html += ">\n"

    # If there are user supplied options to put at the top
    if firstOpt:
        html += "   " + firstOpt
    else:
        # Backwards compatibity requires this default firstOpt
        html += "   " + "<option value='' selected>&nbsp;</option>\n"

    # Add data from the query
    for row in rows:
        option = pattern % tuple(row)
        html += "   %s\n" % option

    # Final options
    if lastOpt:
        html += lastOpt
    # Termination
    html += "  </select>\n"

    return html

#----------------------------------------------------------------------
# Generate the top portion of an advanced search form.
#----------------------------------------------------------------------
def startAdvancedSearchPage(session, title, script, fields, buttons, subtitle,
                            conn, errors = "", extraField = None):
    """
    Create a form to take in parameters for advanced searches.
    This routine creates many types of forms, based on passed parameters.

    Pass:
        session   - String identifying the CDR login session
        title     - HTML TITLE of the form on screen.
        script    - Name of the ACTION script in cgi-bin/cdr that will
                    process the FORM node created by this script.
        fields    - A sequence of sequences.  Each element of the containing
                    sequence describes one field of the form.  The sequence
                    with an individual field description contains:
                        Label to display to the user.
                        HTML form field name.
                        Optional function identifier to invoke to generate
                            additional information, such as a select list.
                            If present, the function will be called, passing
                            an active database connection and the name of
                            the HTML form field for which the function is
                            being invoked, i.e., field[1] of this sequence.
                    Each field is displayed as a row in a table on the form.
        buttons   - A sequence of sequences defining form buttons.
                    Each inner sequence contains three elements:
                        Button type, e.g., 'submit', 'reset'
                        Button HTML element name, e.g., 'HelpButton'
                        Button Label, e.g., 'Help'
                    Buttons are displayed in a row at the bottom of the form.
        subtitle  - A subtitle displayed to the user, immediately under the
                    title "CDR Advanced Search"
        conn      - An active database connection, passed back in a script
                    function invocation if needed.  May be None if there
                    are no function needing to be invoked.  See
                    "Optional function identifier" above.
        errors    - An optional error string, displayed near the top of the
                    form page when it is desirable to tell a user that
                    an error occurred.
        extraField- Optional extra rows to add to the form table between the
                    form fields and the buttons at the bottom.

    Return:
        A string of HTML containing the base content of the form.  It is the
        caller's responsibility to add anything else desired to the bottom of
        the form, then add the closing </FORM>, </BODY>, and </HTML> tags.
        The caller may then send the form page to the client.
"""
    html = """\
<!DOCTYPE HTML PUBLIC '-//IETF//DTD HTML//EN'>
<HTML>
 <HEAD>
  <TITLE>%s</TITLE>
  <META         HTTP-EQUIV  = "Content-Type"
                CONTENT     = "text/html; charset=iso-8859-1">
  <STYLE        TYPE        = "text/css">
   <!--
    *.header  { font-family: Arial, Helvietica, sans-serif;
                font-size: x-large;
                white-space: nowrap;
                color: #000066 }
    *.subhdr  { font-family: Arial, Helvietica, sans-serif;
                font-size: large;
                white-space: nowrap;
                color: #000066 }
    *.page    { font-family: Arial, Helvietica, sans-serif;
                font-size: medium;
                white-space: nowrap;
                color: #000066 }
   -->
  </STYLE>
 </HEAD>
 <BODY          BGCOLOR     = "#CCCCFF">
  <FORM         METHOD      = "GET"
                ACTION      = "%s/%s">
   <INPUT       TYPE        = "hidden"
                NAME        = "%s"
                VALUE       = "%s">
   <TABLE       WIDTH       = "100%%"
                BORDER      = "0"
                CELLSPACING = "0">
    <TR         BGCOLOR     = "#6699FF">
     <TD        HEIGHT      = "26"
                COLSPAN     = "2"
                class       = "header">CDR Advanced Search
     </TD>
    </TR>
    <TR         BGCOLOR     = "#FFFFCC">
     <TD        COLSPAN     = "2"
                class       = "subhdr">%s</TD>
    <TR>
    <TR>
     <TD        style       = "white-space: nowrap;"
                COLSPAN     = "2">&nbsp;</TD>
    </TR>
""" % (title, BASE, script, SESSION, session, subtitle)

    if errors:
        html += """\
    <TR>
     <TD ALIGN="left" COLSPAN="2">
      %s
     </TD>
    </TR>
""" % errors

    for field in fields:
        if len(field) == 2:
            html += """\
    <TR>
     <TD        NOWRAP
                ALIGN       = "right"
                class       = "page">%s &nbsp; </TD>
     <TD        WIDTH       = "55%%"
                ALIGN       = "left">
      <INPUT    TYPE        = "text"
                NAME        = "%s"
                SIZE        = "60">
     </TD>
    </TR>
""" % field
        else:
            html += """\
    <TR>
     <TD        NOWRAP
                ALIGN       = "right"
                class       = "page">%s &nbsp; </TD>
     <TD        WIDTH       = "55%%"
                ALIGN       = "left">
%s
     </TD>
    </TR>
""" % (field[0], field[2](conn, field[1]))

    if len(fields) > 1:
        html += """\
    <TR>
     <TD        NOWRAP
                WIDTH       = "15%"
                class       = "page"
                VALIGN      = "top"
                ALIGN       = "right">Search Connector &nbsp; </TD>
     <TD        WIDTH       = "30%"
                ALIGN       = "left">
      <SELECT   NAME        = "Boolean"
                SIZE        = "1">
       <OPTION  SELECTED>AND</OPTION>
       <OPTION>OR</OPTION>
      </SELECT>
     </TD>
    </TR>"""

    if extraField:
        if type(extraField[0]) not in (type([]), type(())):
            extraField = [extraField]
        for ef in extraField:
            html += """\
    <TR>
     <TD        NOWRAP
                class       = "page"
                VALIGN      = "top"
                ALIGN       = "right">%s &nbsp; </TD>
     <TD        WIDTH       = "55%%">
%s
     </TD>
    </TR>
""" % (ef[0], ef[1])

    html += """\
    <TR>
     <TD        WIDTH       = "15%">&nbsp;</TD>
     <TD        WIDTH       = "55%">&nbsp;</TD>
    </TR>
   </TABLE>
   <TABLE       WIDTH       = "100%"
                BORDER      = "0">
    <TR>
     <TD        COLSPAN     = "2">&nbsp; </TD>
"""

    for button in buttons:
        if button[0].lower() == 'button':
            html += """\
     <TD        WIDTH       = "13%%"
                ALIGN       = "center">
      <INPUT    TYPE        = "button"
                ONCLICK     = %s
                VALUE       = "%s">
     </TD>
""" % (xml.sax.saxutils.quoteattr(button[1]), button[2])
        else:
            html += """\
     <TD        WIDTH       = "13%%"
                ALIGN       = "center">
      <INPUT    TYPE        = "%s"
                NAME        = "%s"
                VALUE       = "%s">
     </TD>
""" % button

    html += """\
     <TD        WIDTH       = "33%">&nbsp;</TD>
    </TR>
   </TABLE>
   <BR>
"""

    return html


#----------------------------------------------------------------------
# Generate the top portion of an advanced search form.
#----------------------------------------------------------------------
def addNewFormOnPage(session, script, fields, buttons, subtitle,
                            conn, errors = "", extraField = None):

    html = """\
  <FORM         METHOD      = "GET"
                ACTION      = "%s/%s">
   <INPUT       TYPE        = "hidden"
                NAME        = "%s"
                VALUE       = "%s">
   <TABLE       WIDTH       = "100%%"
                BORDER      = "0"
                CELLSPACING = "0">
    <!-- TR         BGCOLOR     = "#6699FF">
     <TD        HEIGHT      = "26"
                COLSPAN     = "2"
                class       = "header">CDR Advanced Search</TD>
    </TR -->
    <TR         BGCOLOR     = "#FFFFCC">
     <TD        COLSPAN     = "2"
                class       = "subhdr">%s</TD>
    <TR>
    <TR>
     <TD        NOWRAP
                COLSPAN     = "2">&nbsp;</TD>
    </TR>
""" % (BASE, script, SESSION, session, subtitle)

    if errors:
        html += """\
    <TR>
     <TD ALIGN="left" COLSPAN="2">
      %s
     </TD>
    </TR>
""" % errors

    for field in fields:
        if len(field) == 2:
            html += """\
    <TR>
     <TD        NOWRAP
                ALIGN       = "right"
                class       = "page">%s &nbsp; </TD>
     <TD        WIDTH       = "55%%"
                ALIGN       = "left">
      <INPUT    TYPE        = "text"
                NAME        = "%s"
                SIZE        = "60">
     </TD>
    </TR>
""" % field
        else:
            html += """\
    <TR>
     <TD        NOWRAP
                ALIGN       = "right"
                class       = "page">%s &nbsp; </TD>
     <TD        WIDTH       = "55%%"
                ALIGN       = "left">
%s
     </TD>
    </TR>
""" % (field[0], field[2](conn, field[1]))

    if len(fields) > 1:
        html += """\
    <TR>
     <TD        NOWRAP
                WIDTH       = "15%"
                class       = "page"
                VALIGN      = "top"
                ALIGN       = "right">Search Connector &nbsp; </TD>
     <TD        WIDTH       = "30%"
                ALIGN       = "left">
      <SELECT   NAME        = "Boolean"
                SIZE        = "1">
       <OPTION  SELECTED>AND</OPTION>
       <OPTION>OR</OPTION>
      </SELECT>
     </TD>
    </TR>"""

    if extraField:
        if type(extraField[0]) not in (type([]), type(())):
            extraField = [extraField]
        for ef in extraField:
            html += """\
    <TR>
     <TD        NOWRAP
                class       = "page"
                VALIGN      = "top"
                ALIGN       = "right">%s &nbsp; </TD>
     <TD        WIDTH       = "55%%">
%s
     </TD>
    </TR>
""" % (ef[0], ef[1])

    html += """\
    <TR>
     <TD        WIDTH       = "15%">&nbsp;</TD>
     <TD        WIDTH       = "55%">&nbsp;</TD>
    </TR>
   </TABLE>
   <TABLE       WIDTH       = "100%"
                BORDER      = "0">
    <TR>
     <TD        COLSPAN     = "2">&nbsp; </TD>
"""

    for button in buttons:
        if button[0].lower() == 'button':
            html += """\
     <TD        WIDTH       = "13%%"
                ALIGN       = "center">
      <INPUT    TYPE        = "button"
                ONCLICK     = %s
                VALUE       = "%s">
     </TD>
""" % (xml.sax.saxutils.quoteattr(button[1]), button[2])
        else:
            html += """\
     <TD        WIDTH       = "13%%"
                ALIGN       = "center">
      <INPUT    TYPE        = "%s"
                NAME        = "%s"
                VALUE       = "%s">
     </TD>
""" % button

    html += """\
     <TD        WIDTH       = "33%">&nbsp;</TD>
    </TR>
   </TABLE>
   <BR>
"""

    return html


#----------------------------------------------------------------------
# XXX This functionality has become pretty crusty over time. Replace it.
#
# Construct query for advanced search page.
#
# The caller passes the following arguments:
#
#   searchFields
#     a list of one or more objects with two attributes:
#       * var:       a string containing the content of one of the
#                    fields in the HTML search form
#       * selectors: this can be either a single string or a list;
#                    the normal case is the list, each member of which
#                    is a string identifying the path(s) to be matched
#                    in the query_term table; if a string contains an
#                    ampersand, then the WHERE clause to find the
#                    paths will use the SQL 'LIKE' keyword; otherwise
#                    that clause with use '=' for an exact match.
#                    If the selector is a single string, rather than
#                    a list, it represents a column in the document
#                    table which is to be compared with the value of
#                    the var member of the SearchField object (for an
#                    example, see SummarySearch.py, which has a
#                    SearchField object for checking a value in the
#                    active_status column of the document table
#
#   boolOp
#     the string value 'AND' or 'OR' - used as a boolean connector
#     between the WHERE clauses for each of the search fields passed
#     in by the first argument.
#
#   docType
#     either a string identifying which document type the result
#     set must come from, or a list of strings identifying a choice
#     of document types from the which the result set can be drawn.
#
# Modified 2003-09-11 RMK as follows:
#   If the `selectors' attribute of a SearchField object is a list,
#   then for any string in that list, if the string ends in the
#   substring "/@cdr:ref[int_val]" then instead of matching the
#   contents of the `var' attribute against the `value' column of
#   the query_term table, an lookup will be added to the SQL query
#   to find occurrences of the target string (in the var attribute)
#   in the title column of the document table, joined to the int_val
#   column of the query_term table.  This allow the users to perform
#   advanced searches which include matches against titles of linked
#   documents.
#----------------------------------------------------------------------
exampleQuery = """\
SELECT d.id
  FROM document d
  JOIN doc_type t
    ON t.id = d.doc_type
   AND t.name = 'Citation'
 WHERE d.id IN (SELECT doc_id
                  FROM query_term
                 WHERE value LIKE '%immunohistochemical%'
                   AND path = '/Citation/PubmedArticle/%/Article/%Title'
                    OR value LIKE '%immunohistochemical%'
                   AND path = '/Citation/PDQCitation/CitationTitle')
   AND d.id IN (SELECT doc_id
                  FROM query_term
                 WHERE value = 'Histopathology'
                   AND path = '/Citation/PubmedArticle/MedlineCitation/MedlineJournalInfo/MedlineTA'
                    OR path = '/Citation/PDQCitation/PublicationDetails/PublishedIn/@cdr:ref'
                   AND int_val IN (SELECT id
                                     FROM document
                                    WHERE title = 'Histopathology'))"""

def constructAdvancedSearchQuery(searchFields, boolOp, docType):
    where      = ""
    strings    = ""
    boolOp     = boolOp == "AND" and " AND " or " OR "

    for searchField in searchFields:

        #--------------------------------------------------------------
        # Skip empty fields.
        #--------------------------------------------------------------
        if searchField.var:

            queryOp  = getQueryOp(searchField.var)  # '=' or 'LIKE'
            queryVal = getQueryVal(searchField.var) # escape single quotes

            #----------------------------------------------------------
            # Remember the fields' values in a single string so we can
            # show it to the user later, reminding him what he searched
            # for.
            #----------------------------------------------------------
            if strings: strings += ' '
            strings += queryVal.strip()

            #----------------------------------------------------------
            # Start another portion of the WHERE clause.
            #----------------------------------------------------------
            if not where:
                where = " WHERE "
            else:
                where += boolOp

            #----------------------------------------------------------
            # Handle special case of match against a column in the
            # document table.
            #----------------------------------------------------------
            if type(searchField.selectors) == type(""):
                where += "(d.%s %s '%s')" % (searchField.selectors,
                                             queryOp,
                                             queryVal)
                continue

            #----------------------------------------------------------
            # Build up a portion of the WHERE clause to represent this
            # search field.
            #----------------------------------------------------------
            where += "d.id IN (SELECT doc_id FROM query_term"
            prefix = " WHERE "

            #----------------------------------------------------------
            # Build up a sub-select which checks to see if the value
            # desired for this field can be found in any of the
            # paths identified by the SearchField object's selectors
            # attribute.
            #----------------------------------------------------------
            for selector in searchField.selectors:
                pathOp = selector.find("%") == -1 and "=" or "LIKE"

                #------------------------------------------------------
                # Handle the normal case: a string stored in the doc.
                #------------------------------------------------------
                if not selector.endswith("/@cdr:ref[int_val]"):
                    where += ("%spath %s '%s' AND value %s '%s'"
                              % (prefix, pathOp, selector, queryOp, queryVal))
                else:

                    #--------------------------------------------------
                    # Special code to handle searches for linked
                    # documents.  We need a sub-subquery to look at
                    # the title fields of the linked docs.
                    #--------------------------------------------------
                    where += ("%spath %s '%s' AND int_val IN "
                              "(SELECT id FROM document WHERE title %s '%s')"
                              % (prefix, pathOp, selector[:-9],
                                 queryOp, queryVal))
                prefix = " OR "
            where += ")"

    #------------------------------------------------------------------
    # If the user didn't fill in any fields, we can't make a query.
    #------------------------------------------------------------------
    if not where:
        return (None, None)

    #------------------------------------------------------------------
    # Join the document and doc_type tables.  We could be looking for
    # a single document type or more than one document type.  If we're
    # looking for more than one document type, the query has to get
    # the document type in the result set for each of the documents
    # found.
    #------------------------------------------------------------------
    # Adding the docytype as a string to the query output since we
    # may need to distinguish between InScope and CTGov protocols later
    # on.
    # -----------------------------------------------------------------
    if type(docType) == type(""):
        query = ("SELECT DISTINCT d.id, d.title, '%s' FROM document d "
                 "JOIN doc_type t ON t.id = d.doc_type AND t.name = '%s'"
                 % (docType, docType))
    else:
        query = ("SELECT DISTINCT d.id, d.title, t.name FROM document d "
                 "JOIN doc_type t ON t.id = d.doc_type AND t.name IN (")
        sep = ""
        for dt in docType:
            query += "%s'%s'" % (sep, dt)
            sep = ","
        query += ")"

    query += where + " ORDER BY d.title"
    return (query, strings)

#----------------------------------------------------------------------
# Construct top of HTML page for advanced search results.
#----------------------------------------------------------------------
def advancedSearchResultsPageTop(subTitle, nRows, strings):
    return u"""\
<!DOCTYPE HTML PUBLIC '-//IETF//DTD HTML//EN'>
<HTML>
 <HEAD>
  <TITLE>CDR %s Search Results</TITLE>
  <META   HTTP-EQUIV = "Content-Type"
             CONTENT = "text/html; charset=iso-8859-1">
  <STYLE        TYPE = "text/css">
   <!--
    *.header  { font-family: Arial, Helvietica, sans-serif;
                font-size: x-large;
                white-space: nowrap;
                color: #000066 }
    *.subhdr  { font-family: Arial, Helvietica, sans-serif;
                font-size: large;
                white-space: nowrap;
                color: #000066 }
    *.page    { font-family: Arial, Helvietica, sans-serif;
                font-size: medium;
                color: #000066 }
    :link            { color: navy }
    :link:visited    { color: navy }
    :link:hover      { background: #FFFFCC; }
    tr.rowitem:hover { background: #DDDDDD; } /* Does not work for IE */
   -->
  </STYLE>
 </HEAD>
 <BODY       BGCOLOR = "#CCCCFF">
  <TABLE       WIDTH = "100%%"
              BORDER = "0"
         CELLSPACING = "0"
               class = "page">
   <TR       BGCOLOR = "#6699FF">
    <TD       HEIGHT = "26"
             COLSPAN = "4"
               class = "header">CDR Advanced Search Results</TD>
   </TR>
   <TR       BGCOLOR = "#FFFFCC">
    <TD      COLSPAN = "4">
     <SPAN     class = "subhdr">%s</SPAN>
    </TD>
   </TR>
   <TR>
    <TD       NOWRAP
             COLSPAN = "4"
              HEIGHT = "20">&nbsp;</TD>
   </TR>
   <TR>
    <TD       NOWRAP
             COLSPAN = "4"
               class = "page">
     <span style="color: black;">%d documents match '%s'</span>
    </TD>
   </TR>
   <TR>
    <TD       NOWRAP
             COLSPAN = "4"
               class = "page">&nbsp;</TD>
   </TR>
""" % (subTitle, subTitle, nRows, cgi.escape(unicode(strings, 'latin-1')))
    # Note cgi.escape call above to block XSS attack vulnerability
    # discovered by Appscan

#----------------------------------------------------------------------
# Construct HTML page for advanced search results.
#----------------------------------------------------------------------
def advancedSearchResultsPage(docType, rows, strings, filter, session = None):
    # We like the display on the web to be pretty.  The docType has been
    # overloaded as title *and* docType.  I'm splitting the meaning here.
    # --------------------------------------------------------------------
    subTitle = docType
    docType  = docType.replace(' ', '')

    html = advancedSearchResultsPageTop(subTitle, len(rows), strings)

    session = session and ("&%s=%s" % (SESSION, session)) or ""
    for i in range(len(rows)):
        docId = "CDR%010d" % rows[i][0]
        title = cgi.escape(rows[i][1])
        dtcol = "<TD>&nbsp;</TD>"
        filt  = filter
        if docType == 'Protocol':
            dt = rows[i][2]

            # This block is only needed if the filter is of type
            # dictionary rather than a string (the filter name).
            # In that case we want to display the docType on the
            # result page and pick the appropriate filter for
            # echo docType
            # --------------------------------------------------
            if len(filter) < 10:
               filt = filter[dt]
               dtcol = """\
    <TD        WIDTH = "10%%"
              VALIGN = "top">%s</TD>
""" % dt

        # XXX Consider using QcReport.py for all advanced search results pages.
        if docType in ("Person", "Organization", "GlossaryTermConcept",
                       "GlossaryTermName"):
            href = "%s/QcReport.py?DocId=%s%s" % (BASE, docId, session)
        elif docType == 'Protocol' and dt == "CTGovProtocol":
            href = "%s/QcReport.py?DocId=%s%s" % (BASE, docId, session)
        elif docType == "Summary":
            href = "%s/QcReport.py?DocId=%s&ReportType=nm%s" % (BASE, docId,
                                                                session)
        else:
            href = "%s/Filter.py?DocId=%s&Filter=%s%s" % (BASE, docId,
                                                          filt, session)
        html += u"""\
   <TR class="rowitem">
    <TD       NOWRAP
               WIDTH = "5%%"
              VALIGN = "top">
     <DIV      ALIGN = "right">%d.</DIV>
    </TD>
    <TD        WIDTH = "65%%">%s</TD>
%s
    <TD        WIDTH = "10%%"
              VALIGN = "top">
     <A         HREF = "%s">%s</A>
    </TD>
   </TR>
""" % (i + 1, title.replace(";", "; "), dtcol, href, docId)

        # Requested by LG, Issue #193.
        if docType == "Protocol":
            html += "<TR><TD COLWIDTH='3'>&nbsp;</TD></TR>\n"

    return html + "  </TABLE>\n </BODY>\n</HTML>\n"

#----------------------------------------------------------------------
# Create an HTML table from a passed data
#----------------------------------------------------------------------
def tabularize (rows, tblAttrs=None):
    """
    Create an HTML table string from passed data.

    Pass:
        rows = Sequence of rows for the table, each containing
               a sequence of columns.
               If the number of columns is not the same in each row,
               then the caller gets whatever he gets, so it may be
               wise to add columns with content like "&nbsp;" if needed.
               No entity conversions are performed.

        tblAttrs = Optional string of attributes to put in table, e.g.,
               "align='center' border='1' width=95%'"

        We might add rowAttrs and colAttrs if this is worthwhile.
    Return:
        HTML as a string.
    """
    if not tblAttrs:
        html = "<table>\n"
    else:
        html = "<table " + tblAttrs + ">\n"

    for row in rows:
        html += " <tr>\n"
        for col in row:
            html += "  <td>%s</td>\n" % col
        html += " </tr>\n"
    html += "</table>"

    return html


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
# Borrowed from ActiveState's online Python cookbook.
#----------------------------------------------------------------------
def int_to_roman(inNum):
   """
   Convert an integer to Roman numerals.

   Examples:
   >>> int_to_roman(0)
   Traceback (most recent call last):
   ValueError: Argument must be between 1 and 3999

   >>> int_to_roman(-1)
   Traceback (most recent call last):
   ValueError: Argument must be between 1 and 3999

   >>> int_to_roman(1.5)
   Traceback (most recent call last):
   TypeError: expected integer, got <type 'float'>

   >>> for i in range(1, 21): print int_to_roman(i)
   ...
   I
   II
   III
   IV
   V
   VI
   VII
   VIII
   IX
   X
   XI
   XII
   XIII
   XIV
   XV
   XVI
   XVII
   XVIII
   XIX
   XX
   >>> print int_to_roman(2000)
   MM
   >>> print int_to_roman(1999)
   MCMXCIX
   """
   if type(inNum) != type(1):
      raise TypeError, "expected integer, got %s" % type(inNum)
   if not 0 < inNum < 4000:
      raise ValueError, "Argument must be between 1 and 3999"
   ints = (1000, 900,  500, 400, 100,  90, 50,  40, 10,  9,   5,  4,   1)
   nums = ('M',  'CM', 'D', 'CD','C', 'XC','L','XL','X','IX','V','IV','I')
   result = ""
   for i in range(len(ints)):
      count = int(inNum / ints[i])
      result += nums[i] * count
      inNum -= ints[i] * count
   return result

#--------------------------------------------------------------------
# Colorize differences in the report.
# Adapted from Bob's EmailerReports.py on the Electronic mailer server.
#--------------------------------------------------------------------
def colorDiffs(report, subColor='#FAFAD2', addColor='#F0E68C',
                       atColor='#87CEFA'):
    """
    Colorizes the output of the GNU diff utility based on whether
    lines begin with a '-', '+' or '@'.

    Pass:
        Text of report to colorize.
        Color for '-' lines, default = Light goldenrod yellow
        Color for '+' lines, default = Khaki
        Color for '@' lines, default = Light sky blue
    """
    wrapper = textwrap.TextWrapper(subsequent_indent=' ', width=90)
    lines = report.splitlines()
    for i in xrange(len(lines)):
        color = None
        if lines[i].startswith('-'):
            color = subColor
        elif lines[i].startswith('+'):
            color = addColor
        elif lines[i].startswith('@'):
            color = atColor

        if color:
            lines[i] = "<span style='background-color: %s'>%s</span>" % \
                        (color, wrapper.fill(lines[i]))
        else:
            lines[i] = wrapper.fill(lines[i])
    return "\n".join(lines)

def markupTagForPrettyXml(match):
    s = match.group(1)
    if s.startswith('/'):
        return "</@@TAG-START@@%s@@END-SPAN@@>" % s[1:]
    trailingSlash = ''
    if s.endswith('/'):
        s = s[:-1]
        trailingSlash = '/'
    pieces = re.split("\\s", s, 1)
    if len(pieces) == 1:
        return "<@@TAG-START@@%s@@END-SPAN@@%s>" % (s, trailingSlash)
    tag, attrs = pieces
    pieces = ["<@@TAG-START@@%s@@END-SPAN@@" % tag]
    for attr, delim in re.findall("(\\S+=(['\"]).*?\\2)", attrs):
        name, value = attr.split('=', 1)
        pieces.append(" @@NAME-START@@%s=@@END-SPAN@@"
                      "@@VALUE-START@@%s@@END-SPAN@@" % (name, value))
    pieces.append(trailingSlash)
    pieces.append('>')
    return "".join(pieces)

def makeXmlPretty(doc):
    import lxml.etree as etree
    if type(doc) is unicode:
        doc = doc.encode("utf-8")
    tree = etree.XML(doc)
    doc = unicode(etree.tostring(tree, pretty_print=True))
    doc = re.sub("<([^>]+)>", markupTagForPrettyXml, doc)
    doc = cgi.escape(doc)
    doc = doc.replace('@@TAG-START@@', '<span class="xml-tag-name">')
    doc = doc.replace('@@NAME-START@@', '<span class="xml-attr-name">')
    doc = doc.replace('@@VALUE-START@@', '<span class="xml-attr-value">')
    doc = doc.replace('@@END-SPAN@@', '</span>')
    return doc

def makeCssForPrettyXml(tagNameColor="blue", attrNameColor="maroon",
                        attrValueColor="red", tagNameWeight="bold"):
    return u"""\
<style type="text/css">
 .xml-tag-name { color: %s; font-weight: %s; }
 .xml-attr-name { color: %s; }
 .xml-attr-value { color: %s; }
</style>
""" % (tagNameColor, tagNameWeight, attrNameColor, attrValueColor)

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
    if hasattr(val, "__iter__"):
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
        if isinstance(values, basestring):
            values = [values]
        cval = val
        if icase:
            values = [v.lower() for v in values]
            cval = val.lower()
        if cval in values:
            return True
        fp = open("d:/tmp/val-parm-val.log", "a")
        fp.write("cval=%s values=%s\n" % (cval, values))
        fp.close()
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
    from email.utils import parseaddr
    (name, email) = parseaddr(val)

    # If it's completely screwed up
    if not email:
        return _valParmHelper(val, bailout, reveal, msg)

    # Simple validation, may improve it later, but full RFC requires
    # an incredible thousand character regex.
    match = re.search(r"[^@]+@[^@\.]+\.[^@]+$", email)
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

def log_fields(fields, **opts):
    """
    Log the parameters sent to a CGI script.

    Pass:
        fields    - from cgi.FieldStorage()
        program   - name of script to be logged (optional)
        logfile   - optional override of cdr.DEFAULT_LOGFILE

    Return:
        nothing
    """
    program = opts.get("program")
    logfile = opts.get("logfile")
    values = []
    for name in fields.keys():
        field = fields[name]
        if hasattr(field, "value"):
            value = field.value
        else:
            value = [item.value for item in field]
        values.append((name, value))
    if program:
        message = "%s called with %s" % (repr(program), repr(dict(values)))
    else:
        message = repr(dict(values))
    if logfile:
        cdr.logwrite(message, logfile)
    else:
        cdr.logwrite(message)
