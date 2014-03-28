#----------------------------------------------------------------------
#
# $Id$
#
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
#
#----------------------------------------------------------------------

#----------------------------------------------------------------------
# Import external modules needed.
#----------------------------------------------------------------------
import cgi, cdr, cdrdb, sys, re, string, socket, xml.sax.saxutils, textwrap
import datetime
import time, os
import lxml.html
import lxml.html.builder
import xlwt

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
HEADER   = u"""\
<!DOCTYPE HTML PUBLIC '-//W3C//DTD HTML 4.01 Transitional//EN'
                      'http://www.w3.org/TR/html4/loose.dtd'>
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

class Page:
    """
    Object used to build a web page.

    Sample usage:

        form = cdrcgi.Page('Simple Report', subtitle='Documents by Title',
                           buttons=('Submit', 'Admin')
                           action='simple-report.py')
        form.add('<fieldset>')
        form.add(cdrcgi.Page.B.LEGEND('Select Documents By Title'))
        form.add_text_field('title', 'Containing')
        form.add('</fieldset>')
        form.add_output_options()
        form.send()
    """

    INDENT = u"  "
    JS = ("/js/jquery.js", "/js/CdrCalendar.js")
    STYLESHEETS = ("/stylesheets/CdrCalendar.css", "/stylesheets/cdr.css")
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
            body_classes  e.g., 'report'
        """
        self._finished = False
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
        self._start()

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
            label        string to indentify the checkbox to the user
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
            label        string to indentify the radio button to the user
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

            classes         if present, used as the 'class' attribute for
                            the input element.  May include multiple space
                            separated class names.
            wrapper_classes classes to be added to the div wrapper
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
        classes = kwargs.get("classes") or kwargs.get("class_")
        classes = classes or kwargs.get("class")
        if classes:
            if type(classes) in (list, tuple, set):
                classes = " ".join(classes)
            field.set("class", classes)
        if "value" in kwargs:
            field.set("value", unicode(kwargs["value"]))
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
            return u"%s%s\n" % (indent, block)
        lines = block.splitlines()
        return u"".join([u"%s%s\n" % (indent, line) for line in lines])

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
        label = Page.B.LABEL(Page.B.FOR(widget_id), label)
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
            self.add("""<form action="/cgi-bin/cdr/%s" method="%s">""" %
                     (self._action, self._method))
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
                self.add(Page.B.INPUT(name="Session", value=self._session,
                                      type="hidden"))
    def _finish(self):
        """
        Helper function called by the send() method.
        """
        if not self._finished:
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
        """
        self._title = title
        self._tables = tables
        self._options = options

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

    def _send_html(self):
        """
        Internal helper method for Report.send()
        """
        banner = self._options.get("banner")
        subtitle = self._options.get("subtitle")
        stylesheets=["/stylesheets/cdr.css"]
        page = Page(self._title, banner=banner, subtitle=subtitle, js=[],
                    body_classes="report", stylesheets=stylesheets)
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
                if "width" in column._options:
                    width = column._options["width"]
                    cell.set("style", "width:%s;" % width)
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
        page.send()

    def _send_excel_workbook(self):
        """
        Internal helper method for Report.send()
        """
        book = xlwt.Workbook(encoding="UTF-8")
        xlwt.add_palette_colour("hdrbg", 0x21)
        book.set_colour_RGB(0x21, 153, 52, 102) #993366
        borders = "borders: top thin, bottom thin, left thin, right thin"
        self._header_style = xlwt.easyxf("pattern: pattern solid, fore_colour "
                                         "hdrbg; font: colour white, bold True;"
                                         "align: wrap True, vert centre, "
                                         "horiz centre;" + borders)
        self._banner_style = xlwt.easyxf("pattern: pattern solid, fore_colour "
                                         "hdrbg; font: colour white, bold True,"
                                         "height 240; align: wrap True,"
                                         "vert centre, horiz centre;"
                                         + borders)
        self._data_style = xlwt.easyxf("align: wrap True, vert top;"
                                       + borders)
        self._bold_data_style = xlwt.easyxf("align: wrap True, vert top; "
                                            "font: bold True;" + borders)
        count = 1
        for table in self._tables:
            self._add_worksheet(book, table, count)
            count += 1
        import msvcrt
        now = datetime.datetime.now().strftime("%Y%m%d%H%M%S")
        book_name = self._title.replace(" ", "").replace("/", "-")
        book_name += "-%s.xls" % now
        msvcrt.setmode(sys.stdout.fileno(), os.O_BINARY)
        print "Content-type: application/vnd.ms-excel"
        print "Content-disposition: attachment; filename=%s" % book_name
        print
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
                values = "\n".join(Report.Cell._get_values(cell))
                style = self._data_style
                if isinstance(cell, self.Cell):
                    if cell._sheet_style:
                        style = cell._sheet_style
                    elif cell._bold:
                        style = self._bold_data_style
                    if cell._href:
                        vals = values.replace('"', '""')
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

    class Table:
        """
        One of (possibly) multiple tables in a CDR report
        """
        def __init__(self, columns, rows, **options):
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
            self._sheet_style = options.get("sheet_style")
            self._callback = options.get("callback")
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
            if self._classes:
                td.set("class", " ".join(self._classes))
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
        def _get_values(cell):
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
            if type(values) not in (list, tuple):
                return [unicode(values)]
            elif not values:
                return [""]
            return [unicode(v) for v in values]

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
# Get a session ID based on current form field values.
#----------------------------------------------------------------------
def getSession(fields):

    # If we already have a Session field value, use it.
    if fields.has_key(SESSION):
        session = fields[SESSION].value
        if len(session) > 0:
            return session

    # Check for missing fields.
    if not fields.has_key(USERNAME) or not fields.has_key(PASSWORD):
        return None
    userId = fields[USERNAME].value
    password = fields[PASSWORD].value
    if len(userId) == 0 or len(password) == 0:
        return None

    # Log on to the CDR Server.
    if fields.has_key(PORT):
        session = cdr.login(userId, password, port = cdr.getPubPort())
    else:
        session = cdr.login(userId, password)
    if session.find("<Err") >= 0: return None
    else:                         return session

#----------------------------------------------------------------------
# Get the name of the submitted request.
#----------------------------------------------------------------------
def getRequest(fields):

    # Make sure the request field exists.
    if not fields.has_key(REQUEST): return None
    else:                           return fields[REQUEST].value

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
def bail(message, banner = "CDR Web Interface", extra = None, logfile = None):
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

    # Create the page header.
    title   = "CDR Administration"
    section = "Login Screen"
    buttons = ["Log In"]
    hdr     = header(title, title, section, "Admin.py", buttons)

    # Perform the logout.
    error = cdr.logout(session)
    message = error or "Session Logged Out Successfully"

    # Put up the login screen.
    form = """\
        <H3>%s</H3>
           <TABLE CELLSPACING='0'
                  CELLPADDING='0'
                  BORDER='0'>
            <TR>
             <TD ALIGN='right'>
              <B>CDR User ID:&nbsp;</B>
             </TD>
             <TD><INPUT NAME='UserName'></TD>
            </TR>
            <TR>
             <TD ALIGN='right'>
              <B>CDR Password:&nbsp;</B>
             </TD>
             <TD><INPUT NAME='Password'
                        TYPE='password'>
             </TD>
            </TR>
           </TABLE>
          </FORM>
         </BODY>
        </HTML>\n""" % message

    sendPage(hdr + form)

#----------------------------------------------------------------------
# Display the CDR Administation Main Menu.
#----------------------------------------------------------------------
def mainMenu(session, news = None):

    # Save the session text, before converting to "?Session=session"
    sessionText = session

    userPair = cdr.idSessionUser(session, session)
    session  = "?%s=%s" % (SESSION, session)
    title    = "CDR Administration"
    section  = "Main Menu"
    buttons  = []
    hdr      = u"" + header(title, title, section, "", buttons)
    extra    = news and ("<H2>%s</H2>\n" % news) or ""
    menu     = """\
    <ol>
"""
    # We don't use EditFilters anymore
    # --------------------------------
    #try:
    #    if userPair[0].lower() == 'venglisc':
    #        menu += """\
    # <li>
    #  <a href='%s/EditFilters.py%s'>Manage Filters (Just for you, Volker!)</a>
    # </li>
#""" % (BASE, session)
    #except:
    #    pass

    try:
        # Identify the groups of the user
        # -------------------------------
        # userInfo = cdr.getUser((userPair[0], userPair[1]), userPair[0])
        userInfo = cdr.getUser(sessionText, userPair[0])
    except:
        bail('Unable to identify permissions for user. '
             'Has your session timed out?')

    # If returne from getUser is a string, it's an error message
    if type(userInfo) == type(""):
        bail(userInfo)

    # Creating a menu for users with only GUEST permission and one
    # for all others
    # ------------------------------------------------------------
    if 'GUEST' in userInfo.groups and len(userInfo.groups) < 2:
        for item in (
            ('GuestUsers.py',    'Guest User'                      ),
            ('Logout.py',        'Log Out'                         )
            ):
            menu += """\
     <li><a href='%s/%s%s'>%s</a></li>
""" % (BASE, item[0], session, item[1])
    else:
        for item in (
            ('BoardManagers.py', 'OCCM Board Managers'             ),
            ('CiatCipsStaff.py', 'CIAT/OCCM Staff'                 ),
            ('DevSA.py',         'Developers/System Administrators'),
            ('GuestUsers.py',    'Guest User'                      ),
            ('Logout.py',        'Log Out'                         )
            ):
            menu += """\
     <li><a href='%s/%s%s'>%s</a></li>
""" % (BASE, item[0], session, item[1])

    sendPage(hdr + extra + menu + """\
    </ol>
  </form>
 </body>
</html>""")

#----------------------------------------------------------------------
# Navigate to menu location or publish preview.
#----------------------------------------------------------------------
def navigateTo(where, session, **params):
    url = "http://%s%s/%s?%s=%s" % (WEBSERVER,
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
# Generate picklist for countries.
#----------------------------------------------------------------------
def countryList(conn, fName):
    query  = """\
  SELECT d.id, d.title
    FROM document d
    JOIN doc_type t
      ON t.id = d.doc_type
   WHERE t.name = 'Country'
ORDER BY d.title
"""
    pattern = "<option value='CDR%010d'>%s &nbsp;</option>"
    return generateHtmlPicklist(conn, fName, query, pattern)

#----------------------------------------------------------------------
# Generate picklist for states.
#----------------------------------------------------------------------
def stateList(conn, fName):
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
    return generateHtmlPicklist(conn, fName, query, pattern)

#----------------------------------------------------------------------
# Generate picklist for GlossaryAudience.
#----------------------------------------------------------------------
def glossaryAudienceList(conn, fName):
    defaultOpt = "<option value='' selected>Select an audience...</option>\n"
    query  = """\
SELECT DISTINCT value, value
           FROM query_term
          WHERE path IN ('/GlossaryTermConcept/TermDefinition/Audience',
               '/GlossaryTermConcept/TranslatedTermDefinition/Audience')
       ORDER BY 1"""
    pattern = "<option value='%s'>%s&nbsp;</option>"
    return generateHtmlPicklist(conn, fName, query, pattern,
                                firstOpt = defaultOpt)

#----------------------------------------------------------------------
# Generate picklist for GlossaryTermStatus.
#----------------------------------------------------------------------
def glossaryTermStatusList(conn, fName,
                           path = '/GlossaryTermName/TermNameStatus'):
    defaultOpt = "<option value='' selected>Select a status...</option>\n"
    query  = """\
SELECT DISTINCT value, value
           FROM query_term
          WHERE path = '%s'
       ORDER BY 1""" % path
    pattern = "<option value='%s'>%s&nbsp;</option>"
    return generateHtmlPicklist(conn, fName, query, pattern,
                                firstOpt = defaultOpt)


#----------------------------------------------------------------------
# Generate picklist for GlossaryTermStatus.
#----------------------------------------------------------------------
def glossaryTermDictionaryList(conn, fName):
    defaultOpt = "<option value='' selected>Select a dictionary...</option>\n"
    query  = """\
SELECT DISTINCT value, value
           FROM query_term
          WHERE path IN ('/GlossaryTermConcept/TermDefinition/Dictionary',
               '/GlossaryTermConcept/TranslatedTermDefinition/Dictionary')
       ORDER BY 1"""
    pattern = "<option value='%s'>%s&nbsp;</option>"
    return generateHtmlPicklist(conn, fName, query, pattern,
                                firstOpt = defaultOpt)


#----------------------------------------------------------------------
# Generic HTML picklist generator.
#
# Note: this only works if the query generates exactly as many
# columns for each row as are needed by the % conversion placeholders
# in the pattern argument, and in the correct order.
#
# For example invocations, see stateList and countryList above.
#----------------------------------------------------------------------
def generateHtmlPicklist(conn, fieldName, query, pattern,
                         selAttrs=None, firstOpt=None, lastOpt=None):
    """
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
""" % (subTitle, subTitle, nRows, unicode(strings, 'latin-1'))

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
""" % (i + 1, string.replace(title, ";", "; "), dtcol, href, docId)

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
