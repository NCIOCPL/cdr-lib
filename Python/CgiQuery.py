#!/usr/bin/python
#----------------------------------------------------------------------
# Base class for CGI database query interface.
# BZIssue::4710
#----------------------------------------------------------------------
from html import escape as html_escape
import cgi, sys, time, cdrcgi
import re
from cdrapi import db

class CgiQuery:

    def __init__(self, conn, system, script):
        "Should be invoked by the derived class's constructor."
        self.conn          = conn
        self.system        = system
        self.script        = script
        self.fields        = fields = cgi.FieldStorage()
        self.doWhat        = fields.getvalue("doWhat") or None
        self.queryName     = fields.getvalue("queries") or None
        self.newName       = fields.getvalue("newName") or None
        self.newQuery      = fields.getvalue("newQuery") or ""
        self.queryText     = fields.getvalue("queryText") or ""
        self.results       = ""
        if self.queryName:
            self.queryName = str(self.queryName, 'utf-8')
        if self.newName:
            self.newName = str(self.newName, 'utf-8')

    def run(self):
        if   self.doWhat == "addQuery"  and self.newName:   self.addQuery()
        elif self.doWhat == "saveQuery" and self.queryName: self.saveQuery()
        elif self.doWhat == "delQuery"  and self.queryName: self.delQuery()
        elif self.doWhat == "runQuery"  and self.queryText: self.runQuery()
        elif self.doWhat == "sendJson"  and self.queryText: self.sendJson()
        elif self.doWhat == "createSS"  and self.queryText:
            self.createSS()
        else:
            try:
                page = self.createPage()
            except Exception as e:
                self.bail(e)
            self.sendPage(page)

    def sendPage(self, page):
        "Display the specified page and exit."
        print("""\
Content-type: text/html; charset=utf-8
Cache-control: no-cache, must-revalidate
""")
        print(page.encode('utf-8'))
        sys.exit(0)

    def bail(self, message):
        "Display an error message and exit."
        sysName = html_escape(self.system)
        message = html_escape(message)
        self.sendPage("""\
<html>
 <head>
  <title>%s query failure</title>
 </head>
 <body>
  <h2>%s query failure</h2>
  <b>%s</b>
 </body>
</html>
""" % (sysName, sysName, message))

    def getQueriesHtml(self, queryKeys):
        "Create <option> elements for the cached queries."
        current = self.queryName and html_escape(self.queryName, 1) or None
        html = [""]
        for q in queryKeys:
            sel = q == current and " SELECTED" or ""
            html.append("""<option value = "%s"%s>%s</option>\n""" %
                        (q, sel, q))
        return "".join(html)

    def getQueriesDict(self, queries):
        html = [""]
        for q in list(queries.keys()):
            key = (q.replace("\r", "").replace("\n", "\\n").
                   replace("&amp;", "&").replace("&lt;", "<").
                   replace("&gt;", ">").replace("&quot;", '"'))
            if queries[q]:
                val = queries[q].replace("\r", "").replace("\n", "\\n")
            else:
                val = ""
            html.append('queries["%s"] = "%s";\n' %
                        (key.replace('"', '\\"'), val.replace('"', '\\"')))
        return "".join(html)

    def addQuery(self):
        "Default implementation.  Override as appropriate."
        try:
            cursor = self.conn.cursor()
            cursor.execute("INSERT INTO query (name, value) VALUES (?, ?)",
                           (self.newName, self.newQuery))
            self.conn.commit()
            self.queryName = self.newName
            self.queryText = self.newQuery
        except Exception as info:
            self.bail("Failure adding query: %s" % html_escape(str(info)))

    def saveQuery(self):
        "Default implementation.  Override as appropriate."
        try:
            cursor = self.conn.cursor()
            cursor.execute("UPDATE query SET value = ? WHERE name = ?",
                           (self.queryText, self.queryName))
            self.conn.commit()
        except Exception as info:
            self.bail("Failure saving query: %s" % html_escape(str(info)))

    def delQuery(self):
        "Default implementation.  Override as appropriate."
        try:
            cursor = self.conn.cursor()
            cursor.execute("DELETE FROM query WHERE name = ?", self.queryName)
            self.conn.commit()
        except Exception as info:
            self.bail("Failure deleting query: %s" % html_escape(str(info)))

    def getQueries(self):
        "Default implementation.  Override as appropriate."
        try:
            cursor = self.conn.cursor()
            cursor.execute("SELECT name, value FROM query")
            queries = {}
            for row in cursor.fetchall():
                queries[html_escape(row[0], 1)] = row[1]
            return queries
        except:
            raise
            self.bail("Failure loading cached queries")

    def sendJson(self):
        try:
            cursor = self.conn.cursor()
            start = time.time()
            cursor.execute(self.queryText)
            elapsed = time.time() - start
            if not cursor.description:
                self.bail("No query results")
            rows = [list(row) for row in cursor.fetchall()]
            payload = dict(columns=cursor.description, rows=rows)
        except Exception as e:
            args = html_escape(self.queryText), html_escape(e)
            self.bail("Failure executing query:\n{}\n{}".format(args))
        try:
            print("Content-type: application/json")
            print()
            print(json.dumps(payload, default=str, indent=2))
        except Exception as e:
            self.bail(f"Failure serializing json results: {e}")

    def runQuery(self):
        try:
            cursor = self.conn.cursor()
            start = time.time()
            cursor.execute(self.queryText)
            elapsed = time.time() - start
            html = ["<table border = '0' cellspacing = '1' cellpadding = '1'>"]
            html.append("<tr>\n")
            if not cursor.description:
                self.bail('No query results')
            for col in cursor.description:
                col = col and html_escape(col[0]) or "&nbsp;"
                html.append("<th>%s</th>\n" % col)
            html.append("</tr>\n")
            row = cursor.fetchone()
            classes = ('odd', 'even')
            rowNum = 1
            while row:
                cls = classes[rowNum % 2]
                rowNum += 1
                html.append("<tr>\n")
                for col in row:
                    if col is None:
                        val = "&nbsp;"
                    else:
                        val = html_escape("%s" % col) or "&nbsp;"
                    html.append("<td valign='top' class='%s'>%s</td>\n" %
                                (cls, val))
                html.append("</tr>\n")
                row = cursor.fetchone()
            html.append("""\
  <tr>
   <th class='total' colspan='99'>%d row(s) retrieved (%.3f seconds)</th>
  </tr>
""" % (rowNum - 1, elapsed))
            html.append("</table>\n")
            self.results = "".join(html)
        except Exception as e:
            args = html_escape(self.queryText), html_escape(e)
            self.bail("Failure executing query:\n%s\n%s" % args)

    def createSS(self):
        "Create Excel spreadsheet from query results"
        try:
            if sys.platform == "win32":
                import os, msvcrt
                msvcrt.setmode(sys.stdout.fileno(), os.O_BINARY)
            styles = cdrcgi.ExcelStyles()
            sheet = styles.add_sheet("Ad-hoc Query Report")
            cursor = self.conn.cursor()
            start = time.time()
            cursor.execute(self.queryText)
            secs = time.time() - start
            if not cursor.description:
                raise Exception('No query results')
            colNum = 1
            for i, info in enumerate(cursor.description):
                sheet.write(0, i, info and info[0] or "", styles.bold)
            values = cursor.fetchone()
            row = 1
            while values:
                for i, value in enumerate(values):
                    sheet.write(row, i, value)
                values = cursor.fetchone()
                row += 1
        except Exception as e:
            args = html_escape(self.queryText), html_escape(e)
            self.bail("Failure executing query:\n%s\n%s" % args)
        try:
            footer = "%d row(s) retrieved (%.3f seconds)" % (row - 1, secs)
            sheet.write_merge(row, row, 0, len(cursor.description) - 1, footer)
            now = time.strftime("%Y%m%d%H%M%S")
            if self.queryName:
                name = self.queryName.lower().replace(" ", "_")
                name = re.sub("[^0-9a-z_-]*", "", name)
            else:
                name = "ad-hoc-query"
            name = "{}-{}.xls".format(name, now)
            print("Content-type: application/vnd.ms-excel")
            print("Content-Disposition: attachment; filename={}".format(name))
            print()
            styles.book.save(sys.stdout)
        except Exception as e:
            self.bail("Failure generating spreadsheet: %s" % e)

    def createPage(self):
        queries     = self.getQueries()
        queryKeys   = sorted(queries.keys())
        queriesHtml = self.getQueriesHtml(queryKeys)
        queriesDict = self.getQueriesDict(queries)

        # If we don't already have a selected query, select the first one.
        #if queryKeys and not self.queryText and not self.queryName:
        #    queryName = queryKeys[0]
        #    if queries.has_key(queryName):
        #        self.queryText = html_escape(queries[queryName])
        html = """\
<html>
 <head>
  <title>%s Query Interface</title>
  <meta http-equiv="Pragma" content="no-cache">
  <meta http-equiv="Content-Type" content="text/html;charset=utf-8" />
  <style type = 'text/css'>
   th { background: olive; color: white; }
   th.total { background: olive; color: white; font-weight: bold; }
   td.odd { font-size: 10pt; background: beige; color: black;
            font-family: Arial; }
   td.even { font-size: 10pt; background: wheat; color: black;
            font-family: Arial; }
   option { background: beige; color: black; }
   select { background: beige; color: black; }
   textarea { background: beige; color: black; }
  </style>
  <script language = 'JavaScript'><!--
    var queries = new Object();
%s
    function selQuery() {
        var frm = document.forms[0];
        var sel = frm.queries.selectedIndex;
        if (sel >= 0) {
            var key = frm.queries[sel].value;
            frm.queryText.value = queries[key];
        }
    }

    function addNewQuery() {
        addQuery("");
    }

    function runQuery() {
        var frm = document.forms[0];
        frm.doWhat.value = "runQuery";
        frm.submit();
    }

    function sendJson() {
        var frm = document.forms[0];
        frm.doWhat.value = "sendJson";
        frm.submit();
    }

    function excel() {
        var frm = document.forms[0];
        frm.doWhat.value = "createSS";
        frm.submit();
    }

    function saveQuery() {
        var frm = document.forms[0];
        var sel = frm.queries.selectedIndex;
        if (sel >= 0) {
            frm.doWhat.value = "saveQuery";
            frm.submit();
        }
    }

    function delQuery() {
        var frm = document.forms[0];
        var sel = frm.queries.selectedIndex;
        if (sel >= 0) {
            frm.doWhat.value = "delQuery";
            frm.submit();
        }
    }

    function addQuery(value) {
        var frm = document.forms[0];
        var name = prompt("Name for new query?", "");
        if (name) {
            if (queries[name]) {
                alert("Query name '" + name + "' already used.");
                return;
            }
            frm.doWhat.value = "addQuery";
            frm.newName.value = name;
            frm.newQuery.value = value;
            frm.submit();
        }
    }

    function cloneQuery() { addQuery(document.forms[0].queryText.value); }

   // -->
  </script>
 </head>
 <body bgcolor='#eeeeee'>
  <form action='%s' method = 'POST'>
   <table border='0'>
    <tr>
     <th>Queries</th>
     <th>Query</th>
    </tr>
    <tr>
     <td valign = 'center'>
      <select name = 'queries' size = '10' onChange = 'selQuery();'>
       %s
      </select>
     </td>
     <td valign = 'center'>
      <textarea name = 'queryText' rows = '10' cols = '60'>%s</textarea>
     </td>
    </tr>
    <tr>
     <td colspan = '2' align = 'center'>
      <input type="button" onClick="runQuery()" value="Submit">&nbsp;
      <input type="button" onClick="excel()" value="Excel">&nbsp;
      <input type="button" onClick="sendJson()" value="JSON">&nbsp;
      <input type="button" onClick="saveQuery()" value="Save">&nbsp;
      <input type="button" onClick="delQuery()" value="Delete">&nbsp;
      <input type="button" onClick="addNewQuery()" value="New">&nbsp;
      <input type="button" onClick="cloneQuery()" value="Clone">
      <input type="hidden" name="doWhat" value="nothing">
      <input type="hidden" name="newName" value="">
      <input type="hidden" name="newQuery" value="">
      <input type="hidden" name="pageId" value="%f">
     </td>
    </tr>
   </table>
  </form>
%s
 </body>
</html>
""" % (self.system, queriesDict, self.script, queriesHtml,
       self.queryText.replace("\r", ""), time.clock(), self.results)
        return html
