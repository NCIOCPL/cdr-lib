#!/usr/bin/python
#----------------------------------------------------------------------
# Base class for CGI database query interface.
# BZIssue::4710
#----------------------------------------------------------------------
import cgi, sys, time, cdrdb, cdrcgi

class CgiQuery:

    def __init__(self, conn, system, script, timeout = 30):
        "Should be invoked by the derived class's constructor."
        self.conn          = conn
        self.system        = system
        self.script        = script
        self.timeout       = timeout
        self.fields        = fields = cgi.FieldStorage()
        self.doWhat        = fields.getvalue("doWhat") or None
        self.queryName     = fields.getvalue("queries") or None
        self.newName       = fields.getvalue("newName") or None
        self.newQuery      = fields.getvalue("newQuery") or ""
        self.queryText     = fields.getvalue("queryText") or ""
        self.results       = ""
        if self.queryName:
            self.queryName = unicode(self.queryName, 'utf-8')
        if self.newName:
            self.newName = unicode(self.newName, 'utf-8')

    def run(self):
        if   self.doWhat == "addQuery"  and self.newName:   self.addQuery()
        elif self.doWhat == "saveQuery" and self.queryName: self.saveQuery()
        elif self.doWhat == "delQuery"  and self.queryName: self.delQuery()
        elif self.doWhat == "runQuery"  and self.queryText: self.runQuery()
        elif self.doWhat == "createSS"  and self.queryText:
            self.createSS()
            return
        try:
            page = self.createPage()
        except Exception, e:
            self.bail(e)
        self.sendPage(page)

    def sendPage(self, page):
        "Display the specified page and exit."
        print """\
Content-type: text/html; charset=utf-8
Cache-control: no-cache, must-revalidate
"""
        print page.encode('utf-8')
        sys.exit(0)

    def bail(self, message):
        "Display an error message and exit."
        sysName = cgi.escape(self.system)
        message = cgi.escape(message)
        self.sendPage(u"""\
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
        current = self.queryName and cgi.escape(self.queryName, 1) or None
        html = [u""]
        for q in queryKeys:
            sel = q == current and u" SELECTED" or u""
            html.append(u"""<option value = "%s"%s>%s</option>\n""" %
                        (q, sel, q))
        return u"".join(html)

    def getQueriesDict(self, queries):
        html = [u""]
        for q in queries.keys():
            key = (q.replace(u"\r", u"").replace(u"\n", u"\\n").
                   replace(u"&amp;", u"&").replace(u"&lt;", u"<").
                   replace(u"&gt;", u">").replace(u"&quot;", u'"'))
            if queries[q]:
                val = queries[q].replace(u"\r", u"").replace(u"\n", u"\\n")
            else:
                val = u""
            html.append(u'queries["%s"] = "%s";\n' %
                        (key.replace(u'"', u'\\"'), val.replace(u'"', u'\\"')))
        return u"".join(html)

    def addQuery(self):
        "Default implementation.  Override as appropriate."
        try:
            cursor = self.conn.cursor()
            cursor.execute("INSERT INTO query (name, value) VALUES (?, ?)",
                           (self.newName, self.newQuery))
            self.conn.commit()
            self.queryName = self.newName
            self.queryText = self.newQuery
        except Exception, info:
            self.bail("Failure adding query: %s" % cgi.escape(str(info)))

    def saveQuery(self):
        "Default implementation.  Override as appropriate."
        try:
            cursor = self.conn.cursor()
            cursor.execute("UPDATE query SET value = ? WHERE name = ?",
                           (self.queryText, self.queryName))
            self.conn.commit()
        except Exception, info:
            self.bail("Failure saving query: %s" % cgi.escape(str(info)))

    def delQuery(self):
        "Default implementation.  Override as appropriate."
        try:
            cursor = self.conn.cursor()
            cursor.execute("DELETE FROM query WHERE name = ?", self.queryName)
            self.conn.commit()
        except Exception, info:
            self.bail("Failure deleting query: %s" % cgi.escape(str(info)))

    def getQueries(self):
        "Default implementation.  Override as appropriate."
        try:
            cursor = self.conn.cursor()
            cursor.execute("SELECT name, value FROM query")
            queries = {}
            for row in cursor.fetchall():
                queries[cgi.escape(row[0], 1)] = row[1]
            return queries
        except:
            raise
            self.bail("Failure loading cached queries")

    def runQuery(self):
        try:
            cursor = self.conn.cursor()
            start = time.time()
            cursor.execute(self.queryText, timeout = self.timeout)
            elapsed = time.time() - start
            html = [u"<table border = '0' cellspacing = '1' cellpadding = '1'>"]
            html.append(u"<tr>\n")
            if not cursor.description:
                self.bail('No query results')
            for col in cursor.description:
                col = col and cgi.escape(col[0]) or u"&nbsp;"
                html.append(u"<th>%s</th>\n" % col)
            html.append(u"</tr>\n")
            row = cursor.fetchone()
            classes = (u'odd', u'even')
            rowNum = 1
            while row:
                cls = classes[rowNum % 2]
                rowNum += 1
                html.append(u"<tr>\n")
                for col in row:
                    val = col and cgi.escape(u"%s" % col) or u"&nbsp;"
                    html.append(u"<td valign='top' class='%s'>%s</td>\n" %
                                (cls, val))
                html.append(u"</tr>\n")
                row = cursor.fetchone()
            html.append(u"""\
  <tr>
   <th class='total' colspan='99'>%d row(s) retrieved (%.3f seconds)</th>
  </tr>
""" % (rowNum - 1, elapsed))
            html.append(u"</table>\n")
            self.results = u"".join(html)
        except cdrdb.Error, info:
            self.bail("Failure executing query:\n%s\n%s" % (
                cgi.escape(self.queryText),
                cgi.escape(info[1][0])))

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
            cursor.execute(self.queryText, timeout = self.timeout)
            secs = time.time() - start
            if not cursor.description:
                raise Exception('No query results')
            colNum = 1
            for i, info in enumerate(cursor.description):
                sheet.write(0, i, info and info[0] or u"", styles.bold)
            values = cursor.fetchone()
            row = 1
            while values:
                for i, value in enumerate(values):
                    sheet.write(row, i, value)
                values = cursor.fetchone()
                row += 1
            footer = u"%d row(s) retrieved (%.3f seconds)" % (row - 1, secs)
            sheet.write_merge(row, row, 0, len(cursor.description) - 1, footer)
            now = time.strftime("%Y%m%d%H%M%S")
            print "Content-type: application/vnd.ms-excel"
            print "Content-Disposition: attachment; filename=sdlm-%s.xls" % now
            print
            styles.book.save(sys.stdout)
        except cdrdb.Error, info:
            self.bail("Failure executing query:\n%s\n%s" % (
                cgi.escape(self.queryText),
                cgi.escape(info[1][0])))
        except Exception, e:
            self.bail("Failure generating spreadsheet: %s" % e)

    def createPage(self):
        queries     = self.getQueries()
        queryKeys   = queries.keys()
        queryKeys.sort()
        queriesHtml = self.getQueriesHtml(queryKeys)
        queriesDict = self.getQueriesDict(queries)

        # If we don't already have a selected query, select the first one.
        #if queryKeys and not self.queryText and not self.queryName:
        #    queryName = queryKeys[0]
        #    if queries.has_key(queryName):
        #        self.queryText = cgi.escape(queries[queryName])
        html = u"""\
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
      <input type = 'button' onClick = 'runQuery()' value = 'Submit' />&nbsp;
      <input type = 'button' onClick = 'excel()' value = 'Excel' />&nbsp;
      <input type = 'button' onClick = 'saveQuery()' value = 'Save' />&nbsp;
      <input type = 'button' onClick = 'delQuery()' value = 'Delete' />&nbsp;
      <input type = 'button' onClick = 'addNewQuery()' value = 'New' />&nbsp;
      <input type = 'button' onClick = 'cloneQuery()' value = 'Clone' />
      <input type = 'hidden' name = 'doWhat' value = 'nothing' />
      <input type = 'hidden' name = 'newName' value = '' />
      <input type = 'hidden' name = 'newQuery' value = '' />
      <input type = 'hidden' name = 'pageId' value = '%f' />
     </td>
    </tr>
   </table>
  </form>
%s
 </body>
</html>
""" % (self.system, queriesDict, self.script, queriesHtml,
       self.queryText.replace(u"\r", u""), time.clock(), self.results)
        return html
