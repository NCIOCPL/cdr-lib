#!/usr/bin/python
#----------------------------------------------------------------------
#
# $Id: CgiQuery.py,v 1.6 2005-01-24 23:03:22 bkline Exp $
#
# Base class for CGI database query interface.
#
# $Log: not supported by cvs2svn $
# Revision 1.5  2003/09/11 12:34:27  bkline
# Made it possible to override the default timeout for queries.
#
# Revision 1.4  2003/07/29 13:10:26  bkline
# Removed redundant cgi escaping.
#
# Revision 1.3  2003/04/09 21:57:40  bkline
# Fixed encoding bug; fixed exception bug.
#
# Revision 1.2  2003/03/04 22:52:36  bkline
# Added test to make sure object was not null before doing string
# replacement.
#
# Revision 1.1  2002/12/10 13:35:58  bkline
# Base class for ad-hoc SQL query tool with WEB interface.
#
#----------------------------------------------------------------------
import cgi, re, sys, time, cdrdb

unicodePattern = re.compile(u"([\u0080-\uffff])")
def encode(unicodeString):
    return re.sub(unicodePattern,
                  lambda match: u"&#x%X;" % ord(match.group(0)[0]),
                  unicodeString).encode('ascii')
class CgiQuery:

    def __init__(self, conn, system, script, timeout = 30):
        "Should be invoked by the derived class's constructor."
        self.conn          = conn
        self.system        = system
        self.script        = script
        self.timeout       = timeout
        self.fields        = fields = cgi.FieldStorage()
        self.doWhat        = fields and fields.getvalue("doWhat") or None
        self.queryName     = fields and fields.getvalue("queries") or None
        self.newName       = fields and fields.getvalue("newName") or None
        self.newQuery      = fields and fields.getvalue("newQuery") or ""
        self.queryText     = fields and fields.getvalue("queryText") or ""
        self.results       = ""
        self.decodePattern = re.compile(u"([\u0080-\uffff])")

    def run(self):
        if   self.doWhat == "addQuery"  and self.newName:   self.addQuery()
        elif self.doWhat == "saveQuery" and self.queryName: self.saveQuery()
        elif self.doWhat == "delQuery"  and self.queryName: self.delQuery()
        elif self.doWhat == "runQuery"  and self.queryText: self.runQuery()
        self.sendPage(self.createPage())

    def sendPage(self, page):
        "Display the specified page and exit."
        page = re.sub(self.decodePattern,
                      lambda match: u"&#x%X;" % ord(match.group(0)[0]),
                      page)
        print """\
Content-type: text/html
Cache-control: no-cache, must-revalidate

%s""" % page.encode('latin-1')
        sys.exit(0)
    
    def bail(self, message):
        "Display an error message and exit."
        sysName = cgi.escape(self.system)
        message = cgi.escape(message)
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
        current = self.queryName and cgi.escape(self.queryName, 1) or None
        html = ""
        for q in queryKeys:
            sel = q == current and " SELECTED" or ""
            html += """<option value = "%s"%s>%s</option>\n""" % (q, sel, q)
        return html

    def getQueriesDict(self, queries):
        html = ""
        for q in queries.keys():
            key = q.replace("\r", "").replace("\n", "\\n")
            if queries[q]:
                val = queries[q].replace("\r", "").replace("\n", "\\n")
            else:
                val = ""
            html += 'queries["%s"] = "%s";\n' % (key, val.replace('"', '\\"'))
        return html

    def dbCleanString(self, str):
        "Default implementation.  Override as appropriate."
        return str.replace("'", "''").replace("\\", "\\\\")

    def addQuery(self):
        "Default implementation.  Override as appropriate."
        try:
            name = self.dbCleanString(self.newName)
            val = self.dbCleanString(self.newQuery)
            q = "INSERT INTO query(name, value) VALUES('%s', '%s')" % (name, 
                                                                       val)
            cursor = self.conn.cursor()
            cursor.execute(q)
            self.conn.commit()
            self.queryName = self.newName
            self.queryText = self.newQuery
        except StandardError, info:
            self.bail("Failure adding query: %s" % cgi.escape(str(info)))

    def saveQuery(self):
        "Default implementation.  Override as appropriate."
        try:
            name = self.dbCleanString(self.queryName)
            val = self.dbCleanString(self.queryText)
            q = "UPDATE query SET value = '%s' WHERE name = '%s'" % (val, name)
            cursor = self.conn.cursor()
            cursor.execute(q)
            self.conn.commit()
        except StandardError, info:
            self.bail("Failure adding query: %s" % cgi.escape(str(info)))

    def delQuery(self):
        "Default implementation.  Override as appropriate."
        try:
            name = self.dbCleanString(self.queryName)
            q = "DELETE FROM query WHERE name = '%s'" % name
            cursor = self.conn.cursor()
            cursor.execute(q)
            self.conn.commit()
        except StandardError, info:
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
            cursor.execute(self.queryText, timeout = self.timeout)
            html = "<table border = '0' cellspacing = '1' cellpadding = '1'>"
            html += "<tr>\n"
            if not cursor.description:
                self.bail('No query results')
            for col in cursor.description:
                col = col and cgi.escape(col[0]) or "&nbsp;"
                html += "<th>%s</th>\n" % col
            html += "</tr>\n"
            row = cursor.fetchone()
            classes = ('odd', 'even')
            rowNum = 1
            while row:
                cls = classes[rowNum % 2]
                rowNum += 1
                html += "<tr>\n"
                for col in row:
                    if type(col) == type(u""):
                        col = encode(col)
                    val = col and str(col) or "&nbsp;"
                    html += "<td valign='top' class='%s'>%s</td>\n" % (cls,
                                                                       val)
                html += "</tr>\n"
                row = cursor.fetchone()
            html += "</table>"
            self.results = html
        except cdrdb.Error, info:
            self.bail("Failure executing query:\n%s\n%s" % (
                cgi.escape(self.queryText),
                cgi.escape(info[1][0])))
        
    def createPage(self):
        queries     = self.getQueries()
        queryKeys   = queries.keys()
        queryKeys.sort()
        queriesHtml = self.getQueriesHtml(queryKeys)
        queriesDict = self.getQueriesDict(queries)

        # If we don't already have a selected query, select the first one.
        if queryKeys and not self.queryText and not self.queryName:
            queryName = queryKeys[0]
            if queries.has_key(queryName):
                queryText = cgi.escape(queries[queryName])
        html = """\
<html>
 <head>
  <title>%s Query Interface</title>
  <meta http-equiv="Pragma" content="no-cache">
  <style type = 'text/css'>
   th { background: olive; color: white; }
   td.odd { font-size: 10pt; background: beige; color: black;
            font-family: Arial; }
   td.even { font-size: 10pt; background: wheat; color: black;
            font-family: Arial; }
   option { background: beige; color: black; }
   select { background: beige; color: black; }
   textarea { background: beige; color: black; }
  </style>
  <script language = 'JavaScript'>
   <!--
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

   -->
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
     </td>
    </tr>
    <tr>
     <td colspan = '2' align = 'center'>
      <input type = 'button' onClick = 'runQuery()' value = 'Submit' />&nbsp;
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
 <HEAD>
  <META HTTP-EQUIV="PRAGMA" CONTENT="NO-CACHE">
 </HEAD>
</html>
""" % (self.system, queriesDict, self.script, queriesHtml, 
       self.queryText.replace("\r", ""), time.clock(), self.results)
        return html
