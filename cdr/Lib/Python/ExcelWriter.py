#----------------------------------------------------------------------
#
# $Id: ExcelWriter.py,v 1.6 2006-10-28 14:33:05 bkline Exp $
#
# Generates Excel workbooks using 2003 XML format.
#
# $Log: not supported by cvs2svn $
# Revision 1.5  2006/05/04 15:50:31  bkline
# Replaced newlines with character entities.
#
# Revision 1.4  2005/11/22 14:43:11  bkline
# Modified test again for writing cell values (avoid writing empty
# strings for DateTime values).
#
# Revision 1.3  2005/11/22 14:06:35  bkline
# Changed test of cell value from if cell.value to if ... is not None
# for output.
#
# Revision 1.2  2005/11/10 14:55:50  bkline
# Fixed indentation bug in output.
#
# Revision 1.1  2005/10/27 21:31:13  bkline
# Module to generate Excel workbooks using 2002/2003 XML format.
#
#----------------------------------------------------------------------
import xml.sax.saxutils, sys, time, tempfile, os

def fix(me):
    if type(me) in (str, unicode):
        return xml.sax.saxutils.escape(me).replace('\n', '&#10;')
    return unicode(me)

def quoteattr(a):
    return xml.sax.saxutils.quoteattr(a)

class Workbook:
    def __init__(self, author = None, company = None):
        self.props  = Properties(author, company)
        self.styles = []
        self.sheets = []
        self.addStyle('Default', 'Normal', 
                      alignment = Alignment(vertical = 'Bottom'))
    def addWorksheet(self, name, style = None, height = None,
                     frozenRows = None, frozenCols = None):
        sheet = Worksheet(name, style, height, frozenRows, frozenCols)
        self.sheets.append(sheet)
        return sheet
    def addStyle(self, styleId = None, name = None, alignment = None,
                 borders = None, font = None, interior = None,
                 numFormat = None):
        style = Style(styleId, name, alignment, borders, font, interior,
                      numFormat)
        self.styles.append(style)
        return style.id

    def write(self, fobj, asXls = False):
        if asXls:
            tempDir  = tempfile.gettempdir()
            baseName = os.path.join(tempfile.gettempdir(),
                                    "%s-%s" % (os.getpid(), time.time()))
            xmlName  = "%s.xml" % baseName
            xlsName  = "%s.xls" % baseName
            xmlFile  = file(xmlName, "w")
            self.__write(xmlFile)
            xmlFile.close()
            script = "d:\\cdr\\lib\\Perl\\xml2xls.pl"
            perl = "D:\\Perl\\bin\\perl.EXE"
            command = "%s %s %s %s 2>&1" % (perl, script, xmlName, xlsName)
            commandStream = os.popen('%s 2>&1' % command)
            output = commandStream.read()
            code = commandStream.close()
            if code:
                raise Exception("ExcelWriter.write(): %d: %s" % (code, output))
            xlsFile = file(xlsName, "rb")
            xlsData = xlsFile.read()
            xlsFile.close()
            fobj.write(xlsData)
        else:
            self.__write(fobj)
            
            
    def __write(self, fobj):
        fobj.write("""\
<?xml version="1.0" encoding="utf-8"?>
<?mso-application progid="Excel.Sheet"?>
<Workbook          xmlns = "urn:schemas-microsoft-com:office:spreadsheet"
                 xmlns:x = "urn:schemas-microsoft-com:office:excel"
                xmlns:ss = "urn:schemas-microsoft-com:office:spreadsheet">
""")
        self.props.write(fobj)
        if self.styles:
            fobj.write("""\
 <Styles>
""")
            for style in self.styles:
                style.write(fobj)
            fobj.write("""\
 </Styles>
""")
        for sheet in self.sheets:
            sheet.write(fobj)
        fobj.write("""\
</Workbook>
""")

class Properties:
    def __init__(self, author = None, company = None):
        self.author  = author or 'CDR'
        self.company = company or 'CIPS'
    def write(self, fobj):
        u = u"""\
 <DocumentProperties>
  <Author>%s</Author>
  <Created>%s</Created>
  <Company>%s</Company>
 </DocumentProperties>
""" % (fix(self.author),
       time.strftime('%Y-%m-%dT%H:%M:%SZ', time.gmtime()),
       fix(self.company))
        fobj.write(u.encode('utf-8'))

class Style:
    __nextId = 1
    def __getId(c):
        nextId = "s%d" % c.__nextId
        c.__nextId += 1
        return nextId
    __getId = classmethod(__getId)
    def __init__(self, styleId = None, name = None, alignment = None,
                 borders = None, font = None, interior = None,
                 numFormat = None):
        self.id        = styleId or Style.__getId()
        self.name      = name
        self.alignment = alignment
        self.borders   = borders
        self.font      = font
        self.interior  = interior
        self.numFormat = numFormat
    def write(self, fobj):
        u = [u'  <Style ss:ID=%s' % quoteattr(self.id)]
        if self.name:
            u.append(u' ss:Name=%s' % quoteattr(self.name))
        u.append(u'>\n')
        fobj.write(u"".join(u).encode('utf-8'))
        if self.alignment:
            self.alignment.write(fobj)
        if self.borders:
            self.borders.write(fobj)
        if self.font:
            self.font.write(fobj)
        if self.interior:
            self.interior.write(fobj)
        if self.numFormat:
            u = (u"   <NumberFormat ss:Format=%s/>\n" %
                 quoteattr(self.numFormat))
            fobj.write(u.encode('utf-8'))
        fobj.write('  </Style>\n')

class Alignment:
    def __init__(self, horizontal = None, vertical = None, wrap = None):
        self.horizontal = horizontal
        self.vertical   = vertical
        self.wrap       = wrap
    def write(self, fobj):
        fobj.write('   <Alignment')
        if self.horizontal:
            fobj.write(' ss:Horizontal="%s"' % self.horizontal)
        if self.vertical:
            fobj.write(' ss:Vertical="%s"' % self.vertical)
        if self.wrap is not None:
            fobj.write(' ss:WrapText="%s"' % (self.wrap and "1" or "0"))
        fobj.write('/>\n')

class Borders:
    def __init__(self, bottom = None, left = None, right = None, top = None):
        self.bottom = bottom
        self.left   = left
        self.right  = right
        self.top    = top
    def write(self, fobj):
        fobj.write('   <Borders>\n')
        if self.bottom:
            self.bottom.write(fobj, 'Bottom')
        if self.left:
            self.left.write(fobj, 'Left')
        if self.right:
            self.right.write(fobj, 'Right')
        if self.top:
            self.top.write(fobj, 'Top')
        fobj.write('   </Borders>\n')

class Border:
    def __init__(self, lineStyle = 'Continuous', weight = '1'):
        self.lineStyle = lineStyle
        self.weight    = weight
    def write(self, fobj, position):
        fobj.write('    <Border ss:Position="%s" ss:LineStyle="%s"'
                   ' ss:Weight="%s"/>\n' % (position,
                                            self.lineStyle,
                                            self.weight))
class Font:
    def __init__(self, color = None, underline = None, name = None,
                 family = None, bold = None, italic = None, size = None):
        self.color     = color
        self.underline = underline
        self.name      = name
        self.family    = family
        self.bold      = bold
        self.italic    = italic
        self.size      = size
    def write(self, fobj):
        fobj.write('   <Font')
        if self.color:
            fobj.write(' ss:Color="%s"' % self.color)
        if self.underline:
            fobj.write(' ss:Underline="%s"' % self.underline)
        if self.name:
            u = u' ss:FontName="%s"' % self.name
            fobj.write(u.encode('utf-8'))
        if self.family:
            u = u' x:Family="%s"' % self.family
            fobj.write(u.encode('utf-8'))
        if self.bold is not None:
            fobj.write(' ss:Bold="%s"' % (self.bold and "1" or "0"))
        if self.italic is not None:
            fobj.write(' ss:Italic="%s"' % (self.italic and "1" or "0"))
        if self.size:
            fobj.write(' ss:Size="%s"' % self.size)
        fobj.write('/>\n')

class Interior:
    def __init__(self, color, pattern = 'Solid'):
        self.color   = color
        self.pattern = pattern
    def write(self, fobj):
        fobj.write('   <Interior ss:Color="%s" ss:Pattern="%s"/>\n' %
                   (self.color, self.pattern))

class Worksheet:
    def __init__(self, name, style = None, height = None, frozenRows = None,
                 frozenCols = None):
        self.name       = name
        self.rows       = {}
        self.cols       = {}
        self.style      = style
        self.height     = height
        self.frozenRows = frozenRows
        self.frozenCols = frozenCols
        
    def addRow(self, index, style = None, height = None):
        row = Row(index, style, height)
        self.rows[index] = row
        return row
    def addCol(self, index, width, auto = None):
        col = Column(index, width, auto)
        self.cols[index] = col
        return col
    def write(self, fobj):
        u = u" <Worksheet ss:Name=%s" % quoteattr(self.name)
        fobj.write(u.encode('utf-8'))
        if self.rows or self.cols or self.style:
            fobj.write(">\n  <Table")
            if self.style:
                u = u" ss:StyleID=%s" % quoteattr(self.style)
                fobj.write(u.encode('utf-8'))
            if self.height:
                fobj.write(' ss:DefaultRowHeight="%s"' % self.height)
            fobj.write('>\n')
            if self.cols:
                keys = self.cols.keys()
                keys.sort()
                index = 1
                for key in keys:
                    col = self.cols[key]
                    col.write(fobj, index != col.index and col.index or None)
                    index = col.index + 1
            if self.rows:
                keys = self.rows.keys()
                keys.sort()
                index = 1
                for key in keys:
                    row = self.rows[key]
                    row.write(fobj, index != row.index and row.index or None)
                    index = row.index + 1
            fobj.write("  </Table>\n")
            if self.frozenRows or self.frozenCols:
                fobj.write("""\
  <WorksheetOptions xmlns="urn:schemas-microsoft-com:office:excel">
   <Unsynced/>
   <FreezePanes/>
   <FrozenNoSplit/>
""")

                if self.frozenRows and self.frozenCols:
                    panes = (3, 1, 2, 0)
                elif self.frozenRows:
                    panes = (3, 2)
                else:
                    panes = (3, 1)
                if self.frozenRows:
                    fobj.write("""\
   <SplitHorizontal>%d</SplitHorizontal>
   <TopRowBottomPane>%d</TopRowBottomPane>
""" % (self.frozenRows, self.frozenRows))
                if self.frozenCols:
                    fobj.write("""\
   <SplitVertical>%d</SplitVertical>
   <LeftColumnRightPane>%d</LeftColumnRightPane>
""" % (self.frozenCols, self.frozenCols))
                fobj.write("""\
   <ActivePane>%d</ActivePane>
   <Panes>
""" % panes[-1])
                for pane in panes:
                    fobj.write("""\
    <Pane>
     <Number>%d</Number>
    </Pane>
""" % pane)
                fobj.write("""\
   </Panes>
  </WorksheetOptions>
""")
            fobj.write(" </Worksheet>\n")
        else:
            fobj.write("/>\n")

class Column:
    def __init__(self, index, width, auto = None):
        self.index = index
        self.width = width
        self.auto  = auto
    def write(self, fobj, index = None):
        fobj.write('   <Column')
        if index:
            fobj.write(' ss:Index="%d"' % index)
        fobj.write(' ss:Width="%s"' % self.width)
        if self.auto is not None:
            fobj.write(' ss:AutoFitWidth="%s"' % (self.auto and "1" or "0"))
        fobj.write('/>\n')

class Row:
    def __init__(self, index, style = None, height = None):
        self.index  = index
        self.style  = style
        self.cells  = {}
        self.height = height
    def addCell(self, index, value = None, dataType = 'String', style = None,
                mergeAcross = None, mergeDown = None, href = None,
                tooltip = None, formula = None):
        cell = Cell(index, value, dataType, style, mergeAcross, mergeDown,
                    href, tooltip, formula)
        self.cells[index] = cell
        #sys.stderr.write("row %d col %d: num cells is %d\n" %
        #                 (self.index, index, len(self.cells)))
        return cell
    def write(self, fobj, index = None):
        #sys.stderr.write("write(%d): num cells is %d\n" %
        #                 (self.index, len(self.cells)))
        fobj.write('   <Row')
        if index:
            fobj.write(' ss:Index="%d"' % self.index)
        if self.style:
            u = u' ss:StyleID=%s' % quoteattr(self.style)
            fobj.write(u.encode('utf-8'))
        if self.height:
            fobj.write(' ss:AutoFitHeight="0" ss:Height="%s"' % self.height)
        if self.cells:
            fobj.write('>\n')
            keys = self.cells.keys()
            keys.sort()
            index = 1
            for key in keys:
                cell = self.cells[key]
                if cell.index == index:
                    cell.write(fobj)
                else:
                    cell.write(fobj, cell.index)
                index = cell.index + 1
            fobj.write('   </Row>\n')
        else:
            fobj.write('/>\n')
            
class Cell:
    def __init__(self, index, value = None, dataType = 'String', style = None,
                 mergeAcross = None, mergeDown = None, href = None,
                 tooltip = None, formula = None):
        self.index       = index
        self.value       = value
        self.dataType    = dataType
        self.mergeAcross = mergeAcross
        self.mergeDown   = mergeDown
        self.style       = style
        self.href        = href
        self.formula     = formula
        self.tooltip     = tooltip
    def write(self, fobj, index = None):
        u = [u'    <Cell']
        if index:
            u.append(u' ss:Index="%d"' % index)
        if self.style:
            u.append(u" ss:StyleID=%s" % quoteattr(self.style))
        if self.href:
            u.append(u" ss:HRef=%s" % quoteattr(self.href))
        if self.tooltip:
            u.append(u" x:HRefScreenTip=%s" % quoteattr(self.tooltip))
        if self.mergeAcross:
            u.append(u' ss:MergeAcross="%d"' % self.mergeAcross)
        if self.mergeDown:
            u.append(u' ss:MergeDown="%d"' % self.mergeDown)
        if self.formula:
            u.append(u" ss:Formula=%s" % quoteattr(self.formula))
        if self.value or self.value == 0:
            u.append(u'><Data ss:Type="%s">%s</Data></Cell>\n' %
                     (self.dataType, fix(self.value)))
        else:
            u.append(u'/>\n')
        fobj.write(u"".join(u).encode('utf-8'))

class StringSink:
    def __init__(self, s = None):
        self.__pieces = s and [s] or []
    def __repr__(self):
        return "".join(self.__pieces)
    def write(self, s):
        self.__pieces.append(s)
    def __getattr__(self, name):
        if name == 's':
            return "".join(self.__pieces)
        raise AttributeError

if __name__ == '__main__':
    wb = Workbook(u'Bob Kline', u'RK Systems')
    border = Border()
    borders = Borders(border, border, border, border)
    st1 = wb.addStyle(alignment = Alignment('Center', 'Bottom'),)
    st2 = wb.addStyle(name = 'Hyperlink', font = Font('#0000FF', 'Single'))
    st3 = wb.addStyle(font = Font('#FF0000'), interior = Interior('#FFFF00'))
    st4 = wb.addStyle(font = Font('#559955'))
    st5 = wb.addStyle(font = Font('#999999'), borders = borders)
    st6 = wb.addStyle(font = Font('#00FF00',
                                  name = 'Times New Roman', size = 12))
    ws = wb.addWorksheet('Sheet1', st6, 25, frozenRows = 1)
    ws.addCol(12, 100)
    row = ws.addRow(1)
    row.addCell(1, u'This is a banner\u2026', mergeAcross = 4, style = st1)
    row = ws.addRow(2)
    row.addCell(1, 3.14159, 'Number', style = st4)
    row.addCell(2, 3.1, 'Number', formula = '=A2+3.4', style = st3)
    row = ws.addRow(17, height = 40)
    row.addCell(7, 'foo', href = 'http://www.rksystems.com?foo=bar&x=y',
                style = st2, tooltip = 'Click me!')
    row.addCell(10, 'bar')
    row = ws.addRow(27)
    row.addCell(12, 666, 'Number', style = st5)
    wb.addWorksheet('Another Sheet')
    ss = StringSink()
    wb.write(ss)
    sys.stdout.write(ss.s)
    f = file("ExcelWriterTest.xls", "wb")
    wb.write(f, True)
    f.close()
