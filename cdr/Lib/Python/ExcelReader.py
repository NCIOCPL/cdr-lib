"""

    Module for extracting data from Excel 97+ workbooks.


    Summary

        import ExcelReader
        book = ExcelReader.Workbook('foo.xls')
        for sheet in book:
            for row in sheet.rows:
                for cell in row.cells:
                    rawValue = cell.val
                    formattedValue = str(cell)
                    rowNumber = cell.row
                    colNumber = cell.col
                    url = cell.hlink
                    ....
                    
        The loops above walk through all of the defined cells in the
        sheets, by iterating through the 'rows' sequence of each Sheet
        object, and the 'cells' sequence of each Row object.  As
        illustrated above, the Cell objects know their actual positions
        (row and column) in the sheet.  Positions are zero-based: the
        upper-left cell for each sheet is at row 0, column 0.  Cell
        objects have val members, which refer to the raw data contained
        in the cell, which may be a string (8-bit or Unicode), integer,
        or floating-point value.  The formatted string returned by the
        builtin str() function takes care of properly representing
        numbers which are intended to convey dates and/or times, and 
        converts the string to utf-8 encoding.  The url member of a 
        Cell object will be None or a Unicode string, depending on 
        whether a hyperlink URL has been associated with the cell.

        An alternate approach for iterating through the cells on a sheet
        starts at the top row of the sheet, regardless of whether there
        are any defined cells in that row, and visits all the rows up to
        and including the last row in the sheet which contains at least
        one defined cell.  If there are no defined cells in a sheet,
        this approach to iterating through the sheet will visit no rows.
        For each row visited, this iteration approach starts with the
        first column in the row, regardless of whether the cell at that
        position is defined, and visits all the cells in the row up to
        and including the last cell which is defined.  If no cells are
        defined for a given row, this approach to iterating through the
        row will visit no cells.  The syntax for this alternate
        iteration method omits the use of the 'rows' member of the Sheet
        object and the 'cells' member of the Row object, and instead
        iterates directly on the Sheet and Row objects themselves, as
        illustrated here:

            import ExcelReader
            book = ExcelReader.Workbook('bar.xls')
            for sheet in book:
                for row in sheet:
                    for cell in row:
                        ...

        It is also possible to select a worksheet either by its name or
        by its position in the sequence of worksheets:

            sheet = book["Glossary Term Phrases"]

                or

            sheet = book[0]

        Similarly, you can select a single row directly:

            # Get row 2 of the worksheet.
            row = sheet[1]

            # Get the second of the non-empty rows on the sheet.
            row = sheet.rows[1]

        Same with cells:

            # Get the cell at column 1:
            cell = row[0]

            # Get the first defined cell on the row.
            cell = row.cells[0]

        Finally, note that it is possible to generate an XML document
        encoded in a UTF-8 string from a workbook, using the toXml()
        method of the Workbook object, so that later on you can get
        to the data on a machine which does does not have this module,
        but which does have the standard XML parsing tools:

            open("foo.xml", "w").write(book.toXml())

        By default, this method produces a hierarchy that looks like
        this:

            Book
                Sheet [with name attribute]
                    Row [with number attribute]
                        Col [with number attribute]
                        ...
                    ...
                ...

        An optional Boolean 'flat' argument (default is False) can be
        passed to the toXml() method, in which case the Row and Col
        elements are collapsed into a flat sequence of Cell elements:

            Book
                Sheet [with name attribute]
                    Cell [with row and col attributes]
                    ... 

        The Col (or Cell) elements carry the value of the sheet's cell
        as a text child node, as well as an optional hlink attribute
        for those cells with which a hyperlink is associated.  Only
        defined cells are represented in the XML document.

    Restrictions

        * This module requires Python 2.2 or later.
        * The module only handles BIFF8 files (Excel 97 and later)
        * The module only handles values and hyperlinks (no charts,
          etc.)
        * The module provides no support for modifying or writing
          workbooks.
        * No support is provided for evaluation of formulas; however,
          Excel caches the result of the formulas, unless this feature
          has been turned off (which rarely happens), and the module
          stores these cached values as the values of cells with
          formulas.  The interface does not distinguish between values 
          which are stored directly in cells, and those which were
          found as the cached results of formulas.

    To Do

        * Add support for BOOLEAN records
        * Add handling for DIMENSIONS records

    See Also

        Spreadsheet.WriteExcel module
"""

#----------------------------------------------------------------------
#
# $Id: ExcelReader.py,v 1.2 2004-10-11 00:14:52 bkline Exp $
#
# Module for extracting cell values from Excel spreadsheets.
#
# $Log: not supported by cvs2svn $
# Revision 1.1  2004/10/10 19:09:50  bkline
# Support for reading Excel workbooks.
#
#----------------------------------------------------------------------

import OleStorage, struct, sys

__all__ = ['Workbook']

DEBUG = False

class Workbook:

    """Top-level object used to provide read-only access to Excel
    Workbooks."""


    def __init__(self, fileName):

        """Loads a workbook from the named disk file."""

        # Initial values for workbook's members.
        self.formats    = self.__builtinFormats()
        self.biffVer    = 'BIFF8'
        self.flag1904   = False
        self.xf         = []
        self.fonts      = []
        self.sst        = []
        self.sheets     = []
        self.__sheets   = {}
        self.buf        = self.__loadWorkbookStream(fileName)

        # Load the workbook's global block.
        self.__parseGlobals()

        # Load the data for each of the worksheets in the book.
        for sheet in self.sheets:
            sheet.load(self)
        
    def toXml(self, flat = False):

        """Generates an XML representation of the workbook, using
        UTF-8 encoding.  Not exceptionally fast for very large files,
        but handy nevertheless for making it possible to get to the
        data on another machine where this module is not available,
        but the standard XML parsing tool are.  Pass in flat = True
        to roll out the Row (Col, Col, Col) into a flat (Cell, Cell,
        Cell)."""

        import xml.sax.saxutils
        structure = flat and "cells" or "rows"
        x = u"<?xml version='1.0' encoding='utf-8'?>\n<Book>"
        for s in self.sheets:
            x += u"<Sheet name='%s'>" % s.name
            for row in s.rows:
                if structure == "rows":
                    x += u"<Row number='%d'>" % row.number
                for cell in row.cells:
                    if DEBUG:
                        sys.stderr.write((u"%s\n" %
                                    repr(cell)).encode('utf-8'))
                    try:
                        val = cell.format()
                    except:
                        sys.stderr.write(OleStorage.showBytes(cell.val))
                        raise
                    val = xml.sax.saxutils.escape(val)
                    hlink = u""
                    if cell.hlink and cell.hlink.url:
                        hlink = (u" hlink=%s" % 
                                 xml.sax.saxutils.quoteattr(cell.hlink.url))
                    if structure == "rows":
                        x += (u"<Col number='%d'%s>%s</Col>" % 
                                (cell.col, hlink, val))
                    else:
                        x += (u"<Cell row='%d' col='%d'%s>%s</Cell>" % 
                                (cell.row, cell.col, hlink, val))
                if structure == "rows":
                    x += u"</Row>"
            x += u"</Sheet>"
        x += u"</Book>"
        return x.encode('utf-8')

    def __loadWorkbookStream(self, fileName):
        oleStorage = OleStorage.OleStorage(fileName)
        bookStream = oleStorage.getRootDirectory().open("Workbook")
        if not bookStream:
            bookStream = oleStorage.getRootDirectory().open("Book")
        if not bookStream:
            raise Exception("Excel workbook not found")
        return bookStream.read()

    def __parseGlobals(self):

        # Sanity check for top of globals block
        record = Record(self.buf, 0)
        if record.id != 0x0809:
            raise Exception("expected BOF for workbook globals")
        if struct.unpack("<h", record.data[2:4])[0] != 5:
            raise Exception("expected workbook globals block")

        # Read to EOF
        offset = record.size + 4
        record = Record(self.buf, offset)
        while record.id != 0x000A: 

            # BOUNDSHEET record
            if record.id == 0x0085:
                sheet = Worksheet(record.data)
                if sheet.type == "worksheet":
                    self.sheets.append(sheet)
                    self.__sheets[sheet.name] = sheet

            # XF record
            elif record.id == 0x00E0:
                font, format = struct.unpack("<2h", record.data[:4])
                self.xf.append((font, format))

            # FORMAT record
            elif record.id == 0x041E:
                format = Format(buf = record.data)
                self.formats[format.id] = format

            # FONT record
            elif record.id == 0x0031:
                font = Font(record.data)
                self.fonts.append(font)

            # FLAG1904 record
            elif record.id == 0x0022:
                self.flag1904 = (struct.unpack("<h", record.data)[0] and True
                                                                      or False)

            # SST record (CONTINUE records make this ugly)
            elif record.id == 0x00FC:
                (dummy, count) = struct.unpack("<2l", record.data[:8])
                if DEBUG: sys.stderr.write("SST count=%d\n" % count)
                i, pos, buf = 0, 0, record.data[8:]
                while i < count:
                    if pos >= len(buf):
                        offset += record.size + 4
                        record = Record(self.buf, offset)
                        if record.id != 0x003C:
                            raise Exception(u"Expected CONTINUE record")
                        if DEBUG: sys.stderr.write("CONTINUE record\n")
                        pos, buf = 0, record.data
                    s = UnicodeString(buf[pos:])

                    # Check for nasty split in middle of string. :-<}
                    if s.length > len(s.value):
                        if DEBUG:
                            msg = (u"s.length = %d len(s.value) = %d "
                                   u"s.value = %s\n" % (s.length, 
                                                        len(s.value),
                                                        s.value))
                            sys.stderr.write(msg.encode('latin-1', 'replace'))
                        remaining = s.length - len(s.value)
                        offset += record.size + 4
                        record = Record(self.buf, offset)
                        if record.id != 0x003C:
                            raise Exception(u"Expected CONTINUE record")
                        if DEBUG:
                            sys.stderr.write("CONTINUE: %s\n" %
                                    OleStorage.showBytes(record.data[:16]))
                        buf = struct.pack("<h", remaining) + record.data
                        s2 = UnicodeString(buf)
                        if s2.length != remaining:
                            raise Exception(u"string continuation expected "
                                            u"%d characters, got %d" %
                                            (remaining, s2.length))
                        self.sst.append(s.value + s2.value)
                        pos = s2.nbytes
                    else:
                        self.sst.append(s.value)
                        pos += s.nbytes
                    if DEBUG:
                        try:
                            msg = u"sst[%d] = %s\n" % (i, self.sst[i])
                            sys.stderr.write(msg.encode('latin-1', 'replace'))
                        except:
                            sys.stderr.write("type(self.sst[i]=%s\n"
                                    % type(self.sst[i]))
                            sys.stderr.write(OleStorage.showBytes(self.sst[i])
                                    + "\n")
                            raise
                    i += 1
                    
            # Conditional code for showing unrecognized records.
            elif DEBUG and record.id:
                b = OleStorage.showBytes(record.data[:16])
                sys.stderr.write("type=%04X size=%d b=%s\n" % 
                                 (record.id, record.size, b))

            # Move to the next record
            offset += record.size + 4
            record = Record(self.buf, offset)

    def __getitem__(self, which):
        sheet = self.__sheets.get(which)
        if sheet: return sheet
        try:
            index = int(which)
            return self.sheets[which]
        except:
            raise IndexError

    def __builtinFormats(self):
        return {
            0x00: Format(0x00, '@'),
            0x01: Format(0x01, '0'),
            0x02: Format(0x02, '0.00'),
            0x03: Format(0x03, '#,##0'),
            0x04: Format(0x04, '#,##0.00'),
            0x05: Format(0x05, '($#,##0_);($#,##0)'),
            0x06: Format(0x06, '($#,##0_);[Red]($#,##0)'),
            0x07: Format(0x07, '($#,##0.00_);($#,##0.00_)'),
            0x08: Format(0x08, '($#,##0.00_);[Red]($#,##0.00_)'),
            0x09: Format(0x09, '0%'),
            0x0A: Format(0x0A, '0.00%'),
            0x0B: Format(0x0B, '0.00E+00'),
            0x0C: Format(0x0C, '# ?/?'),
            0x0D: Format(0x0D, '# ??/??'),
            0x0E: Format(0x0E, 'm-d-yy'),
            0x0F: Format(0x0F, 'd-mmm-yy'),
            0x10: Format(0x10, 'd-mmm'),
            0x11: Format(0x11, 'mmm-yy'),
            0x12: Format(0x12, 'h:mm AM/PM'),
            0x13: Format(0x13, 'h:mm:ss AM/PM'),
            0x14: Format(0x14, 'h:mm'),
            0x15: Format(0x15, 'h:mm:ss'),
            0x16: Format(0x16, 'm-d-yy h:mm'),
            # Missing values are set by locale
            0x25: Format(0x25, '(#,##0_);(#,##0)'),
            0x26: Format(0x26, '(#,##0_);[Red](#,##0)'),
            0x27: Format(0x27, '(#,##0.00);(#,##0.00)'),
            0x28: Format(0x28, '(#,##0.00);[Red](#,##0.00)'),
            0x29: Format(0x29,
                                   '_(*#,##0_);_(*(#,##0);_(*"-"_);_(@_)'),
            0x2A: Format(0x2A,
                                   '_($*#,##0_);_($*(#,##0);_(*"-"_);_(@_)'),
            0x2B: Format(0x2B,
                            '_(*#,##0.00_);_(*(#,##0.00);_(*"-"??_);_(@_)'),
            0x2C: Format(0x2C,
                            '_($*#,##0.00_);_($*(#,##0.00);_(*"-"??_);_(@_)'),
            0x2D: Format(0x2D, 'mm:ss'),
            0x2E: Format(0x2E, '[h]:mm:ss'),
            0x2F: Format(0x2F, 'mm:ss.0'),
            0x30: Format(0x30, '##0.0E+0'),
            0x31: Format(0x31, '@')
        }

class Record:
    def __init__(self, buf, offset):
        dataStart = offset + 4
        (self.id, self.size) = struct.unpack("<2h", buf[offset:dataStart])
        dataEnd = dataStart + self.size
        self.data = buf[dataStart:dataEnd]

class ExcelTime:

    def isLeapYear(self):
        if self.year % 4: return False
        if self.year % 100: return True
        if self.year % 400 == 0: return True
        return False

    def format(self):
        if not self.year and not self.day and not self.month:
            return u"%02d:%02d:%02d.%03d" % (self.hour,
                                             self.minutes,
                                             self.seconds,
                                             self.millis)
        elif (not self.hour and not self.minutes and not self.seconds and
              not self.millis):
            return u"%04d-%02d-%02d" % (self.year,
                                        self.month + 1,
                                        self.day)
        return u"%04d-%02d-%02d %02d:%02d:%02d.%03d" % (self.year,
                                                        self.month + 1,
                                                        self.day,
                                                        self.hour,
                                                        self.minutes,
                                                        self.seconds,
                                                        self.millis)
        
class ExcelTimeFromNumber(ExcelTime):
    def __init__(self, t, flag1904 = False):
        daysInMonth = ((31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31),
                       (31, 29, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31))
        self.year = self.month = self.day = 0
        self.hour = self.minutes = self.seconds = self.millis = 0
        days = int(t)
        fraction = t - days
        if days:
            daysInYear = 366 # bug: Microsoft thinks 1900 is a leap year!
            if flag1904:
                self.year = 1904
                days += 1
                self.weekDay = (days + 4) % 7
            else:
                self.year = 1900
                self.weekDay = (days + 6) % 7
            while days > daysInYear:
                days -= daysInYear
                self.year += 1
                daysInYear = self.isLeapYear() and 366 or 365

            # Excel bug: Microsoft thinks 1900 is a leap year!
            idx = self.isLeapYear() or self.year == 1900 and 1 or 0
            while 1:
                dim = daysInMonth[idx][self.month]
                if days <= dim:
                    break
                days -= dim
                self.month += 1
            self.day = days

        if fraction:
            fraction    += 0.0005 / 86400.0
            fraction    *= 24.0
            self.hour    = int(fraction)
            fraction    -= self.hour
            fraction    *= 60.0
            self.minutes = int(fraction)
            fraction    -= self.minutes
            fraction    *= 60.0
            self.seconds = int(fraction)
            fraction    -= self.seconds
            fraction    *= 1000.0
            self.millis  = int(fraction)

class Worksheet:

    def __init__(self, data):
        self.cells = {}
        self.__links = []
        self.pos, vis, typ, = struct.unpack("<l2b", data[:6])
        typ &= 0x0F
        if   vis == 0: self.visibility = "visible"
        elif vis == 1: self.visibility = "hidden"
        elif vis == 2: self.visibility = "strong hidden"
        else:          self.visibility = "unknown"
        if   typ == 0: self.type = "worksheet"
        elif typ == 2: self.type = "chart"
        elif typ == 6: self.type = "Visual Basic module"
        else:          self.type = "unknown"
        self.name = UnicodeString(data[6:], 1).value

    def __getitem__(self, index):
        return self.__rows.get(index, Row(index))

    def __iter__(self):
        lastRowNumber = self.rows and self.rows[-1].number or -1
        return Worksheet.Iter(self.__rows, lastRowNumber)

    class Iter:
        def __init__(self, rowDict, lastRowNumber):
            self.__rowDict = rowDict
            self.__lastRow = lastRowNumber
            self.__next    = 0
        def next(self):
            if self.__next > self.__lastRow:
                raise StopIteration
            num = self.__next
            self.__next += 1
            row = self.__rowDict.get(num, Row(num))
            return row

    def load(self, book):
        """
        Each data cell is one of the following types of record:
            BLANK | BOOLERR | LABELSST | MULBLANK | MULRK | NUMBER | RK
        We ignore MULBLANK and BOOLERR (at least for now).
        """

        # Sanity check for the worksheet's block start.
        offset = self.pos
        record = Record(book.buf, offset)
        if record.id != 0x0809:
            raise Exception("expected BOF for worksheet block")
        if struct.unpack("<h", record.data[2:4])[0] != 16:
            raise Exception("expected worksheet block")

        # Read to EOF.
        offset += record.size + 4
        record = Record(book.buf, offset)
        while record.id != 0x000A:

            # RK record
            if record.id == 0x027E:
                (row, col, xf, val) = struct.unpack("<3hl", record.data)
                val = self.__parseRk(val)
                self.cells[(row,col)] = Cell(row, col, book,
                                                      xf, val)

            # MULRK record
            elif record.id == 0x00BD:
                (r, c) = struct.unpack("<hh", record.data[:4])
                start = 4
                end   = 10
                while end < len(record.data):
                    xf, rk = struct.unpack("<hl", record.data[start:end])
                    val = self.__parseRk(rk)
                    self.cells[(r, c)] = Cell(r, c, book, xf, val)
                    start += 6
                    end   += 6
                    c     += 1

            # LABELSST record
            elif record.id == 0x00FD:
                (row, col, xf, idx) = struct.unpack("<3hl", record.data)
                if not book.sst:
                    s = u"*** MISSING SHARED STRING TABLE ***"
                else:
                    try:
                        s = book.sst[idx]
                    except:
                        s = u"*** SST INDEX VALUE %d OUT OF RANGE" % idx
                self.cells[(row,col)] = Cell(row, col, book, 
                                                      xf, s)
                s = OleStorage.showUnicode(s)

            # NUMBER record
            elif record.id == 0x0203:
                (r, c, xf, v) = struct.unpack("<3hd", record.data)
                self.cells[(r, c)] = Cell(r, c, book, xf, v)

            # FORMULA record
            elif record.id == 0x0006:
                (r, c, xf) = struct.unpack("<3h", record.data[:6])
                result = record.data[6:14]
                flags = struct.unpack("h", record.data[14:16])[0]
                data = record.data[20:]
                v = None
                if result[6:8] == '\xFF\xFF':
                    if result[0] == '\x00':
                        offset += record.size + 4
                        record = Record(book.buf, offset)
                        if record.id == 0x0207:
                            v = UnicodeString(record.data).value
                    elif result[0] == '\x03':
                        v = u""
                else:
                    v = struct.unpack("<d", result)[0]
                if v != None:
                    self.cells[(r, c)] = Cell(r, c, book, xf, v)

            # HLINK record
            elif record.id == 0x01B8:
                hlink = Hyperlink(record.data)
                if DEBUG:
                    sys.stderr.write("HLINK: %s\n" % hlink.url)
                self.__links.append(hlink)

            # Show unhandled records.
            elif DEBUG and record.id:
                b = OleStorage.showBytes(record.data[:16])
                sys.stderr.write("type=%04X size=%d b=%s\n" % 
                                (record.id, record.size, b))

            # Move to the next record
            offset += record.size + 4
            record = Record(book.buf, offset)

        # Connect the hyperlinks with URLs to the cells.
        for link in self.__links:
            if link.url:
                r = link.firstRow
                while r <= link.lastRow:
                    c = link.firstCol
                    while c <= link.lastCol:
                        key = (r, c)
                        cell = self.cells.get(key)
                        if not cell:
                            cell = Cell(r, c, book)
                            self.cells[key] = cell
                        cell.hlink = link
                        c += 1
                    r += 1

        # Build an array of Rows
        self.rows = []
        self.__rows = {}
        keys = self.cells.keys()
        keys.sort()
        currentRow = None
        for key in keys:
            cell = self.cells[key]
            if cell.row != currentRow:
                row = Row(cell.row)
                currentRow = cell.row
                self.rows.append(row)
                self.__rows[cell.row] = row
            row.addCell(cell)

    def __parseRk(self, v):
        cents = v & 1 and True or False
        if v & 2:
            v >>= 2
            return cents and v / 100.0 or v
        v &= 0xFFFFFFFC
        s = "\0\0\0\0" + struct.pack("<L", v)
        v = struct.unpack("<d", s)[0]
        return cents and v / 100.0 or v

class Hyperlink:
    def __init__(self, buf):
        (self.firstRow, self.lastRow, self.firstCol, self.lastCol,
         guid, dummy, flags) = struct.unpack("<4h16s2L", buf[:32])
        self.description = None
        self.targetFrame = None
        self.textMark = None
        self.url = None
        offset = 32
        self.absolute = (flags & 0x02) and True or False
        if (flags & 0x14) == 0x14:
            length = struct.unpack("<L", buf[offset:offset+4])[0]
            offset += 4
            self.description = unicode(buf[offset:offset+length*2],
                                       "utf-16-le")[:-1]
            offset += length * 2
        if flags & 0x80:
            length = struct.unpack("<L", buf[offset:offset+4])[0]
            offset += 4
            self.targetFrame = unicode(buf[offset:offset+length * 2],
                                       "utf-16-le")[:-1]
            offset += length * 2
        if flags & 0x03:
            guid = buf[offset:offset+16]
            if (guid == "\xE0\xC9\xEA\x79\xF9\xBA\xCE\x11"
                        "\x8C\x82\x00\xAA\x00\x4B\xA9\x0B"):
                offset += 16
                length = struct.unpack("<L", buf[offset:offset+4])[0]
                offset += 4
                self.url = unicode(buf[offset:offset+length],
                                   "utf-16-le")[:-1]
        
class UnicodeString:

    def __init__(self, buf, lenSize = 2):
        if lenSize == 1:
            self.length, flags = struct.unpack("bb", buf[:2])
            pos = 2
        else:
            self.length, flags = struct.unpack("<hb", buf[:3])
            pos = 3
        self.utf16 = (flags & 1) and True or False
        self.farEast = (flags & 4) and True or False
        self.rtf = (flags & 8) and True or False
        self.nbytes = pos
        if self.rtf:
            self.nbytes += struct.unpack("<h", buf[pos:pos+2])[0] * 4
            pos += 2
        if self.farEast:
            self.nbytes += struct.unpack("<l", buf[pos:pos+4])[0]
            pos += 4
        if self.utf16:
            self.value = unicode(buf[pos:pos+self.length*2], "utf-16-le")
            pos += self.length * 2
            self.nbytes += self.length * 2
        else:
            self.value = unicode(buf[pos:pos+self.length], "latin-1")
            self.nbytes += self.length

class Cell:

    def __init__(self, row, col, book, xf = 0, val = None):
        self.row   = row
        self.col   = col
        self.xf    = xf
        self.val   = val
        self.book  = book
        self.hlink = None
    def __str__(self): return self.format().encode("utf-8")
    def __repr__(self):
        return u"[Cell: row=%d col=%d value=%s]" % (self.row, self.col,
                                                    self.format())
    def format(self):
        fmt = self.book.formats[self.book.xf[self.xf][1]]
        if fmt.type == 'datetime' and type(self.val) in (type(9), type(9.9)):
            return ExcelTimeFromNumber(self.val).format()
        # Strip superfluous decimal portion.
        elif type(self.val) == type(9.9) and not self.val % 1:
            return u"%d" % self.val
        else:
            return u"%s" % self.val

class Font:
    def __init__(self, buf):
        (self.twips, flags, self.color, self.weight, esc, under,
         family, charset) = struct.unpack("<5h3b", buf[:13])
        self.bold = (flags & 1) and True or False
        self.italic = (flags & 2) and True or False
        self.underlined = (flags & 4) and True or False
        self.strikeout = (flags & 8) and True or False
        self.superscript = (esc & 1) and True or False
        self.subscript = (esc & 2) and True or False
        if under == 0: self.underlineType = "none"
        elif under == 1: self.underlineType = "single"
        elif under == 2: self.underlineType = "double"
        elif under == 0x21: self.underlineType = "single accounting"
        elif under == 0x22: self.underlineType = "double accounting"
        else: self.underlineType = "unknown"
        if family == 0: self.family = "none"
        elif family == 1: self.family = "roman"
        elif family == 2: self.family = "swiss"
        elif family == 3: self.family = "modern"
        elif family == 4: self.family = "script"
        elif family == 5: self.family = "decorative"
        else: self.family = "unknown"
        self.name = UnicodeString(buf[14:], 1).value

class Format:
    import re
    brackets = re.compile(r"\[[^]]*\]")
    def __init__(self, id = None, pattern = None, buf = None):
        import re
        if buf:
            self.id = struct.unpack("<h", buf[:2])[0]
            self.pattern = UnicodeString(buf[2:]).value
        else:
            self.id = id
            self.pattern = pattern
        pattern = Format.brackets.sub(u"", self.pattern)
        if pattern == '@':
            self.type = 'general'
        elif pattern == '0':
            self.type = 'integer'
        elif pattern.find('#') != -1 or pattern.find('9') != -1:
            self.type = 'number'
        elif pattern.find('0') != -1:
            self.type = 'number'
        else:
            self.type = 'datetime'

class Row:
    """Represents the set of active cells on a given worksheet row."""
    def __init__(self, number):
        self.number = number
        self.cells = []
        self.__cells = {}
    def addCell(self, cell):
        self.cells.append(cell)
        self.__cells[cell.col] = cell
    def __getitem__(self, index):
        return self.__cells.get(index) or None
    def __iter__(self):
        lastCellNumber = self.cells and self.cells[-1].col or -1
        return Row.Iter(self.__cells, lastCellNumber)
    class Iter:
        def __init__(self, cellDict, lastCellNumber):
            self.__cellDict = cellDict
            self.__lastCell = lastCellNumber
            self.__next     = 0
        def next(self):
            if self.__next > self.__lastCell:
                raise StopIteration
            cell = self.__cellDict.get(self.__next)
            self.__next += 1
            return cell

if __name__ == "__main__":
    #DEBUG  = True
    book   = Workbook(sys.argv[1])
    flag   = len(sys.argv) > 2 and sys.argv[2] != "rows"
    doc    = book.toXml(flag)
    sys.stdout.write(doc)
