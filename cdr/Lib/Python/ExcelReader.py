#!/usr/bin/python

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
# $Id: ExcelReader.py,v 1.10 2005-03-10 20:33:30 bkline Exp $
#
# Module for extracting cell values from Excel spreadsheets.
#
# $Log: not supported by cvs2svn $
# Revision 1.9  2005/03/05 05:54:13  bkline
# "The font with index 4 is omitted in all BIFF versions.  This means the
# first four fonts have zero-based indexes, and the fifth font and all
# following fonts are referenced with one-based indexes." - OpenOffice
# documentation for Excel BIFF formats.  Fixed code to conform to this
# specification.
#
# Revision 1.8  2005/03/04 22:09:59  bkline
# Fixed font indexing problem.
#
# Revision 1.7  2005/03/03 14:42:30  bkline
# Fixed documentation typos.
#
# Revision 1.6  2005/01/06 22:55:55  bkline
# Added more debugging information; fixed RTF and Far East length
# calculation bugs.
#
# Revision 1.5  2004/11/04 13:13:58  bkline
# Removed superfluous quote mark in doc string.
#
# Revision 1.4  2004/10/12 11:36:29  bkline
# Fixed name of fileBuf argument to OleStorage constructor.
#
# Revision 1.3  2004/10/12 01:27:09  bkline
# Sped up the XML document generation by almost two orders of magnitude,
# by using the string join operator on a list of strings for the pieces
# of the document rather than string concatenation.  Fleshed out the
# module's documentation.
#
# Revision 1.2  2004/10/11 00:14:52  bkline
# Added support for CONTINUE records.  Fixed an encoding bug (str(cell)
# now returns a UTF-8--encoded string).
#
# Revision 1.1  2004/10/10 19:09:50  bkline
# Support for reading Excel workbooks.
#
#----------------------------------------------------------------------

import OleStorage, struct, sys

__all__ = ['Workbook']

DEBUG = False

class Workbook:

    """
    Top-level object used to provide read-only access to Excel
    Workbooks.

    Attributes:

        sheets       - list of Worksheet object contained in
                       this book
        formats      - dictionary of Format objects associated
                       with this file
        fonts        - list of Font objects used by this
                       workbook
        sst          - shared string table
        biffVer      - always 'BIFF8' in this version of the
                       module
        flag1904     - if true, dates are calculated as the
                       number of days which have elapsed since
                       January 1, 1904; otherwise, dates are
                       calculated as the number of days which
                       have elapsed since December 31, 1899
        xf           - list of extended format indexes; each
                       member of the list is a two-member
                       tuple, the first element of which is
                       an index into the fonts table, and the
                       other element of which is an index
                       into the formats table
        buf            8-bit character string holding the
                       original content of the workbook
                       stream for this file
    """


    def __init__(self, fileName = None, fileBuf = None):

        """
        Loads a workbook from the named disk file or from the
        contents of an Excel file in an 8-bit string in memory.
        Pass in either the path of a disk file as the fileName
        parameter, or the in-memory string as the fileBuf
        parameter.

        Parsing the file is a two-step process.  First the top
        block of the stream, containing global settings for
        the workbook, is loaded.  Included with those global
        settings is the name and starting location in the
        stream for each of the worksheets contained in the
        book.  The next step iterates through each of the
        worksheets identified in the first step and loads the
        contents of each of the defined cells in the sheet,
        as well as any hyperlinks associated with cells in
        the sheet.
        """

        # Initial values for workbook's members.
        self.formats    = self.getBuiltinFormats()
        self.biffVer    = 'BIFF8'
        self.flag1904   = False
        self.xf         = []
        self.fonts      = []
        self.sst        = []
        self.sheets     = []
        self.__sheets   = {}
        self.buf        = self.loadWorkbookStream(fileName, fileBuf)

        # Load the workbook's global block.
        self.parseGlobals()

        # Load the data for each of the worksheets in the book.
        for sheet in self.sheets:
            sheet.load(self)
        
    def toXml(self, flat = False):

        """Generates an XML representation of the workbook, using
        UTF-8 encoding.  Useful for storing the data in a format
        which is readable on another machine where this module is
        not available, but the standard XML parsing tools are.

        By default, the XML document wraps all of the cells on
        a given row in a Row element, with each cell in a Col
        child element.  For example (indenting added for
        readability):

            <?xml version='1.0' encoding='utf-8'?>
            <Book>
             <Sheet name='PI Mapping Table'>
              <Row number='0'>
               <Col number='0'>CTEP ID</Col>
               <Col number='1'>CDR ID</Col>
              </Row>
              <Row number='1'>
               <Col number='0'>A4093</Col>
               <Col number='1'>74822</Col>
              </Row>
             </Sheet>
             <Sheet name='Sheet2'/>
            </Book>

        There is an optional parameter ('flat') which if passed a
        value which evaluates to True, flattens out the structure
        so that each Sheet element contains a sequence of Cell
        child elements:
        
            <?xml version='1.0' encoding='utf-8'?>
            <Book>
             <Sheet name='PI Mapping Table'>
              <Cell row='0' col='0'>CTEP ID</Col>
              <Cell row='0' col='1'>CDR ID</Col>
              <Cell row='1' col='0'>A4093</Col>
              <Cell row='1' col='1'>74822</Col>
             </Sheet>
             <Sheet name='Sheet2'/>
            </Book>

        An optional 'hlink' attribute is added to the Col or Cell
        element for any cell with which a hyperlink URL is
        associated.
        """

        import xml.sax.saxutils
        structure = flat and "cells" or "rows"
        x = [u"<?xml version='1.0' encoding='utf-8'?>\n<Book>"]
        for s in self.sheets:
            x.append(u"<Sheet name='%s'>" % s.name)
            for row in s.rows:
                if structure == "rows":
                    x.append(u"<Row number='%d'>" % row.number)
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
                        x.append(u"<Col number='%d'%s>%s</Col>" % 
                                 (cell.col, hlink, val))
                    else:
                        x.append(u"<Cell row='%d' col='%d'%s>%s</Cell>" % 
                                 (cell.row, cell.col, hlink, val))
                if structure == "rows":
                    x.append(u"</Row>")
            x.append(u"</Sheet>")
        x.append(u"</Book>")
        return u"".join(x).encode('utf-8')

    def loadWorkbookStream(self, fileName = None, fileBuf = None):
        
        """
        Extracts the Workbook stream as an 8-bit-character string
        from an Excel BIFF8 file.  Pass fileName to specify the path
        (relative or absolute) where the disk file is located; or
        pass fileBuf to provide the contents of the file alreay
        loaded into memory (useful for files retrieved from a server
        directly, such as an HTTP server).
        """
        
        if fileName:
            oleStorage = OleStorage.OleStorage(name = fileName)
        elif fileBuf:
            oleStorage = OleStorage.OleStorage(buf = fileBuf)
        else:
            raise Exception("fileName or fileBuf must be specified")
        bookStream = oleStorage.getRootDirectory().open("Workbook")
        if not bookStream:
            bookStream = oleStorage.getRootDirectory().open("Book")
        if not bookStream:
            raise Exception("Excel workbook not found")
        return bookStream.read()

    def parseGlobals(self):

        """
        Loads the pieces we're interested in from the top block
        of the Workbook stream.  A block in a BIFF file consists
        of a sequence of Records, each of which starts with a
        two-byte identifier holding the record type, followed
        by a two-byte integer for the length in bytes of the
        remainder of the record.  The first record of a block
        is a BOF record, and the block ends with an EOF record.
        In addition to those two record types, the following
        record types are processed here:

            BOUNDSHEET - contains the name and starting position
                         for each worksheet in the book
            XF         - contains a pair of indexes into the
                         fonts and formats for the workbook
            FORMAT     - contains instructions for displaying
                         the contents of a worksheet cell
            FONT       - contains font characteristics for
                         displaying all or portions of a
                         string
            SST        - a single record containing all of
                         the strings used in the workbook cells
            CONTINUE   - additional records used for the
                         shared string table when it cannot
                         fit into the maximum size of a
                         record (8228 bytes in BIFF8, including
                         the 4 bytes of the record header)

        All other records are ignored in this version of the
        module.
        """
        
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
                i, pos, buf = 0, 0, record.data[8:]
                if DEBUG: 
                    sys.stderr.write("SST count=%d len(buf)=%d offset=%d\n" % 
                                     (count, len(buf), offset))
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
                            msg = u"sst[%d] = %s pos=%d\n" % (i, self.sst[i],
                                                              pos)
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

        """
        Enables finding a Worksheet by name (if the argument is
        a string) or position (if an integer is passed).  If
        there is no matching sheet an IndexError exception is
        raised.
        """

        sheet = self.__sheets.get(which)
        if sheet: return sheet
        try:
            index = int(which)
            return self.sheets[which]
        except:
            raise IndexError

    def getBuiltinFormats(self):
        
        """Returns a pre-built dictionary of the formats assumed
        by Excel."""
        
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

class Worksheet:

    """
    Object representing one of the sheets contained in the Workbook.

    Attributes:

        name       - string used to identify the sheet
        cells      - dictionary of all defined cells contained in the
                     sheet, indexed by (row, column) tuples
        pos        - the starting location of the sheet's block in
                     the book's stream
        visibility - 'visible' (the normal case)
                     'hidden' (the sheet is not displayed)
                     'strong hidden' (controlled by Visual Basic code)
                     'unknown' (should never happen)
        type       - 'worksheet' (the only kind we're interested in)
                     'chart' (not handled) 
                     'Visual Basic module' (also ignored)
                     'unknown' (should never happen)
        __links    - private storage for list of hyperlinks until
                     we get them connected to their individual cells
    """
    
    def __init__(self, data):
        
        "Extracts the Worksheet information from its binary encoding"
        
        self.pos, vis, typ, = struct.unpack("<l2b", data[:6])
        typ &= 0x0F
        if   vis == 0: self.visibility = "visible"
        elif vis == 1: self.visibility = "hidden"
        elif vis == 2: self.visibility = "strong hidden"
        else:          self.visibility = "unknown"
        if   typ == 0: self.type       = "worksheet"
        elif typ == 2: self.type       = "chart"
        elif typ == 6: self.type       = "Visual Basic module"
        else:          self.type       = "unknown"
        self.name                      = UnicodeString(data[6:], 1).value
        self.cells                     = {}
        self.__links                   = []

    def __getitem__(self, index):

        """Returns the row for the position specified if one
        exists; otherwise returns an empty row."""
        
        return self.__rows.get(index, Row(index))

    def __iter__(self):

        """Invoked under the covers by the Python engine for
        code which asks to iterate over the sheet's rows
        (for row in sheet: ....).  Starts at the first row,
        regardless of whether the row is empty, and continues
        until the last row containing at least one defined
        cell.  If the sheet has no defined cells, no rows
        are returned."""
        
        lastRowNumber = self.rows and self.rows[-1].number or -1
        return Worksheet.Iter(self.__rows, lastRowNumber)

    class Iter:
        "Internal support class for iterating over the sheet's rows"
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
        Parse and assemble the information we need from the block
        for this Worksheet:

           1. Extract the records we want:

               * RK       - contains a single numeric value, stored
                            in a compact format (30-bit integer
                            or floating-point value, with flag
                            indicating possible multiplier of 100)
               * NUMBER   - single IEEE floating point value
               * MULRK    - contains continguous numeric values on
                            the same row
               * LABELSST - points to string in the shared string
                            table
               * FORMULA  - we don't really do anything with the
                            formula itself, but we extract the
                            cached value from Excel's evaluation
                            of the formula from the STRING record
                            which immediately follows this record
               * STRING   - stores cached value of formula
               * HLINK    - hyperlink URL associated with one or
                            more cells

           2. Tie the hyperlink URLs to the cells to which they
              belong

           3. Construct the list of Row objects from the
              collected cells, and populate each object
              with the cells defined for that row.
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
                val = self.parseRk(val)
                self.cells[(row,col)] = Cell(row, col, book,
                                                      xf, val)

            # MULRK record
            elif record.id == 0x00BD:
                (r, c) = struct.unpack("<hh", record.data[:4])
                start = 4
                end   = 10
                while end < len(record.data):
                    xf, rk = struct.unpack("<hl", record.data[start:end])
                    val = self.parseRk(rk)
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

                        # STRING record
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

    def parseRk(self, v):
        
        """
        Extract a compactly packed numeric value.  The bits are
        used as follows:

            Bit     Mask     Contents
             0   00000001H   0=value not changed
                             1=value is multipled by 100
             1   00000002H   0=floating-point value (IEEE
                               type 754, with the 34 least
                               significant bits cleared)
                             1=signed 30-bit integer value
           31-2  FFFFFFFCH   Encoded value
        """
        
        cents = v & 1 and True or False
        if v & 2:
            v >>= 2
            return cents and v / 100.0 or v
        v &= 0xFFFFFFFCL
        s = "\0\0\0\0" + struct.pack("<L", v)
        v = struct.unpack("<d", s)[0]
        return cents and v / 100.0 or v

class Row:
    """
    Collection of all active cells in a single worksheet row.

    Attributes:

        number     - count of active cells in the row
        cells      - sequence of Cell objects
    """
    
    def __init__(self, number):
        "Creates an empty Row object."
        self.number = number
        self.cells = []
        self.__cells = {}

    def addCell(self, cell):
        "Appends a Cell object to the sequence, and indexes it."
        self.cells.append(cell)
        self.__cells[cell.col] = cell
        
    def __getitem__(self, index):
        
        """
        Returns the cell corresponding to the specified column,
        if one is defined at that position; otherwise returns
        None.
        """
        
        return self.__cells.get(index) or None
        
    def __iter__(self):

        """Invoked under the covers by the Python engine for
        code which asks to iterate over the row's columns
        (for col in row: ....).  Starts at the first column
        regardless of whether the cell at that position has
        been defined, and continues until the last column
        containing a defined cell.  If the row has no
        defined cells, no cells are returned."""
        
        lastCellNumber = self.cells and self.cells[-1].col or -1
        return Row.Iter(self.__cells, lastCellNumber)

    class Iter:
        "Internal support class for iterating over the row's columns"
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

class Cell:

    """
    Represents a single row/column location on a spreadsheet, with
    its value, formatting information, and optional hyperlink.

    Attributes:

        row, col   - coordinates (zero-based) for the cell's location
        font       - reference to font information for the cell
        fmt        - reference to formatting information for the cell
        val        - raw value for the cell (Unicode string, integer,
                     floating-point value, or None if the cell is
                     empty)
        book       - reference to the enclosing Workbook object
        hlink      - optional hyperlink URL (or None)
    """

    def __init__(self, row, col, book, xf = 0, val = None):
        """Constructs a new Cell object.  Omit the xf and val
        parameters for an empty cell.  Empty cells are only
        represented by a Cell object when there is a hyperlink
        stored at an otherwise empty position on the sheet.
        Hyperlinks are added after the object has been created."""
        self.row   = row
        self.col   = col
        self.font  = None
        self.fmt   = None
        self.val   = val
        self.book  = book
        self.hlink = None
        if xf:
            fontIndex, formatIndex = book.xf[xf]
            if fontIndex >= 4:
                fontIndex -= 1
            self.font = book.fonts[fontIndex]
            self.fmt  = book.formats[formatIndex]
        if DEBUG:
            sys.stderr.write("%s\n" % repr(self).encode('utf-8'))

    def __str__(self):
        """Implements the behavior of the builtin str() function,
        creating a UTF-8 string for the cell's formatted value."""
        return self.format().encode("utf-8")

    def __repr__(self):
        """Creates a Unicode representation of the Cell, suitable
        for debugging output."""
        rep = u"[Cell: row=%d col=%d value=%s" % (self.row, self.col,
                                                  self.format())
        if self.hlink:
            rep += u"hlink=%s" % self.hlink
        return rep + u"]"
    
    def format(self):
        """Transforms the raw cell value into an appropriate Unicode
        string representation, using the following approach:

            if there is no value (empty cell) return an empty string
            otherwise, if the Python type for the value is numeric:
                if the format type is 'datetime':
                    use the ExcelTime's format() method
                otherwise, if there is no fractional part to the value:
                    format the value as an integer ('9' not '9.0')
            otherwise:
                return the standard Python string representation
        """
        
        if self.val is None: return u""
        if (self.fmt.type == 'datetime' and
            type(self.val) in (type(9), type(9.9))):
            return ExcelTimeFromNumber(self.val).format()
        # Strip superfluous decimal portion.
        elif type(self.val) == type(9.9) and not self.val % 1:
            return u"%d" % self.val
        else:
            return u"%s" % self.val

class Font:
    """
    Character display settings for Cell values.

    Attributes:

        bold          - redundant Boolean value (see also weight)
        italic        - True or False
        underlined    - True or False; see also underlineType
        strikeout     - True or False
        superscript   - True or False
        subscript     - True or False
        underlineType - 'none', 'single', 'double', 'single accounting', 
                        'double accounting', or 'unknown'
        family        - 'none', 'roman', 'swiss', 'modern', 'script',
                        'decorative', or 'unknown'
        name          - Unicode string for font's name
    """
    
    def __init__(self, buf):
        "Unpacks font information from binary encoding."
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

    """
    Records the pattern chosen by the spreadsheet's author for
    formatting cell data.

    Attributes:

        pattern    - specific formatting instructions encoded
                     in Excel's pattern language
        id         - unique number for this format
        type       - data type ('general', 'integer', 'number',
                     or 'datatime')
    """
    
    import re
    """Needed for compiling regular expression for stripping
    brackets."""
    
    brackets = re.compile(r"\[[^]]*\]")
    """Compiled regular expression used to strip bracketed portions
    from the formatting string when determining the format's basic
    data type."""
    
    def __init__(self, id = None, pattern = None, buf = None):

        """Constructs a Format object from the separate id and
        pattern (passed in directly for the built-in formats),
        or by extracting the information from its encoding
        in the buf parameter (for custom formats used in this
        spreadsheet)."""

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
        elif '#' in pattern or '9' in pattern or '0' in pattern:
            self.type = 'number'
        else:
            self.type = 'datetime'

class Hyperlink:

    """
    Contains a URL associated with one or more of the cells in
    a worksheet.

    Attributes;

        url                - Unicode string for the link
        targetFrame        - optional string for HTML frame
        description        - optional string explaining the URL
        absolute           - True=URL is absolute
                             False=URL is relative
        firstRow, lastRow  - corners of the rectangle of cells
        firstCol, lastCol    for this hyperlink
    """
    
    def __init__(self, buf):
        "Unpacks the hyperlink information from its binary encoding."
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

    """
    Used to decode information stored in an Excel workbook for
    a Unicode string.

    Attributes:

        length     - character count for the string
        nbytes     - number of bytes used to store the string's
                     information; used to determine how far
                     to move forward in a multi-string buffer
                     in order to find the next string
        utf16      - flag indicating whether the string was 
                     stored as 16-bit or 8-bit characters
        rtf        - flag indicating whether formatting
                     information for segments of the string
                     were stored with the string (not used
                     by this implementation)
        farEast    - flag indicating whether additional
                     information supporting far-Eastern
                     languages was stored with the string
                     (not used by this implementation)
    """
    
    def __init__(self, buf, lenSize = 2):
        "Unpacks the string information from its binary encoding."
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
        if DEBUG and (self.rtf or self.farEast):
            flagArray = []
            if self.rtf: flagArray = ['rtf']
            if self.farEast: flagArray.append('farEast')
            if self.utf16: flagArray.append('utf16')
            sys.stderr.write("unicode len: %d; lensize=%d; "
                             "flags: %s; buf: %s\n" %
                             (self.length, lenSize, "+".join(flagArray),
                              OleStorage.showBytes(buf[:255])))
        if self.rtf:
            self.nbytes += struct.unpack("<h", buf[pos:pos+2])[0] * 4 + 2
            pos += 2
        if self.farEast:
            self.nbytes += struct.unpack("<l", buf[pos:pos+4])[0] + 4
            pos += 4
        if self.utf16:
            self.value = unicode(buf[pos:pos+self.length*2], "utf-16-le")
            pos += self.length * 2
            self.nbytes += self.length * 2
        else:
            self.value = unicode(buf[pos:pos+self.length], "latin-1")
            self.nbytes += self.length

class Record:

    """
    Object representing one of the sequential records stored in
    an Excel workbook.

    Attributes:

        id         - identifier giving the Record's type
        size       - number of bytes in the payload for the Record
                     (not including the 4 header bytes used to
                     store the id and size)
        data       - buffer containing the Record's information
    """
    
    def __init__(self, buf, offset):
        "Unpack the id, size, and data for the current Record."
        dataStart = offset + 4
        (self.id, self.size) = struct.unpack("<2h", buf[offset:dataStart])
        dataEnd = dataStart + self.size
        self.data = buf[dataStart:dataEnd]

class ExcelTime:

    """
    Base class for Excel date/time information.

    Attributes:

        year    - full integer for the year (not an offset from 1900)
        month   - zero-based index of the date's month (0=January)
        day     - day of the month (0 for time-only objects)
        hour    - number of hours past midnight (0-23)
        minutes - number of minutes past the start of the hour (0-59)
        seconds - number of seconds past the start of the minute
        millis  - thousandths of a second
    """
    
    def isLeapYear(self):
        
        """
        Determines whether the object's year is a leap year.  Gives
        the correct answer for 1900, even though there is a bug in
        Excel, which considers 1900 to be a leap year.
        """
        
        if self.year % 4: return False
        if self.year % 100: return True
        if self.year % 400 == 0: return True
        return False

    def format(self):
        
        """
        Converts the date/time object to the ISO format for
        the object's value.  Omits the date portion if the
        year, month, and day members are zero.  Omits the
        time portion if the time is exactly midnight.
        """
        
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
    
    """
    Subclass responsible for parsing the object's value from
    the floating-point representation used by Excel.
    """
    
    def __init__(self, t, flag1904 = False):
        
        """
        Splits the floating-point representation for an Excel
        date/time value into the integer (representing the date)
        and fractional (representing the time) portions.  The
        integer is the number of days since December 31, 1899,
        unless the workbooks 1904 flag is set, in which case
        the integer represents the number of days since January 1,
        1900.  There is a bug in Excel, which considers 1900 to
        be a leap year.
        """
        
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

if __name__ == "__main__":
    DEBUG  = True
    book   = Workbook(sys.argv[1])
    flag   = len(sys.argv) > 2 and sys.argv[2] != "rows"
    doc    = book.toXml(flag)
    sys.stdout.write(doc)
