"""

Summary:

    Extracts streams from Microsoft's Structured Storage files.

    Provides read access to Microsoft's compound storage files ("OLE2
    Structured Storage").  The basic idea of this file format is that
    it provides a virtual file system inside a single file, with the
    storages performing the role of directories, and streams inside
    the storages analogous to individual files in a file system.  The
    storages can be nested, providing the equivalent of subdirectories.

    Handles endian issues portably, regardless of the byte ordering
    in the file or for the native operating system.

Example usage:

    import OleStorage
    storageObj = OleStorage.OleStorage(name = 'foo.xls')
    # or storageObj = OleStorage.OleStorage(buf = fileBuf)
    rootDir = storageObject.getRootDirectory()
    streamObj = rootDir.open("Workbook")
    buf = streamObj.read()
    
"""

#----------------------------------------------------------------------
#
# $Id: OleStorage.py,v 1.3 2004-10-20 12:58:11 bkline Exp $
#
# Module for reading OLE2 structured storage files.
#
# $Log: not supported by cvs2svn $
# Revision 1.2  2004/10/12 12:57:11  bkline
# Added alternate method of invoking the OleStorage constructor, passing
# an in-memory buffer instead of the pathname of a disk file.
#
# Revision 1.1  2004/10/10 19:09:50  bkline
# Support for reading Excel workbooks.
#
#----------------------------------------------------------------------
import struct, sys

MAGIC         = "\xD0\xCF\x11\xE0\xA1\xB1\x1A\xE1"
HEADER_SIZE   = 512
BIG_ENDIAN    = '\xFF\xFE'
LITTLE_ENDIAN = '\xFE\xFF'
END_OF_CHAIN  = -2
EMPTY         = 0
USER_STORAGE  = 1
USER_STREAM   = 2
LOCK_BYTES    = 3
PROPERTY      = 4
ROOT_STORAGE  = 5
RED           = 0
BLACK         = 1


def doDump(file, what):
    "Debug logging function."
    file.write(what)
def showBytes(buf):
    "Convert binary string of bytes to space-separated hex codes."
    s = ''
    for b in buf:
        s += "%02X " % ord(b)
    return s
def showUnicode(buf):
    """
    Converts Unicode string to ASCII equivalent, replacing non-ASCII
    characters to <2-byte-hex-code> tokens.
    """
    s = ''
    for c in buf:
        n = ord(c)
        if n < 32 or n > 126:
            s += "<%02X>" % n
        else:
            s += chr(n)
    return s

class OleStorage:
    """
    Object which holds the navigation information for a Microsoft
    compound storage file.  None of the state information is
    exposed in the public interface.  Usage of the class is
    entirely through the public methods.
    """
    def __init__(self, name = None, fileBuf = None):
        """Extracts the control information from a named file or
        from an in-memory string.

        Pass:

            name  - absolute or relative path name for disk file; or
            buf   - in-memory contents of structured storage file
        """
        if name:
            self.__name = name
            self.__file = open(name, 'rb')
            buf = self.__file.read(HEADER_SIZE)
        elif buf:
            self.__name = None
            self.__file = None
        else:
            raise Exception("must provide filename or buffer")
        self.__magicId   = self.__checkMagic(buf[0:8])
        self.__uid       = buf[8:24]
        self.__revNum    = buf[24:26]
        self.__verNum    = buf[26:28]
        self.__byteOrder = buf[28:30]
        self.__setUnpackStrings()
        ssz = self.getShort(buf[30:32])
        sssz = self.getShort(buf[32:34])
        self.__sectorSize = 2 ** ssz
        self.__shortSectorSize = 2 ** sssz
        self.__secTableSectors = self.getLong(buf[44:48])
        self.__firstDirSector = self.getLong(buf[48:52])
        self.__stdStreamMinSize = self.getLong(buf[56:60])
        self.__firstShortSectorTableSector = self.getLong(buf[60:64])
        self.__numShortSectorTableSectors = self.getLong(buf[64:68])
        self.__firstMasterTableSector = self.getLong(buf[68:72])
        self.__numMasterTableSectors = self.getLong(buf[72:76])
        self.__file.seek(0, 2)
        self.__size = self.__file.tell()
        self.__numSectors = int((self.__size - HEADER_SIZE) /
                                self.__sectorSize)
        if self.__numSectors < 2:
            raise Exception("Document truncated")
        self.__loadSat(buf[76:])
        self.__loadSsat()
        self.__loadDir()
        rootEntry = self.__dir[0]
        rootStream = OleStorage.Stream(rootEntry.start, rootEntry.size, False,
                                       self)
        self.__shortStreamContainerStream = rootStream.read()
        entries = self.buildTree(rootEntry.root)
        self.__rootDir = OleStorage.Directory(entries, self)

    def getRootDirectory(self):
        "Access method for top-level storage object."
        return self.__rootDir
    
    def isShortStream(self, size):
        "True if stream is short enough for special storage."
        return size < self.__stdStreamMinSize
    def readSector(self, sectorId):
        "Loads a sector from the file, specified by id."
        self.__file.seek(self.__getSectorOffset(sectorId))
        return self.__file.read(self.__sectorSize)
    def readShortSector(self, sectorId):
        "Loads a short sector from a special storage area."
        start = self.__shortSectorSize * sectorId
        end   = start + self.__shortSectorSize
        return self.__shortStreamContainerStream[start:end]
    def buildTree(self, root, tree = None):
        "Builds a directory list by recursively traversing its tree."
        if tree is None:
            tree = []
        entry = self.__dir[root]
        if entry.left >= 0:
            self.buildTree(entry.left, tree)
        tree.append(entry)
        if entry.right >= 0:
            self.buildTree(entry.right, tree)
        return tree
            
    def getShort(self, s):
        "Unpack a signed short integer from the buffer."
        return struct.unpack(self.__SHORT, s)[0]
    def getLong(self, s):
        "Unpack a signed 4-byte integer from the buffer."
        return struct.unpack(self.__LONG, s)[0]
    def getLongLong(self, s):
        "Unpack a signed 8-byte integer from the buffer."
        return struct.unpack(self.__LONGLONG, s)[0]
    def getFloat(self, s):
        "Unpack a 4-byte floating-point value from the buffer."
        return struct.unpack(self.__FLOAT, s)[0]
    def getDouble(self, s):
        "Unpack an 8-byte floating-point value from the buffer."
        return struct.unpack(self.__DOUBLE, s)[0]
    def getUnicode(self, s):
        "Extract a Unicode string of 16-bit characters from the buffer."
        return unicode(s, self.__UTF16)
    def __getSectorOffset(self, sectorId):
        "Calculate the starting position for a sector from its ID."
        return HEADER_SIZE + self.__sectorSize * sectorId
    def __getShortSectorOffset(self, sectorId):
        """Calculate the starting position for a short sector
        from its ID."""
        return self.__shortSectorSize * sectorId
    def __loadSsat(self):
        "Load the short sector allocation table from the buffer."
        unpackString = "%s%dl" % (self.__ORDER, self.__sectorSize / 4)
        self.ssat = []
        sid = self.__firstShortSectorTableSector
        while sid >= 0:
            buf = self.readSector(sid)
            self.ssat += struct.unpack(unpackString, buf)
            sid = self.sat[sid]
    def __checkMagic(self, s):
        "Verify the integrity of the file by checking its header bytes."
        if s != MAGIC:
            raise Exception("Not a structured storage file")
        return s
    def __loadDir(self):
        self.__dir = []
        sid = self.__firstDirSector
        while sid >= 0:
            buf = self.readSector(sid)
            offset = 0
            while offset < len(buf):
                entry = OleStorage.DirectoryEntry(buf[offset:offset+128], self)
                self.__dir.append(entry)
                offset += 128
            sid = self.sat[sid]
    def __loadSat(self, buf):
        unpackString = "%s%dl" % (self.__ORDER, self.__sectorSize / 4)
        self.sat = []
        offset = 0
        while offset < len(buf):
            sid = self.getLong(buf[offset:offset+4])
            if sid >= 0:
                self.__readSatSector(sid, unpackString)
            offset += 4
        nextSector = self.__firstMasterTableSector
        while nextSector != END_OF_CHAIN:
            buf = self.readSector(nextSector)
            nextSector = self.getLong(buf[-4:])
            offset = 0
            while offset < len(buf) - 4:
                sid = self.getLong(buf[offset:offset+4])
                if sid >= 0:
                    self.__readSatSector(sid, unpackString)
                offset += 4
    def __readSatSector(self, sid, unpackString):
        buf = self.readSector(sid)
        self.sat += struct.unpack(unpackString, buf)
    def __setUnpackStrings(self):
        order = self.__byteOrder == LITTLE_ENDIAN and '<' or '>'
        self.__ORDER    = order
        self.__SHORT    = order + 'h'
        self.__LONG     = order + 'l'
        self.__LONGLONG = order + 'q'
        self.__FLOAT    = order + 'f'
        self.__DOUBLE   = order + 'd'
        self.__UTF16    = order == '<' and 'utf-16-le' or 'utf-16-be'
    def dump(self, f):
        "Debugging method to dump the control information for the file."
        doDump(f, "   magic ID: %s\n" % showBytes(self.__magicId))
        doDump(f, "        UID: %s\n" % showBytes(self.__uid))
        doDump(f, "   revision: %s\n" % showBytes(self.__revNum))
        doDump(f, "    version: %s\n" % showBytes(self.__verNum))
        doDump(f, " byte order: %s\n" % (self.__byteOrder == LITTLE_ENDIAN
                                         and "little endian" or
                                             "big endian"))
        doDump(f, "sector size: %d\n" % self.__sectorSize)
        doDump(f, "   short ss: %d\n" % self.__shortSectorSize)
        doDump(f, " stab sects: %d\n" % self.__secTableSectors)
        doDump(f, "  dir start: %d\n" % self.__firstDirSector)
        doDump(f, "  min ssize: %d\n" % self.__stdStreamMinSize)
        doDump(f, " first ssts: %d\n" % self.__firstShortSectorTableSector)
        doDump(f, "   num ssts: %d\n" % self.__numShortSectorTableSectors)
        doDump(f, "  first mts: %d\n" % self.__firstMasterTableSector)
        doDump(f, "    num mts: %d\n" % self.__numMasterTableSectors)
        doDump(f, "   sat size: %d\n" % len(self.sat))
        doDump(f, "  ssat size: %d\n" % len(self.ssat))
        doDump(f, "    dir len: %d\n" % len(self.__dir))
        #doDump(f, "        sat:")
        #for sid in self.sat:
        #    doDump(f, " %d" % sid)
        #doDump(f, '\n')
        #doDump(f, "       ssat:")
        #for sid in self.ssat:
        #    doDump(f, " %d" % sid)
        #doDump(f, '\n')
        for entry in self.__dir:
            entry.dump(f)
    class DirectoryEntry:
        """
        Holds control information for a single directory entry.

        Attributes:

            name          - Unicode string for the entry's name
            type          - valid values:
                              * USER_STREAM (2)
                              * USER_STORAGE (1)
                              * ROOT_STORAGE (5)
            color         - RED (0) or BLACK (1) for tree balancing
            left, right   - children of the entry node in the tree
            root          - finds the nodes of a directory tree
            start         - offset position for a stream
            size          - number of bytes in a stream
        """
        def __init__(self, buf, oleStorage):
            nameLen       = oleStorage.getShort(buf[64:66]) - 2
            nameBytes     = buf[0:nameLen]
            self.name     = oleStorage.getUnicode(nameBytes)
            self.type     = ord(buf[66])
            self.color    = ord(buf[67])
            self.left     = oleStorage.getLong(buf[68:72])
            self.right    = oleStorage.getLong(buf[72:76])
            self.root     = oleStorage.getLong(buf[76:80])
            self.uid      = buf[80:96]
            self.flags    = buf[96:100]
            self.created  = buf[100:108]
            self.modified = buf[108:116]
            self.start    = oleStorage.getLong(buf[116:120])
            self.size     = oleStorage.getLong(buf[120:124])
        def dump(self, f):
            doDump(f, "-" * 70 + "\n")
            doDump(f, "       name: %s\n" % showUnicode(self.name))
            doDump(f, "       type: %d\n" % self.type)
            doDump(f, "      color: %s\n" % (self.color == RED and "red" or
                                                                   "black"))
            doDump(f, "       left: %d\n" % self.left)
            doDump(f, "      right: %d\n" % self.right)
            doDump(f, "       root: %d\n" % self.root)
            #doDump(f, "        uid: %s\n" % showBytes(self.uid))
            #doDump(f, "      flags: %s\n" % showBytes(self.flags))
            doDump(f, "      start: %d\n" % self.start)
            doDump(f, "       size: %d\n" % self.size)

    class Stream:
        """
        Represents a user stream (the equivalent of a file in the
        standard filesystem).

        Attributes:

            start     - initial position of the stream's bytes
            size      - number of bytes in the stream
            storage   - reference to the OleStorage object which
                        owns this stream
            short     - True=stream is small enough for special
                        storage

        Methods:

            read()    - loads the bytes for a structure storage
                        stream into an 8-bit-character string
        """
        def __init__(self, start, size, short, storage):
            self.start   = start
            self.size    = size
            self.storage = storage
            self.short   = short
        def read(self):
            doc = ''
            sid = self.start
            while sid >= 0:
                if self.short:
                    doc += self.storage.readShortSector(sid)
                    sid = self.storage.ssat[sid]
                else:
                    doc += self.storage.readSector(sid)
                    sid = self.storage.sat[sid]
            if len(doc) < self.size:
                raise Exception("stream truncated")
            return doc[:self.size]
    class Directory:

        """
        Structured storage directory information for one storage
        object.

        Properties:

            entries    - list of directory contents (streams and
                         nexted directories)
            storage    - reference to structure storage object
                         which owns this directory

        Methods:

            open(name) - creates and returns a Stream or Storage
                         object for the named directory member if
                         it exists; otherwise raises an exception
        """
        
        def __init__(self, entries, storage):
            self.entries = entries
            self.storage = storage
        def open(self, name):
            for entry in self.entries:
                if entry.name == name:
                    if entry.type == USER_STREAM:
                        start = entry.start
                        size  = entry.size
                        short = self.storage.isShortStream(size)
                        return OleStorage.Stream(start, size, short,
                                                 self.storage)
                    elif entry.type == USER_STORAGE:
                        entries = self.storage.buildTree(entry.root)
                        return OleStorage.Directory(entries, self.storage)
                    raise Exception("don't know how to open %s" % name)
            return None

if __name__ == "__main__":
    oleStorage = OleStorage(sys.argv[1])
    oleStorage.dump(sys.stdout)
    stream = oleStorage.getRootDirectory().open("Workbook")
    doc = stream.read()
    file = open("Workbook", "wb")
    file.write(doc)
    file.close()
