"""Convert XML to LaTeX

A module called by the CDR mailer program to convert XML to LaTeX
for fancy printing of documents sent out in mailers.

See cdrxmllatex.txt for high level documentation of this module.
"""

import sys, os, time, string, re, xml.dom.minidom, UnicodeToLatex

from cdrlatexlib import XProc, ProcParms, XmlLatexException, findControls


####################################################################
# Globals
####################################################################
G_outString = ""        # Output string
G_debug     = 0         # True=output debugging info


####################################################################
# Constants
####################################################################

# How to order an output XProc relative to the record as a whole, or
#   to surrounding XProc objects.

# Flags controlling continued execution of a chain of procedures
EXEC_OK        = 0  # Everything fine, keep working
EXEC_DONE      = 1  # Stop executing chain, we're done

####################################################################
# Classes
####################################################################


#-------------------------------------------------------------------
# Timer
#
#   Used for profiling performance during development.
#-------------------------------------------------------------------
class Timer:

    # Static variables
    startTime = curTime = time.clock()

    # Start the clock running
    def startClock(self):
        startTime = curTime = time.clock()

    # Write a time stamp to stderr
    def stamp(self,msg):
        now = time.clock()
        # sys.stderr.write ("%f, %f: %s\n" % (now - Timer.curTime,
        #                  now - Timer.startTime, msg))
        Timer.curTime = now


#-------------------------------------------------------------------
# ProcNode
#
#   A structure relating dom nodes in the input tree to their
#   processing instructions, and to related related nodes in the
#   output tree.
#
#   An tree of ProcNodes is created containing references to all
#   of the dom nodes to be output, in the order of output.
#
#   Not every output dom node need appear in the tree.  If a particular
#   node is processed by functions which know how to deal with the
#   children of that node, or for that matter, know how to deal with
#   any other node, the node may not be referenced in this tree.
#
#
#-------------------------------------------------------------------
class ProcNode:

    #---------------------------------------------------------------
    # Construct a ProcNode
    #
    # Pass:
    #   Reference to input dom node if there is one.
    #      There won't be if the next thing to do is process something
    #      independently of any specific xml element.
    #   Reference to processing instructions.
    #   Reference to ordering string used to sort these things.
    #---------------------------------------------------------------
    def __init__(self, inDom, xProc, ordinal):
        self.dom     = inDom
        self.xProc   = xProc
        self.ordinal = ordinal
        self.sibling = None
        self.child   = None

    #---------------------------------------------------------------
    # Sort comparison routine for sorting ProcNodes
    #---------------------------------------------------------------
    def __cmp__ (self, other):
        if other == None:
            return 1
        if self.ordinal < other.ordinal:
            return -1
        if self.ordinal > other.ordinal:
            return 1
        return 0

    #---------------------------------------------------------------
    # execProcs
    #   Execute pre or post Procs
    #
    # Pass:
    #   Reference to top node of the document, in case access to
    #     other parts of the record are needed.
    #   Reference to a dom node to process.  May be None if this
    #     XProc was generated for a fixed routine, not associated
    #     with a particular input XML element.
    #   Reference to list of procs - may be pre or post.
    #
    # Return:
    #   Void.
    #
    #   May raise exception if severe error.
    #---------------------------------------------------------------
    def execProcs (self, topNode, curNode, procList):

        # If no proc list to process, we're done
        if procList != None:
            pp = ProcParms (topNode, self.dom, None, '')
            for proc in procList:

                # Find args for called proc, if there are any
                if len (proc) > 1:
                    pp.args = proc[1]
                else:
                    pp.args = None

                # Call it
                rc = proc[0](pp)
                if rc == EXEC_DONE:
                    break

            # If the entire chain produced any output, add it to the Latex
            if pp.getOutput() != '':
                outString (pp.getOutput())

    #---------------------------------------------------------------
    # execute
    #
    #   Execute the instructions in a ProcNode.
    #
    # Pass:
    #   Reference to top node of the document, in case access to
    #     other parts of the record are needed.
    #   Reference to a dom node to process.  May be None if this
    #     XProc was generated for a fixed routine, not associated
    #     with a particular input XML element.
    #
    # Return:
    #   True(1)  = Everything okay to continue.
    #   False(0) = Abort processing of any nodes under this one.
    #
    #   May raise exception if severe error.
    #---------------------------------------------------------------
    def execute (self, topNode):

        # Simplify reference
        xp = self.xProc

        # If nothing to do, skip fancy processing
        if xp != None:

            # If there's a prefix, send it
            if xp.prefix != None:
                outString (xp.prefix)

            # Execute any pre-processing routines
            self.execProcs (topNode, self.dom, xp.preProcs)

        # If this is a text node, it's here in the output tree
        #   because we're supposed to output the text
        if self.dom != None:
            if self.dom.nodeType == self.dom.TEXT_NODE:
                outString (self.dom.nodeValue)

        # If there are child nodes to process, do them all
        child = self.child
        while child != None:
            rc = child.execute (topNode)
            if rc != EXEC_DONE:
                child = child.sibling

        if xp != None:

            # Execute any post-processing routines
            self.execProcs (topNode, self.dom, xp.postProcs)

            # If there's a suffix, send it
            if xp.suffix != None:
                outString (xp.suffix)

        return EXEC_OK

    #---------------------------------------------------------------
    # toString
    #   Dump contents to string for debugging.
    #---------------------------------------------------------------
    def toString (self):
        str='--- ProcNode ---\ndomNode='
        if self.dom != None:
            str += self.dom.nodeName
            if self.dom.nodeType == self.dom.TEXT_NODE:
                str += "\n--Text--\n%s\n--End Text--\n" % self.dom.nodeValue
        else:
            str += 'None'
        str += '  ordinal=('
        if self.ordinal != None:
            for n in self.ordinal:
                if n != None:
                    str += '%d,' % n
                else:
                    str += 'None'
        else:
            str += 'None'
        str += ')'
        str += '  sibling='
        if self.sibling==None:
            str += 'No'
        else:
            str += 'Yes'
        str += '  child='
        if self.child==None:
            str += 'No'
        else:
            str += 'Yes'
        str += '\n'
        if self.xProc != None:
            str += self.xProc.toString()
        else:
            str += "No XProc\n"

        return str


#-------------------------------------------------------------------
# Ctl
#
#   Encapsulates all the information controlling a particular document
#   and format type for LaTeX conversion.
#
# Fields:
#   topNode      - xml dom node for record as a whole.
#   docFmt       - Format info passed by top level caller.
#   fmtType      - Format subtype info passed by top caller.
#   instBlock    - Dictionary of processing instructions.
#   elementIndex - Index into instBlock:
#                    Key   = xml element tag.
#                    Value = Reference to instructions for processing
#                            this tag.
#   procNodeList - A list of procNodes (q.v.) that will be processed
#                    for output.
#   elementOccs  - Dictionary of occurrence counts for elements we're counting
#                    Key   = Element tag
#                     With fully qualified path for ORDER_PARENT/DOCUMENT.
#                     Without it for ORDER_TOP.
#                    Value = Count of occurrences.
#                  The fully qualified path includes parent occurrence numbers
#                    origin 0, e.g.:
#                       /Citation=1
#                       /Citation:0/Author=5
#                       /Citation:0/Author:2/LastName=1
#                    Interpretation:
#                       1 Citation
#                       5 Authors within that citation
#                       1 LastName within the the third Author
#                            (numbered as 0,1,2,3,4)
#
#-------------------------------------------------------------------
class Ctl:

    #---------------------------------------------------------------
    # Constructor
    #---------------------------------------------------------------
    def __init__ (self, topNode, docFmt, fmtType):
        # Save passed information about the doc and format
        self.topNode = topNode
        self.docFmt  = docFmt
        self.fmtType = fmtType

        # Create an empty list to receive ProcNodes for each
        #   of the processing instructions to execute.
        self.procNodeList = []

        # Seed it with an empty procNode that functions as the top level
        self.procNodeList.append (ProcNode (None, XProc(), ()))

        # Create an empty occurrence dictionary so we can track
        #   occurrence counts for elements for which occs are limited
        self.elementOccs = {}

        # Create an empty dictionary of parent orders
        # This is used in ORDER_DOCUMENT output ordering to find out
        #   where all the nodes that are document ordered go under any
        #   given parent.
        # It's a subtle concept perhaps.  See the comments under
        #   createProcessingNode() for ORDER_DOCUMENT.
        self.orderDoc = {}

        # Find the instructions for processing this info
        self.instBlock = findControls (docFmt, fmtType)
        ### self.instBlock = cdrlatexlib.findControls (docFmt, fmtType)

        # Generate indexing info into the instructions
        # Could cache these in a future version if desired,
        #  but this is probably a tiny part of the total mailer
        #  effort for one document, so let's not complicate things
        self.indexOrders()

        # We'll construct a map of nodes beginning with the DOM
        #   node whose ordinal number is 0
        self.domNodeNum = 0

        # Generate an output map of the input record
        self.mapNodes()

        # For debug
        if G_debug > 0:
            print '---Output processing map---\n'
            for n in self.procNodeList:
                print n.toString()
            print '---End of Output processing map---\n'


    #---------------------------------------------------------------
    # indexOrders
    #
    #   Construct indexes allowing us to find out the order in
    #   which stand-alone and element processing instructions are
    #   executed.
    #
    #   The indexes serve two purposes:
    #       To find instructions for processing any element.
    #       To enable us to know the order of occurrence of elements
    #         and program generated data in the output LaTeX stream.
    #---------------------------------------------------------------
    def indexOrders (self):
        order = 0
        self.elementIndex = {}

        # Read all instructions for this mailer type
        # Each instruction in the block is an XProc object
        for xp in self.instBlock:
            # Assume that a string is an element tag
            # Index tag as key to instruction.
            if xp.element != None:
                self.elementIndex[xp.element] = order

            # Instructions not attached to a node is the other alternative
            # Put them directly into the processing list
            # They have no element occurrence numbers, so we use 0
            #   to preserve lengths for matching in the tree build
            #   (Really only there for debugging)
            else:
                nodeOrder = (order, 0)
                self.procNodeList.append (ProcNode (None, xp, nodeOrder))

            # Next instruction object gets the next ordinal number
            order += 1


    #---------------------------------------------------------------
    # createProcessingNodes
    #
    #   Create a list of nodes to be later sorted into processing
    #   order and organized into a tree.
    #
    #   Works recursively.  Call it first on the top node of the
    #   document.  It will process that node then call itself on
    #   each of its child nodes.
    #
    # Pass:
    #   Reference to DOM node to be examined.
    #   Current output order, a list of digit pairs, e.g.,
    #         [(0,0),(4,0),(4,1)]
    #       where each pair of digits represents an array index into
    #       processing instructions for this node, plus an ordinal
    #       element number, as given by a pre-order traverse of the
    #       DOM tree.
    #   Current occurrence path for checks and insertions into the
    #       elementOccs dictionary.
    #       Path looks like: /doctag:0/Title:1/TitleText:0 ... etc.
    #   Current name path for checking for absolute path to element
    #       Path looks like: /doctag/Title/TitleText ... etc.
    #   Quantity to add to the intruction number to derive the actual
    #       sort order for this node.
    #       The addition is always 0 or DOC_ORDER (100000).
    #       The number is set to DOC_ORDER when any left sibling of
    #       a node is processed as ORDER_DOCUMENT.
    #       This forces all right siblings, whether ORDER_PARENT, or
    #       ORDER_DOCUMENT, to come after all previous siblings that
    #       were ORDER_PARENT.
    #---------------------------------------------------------------
    def createProcessingNodes (self, domNode, parentOrder, \
                               occPath, namePath, addToInstNum):

        # A constant node number used in place of an instruction ordinal
        #   for nodes which are ORDER_DOCUMENT.
        # This constant forces all ORDER_DOCUMENT and text nodes to appear
        #   after any ORDER_PARENT nodes, and among themselves, to appear
        #   in the order that the DOM nodes appeared in the XML.
        # It is just an arbitrarily high number
        DOC_ORDER = 100000

        # When processing children of a node, the first child we see with
        #   order == ORDER_DOCUMENT, causes all ORDER_DOCUMENT siblings
        #   after it to be sorted into the order they appear in the document,
        #   and all ORDER_PARENT siblings that come after it to come after
        #   all of the ORDER_DOCUMENTS.
        # [I only argue that the code for this is simple and correct,
        #   not that it's easy to understand.]
        addToSiblingInstNum = 0

        # No order established for this node yet
        nodeOrder = None

        # Append the current element name to the paths to this element
        occPath  += '/' + domNode.nodeName
        namePath += '/' + domNode.nodeName

        # Get the current count of occs of this element
        occCount = self.elementOccs.get (occPath, 0)

        # Update tracking info
        self.elementOccs[occPath] = occCount + 1

        # Setup a new occPath to pass to our descendents
        # Note we're using origin zero here, not occCount + 1
        occPath += ":%d" % occCount

        # See if there's anything to do with the passed domNode
        # First look for an absolute reference
        instNum = self.elementIndex.get (namePath, None)

        # If not found, try for a name relative to the current position
        if (instNum == None):
            instNum = self.elementIndex.get (domNode.nodeName, None)

        # Reference to processing instructions, not established yet
        xp = None

        # If we've found something, process it
        if instNum != None:
            xp = self.instBlock[instNum]

            # This is the domNodeNum-th dom node we're going to output
            self.domNodeNum = self.domNodeNum + 1

            # Create an output order string for it
            # This will be used as a sort key to build the output tree
            #  in proper order
            if xp.order == XProc.ORDER_TOP:
                # Make this a top level element, replacing passed parent order
                nodeOrder = (instNum + addToInstNum, self.domNodeNum)

                # Override assumption about how we check for occurrence paths
                # Because this is ORDER_TOP processing, any element with
                #   this name seen anywhere in the document counts as
                #   an occurrence
                # Note that we are updating the elementOccs dictionary at
                #   two points - a top point and a fully qualified point
                # There's no harm in this and may be some benefit
                # Example:
                #   Assume:
                #     XProc(element='foo' order=XProc.ORDER_TOP), # doctag key
                #     XProc(element='bar' order=XProc.ORDER_TOP)
                #   Produces the following 3 keys
                #     elementOccs['/foo'] = 1
                #     elementOccs['/foo:0/bar'] = 1
                #     elementOccs['/bar'] = 1
                topOccPath = '/' + domNode.nodeName
                occCount = self.elementOccs.get (topOccPath, 0)
                self.elementOccs[topOccPath] = occCount + 1

            elif xp.order == XProc.ORDER_PARENT:
                # Order it within the parent
                nodeOrder = parentOrder + \
                                (instNum + addToInstNum, self.domNodeNum)

            elif xp.order == XProc.ORDER_DOCUMENT:
                # This node is processed:
                #   Within its parent
                #   After all nodes that are before it in the instructions
                #   Before all nodes after it in the instructions
                #   Intermingled with other ORDER_DOCUMENT nodes.
                # We achieve this by using a single major order number
                #   for all ORDER_DOCUMENT nodes within this parent
                nodeOrder = parentOrder + (DOC_ORDER, self.domNodeNum)
                addToSiblingInstNum = DOC_ORDER

                # if not self.orderDoc.has_key (parentOrder):
                #     # We haven't yet seen any ORDER_DOCUMENT nodes, this
                #     #   is the first, set the ordinal number that all of
                #     #   them will use
                #     self.orderDoc[parentOrder] = instNum
                # nodeOrder = parentOrder + (\
                #             self.orderDoc[parentOrder], self.domNodeNum)
            else:
                # Error - can't happen
                raise XmlLatexException (\
                  "Bad inst order number %d - Element %s - BUG" % \
                  (instNum, xp.element))

            # Check to see if we've already processed the max number
            #  of occurrences we're interested in
            if xp.occs != 0 and occCount >= xp.occs:
                # We don't do any more with this element or its children
                return

            # If we're looking only for elements with a specific attribute,
            #   or without a specific attribute, filter them here
            # If failed check, don't descend into subelements
            if xp.checkAttr (domNode) == 0:
                return

            # Append the node to our list of nodes
            # This is the key step that causes this domNode to be
            #   processed at output time
            self.procNodeList.append (ProcNode(domNode, xp, nodeOrder))

            # Is processing of descendents disallowed?
            if xp.descend == 0:
                return

        # Check out all the descendents of this node
        if domNode.hasChildNodes():

            # If we haven't established an explicit order for this node
            if nodeOrder == None:

                # Use the parent node as the order for children
                # This tends to flatten the tree - which is counter-intuitive
                #   but (I believe) correct.
                nodeOrder = parentOrder

            # Process each child
            # addToChildInstNum = addToSiblingInstNum
            addToChildInstNum = 0
            for node in domNode.childNodes:

                # Text children are processed if this is a textOut XProc
                if node.nodeType == node.TEXT_NODE:

                    # If we're supposed to output it
                    if xp != None and xp.textOut:

                        # Increment our node counter
                        self.domNodeNum = self.domNodeNum + 1
                        childOrder = nodeOrder + (DOC_ORDER, self.domNodeNum)
                        addToChildInstNum = DOC_ORDER

                        # childOrder = nodeOrder + (instNum, self.domNodeNum)

                        # Output a new ProcNode for it
                        # No XProc is required.  At output time we'll see that
                        #   the node is a text node and just output it.
                        # Any prefixes, suffixes, etc., are processed only at
                        #   the ELEMENT_NODE level, not the text level.
                        self.procNodeList.append (\
                            ProcNode (node, None, childOrder))

                # Elmenent children get recursed into
                if node.nodeType == node.ELEMENT_NODE:
                    tmp = self.createProcessingNodes (node, nodeOrder, \
                               occPath, namePath, addToChildInstNum)

                    # If any child is in document order, all siblings of
                    #   that child are affected
                    if tmp == DOC_ORDER:
                        addToChildInstNum = DOC_ORDER

        return addToSiblingInstNum


    #---------------------------------------------------------------
    # mapNodes
    #
    #   Construct a tree of processing instructions in the order in
    #   which they are to be executed for this particular input record.
    #
    #   The tree contains a combination of processing instructions with
    #   or without input DOM node references.  Each element in the list
    #   (self.procNodeList) has a sort key which is generated by
    #   considering the order in which the instruction object (a ProcNode)
    #   appears in the list of instructions, and the order in which the
    #   input XML element appeared in the DOM.  See createProcessingNodes()
    #   for how these keys (pairs of numbers) are assigned.
    #
    #   After creating the nodes with the keys, we sort the list by the
    #   keys and, _voila_, we have a list of directions for how to
    #   transform this XML record into LaTeX, in the order in which
    #   to execute them.
    #
    #   We then link the nodes in the list together into a tree.  That
    #   allows us to execute prefixes and preProcs for a node, then
    #   descend to process it's child nodes, then execute postProcs
    #   and suffixes for the node.
    #---------------------------------------------------------------
    def mapNodes (self):

        # Walk the input dom, creating a list of objects
        #   to be processed
        self.createProcessingNodes (self.topNode, (), "", "", 0)

        # Sort the list by the generated node orders
        # ProcNodes know how to sort themselves
        self.procNodeList.sort()

        # Turn the list into a tree
        # The idea here is that:
        #   The length of the sort key gives us the hierarchical level.
        #   When a sort key increases, it can only increase by one.
        # We build the tree using an array of nodes, one element for
        #   each of the most recent seen nodes at that hierarchical level
        # Given our input format, this is even simpler than recursion
        # The tree does not necessarily have a root node.  Example:
        #    A--------->B------>C
        #    |          |       |
        #    D->E->F    G       H->I
        #       |       |
        #       J->K    L->M->N->O
        #
        # See createProcessingNodes() for how the input to this process
        #   is created.
        # I believe the tree creation process is efficient and correct,
        #   but counter-intuitive.  See separate documentation for more
        #   explanation.

        # Create an array of procNodes holding references to the last
        #   seen nodes at each level
        nodeLevel = []

        # Top left (A above) is the first node to process
        nodeLevel.append (self.procNodeList[0])
        lvl = 0

        # Relate the other nodes to it
        for n in self.procNodeList[1:]:

            # Get length of sort key
            nlen = len (n.ordinal)

            # If greater than predecessor, it's a child
            if nlen > len (nodeLevel[lvl].ordinal):
                # Link it
                nodeLevel[lvl].child = n

                # Deepen the nodeLevel of the hierarchy
                nodeLevel.append (n)
                lvl += 1

            # If less, it's a sibling of an ancestor
            # Find the first ancestor with the same length
            else:
                # Neither side of '<' can be less than 1, so
                #   the loop always terminates without running out of data
                while nlen < len (nodeLevel[lvl].ordinal):
                    nodeLevel.pop()
                    lvl -= 1


                # Whether we popped any or not, we're now
                #   at the same level where the new node goes
                # Link the new to the old and replace it in our temp list
                leftSibling = nodeLevel.pop()
                leftSibling.sibling = n
                nodeLevel.append (n)


    #---------------------------------------------------------------
    # getProcTree
    #
    #   Get the top of the tree of ProcNodes.
    #---------------------------------------------------------------
    def getProcTree (self):
        return self.procNodeList[0]

    #---------------------------------------------------------------
    # getDomTree
    #
    #   Get the top of the tree of input DOM nodes.
    #---------------------------------------------------------------
    def getDomTree (self):
        return self.topNode


####################################################################
# Common routines
####################################################################

#-------------------------------------------------------------------
# outString()
#
# Output data.
# Use this so that we have a single place to apply something to all
#   output data, if required.
#
# Uses global _outString as target for output.
#
# Pass:
#   String to output, as format string
#   Optional additional arguments, if required for embedded %formats.
#       Must be in proper format, i.e., tuple or single item.
#
# Return:
#   None.
#-------------------------------------------------------------------

def outString (s, args=None):
    global G_outString

    if args==None:
        tmpStr = s
    else:
        tmpStr = s % args

    if type(tmpStr) == type(u""):
        # Map to Latin-1 and do something with characters that don't fit
        tmpStr = UnicodeToLatex.convert (tmpStr)

    G_outString = G_outString + tmpStr


#-------------------------------------------------------------------
# outClear()
#
# Initialize output data.
#-------------------------------------------------------------------

def outClear ():
    global G_outString
    G_outString = ""


#-------------------------------------------------------------------
# outLatex()
#
# Pre-processor for outString to escape backslashes.
#
# Pass:
#   String to output, as format string
#   Optional additional arguments, if required for embedded %formats.
#       Must be in proper format, i.e., tuple or single item.
#
# Return:
#   None.
#-------------------------------------------------------------------

def outLatex (s, args=None):
    tmp = string.replace (s, "\\", "\\\\")
    outString (tmp, args)


####################################################################
# LatexDoc
#
#   Contains results of a conversion.
####################################################################

class LatexDoc:
    def __init__(self, latex="", msgs=None, status=0, latexPassCount=2):
        self.latex          = latex
        self.msgs           = msgs
        self.status         = status
        self.latexPassCount = latexPassCount

    def getLatex(self):
        return self.latex

    def getMessages(self):
        return self.msgs

    def getStatus(self):
        return self.status

    def getLatexPassCount(self):
        return self.latexPassCount


####################################################################
# makeLatex
#
#   Top level function for converting XML to LaTeX
#
# Pass:
#   xml         XML to be converted, either as a string or a DOM.
#   docFmt      Document format to use, an agreed upon name string.
#   fmtType     Subtype within docFmt, if applicable, another agreed name.
#
# Returns:
#   Tuple of:
#       Object of type LatexDoc containing all desired information.
#       Number of passes through LaTeX required for this document.
#
# Throws:
#   makeLatexException
####################################################################

def makeLatex (Xml, docFmt, fmtType='', params=None):

    global G_debug

    # Turn on debugging if environment variable says to
    if os.environ.has_key ('XMLLATEXDBG'):
        G_debug = 1
    else:
        G_debug = 0

    # Time stamp for profiling
    Timer().startClock()
    Timer().stamp ("--makeLatex called")

    # Must be passed a string or dom, check and convert if required
    if (type (Xml) == type ("") or type (Xml) == type (u'')):
        Timer().stamp ("About to parse document")
        topNode = xml.dom.minidom.parseString(Xml).documentElement
        Timer().stamp ("Finished parsing document")
    elif Xml.__class__ == xml.dom.minidom.Document:
        Timer().stamp ("About to extract document element")
        topNode = Xml.documentElement
        Timer().stamp ("Finished extract of document element")
    elif Xml.__class__ != xml.dom.minidom.Element:
        raise XmlLatexException (
            "Xml argument not type string, DOM, or Element type=%s"
            % str (type (Xml)))
    else:
        topNode = Xml
    Timer().stamp ("Xml parameter checking completed")

    # None = empty string in fmtType
    if fmtType == None:
        fmtType = ''

    # Clear any previous output
    outClear()

    # Save parameters in global dictionary
    G_calling_parms = params

    # Output the preamble required to make Unicode->Latex conversion work
    # XXXX Can't do it here because it can't be the first thing output
    # XXXX Ideally, should NOT do it as part of the fixed constants
    # XXXX   in cdrlatexlib.py
    # outString (UnicodeToLatex.getPreamble())

    # Construct a control object for this docFmt and fmtType
    # Constructor generates index, output tree, etc. for control info
    Timer().stamp ("Loading control table")
    ctl = Ctl (topNode, docFmt, fmtType)

    # Execute the tree
    Timer().stamp ("Executing processing tree")
    ctl.getProcTree().execute(topNode)

    # Get the results
    Timer().stamp ("Returning results")
    doc = LatexDoc (G_outString)

    return doc
