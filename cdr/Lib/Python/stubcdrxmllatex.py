#----------------------------------------------------------------------
#
# $Id: stubcdrxmllatex.py,v 1.1 2002-02-20 12:59:22 bkline Exp $
#
# Stub for mailer LaTeX generation.
#
# $Log: not supported by cvs2svn $
#----------------------------------------------------------------------
import xml.dom.minidom, re

#----------------------------------------------------------------------
# Global variables.
#----------------------------------------------------------------------
funnyChars = re.compile(u"([\"%<>_])")

class Latex:
    def __init__(self, latex): self.latex = latex
    def getLatex(self): return self.latex
    def getMessages(self): return []
    def getStatus(self): return 0
    def getLatexPassCount(self): return 1

def cleanup(match): 
    char = match.group(0)
    if char == u'%': return u"$\\%$"
    if char == u'"': return u"\\tQ{}"
    if char == u'_': return u"\\_"
    return u"$" + char + u"$"

def cleanupText(s):
    return re.sub(funnyChars, cleanup, s)

#----------------------------------------------------------------------
# Extract the text content of a DOM element.
#----------------------------------------------------------------------
def getTextContent(node):
    text = u''
    if  node:
        for n in node.childNodes:
            if n.nodeType == xml.dom.minidom.Node.TEXT_NODE:
                text = text + n.nodeValue
    return cleanupText(text).encode('latin-1')

def getChildNode(parent, name):
    childNodes = parent.getElementsByTagName(name)
    if childNodes: return childNodes[0]
    return None

def makeSummary(doc, mailType):
    docElem = doc.documentElement
    title   = getTextContent(docElem.getElementsByTagName("SummaryTitle")[0])
    sections = []
    for sect in docElem.getElementsByTagName("SummarySection"):
        titleList = sect.getElementsByTagName("Title")
        if titleList:
            sections.append(getTextContent(titleList[0]))

    latex = """\
\\documentclass[letterpaper|11pt]{article}
\\begin{document}
\\title{%s}
\\maketitle
\\section{Board Member}
@@BoardMember@@
\\section{Job Number}
@@MailerDocId@@
\\newpage
\\section{Sections}
\\begin{enumerate}
""" % title
    for section in sections:
        latex +=  "\\item %s\n" % section
    latex += "\\end{enumerate}\n\\end{document}\n"
    return Latex(latex)

def makeProtocol(doc, mailType):
    docElem = doc.documentElement
    titleElem = getChildNode(docElem, "Title")
    if titleElem:
        titleText = getTextContent(titleElem)
        title = titleText
    else:
        title = "*** Unable to find title ***"
        
    latex = """\
\\documentclass[letterpaper|11pt]{article}
\\begin{document}
\\title{%s}
\\maketitle
\\section{Recipient}
@@Recipient@@
\\section{Job Number}
@@MailerDocId@@
""" % title

    if mailType == "SP":
        leadOrg = getChildNode(docElem, "LeadOrg")
        leadOrgName = getTextContent(getChildNode(leadOrg, "OrgName"))
        leadOrgId   = getTextContent(getChildNode(leadOrg, "OrgId"))
        leadOrgStat = getTextContent(getChildNode(leadOrg, "Status"))
        latex += """\
\\section{Lead Organization}
%s (%s)
\\section{Status}
%s
""" % (leadOrgName, leadOrgId, leadOrgStat)
        particOrgs = leadOrg.getElementsByTagName("ParticipatingOrg")
        if particOrgs:
            latex += """\
\\section{Participating Organizations}
\\begin{enumerate}
"""
            for org in particOrgs:
                name = getTextContent(getChildNode(org, "OrgName"))
                id   = getTextContent(getChildNode(org, "OrgId"))
                latex += """\
\\item{%s (%s)}
""" % (name, id)
                contacts = org.getElementsByTagName("Contact")
                if contacts:
                    latex += "\\begin{itemize}\n"
                    for contact in contacts:
                        forename = getTextContent(getChildNode(contact,
                                                               "GivenName"))
                        surname = getTextContent(getChildNode(contact, 
                                                              "Surname"))
                        role = getTextContent(getChildNode(contact, "Role"))
                        latex += """\
\\item{%s %s (%s)}
""" % (forename, surname, role)
                    latex += "\\end{itemize}\n"
            latex += "\\end{enumerate}\n"

        latex += "\\end{document}\n"
        return Latex(latex)
        
    diagnoses = docElem.getElementsByTagName("Diagnosis")
    if diagnoses:
        latex += """\
\\section{Diagnoses}
\\begin{enumerate}
"""
        for diagnosis in diagnoses:
            latex += """\
\\item %s
""" % getTextContent(diagnosis)
    if diagnoses:
        latex += "\\end{enumerate}\n"
    latex += """\
\\section{Abstract}

Magnus es, domine, et laudabilis valde: magna virtus tua, et sapientiae tuae non est numerus. et laudare te vult homo, aliqua portio creaturae tuae, et homo circumferens mortalitem suam, circumferens testimonium peccati sui et testimonium, quia superbis resistis: et tamen laudare te vult homo, aliqua portio creaturae tuae.tu excitas, ut laudare te delectet, quia fecisti nos ad te et inquietum est cor nostrum, donec requiescat in te. da mihi, domine, scire et intellegere, utrum sit prius invocare te an laudare te, et scire te prius sit an invocare te. sed quis te invocat nesciens te? aliud enim pro alio potest invocare nesciens. an potius invocaris, ut sciaris? quomodo autem invocabunt, in quem non crediderunt? aut quomodo credent sine praedicante? et laudabunt dominum qui requirunt eum. quaerentes enim inveniunt eum et invenientes laudabunt eum. quaeram te, domine, invocans te, et invocem te credens in te: praedicatus enim es nobis. invocat te, domine, fides mea, quam dedisti mihi, quam inspirasti mihi per humanitatem filii tui, per ministerium praedicatoris tui.

Et quomodo invocabo deum meum, deum et dominum meum, quoniam utique inme ipsum eum invocabo, cum invocabo eum? et quis locus est in me, quoveniat in me deus meus? quo deus veniat in me, deus, qui fecit caelum et terram? itane, domine deus meus, est quiquam in me, quod capiat te? an vero caelum et terra, quae fecisti et in quibus me fecisti, capiuntte? an quia sine te non esset quidquid est, fit, ut quidquid est capiat te? quoniam itaque et ego sum, quid peto, ut venias in me, quinon essem, nisi esses in me? non enim ego iam in inferis, et tamen etiam ibi es. nam etsi descendero in infernum, ades. non ergo essem, deus meus, non omnino essem, nisi esses in me. an potius non essem, nisi essem in te, ex quo omnia, per quem omnia, in quo omnia? etiam sic, domine, etiam sic. quo te invoco, cum in te sim? aut unde venias in me? quo enim recedam extra caelum et terram, ut inde in me veniat deus meus, qui dixit: caelum et terram ego impleo? 

Capiunt ergone te caelum et terra, quoniam tu imples ea? an imples et restat, quoniam non te capiunt? et quo refundis quidquid impleto caeloet terra restat ex te? an non opus habes, ut quoquam continearis, qui contines omnia, quoniam quae imples continendo imples? non enim vasa, quae te plena sunt, stabilem te faciunt, quia etsi frangantur non effunderis. et cum effunderis super nos, non tu iaces, sed erigis nos,nec tu dissiparis, sed colligis nos. sed quae imples omnia, te toto imples omnia. an quia non possunt te totum capere omnia, partem tui capiunt et eandem partem simul omnia capiunt? an singulas singula et maiores maiora, minores minora capiunt? ergo est aliqua pars tua maior, aliqua minor? an ubique totus es et res nulla te totum capit?

\\end{document}
"""
    return Latex(latex)

def makeLatex(doc, docFormatName, mailTypeName):
    if docFormatName == "Summary":
        return makeSummary(doc, mailTypeName)
    elif docFormatName == "InScopeProtocol":
        return makeProtocol(doc, mailTypeName)
