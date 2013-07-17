#----------------------------------------------------------------------
# Import module to hold common information needed by both
# CTGovUpdateReport.py and CTGovUpdateReportBatch.py.
#
# $Id$
#
#----------------------------------------------------------------------
import cdr

JOB_NAME    = "Imported CTGovProtocols vs. CWDs"
REPORT_BASE = cdr.BASE_DIR + "/reports/"
REPORT_FILE = "CTGov-CWD-diffs.html"
SCRIPT      = "CTGovUpdateReportBatch.py"
REPORT_URL  = cdr.getHostName()[2] + "/cdrreports/%s" % REPORT_FILE
LF          = cdr.DEFAULT_LOGFILE
