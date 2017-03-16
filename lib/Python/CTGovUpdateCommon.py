#----------------------------------------------------------------------
# Import module to hold common information needed by both
# CTGovUpdateReport.py and CTGovUpdateReportBatch.py.
#----------------------------------------------------------------------
import cdr

JOB_NAME    = "Imported CTGovProtocols vs. CWDs"
REPORT_BASE = "D:/cdr/reports/"
REPORT_FILE = "CTGov-CWD-diffs.html"
SCRIPT      = "CTGovUpdateReportBatch.py"
REPORT_URL  = "https://%s.%s/cdrreports/%s" % (cdr.h.host['APPC'][0],
                                               cdr.h.host['APPC'][1],
                                               REPORT_FILE)
LF          = cdr.DEFAULT_LOGFILE
