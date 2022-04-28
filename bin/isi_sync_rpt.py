#!/usr/local/venv/bin/python3
"""Get and report on Isilon sync policies.
"""
__author__ = "michael.s.denney@gmail.com"
__version__ = "2.0.0"

import sys
import os
import pprint
import logging
import io
import copy 
import re
import shutil
import time

os.chdir(os.path.dirname(os.path.realpath(__file__)))
sys.path.append("../pylib")
import hosts
import rpt
import util
###########################################################################
## GLOBALS
###########################################################################
jobsheader = ['Isilon', 'Location', 'ID','State','Action','Bytes Transferred','Duration','End Time','Error Checksum Files Skipped','Error Io Files Skipped','Error Net Files Skipped','File Data Bytes','Files Changed','Job ID','New Files Replicated','Num Retransmitted Files','Policy ID','Retransmitted Files','STF_PHASE_CC_DIR_CHGS End Time','STF_PHASE_CC_DIR_CHGS Start Time','STF_PHASE_CC_DIR_DEL End Time','STF_PHASE_CC_DIR_DEL Start Time','STF_PHASE_CT_DIR_DELS End Time','STF_PHASE_CT_DIR_DELS Start Time','STF_PHASE_CT_DIR_LINKS End Time','STF_PHASE_CT_DIR_LINKS Start Time','STF_PHASE_CT_FILE_DELS End Time','STF_PHASE_CT_FILE_DELS Start Time','STF_PHASE_CT_LIN_UPDTS End Time','STF_PHASE_CT_LIN_UPDTS Start Time','STF_PHASE_FLIP_LINS End Time','STF_PHASE_FLIP_LINS Start Time','STF_PHASE_IDMAP_SEND End Time','STF_PHASE_IDMAP_SEND Start Time','STF_PHASE_SUMM_TREE End Time','STF_PHASE_SUMM_TREE Start Time','Source Directories Created','Source Directories Deleted','Source Directories Linked','Source Directories Unlinked','Source Directories Visited','Source Files Deleted','Sparse Data Bytes','Start Time','Sync Type','Target Directories Created','Target Directories Deleted','Target Directories Linked','Target Directories Unlinked','Target Files Deleted','Target Snapshots','Total Data Bytes','Total Files','Total Network Bytes','Total Phases','Up To Date Files Skipped','User Conflict Files Skipped','Errors']
issueheader = ['Isilon','Location','Policy','Issue']
sheader = ["Isilon","Name","ID","Path","Size","State","Alias Target Name","Has Locks","% Filesystem","Created","Alias Target ID","Expires","Schedule","Shadow Bytes","% Reserve"]
stable = rpt.Table(sheader)
jtable = rpt.Table(jobsheader) 
issuerpt = rpt.Table(issueheader)
###########################################################################
def append_issue(isiname='NA',location='NA',policyname='NA',msg='NA'):
###########################################################################
    """Append issue message to issuerpt"""

    log.debug(msg)
    issuerpt.Isilon.append(isiname)
    issuerpt.Location.append(location)
    issuerpt.Policy.append(policyname)
    issuerpt.Issue.append(msg)
###########################################################################
def check_policy(isiname=None,location=None,inner=None):
###########################################################################
    ##Returns True if issue detected , False if all well.
    policyname=inner['name']
    log.info("Checking policy schedule {}".format(policyname))
    #if inner['enabled'] != 'True':
    pprint.pprint(inner)
    if not inner['enabled']:
        log.info("{} policy not enabled".format(policyname))
        return False
    if inner['schedule'] == 'Manually scheduled':
        log.info("{} policy manually scheduled".format(policyname))
        return False
    if inner['schedule'] == '':
        log.info("{} policy schedule blank".format(policyname))
        return False
    log.debug("CHECKING STATE {}".format(inner['has_sync_state']))
    #if inner['has_sync_state'] != 'True':
    if not inner['has_sync_state']:
        sync = inner['has_sync_state']
        msg = ("has_sync_state = {}".format(sync))
        log.warn("{} {}".format(policyname,msg))
        append_issue(isiname,location,policyname,msg)
    if inner['last_job_state'] not in ('running','finished'):
        ljob = inner['last_job_state']
        msg=("last_job_state = {}".format(ljob))
        log.warn("{} {}".format(policyname,msg))
        append_issue(isiname,location,policyname,msg)
    if inner['conflicted'] :
        confl = inner['conflicted']
        msg = ("conflicted = {}".format(confl))
        log.warn("{} {}".format(policyname,msg))
        append_issue(isiname,location,policyname,msg)
###########################################################################
def policies(isi=None):
###########################################################################
    """\nGets the sync policies and put into table ptable.
       inner[fieldname] calls check_policy and appends to issuerpt 
       if issue detected.
    """
    sync = isi.sync_policies_list
    if not sync:
       return
    #pprint.pprint(sync);sys.exit()
    if 'ptable' not in  globals():
       log.warn("{} ptable not in globals.Creating global ptable object".format(isi.name))
       headers=['Isilon','Location']
       headers.extend(isi.headers_sync_policies_list)
       headers.remove('file_matching_pattern')
       #headers.remove('source_include_directories')
       #headers.remove('source_exclude_directories')
       #headers.remove('linked_service_policies')
       global ptable
       ptable = rpt.Table(headers)
    if not sync:
        return None 
    for id,inner in sync.items():
        ptable.Isilon.append(isi.cname)
        ptable.Location.append(isi.location)
        check_policy(isi.cname,isi.location,inner)
        log.info("Add Isilon=> {} policy=> {}".format(isi.cname,inner['name']))
        #inner['source_include_directories'] = ','.join(inner['source_include_directories'])
        for fieldname in ptable.header:
            if fieldname == 'Isilon':
                continue
            if fieldname == 'Location':
                continue
            ary = getattr(ptable,fieldname)
            try:          
               if type((inner[fieldname])) is list or dict:
                  inner[fieldname] = str(inner[fieldname])
               ary.append(inner[fieldname])
            except KeyError as missing_field_val:
               ary.append(' ')
            setattr(ptable,fieldname,ary)
    #pprint.pprint(ptable.__dict__);sys.exit()
###########################################################################
def jobs(isi=None):
###########################################################################
    """\nGets the sync jobs and put into table jrpt"""
    jobs = isi.sync_reports_list
    if not jobs:
        return None 
    for policy in jobs:
        for jobid,inner in jobs[policy].items():
            log.debug('Policy JobID=>' + policy + ' ' + jobid)
            jtable.Isilon.append(isi.cname)
            jtable.Location.append(isi.location)
            for fieldname in jobsheader:
                if fieldname == 'Isilon':
                    continue
                if fieldname == 'Location':
                    continue
                ary = getattr(jtable,fieldname)
                try:          
                    ary.append(inner[fieldname])
                except KeyError as missing_field_val:
                    ary.append(' ')
                setattr(jtable,fieldname,ary)
###########################################################################
def snap_table(isi=None):
###########################################################################
    """\nBuild the stable for the Snapshots tab."""
    s = isi.snapshot_snapshots_list
    if not s:
        return None
    for id in s:
        if not s[id]['Name'].startswith('SIQ_'):
           continue
        for f in stable.header:
            v = ""
            if f == 'Isilon':
                stable.Isilon.append(isi.cname)
                continue
            v = s[id][f]
            ary  = getattr(stable,f)
            ary.append(v)
            setattr(stable,f,ary)
###########################################################################
def rules(isi=None):
###########################################################################
    """\nGets the sync rules and put into table rtable.
    """
    rules = isi.sync_rules_list
    if not rules:
       return
    if 'rtable' not in  globals():
       log.warn("{} rtable not in globals.Creating global rtable object".format(isi.name))
       headers=['Isilon','Location']
       headers.extend(isi.headers_sync_rules_list)
       global rtable
       rtable = rpt.Table(headers)
    if not rules:
        return None 
    for id,inner in rules.items():
        rtable.Isilon.append(isi.cname)
        rtable.Location.append(isi.location)
        for fieldname in rtable.header:
            if fieldname == 'Isilon':
                continue
            if fieldname == 'Location':
                continue
            ary = getattr(rtable,fieldname)
            try:          
               ary.append(inner[fieldname])
            except KeyError as missing_field_val:
               ary.append(' ')
            setattr(rtable,fieldname,ary)
###########################################################################
def isi_main():
###########################################################################
    """\nLaunch point for iterating through isi hosts."""
 
    log.info("Start ISILON loop")
    op=hosts.SetOpts(opts=opts,flavor='isi')
    for host in sorted(op.hosts):
        util.check_server(host,22)
        if hosts.errors_check(host): continue
        isi=hosts.Isilon(host) 
        isi.cname = isi.cluster_identity['name'] #cluster name
        isi.location = isi.cluster_contact['location']
        rules(isi)
        policies(isi)
        jobs(isi)
        snap_table(isi)
###########################################################################
def mk_spreadsheet(myrpt=None):
###########################################################################
    """\nMake spreadsheet and return file name"""
    spreadsheet = rpt.Workbook(myrpt.tmpname)
    if hosts.errorrpt:
        worksheet = spreadsheet.add_tab(name='Script Issues',
                    data=hosts.errorrpt,header=['Host','Message'])
    if issuerpt.Isilon:
        log.info("Issues detected with sync policies")
        worksheet = spreadsheet.add_tab(name='Policy Issues',data=issuerpt.data,header=issuerpt.header)
    if ptable.data:
        worksheet = spreadsheet.add_tab(name='Sync Policies',data=ptable.data,header=ptable.header)
        worksheet.freeze_panes(1,2)
    if jtable.data:
        worksheet = spreadsheet.add_tab(name='Policy Jobs',data=jtable.data,header=jtable.header)
        worksheet.freeze_panes(1,2)
    if stable.data:
        worksheet = spreadsheet.add_tab(name='Sync Snaps',data=stable.data,header=stable.header)
        worksheet.freeze_panes(1,2)
    if rtable.data:
        worksheet = spreadsheet.add_tab(name='Sync Rules',data=rtable.data,header=rtable.header)
    spreadsheet.close()
    return spreadsheet.filename
###########################################################################
def send_rpt():
###########################################################################
    myrpt = rpt.Rpt(opts=opts)
    
    xlsxfile = mk_spreadsheet(myrpt)
    myrpt.add_attachment(xlsxfile)
    if hosts.errorrpt:
        myrpt.add_css_heading("Errors with script","Red")
        myrpt.add_css_table(data=hosts.errorrpt,
                            header=["Host","Message"])
    if issuerpt.Isilon:
        myrpt.add_css_heading("SyncIQ policies with issues","Red")
        myrpt.add_css_table(data=issuerpt.data,header=issuerpt.header)
    else:
        myrpt.add_css_heading("All SyncIQ policies OK","Green")
        
    m = []
    m.append("Policy alert criteria:")
    m.append("""If policy_enabled equals 'True' and policy_schedule not equals 'manually scheduled' ,check for:""")
    m.append("""1. has_sync_state NOT 'True' """)
    m.append("""2. last_job_state NOT "finished" and NOT "running" """)
    m.append("""3. conflicted "True" """)
    m.append("""  """)
    m.append(opts.script + " " + __version__)
    myrpt.add_css_footer(m)
    myrpt.send_email()
    try:
        shutil.copyfile(xlsxfile,myrpt.dailyname + '.xlsx')
        shutil.copyfile(xlsxfile,myrpt.dailydatename + '.xlsx')
        time.sleep(2)
        os.remove(xlsxfile)
    except (FileNotFoundError,PermissionError) as err:
        log.error(err)
###########################################################################
def main():
###########################################################################
    log.info("START")
    global opts
    opts=hosts.SetOpts(opts=argo,main='hosts',script=True)
    log.debug(pprint.pformat(opts.__dict__))
    isi_main()
    hosts.errors_check(host='NA')
    hosts.errors_alert(opts)
    mk_spreadsheet
    send_rpt()
    log.info("END")

##########################################################################
"""options hash/objects used in script:
     argo = command line args.
     opts = main options from argo and hosts.conf file.
     op   = options from flavor (vnx.conf, netapp.conf, script.conf) 
            combined with opts.
"""
###########################################################################
if __name__ == "__main__":
   argo=hosts.ArgParse(description="Report on Isilon SyncIQ policies and jobs.")
   argo=argo.parse_args()
   if argo.version : print (__version__) ; sys.exit()
   log = logging.getLogger()
   hosts.config_logging(opt=argo)
   log.debug(pprint.pformat(argo.__dict__))
   main()
   sys.exit(0)

## Note, there can be 4 different "Last Job State" states:
#                 failed
#                 needs_attention
#                 finished
#                 running
#
# Policiy alert criteria:
# If policy is Enabled and NOT manually scheduled then check for:
#    1. Sync State = "Yes" 
#    2. Last Job State = NOT "finished"  or NOT "running"
#    3. Conflicted = "No"
#
###########################################################################
def history():
###########################################################################
     """\n1.0.1 initial skel
        1.1.1 logging
        1.1.3 skel
        1.1.4 streamline .conf parsing
        1.3.0 issuerpt and check_policy() and append_issue()
        1.3.1 rpt.Rpt css routines, add_css_footer
        1.3.2 support for #!/usr/local/venv/bin/python3
        1.4.0 support for Cluster Identity Name and Cluster Location
        2.0.0 snapshots and summaryView
     """
     pass
