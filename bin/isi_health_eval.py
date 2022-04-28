#!/usr/local/venv/bin/python3
"""Check on Isilons healthcheck eval"
"""
__author__ = "michael.s.denney@gmail.com"
__version__ = "1.0.2"

import sys
import os
import pprint
import logging
import io
import copy 
import re
import shutil
import time
import configparser
import datetime

os.chdir(os.path.dirname(os.path.realpath(__file__)))
sys.path.append("../pylib")
import hosts
import rpt
import util
###########################################################################
def history():
     """
        1.0.0 initial skel
        1.0.1 determineEval()
        1.0.2 Cluster names instead of SC names
     """
     pass
###########################################################################
## GLOBALS
emailheader =  ['Isilon','Severity','Node','Item','Issue']
etable = rpt.Table(emailheader) 
evalsheader =  ['Isilon','Severity','SevPriority','Node','Item','Issue']
evalstable = rpt.Table(evalsheader) 
SUMS = {
'AppID':0,'SnapSchedule':0,'SnapDuration':0,'SyncSchedule':0 ,'target_snapshot_archive':0,'target_snapshot_expiration':0
}
#controlre = re.compile(r'^\sCONTROL:(.+)')
evalRE = re.compile(r'^\s+(INFO|WARNING|CRITICAL|EMERGENCY)\s+(\d+)\s+\[(.+)\]\s+(\S+)\s*$')
logsRE = re.compile(r'^\s+(Logs:)\s+')
errorRE = re.compile(r'\s+(ERROR)\s+(Check script failed)\s+\d+\s+\[(.+)\]\s+(\S+)')
###########################################################################
def determineEval():
   """Returns the eval ID to use"""
   #evalID = 'basic20220216T0800'
   today = datetime.date.today()
   month = '%02d' % today.month
   day =  '%02d' % today.day
   evalID = "basic{}{}{}T0800".format(today.year,month,day)
   log.info("AppID for this execution = {}".format(evalID))
   return evalID
###########################################################################
def add_to_etable(isi,sev,node,item,issue):
   ary = getattr(etable, 'Isilon')
   ary.append(isi.cname)
   setattr(etable, 'Isilon', ary)

   ary = getattr(etable, 'Severity')
   ary.append(sev)
   setattr(etable, 'Severity', ary)

   ary = getattr(etable, 'Node')
   ary.append(node)
   setattr(etable, 'Node', ary)

   ary = getattr(etable, 'Item')
   ary.append(item)
   setattr(etable, 'Item', ary)

   ary = getattr(etable, 'Issue')
   ary.append(issue)
   setattr(etable, 'Issue', ary)
###########################################################################
def add_to_evalstable(isi,sev,sevpri,node,item,issue):
   """Add row to eval table"""
   ary = getattr(evalstable, 'Isilon')
   ary.append(isi.cname)
   setattr(evalstable, 'Isilon', ary)

   ary = getattr(evalstable, 'Severity')
   ary.append(sev)
   setattr(evalstable, 'Severity', ary)

   ary = getattr(evalstable, 'SevPriority')
   ary.append(sevpri)
   setattr(evalstable, 'SevPriority', ary)

   ary = getattr(evalstable, 'Node')
   ary.append(node)
   setattr(evalstable, 'Node', ary)

   ary = getattr(evalstable, 'Item')
   ary.append(item)
   setattr(evalstable, 'Item', ary)

   ary = getattr(evalstable, 'Issue')
   ary.append(issue)
   setattr(evalstable, 'Issue', ary)
###########################################################################
def getEval(isi=None,evalID=None):
   """Gets the eval from Isilon and put results into evals{}"""
   isi.cmd('isi healthcheck eval view {}'.format(evalID))
   #pprint.pprint(isi.stdout)
   sev = sevpri = node = item = ""
   #sev = ""
   #sevpri = ""
   #node = ""
   #item = ""
   for line in isi.stdout:
      log.debug("RAW LINE {}".format(line))
      mo = evalRE.search(line)
      if mo:
         sev = mo.group(1)
         sevpri  = mo.group(2)
         node = mo.group(3)
         item = mo.group(4)
         continue
      if not sev and not sevpri and not node and not item:
         continue
      me = errorRE.search(line)
      if me:
         sev = me.group(1)
         sevpri  = me.group(2)
         node = me.group(3)
         item = me.group(4)
         continue
      ml = logsRE.search(line)
      if ml:
         sev = 'INFO'
         sevpri = '999'
         node = 'Cluster'
         item = 'Logs'
      log.debug("{} {} {} {} {}".format(isi.cname,sev,node,item,line.lstrip()))
      add_to_evalstable(isi,sev,sevpri,node,item,line.lstrip())
      if sev in ['EMERGENCY']:
         add_to_etable(isi,sev,node,item,line.lstrip())
###########################################################################
##SUMMARY COUNT OF CHECK ISSUES HERE
AppID = 0
Description = 0
SnapSchedule = 0
SyncSchedule = 0
target_snapshot_archive = 0
target_snapshot_expiration = 0
###########################################################################
def isi_main():
    """\nLaunch point for iterating through isi hosts."""
 
    log.info("Start ISILON loop")
    op=hosts.SetOpts(opts=opts,flavor='isi')
    evalID = determineEval()
    if not evalID:
       log.error("Issue determining evalID")
       return False
    for host in sorted(op.hosts):
        util.check_server(host,22)
        if hosts.errors_check(host): continue
        isi=hosts.Isilon(host) 
        try:
           isi.cname = isi.cluster_identity['name'] #cluster name
           isi.location = isi.cluster_contact['location']
        except TypeError:
           log.warn("Issue connecting to {}, continue".format(host))
           continue
        getEval(isi,evalID)
        hosts.errors_check(host)
###########################################################################
def mk_spreadsheet(myrpt=None):
###########################################################################
    """\nMake spreadsheet and return file name"""
    spreadsheet = rpt.Workbook(myrpt.tmpname)
    if hosts.errorrpt:
        worksheet = spreadsheet.add_tab(name='Script Issues',
                    data=hosts.errorrpt,header=['Host','Message'])
    if etable.data:
        worksheet = spreadsheet.add_tab(name='Email rpt',data=etable.data,header=etable.sheader)
        worksheet.freeze_panes(1,2)
    if evalstable.data:
        worksheet = spreadsheet.add_tab(name='Evals',data=evalstable.data,header=evalstable.sheader)
        #worksheet.freeze_panes(1,2)
        #worksheet.set_col_by_name(colname = 'Path ', width = 65)
    spreadsheet.close()
    return spreadsheet.filename
###########################################################################
def send_rpt():
###########################################################################
    myrpt = rpt.Rpt(opts=opts)
    
    ##try except NameError pass in case table not defined
    xlsxfile = mk_spreadsheet(myrpt)
    if argo.attach:
       myrpt.add_attachment(xlsxfile)
    if hosts.errorrpt:
        myrpt.add_css_heading("Errors with script","Red")
        myrpt.add_css_table(data=hosts.errorrpt,
                            header=["Host","Message"])
    #pprint.pprint(myrpt.mailmessage)
    smbUrl = """<a href=" """ + opts.cifs_daily_rpt_dir
    smbUrl = smbUrl  + """\\isi_health_eval">Daily spreadsheet details repository</a>"""
    myrpt.add_css_heading(smbUrl)
    if etable.data:
       myrpt.add_css_heading("EMERGENCY issues detected","Red")
       myrpt.add_css_table(data=etable.data,header=etable.header)
    else:
       myrpt.add_css_heading("All issues less then EMERGENCY","Green")
    m = []
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
    #(pprint.pprint(opts.__dict__)) ; sys.exit()
    log.debug(pprint.pformat(opts.__dict__))
    isi_main()
    hosts.errors_check(host='NA')
    hosts.errors_alert(opts)
    send_rpt()
    hosts.errors_check(host='NA')
    hosts.errors_alert(opts)
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
   argo=hosts.ArgParse(description="Check on Isilon Shares Configs.")
   argo.add_argument('-a','--attach',action='store_true',help='include spreadsheet attachment.')
   argo=argo.parse_args()
   if argo.version : print (argo.script + " Version " + __version__) 
   if argo.history : print (history.__doc__)
   if argo.version or argo.history: sys.exit()
   log = logging.getLogger()
   hosts.config_logging(opt=argo)
   log.debug(pprint.pformat(argo.__dict__)) ; 
   main()
   sys.exit(0)
