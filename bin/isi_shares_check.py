#!/usr/local/venv/bin/python3
"""Check on Isilons shares configs"
"""
__author__ = "michael.s.denney@gmail.com"
__version__ = "2.1.1"

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

os.chdir(os.path.dirname(os.path.realpath(__file__)))
sys.path.append("../pylib")
import hosts
import rpt
import util
###########################################################################
def history():
###########################################################################
     """
        1.0.1 initial skel
        1.1.1 added checks report - itable
        2.0.1 changed to share name as key field. Add share description
        2.0.5 many bug fixed
        2.1.1 Isilon cluster name changed to upper
     """
     pass
###########################################################################
## GLOBALS
###########################################################################
prov = {}
emailheader =  ['Isilon','Zone','Path','Issue']
etable = rpt.Table(emailheader) 
issueheader =  ['Isilon','Zone','Share','Path','Issue']
itable = rpt.Table(issueheader) 
pheader = ['Isilon','Zone','Share','Path','AppID','Description','SnapSchedule','SnapDuration','SyncSchedule','target_snapshot_archive','target_snapshot_expiration']
ptable = rpt.Table(pheader)
excludeHeader = ['IsiPath','Excludes']
excludetable = rpt.Table(excludeHeader)
SUMS = {
'AppID':0,'SnapSchedule':0,'SnapDuration':0,'SyncSchedule':0 ,'target_snapshot_archive':0,'target_snapshot_expiration':0
}
###########################################################################
class MyException(Exception):
    pass
###########################################################################
def emailtable(isi=None):
###########################################################################
    """\nBuild the table for the main email rpt."""
    pprint.pprint("emailtable in development");return
    etable.Isilon.append(isi.cname)
    for field in emailheader:
        if field == 'Isilon':
           continue
        ary = getattr(etable,field)
        myval = None
        try:
            if field == 'Health':
                field = 'Cluster Health'
            myval = st[field]
            if field == 'Cluster Health':
                myval = re.sub('[^a-zA-Z0-9]+', '', myval)
            if field == 'pools':
                myval = ''
                for pool in st[field]:
                    myval += pool + " "
        except KeyError as missing_field_val:
            myval = ' '
        ary.append(myval)
        setattr(etable,field,ary)
###########################################################################
def search_sc_url(isi=None,zone=None,type=None,share=None):
###########################################################################
    """Search and return the SmartConnect Zone name or return " "
    """
    log.debug("searching for SmartConnect Zone {} {}".format(zone,type))
    np = isi.networks_list_pools
    url = ""
    if type == 'smb':
        url = "\\\\{}\\{}".format(isi.cname,share) 
    else:
        url = """{}:{}""".format(isi.cname,share) 
    if not np:
        log.info("isi network list pools not detected from "  + isi.cname)
        return(url)
    for p in np:
        if zone != np[p]['Access Zone'] :
            continue
        sc = np[p]['SmartConnect Zone']
        if type in sc.lower():
            if type == 'smb':
                url = "\\\\{}\\{}".format(sc,share) 
            else:
                url = """{}:{}""".format(sc,share) 
            continue
        if type == 'smb' and 'cifs' in sc.lower():
            url = "\\\\{}\\{}".format(sc,share) 
            continue
    return(url)
###########################################################################
def smb_prov(isi=None):
###########################################################################
    """Get smb shares and populate into prov  dict, return dict for prov_table.
    """
    prov = {}
    smb = isi.smb_shares_list
    if not smb:
        log.info("No smb shares detected from " + isi.cname)
        return(prov)
    try:
        for zone in smb:
            for share,k in smb[zone].items():
                path = smb[zone][share]['Path']
                description = smb[zone][share]['Description']
                ts = { 'Description' : description ,
                       'Path' : path ,
                       'Isilon' : isi.cname,
                       'Type' : 'SMB',
                       'Share': share,
                       'Zone' : zone,
                     }
                prov.setdefault(path,{})
                prov[path].setdefault('smb',[]).append(ts)
    except TypeError:
        pass
    return(prov)
###########################################################################
def aliases_nfs_prov(isi=None,zone=None,path=None):
###########################################################################
    """Return a list of aliases for a zone,path. If no aliases, return a 
       blank(space) list element.
    """
    na = isi.nfs_aliases_list
    #pprint.pprint(na) ; sys.exit()
    ra = [] #return aliases list
    log.debug("Searching aliases for {} {}".format(zone,path))
    try:
        for a in na[zone][path]:
            ra.append(a)
    except KeyError:
        ra.append(' ')
    return(ra)
###########################################################################
def nfs_prov(isi=None,prov=None):
###########################################################################
    """Get nfs shares and populate into prov dict, return dict for prov_table.
       Skip if path already in prov_table (cause we are checking paths, 
       not shares and SMB paths are populated first)
    """
    n = isi.nfs_exports_list
    if not n:
        log.info("No NFS exports detected from " + isi.cname)
        return prov
    np = isi.nfs_paths
    for zone,paths in np.items():
        for path,i in paths.items():
            if path in prov.keys():
               continue
            ts = { 'Path' : path ,
                   'Isilon' : isi.cname,
                   'Type' : 'NFS',
                   #'Share': 'Alias here',
                   'Share': i['ids'],
                   'Zone' : zone,
                   'Multi-Prot' : 'No',
                   #'NFS_ids' : i['ids'],
                 }
            Description = ""
            #pprint.pprint(i)
            try:
                for id in (i['ids'].split()):
                    Description += n[zone][id]['Description'] + ' '
            except KeyError:
                Description += "Error with NFS ID {}".format(id)
            ts['Description'] = Description
            prov.setdefault(path,{})
            al = aliases_nfs_prov(isi,zone,path)
            for alias in al:
                sharename  = alias
                ts['Share name'] = sharename
                if alias != ' ':
                     ts['SmartConnect URL'] =  search_sc_url(isi,zone,'nfs',sharename)
                prov[path].setdefault('nfs',[]).append(ts)
    #pprint.pprint(prov) ; sys.exit()
    return(prov)
###########################################################################
def match_path(path_dict=None,path_match=None):
   """Given a dict called path_dict where the keys are paths, try
       to match the path_match. If a match is found return the value
       of the dict key, other wise raise execption"""
   if not path_dict:
      return False
   for policy_path, inner in path_dict.items():
      match = os.path.commonpath([policy_path, path_match])
      if match == policy_path:
         return(inner)
   raise MyException("NotFound")
###########################################################################
def snap_prov(isi=None,prov=None):
###########################################################################
    """Process the prov dict, looking in snap schedule paths for all provision
       paths. Include looking in parent paths for snap path.
    """
    if not prov:
       log.warn("Missing prov dict")
       return False
    sc = isi.snapshot_sched_list
    if not sc:
       log.info("No snap schedules detected")
       return False
    #make a dict of snap path->{ {Schedule}, {Duration}}
    sc_paths = {}
    for id,schedule in sc.items():
       sc_paths.setdefault(schedule['Path'],{})   
       sc_paths[schedule['Path']]['SnapSchedule'] = schedule['Schedule']
       sc_paths[schedule['Path']]['SnapDuration'] = schedule['Duration']
    for path,d in prov.items():
       for type,shares in prov[path].items():
          for share in shares:
             results = {}
             try:
                results = match_path(sc_paths,path)
                share.update(results)
             except MyException :
                pass
    return(prov)
###########################################################################
def synciq_prov(isi=None,prov=None):
###########################################################################
    """Process the prov dict, looking in syncIQ paths for all provision
       paths. Include looking in parent paths for syncIq policy path.
    """
    if not prov:
       log.warn("Missing prov dict")
       return False
    sp = isi.sync_policies_list
    if not sp:
       log.info("No sync policies detected")
       return False
    #make a dict of  path->schedule,snapshot archive,snapshot expire
    sp_paths = {}
    try:
       for id,policy in sp.items():
          syncPath = policy['source_root_path']
          sp_paths.setdefault(syncPath,{})['SyncSchedule'] = policy['schedule']
          sp_paths[syncPath]['target_snapshot_archive'] = policy['target_snapshot_archive']
          sp_paths[syncPath]['target_snapshot_expiration'] = policy['target_snapshot_expiration']
    except AttributeError:
       log.info("No SyncIQ policies detected from " + isi.cname)
       return False
    except KeyError as err:
       log.error("SyncIQ policies dict key error detected from {}".format(isi.cname,err))
       return False
    for path,d in prov.items():
       for type,shares in prov[path].items():
          for share in shares:
             results = {}
             try:
                results = match_path(sp_paths,path)
                share.update(results)
             except MyException :
                pass
    return(prov)
###########################################################################
appidRE = re.compile(r'AppID:\s?(APP\d+)\s*',re.IGNORECASE)
def appid_prov(isi=None,prov=None):
###########################################################################
    """Process the prov dict, looking in descriptions for "APPID:(appid)"
       Populate AppID if found, else -
    """
    if not prov:
       log.warn("Missing prov dict")
       return False
    for path,d in prov.items():
       for type,shares in prov[path].items():
          for share in shares:
             #print(type,share,shares)
             #print(type,share['Share'],share['Description'])
             appid = '-'
             mo = appidRE.search(share['Description'])
             if mo:
                appid = mo.group(1)
             share['AppID'] = appid
    return(prov)
###########################################################################
def provisions_table(isi=None):
###########################################################################
    """Build the ptable for the Provisions tab.
       First build a dict of smb/nfs combined
       outer = path, then smb/nfs, then  a list of shares.
       Build a smb/nfs combined  dict so to detect multi-protocol easier.       
    """
    try:
        if str(opts.url_detect) == '1':
            if 'SmartConnect URL' not in pheader:
                pheader.append('SmartConnect URL')
                setattr(ptable,'header',pheader)
                setattr(ptable,'SmartConnect URL',[])
        else:
            log.info("url_detect=0,SmartConnect URL will not be used.")
    except AttributeError:
        log.info("url_detect=1 not specified in .conf")
    prov = smb_prov(isi)
    prov = nfs_prov(isi,prov)
    prov = snap_prov(isi,prov)
    prov = synciq_prov(isi,prov)
    prov = appid_prov(isi,prov)
    if not prov:
        return
    for path in prov:
        for type,shares in prov[path].items():
            for share in shares:
                #print("##############",share,path)
                for h in pheader:
                    ary = getattr(ptable,h)
                    try:
                       ary.append(share[h])
                    except KeyError:
                       ary.append('-')
                    setattr(ptable,h,ary)
###########################################################################
def file_to_array(ifile=None):
###########################################################################
    """Opens a text file and returns a list with 1 list per line.
    """
    enclist = ('utf-8','latin-1','utf-16','ascii')
    try:
        for e in enclist:
            try:
                #print("Reading {} as {}".format(ifile,e))
                with open(ifile, mode='r', encoding=e) as fh:
                    farray = fh.read().splitlines()
                    return farray
                #If got here, the failed to read encoding
                log.error("Failed to read encoding for {}"
                              .format(ifile))
                return None
            except (UnicodeDecodeError,UnicodeError) as err:
                log.info(err)
                continue
    except FileNotFoundError as err:
        log.info(err)
        return None
    if not farray:
        return None
    return farray

###########################################################################
def excludeCheck(isiLowName=None,isiPath=None,colNum=None,excludes=None):
###########################################################################
   if not isiLowName:
      log.error("No isiLowName")
      return True
   if not isiPath:
      log.error("No isiPath")
      return True
   if not colNum:
      log.error("No row")
      return True
   print("excludeCheck colNum",colNum)
   print(isiLowName,isiPath,"excludeCheck",ptable.header[colNum])
   return 
#
#pheader = ['Isilon','Zone','Share','Path','AppID','Description','SnapSchedule','SnapDuration','SyncSchedule','target_snapshot_archive','target_snapshot_expiration']
###########################################################################
def check_paths(op=None):
###########################################################################
   """\nCheck all the paths in the ptable. Any path that has a - will
        get marked as issue by adding to itable"""
   colEnd = len(ptable.data[0])
   issueEnd = len(itable.header[0])
   for row in ptable.data:
      isiName = row[0].lower()
      isiLowName = row[0].lower()
      isiPath = (row[3])
      namePath = isiName + isiPath
      log.info("Consider {} {}".format(isiLowName,namePath))
      try:
         if 'all'.lower() in Excludes[namePath].lower():
            log.info("{} {} excluded because ALL specified".format(isiLowName,namePath))
            excludetable.IsiPath.append(namePath)
            excludetable.Excludes.append('ALL')
            continue
      except KeyError:
         pass
      for colNum in range(3,colEnd):
         itemChecked = ptable.header[colNum]
         log.debug("Checking {} {}".format(itemChecked,row[colNum]))
         #The next 2 if's are to not flag if corresponding schedule is missing
         #because the missing scehdule will be the cause
         if itemChecked in ['SnapDuration','target_snapshot_archive']:
            if row[colNum] == '-' and row[colNum - 1] == '-' :
               continue
         #The next 2 if's are to not flag if corresponding schedule is missing
         #because the missing scehdule will be the cause of missing expiration
         if itemChecked in ['target_snapshot_expiration']:
            if row[colNum] == '-' and row[colNum - 2] == '-' :
               continue
         ##Changing tuple val to string so can change value as needed
         checkME = row[colNum]
         if itemChecked in ['SnapDuration'] and checkME == 'null':
            #Changing from False to - so gets flagged as issue
            checkME  = '-' 
         if itemChecked in ['target_snapshot_archive'] and checkME == False:
            #Changing from False to - so gets flagged as issue
            checkME  = '-' 
         if itemChecked in ['target_snapshot_expiration' and checkME == '0']:
            #Changing from False to - so gets flagged as issue
            checkME  = '-' 
         if checkME == '-':
            try:
               if itemChecked.lower() in Excludes[namePath].lower():
                  log.info("{} excluded for checkedItem {}".format(namePath,itemChecked))
                  excludetable.IsiPath.append(namePath)
                  excludetable.Excludes.append(itemChecked)
                  continue
            except KeyError:
               pass
            itable.Isilon.append(isiName.upper())
            itable.Zone.append(row[1])
            itable.Share.append(row[2])
            itable.Path.append(row[3])
            itable.Issue.append(ptable.header[colNum])
            SUMS[ptable.header[colNum]] += 1
            log.debug("Issue with {}".format(ptable.header[colNum]))
###########################################################################
##SUMMARY COUNT OF CHECK ISSUES HERE
AppID = 0
Description = 0
SnapSchedule = 0
SyncSchedule = 0
target_snapshot_archive = 0
target_snapshot_expiration = 0
#print(ptable.sheader);sys.exit()
###########################################################################
def loadExcludes (config_file):
###########################################################################
   """\nCreate a global dict of the excludes conf. Also convert all Isilon
      names to lower case."""
   global Excludes
   Excludes = {}
   conFile = "../etc/{}.excludes".format(config_file)
   config = configparser.ConfigParser()
   config.optionxform = str
   try:
      config.read(conFile)
   except configparser.DuplicateOptionError as err:
      log.error(err)
      return False
   for isiPath,v in config['Excludes'].items():
      #print(isiPath,v)
      kList = isiPath.split('/')
      kList[0] = kList[0].lower()
      isiPath = '/'.join(kList)
      Excludes[isiPath] = v
   return True
###########################################################################
def check_sums():
###########################################################################
   sumheader = []
   for k,v in SUMS.items():
      if v > 0: 
         sumheader.append(k)
   if sumheader:
      global sumtable
      sumtable = rpt.Table(sumheader)
      for k,v in SUMS.items():
         if v > 0: 
            setattr(sumtable,k,[str(v)])
   #print(excludetable.data)
   #print(excludetable.IsiPath);sys.exit()
   #pprint.pprint(sumtable.__dict__);sys.exit()
###########################################################################
def isi_main():
###########################################################################
    """\nLaunch point for iterating through isi hosts."""
 
    log.info("Start ISILON loop")
    op=hosts.SetOpts(opts=opts,flavor='isi')
    if not loadExcludes(op.shortscript):
       log.error("Issue with excludes parse")
       return 
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
        provisions_table(isi)
        #emailtable(isi)
        hosts.errors_check(host)
    check_paths(op)
    check_sums()
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
    try:
       if sumtable.data:
          worksheet = spreadsheet.add_tab(name='Summary count of issues',data=sumtable.data,header=sumtable.sheader)
    except NameError:
       pass
    if ptable.data:
        worksheet = spreadsheet.add_tab(name='Share path checks',data=ptable.data,header=ptable.sheader)
        worksheet.freeze_panes(1,2)
        worksheet.set_col_by_name(colname = 'Path ', width = 65)
    if itable.data:
        worksheet = spreadsheet.add_tab(name='Paths check failed',data=itable.data,header=itable.sheader)
        worksheet.freeze_panes(1,2)
        worksheet.set_col_by_name(colname = 'Path ', width = 65)
    if excludetable.data:
        worksheet = spreadsheet.add_tab(name='Exclude reasons',data=excludetable.data,header=excludetable.sheader)
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
    smbUrl = smbUrl  + """\\isi_shares_check">Daily spreadsheet details repository</a>"""
    #if itable.Isilon:
        #myrpt.add_css_heading("Share paths with issues","Red")
        #myrpt.add_css_table(data=itable.data,header=itable.header)
    try:
       if sumtable:
          myrpt.add_css_heading("Summary count of Share paths with issues","Red")
          myrpt.add_css_table(data=sumtable.data,header=sumtable.sheader)
    except NameError:
       myrpt.add_css_heading("All Isilon share paths OK","Green")
    myrpt.add_css_heading(smbUrl)
    #myrpt.add_css_table(data=etable.data,header=etable.header)
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
