#!/usr/local/venv/bin/python3
"""Report on Isilons.
"""
__author__ = "michael.s.denney@gmail.com"
__version__ = "2.8.0"

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
def history():
###########################################################################
     """
        1.0.0 initial skel
        1.1.0 ntable, nodes tab
        1.2.0 qtable, quotas tab
        1.3.0 nfs_table, NFS Exports tab
        1.4.0 smb_table, SMB Shares tab
        1.5.0 snap_table, Snapshots tab
        1.6.0 snapsched_table, Snapshot Schedules tab
        1.7.0 netpools_table, Netpools Tab, netp
        1.8.0 ptable, Provisions tab
        1.9.0 SmartConnect URL
        1.9.1 quotas tab are now all in GB
        2.0.0 provision tab now included "Map Lookup UID" NFS setting
        2.0.1 set_col_by_name for provisions tab
        2.1.1 Path_Security columns added to provisions tab
        2.1.2 Fixed issue if export gets deleted in middle of rpt
        2.1.3 hosts.errors_check(host) at end of isi_main per host
        2.2.0 filepool_policies tab
        2.2.1 Fix for netpools_table may not have SmartPools license,
              causing missing dict keys.
        2.2.2 Fix for dfs not specified in .conf
        2.3.0 Switch to using #!/usr/local/venv/bin/python3
        2.4.0 Added -a and smb url for spreadsheet repository
        2.5.0 Added cluster identity name and cluster contact location
        2.6.0 Add SyncIQ detect for provisions_table
        2.6.1 Fix for synciq_prov aborting due to changes from isilon.py
        2.7.1 Fix for quotas changing via OneFS8.2.2.0
        2.8.0 zones_table and StoragePool tabs
     """
     pass
###########################################################################
## GLOBALS
###########################################################################
emailheader =  ['Isilon','Location', 'Health', 'OneFS', 'HDD_Size', 'HDD_Used', 'SSD_Size', 'SSD_Used', 'pools']
etable = rpt.Table(emailheader) 
nodesheader = ['Isilon','ID','LNN','Name','Health','IP','Type','Node_Group','Serial','HDD_Storage_Size','HDD_Storage_Used','SSD_Storage_Size','SSD_Storage_Used','Throughput_In','Throughput_Out','bps','Product','RAM','CPU','PROC','GenCode','Mobo','Config','IMB','LCDver','FlshDrv','ChasCnt','Chassis','ChsCode','NetIf','DskExp','NVRam','PwrSupl_PS1','PwrSupl_PS2','Comps']
ntable = rpt.Table(nodesheader)
qheader = ['Isilon','Path','Type','Hard','Soft','AppliesTo','AppLogical','FSLogical','FSPhysical','Physical','ShadowLogical','Files','Over','Grace','Enforced','Adv','Snap','Container','Linked']
qtable = rpt.Table(qheader)
nfsheader = ["Isilon","Paths","ID","Zone","Description","Read Write Clients","Root Clients","Read Only Clients","All Dirs","Map Non Root User","Snapshot","Case Insensitive","Write Filesync Action","No Truncate","Enabled","Readdirplus Prefetch","Readdirplus","Map Failure Primary Group","Map Failure User","Map Non Root Primary Group","Link Max","Block Size","Read Transfer Max Size","Clients","Map Root User","Map Non Root Secondary Groups","Symlinks","Write Unstable Action","Read Transfer Size","Return 32Bit File Ids","Write Filesync Reply","Encoding","Case Preserving","Time Delta","Write Datasync Reply","Map Root Secondary Groups","Map Failure Secondary Groups","Map Root Primary Group","Max File Size","Name Max Size","Write Datasync Action","Can Set Time","Write Transfer Size","Read Transfer Multiple","Security Type","Write Unstable Reply","Chown Restricted","Map Lookup UID","Setattr Asynchronous","Commit Asynchronous","Read Only","Map Retry","Map Full","Directory Transfer Size","Write Transfer Max Size","Write Transfer Multiple"]
nfstable = rpt.Table(nfsheader)
smbheader = ['Isilon','Share Name','Path','Zone','Description','Total','Strict Locking','Access Based Enumeration','Automatically create home directories for users','Impersonate User','Oplocks','Ca Timeout','File Create Mask','Browsable','Allow Delete Readonly','Directory Create Mode','Mangle Map','File Filtering Enabled','Directory Create Mask','Automatically expand user names or domain names','Mangle Byte Start','Change Notify','File Filter Type','Host ACL','Continuously Available','File Create Mode','Access Based Enumeration Root Only','Strict Ca Lockout','Hide Dot Files','Create Permissions','Ca Write Integrity','File Filter Extensions','Strict Flush','Allow Execute Always','Impersonate Guest','Ntfs ACL Support','Client-side Caching Policy',"Acct1","Acct1 Permission","Acct1 Account_Type","Acct1 Perm_type","Acct1 Run_as_root","Acct2","Acct2 Permission","Acct2 Account_Type","Acct2 Perm_type","Acct2 Run_as_root","Acct3","Acct3 Permission","Acct3 Account_Type","Acct3 Perm_type","Acct3 Run_as_root","Acct4","Acct4 Permission","Acct4 Account_Type","Acct4 Perm_type","Acct4 Run_as_root","Acct5","Acct5 Permission","Acct5 Account_Type","Acct5 Perm_type","Acct5 Run_as_root"]
smbtable = rpt.Table(smbheader)
sheader = ["Isilon","Name","ID","Path","Size","State","Alias Target Name","Has Locks","% Filesystem","Created","Alias Target ID","Expires","Schedule","Shadow Bytes","% Reserve"]
stable = rpt.Table(sheader)
scheader = ['Isilon','Name','Path','ID','Schedule','Duration','Pattern','Alias','Next Run','Next Snapshot']
sctable = rpt.Table(scheader)
npheader = ['Isilon','In Subnet','Smart Pool','Description','Aggregation Mode','Pool Interfaces','SmartConnect Service Subnet','Access Zone ID','Access Zone','Ranges','Allocation','SmartConnect Connection Policy','SmartConnect Time to Live','SmartConnect Zone','SmartConnect Auto Unsuspend','SmartConnect Rebalance Policy','IPranges','SmartConnect Failover Policy','SmartConnect Suspended Nodes','Pool Membership']
nptable = rpt.Table(npheader)
pheader = ['Isilon','Path','Type','Share name','Zone','Description','SyncIQpolicy','Multi-Prot','Hard GB','Advisory GB','Files','AppLogical GB','FSLogical GB','FSPhysical GB','Physical GB','NFS_ids','Map_Lookup_UID','Path_Security']
ptable = rpt.Table(pheader)
fpheader = ['Isilon','Name','Apply_Order','Description','State','State_Details','File_Matching_Pattern','Set_Requested_Protection','Data_Access_Pattern','Enable_Coalescer','Data_Storage_Target','Data_SSD_Strategy','Snapshot_Storage_Target','Snapshot_SSD_Strategy','Cloud_Pool','Cloud_Compression_Enabled','Cloud_Encryption_Enabled','Cloud_Data_Retention','Cloud_Incremental_Backup_Retention','Cloud_Full_Backup_Retention','Cloud_Accessibility','Cloud_Read_Ahead','Cloud_Cache_Expiration','Cloud_Writeback_Frequency','Cloud_Archive_Snapshot_Files']
fptable = rpt.Table(fpheader)
###########################################################################
def nodestable(isi=None):
###########################################################################
    """\nBuild the nodestable from nodes info ."""
    q = isi.status_q
    if not q:
        return None
    hw = isi.hw_status
    if not hw:
        return None
    n = isi.status_n
    if not n:
        return None
    for id in sorted(hw):
        ntable.Isilon.append(isi.cname)
        ntable.ID.append(id)
        ntable.LNN.append(n[id].get('LNN',''))
        ntable.Name.append(n[id].get('Name',''))
        ntable.Health.append(n[id].get('Health',''))
        ntable.IP.append(q['nodes'][id].get('IP',''))
        ntable.Type.append(hw[id].get('FamCode',''))
        ntable.Node_Group.append(n[id].get('Member_of_Group',''))
        ntable.Serial.append(hw[id].get('SerNo',''))
        val = q['nodes'][id].get('HDD_Storage_Size','')
        ntable.HDD_Storage_Size.append(val)
        val = q['nodes'][id].get('HDD_Storage_Used','')
        ntable.HDD_Storage_Used.append(val)
        val = q['nodes'][id].get('SSD_Storage_Size','') 
        ntable.SSD_Storage_Size.append(val)
        val = q['nodes'][id].get('SSD_Storage_Used','') 
        ntable.SSD_Storage_Used.append(val)
        ntable.Throughput_In.append(q['nodes'][id].get('Throughput_In',''))
        ntable.Throughput_Out.append(q['nodes'][id].get('Throughput_Out',''))
        ntable.bps.append(q['nodes'][id].get('bps',''))
        ntable.Product.append(hw[id].get('Product',''))
        ntable.RAM.append(hw[id].get('RAM',''))
        ntable.CPU.append(hw[id].get('CPU',''))
        ntable.PROC.append(hw[id].get('PROC',''))
        ntable.GenCode.append(hw[id].get('GenCode',''))
        ntable.Mobo.append(hw[id].get('Mobo',''))
        ntable.Config.append(hw[id].get('Config',''))
        ntable.IMB.append(hw[id].get('IMB',''))
        ntable.LCDver.append(hw[id].get('LCDver',''))
        ntable.FlshDrv.append(hw[id].get('FlshDrv',''))
        ntable.ChasCnt.append(hw[id].get('ChasCnt',''))
        ntable.Chassis.append(hw[id].get('Chassis',''))
        ntable.ChsCode.append(hw[id].get('ChsCode',''))
        ntable.NetIf.append(hw[id].get('NetIf',''))
        ntable.DskExp.append(hw[id].get('DskExp',''))
        ntable.NVRam.append(hw[id].get('NVRam',''))
        ntable.PwrSupl_PS1.append(hw[id].get('PwrSupl_PS1',''))
        ntable.PwrSupl_PS2.append(hw[id].get('PwrSupl_PS2',''))
        ntable.Comps.append(hw[id].get('Comps',''))
###########################################################################
def emailtable(isi=None):
###########################################################################
    """\nBuild the table for the main email rpt."""
    st = isi.status_q_d
    if not st:
        return None
    etable.Isilon.append(isi.cname)
    etable.Location.append(isi.location)
    for field in emailheader:
        if field == 'Isilon':
           continue
        if field == 'Location':
           continue
        if field == 'OneFS':
           isi.version
           etable.OneFS.append(isi.version)
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
def quotastable(isi=None):
###########################################################################
    """\nBuild the table for the quotas tab."""
    q = isi.quota_quotas_list_csv
    if not q:
        return None
    for path,i in q.items():
        for row in i:
           for f in qheader:
               if f == 'Isilon':
                   qtable.Isilon.append(isi.cname)
                   continue
               val = row.get(f,'')
               ary = getattr(qtable,f)
               ary.append(val)
               setattr(qtable,f,ary)
###########################################################################
def perms(p=None):
###########################################################################
    """\nProcess the accounts permissions for each smb share."""
    max_acct = 0
    phead = ['Account','Account_Type','Permission','Perm_type','Run_as_root']
    cnt = 0
    for k in p:
        cnt += 1
        ac = "Acct" + str(cnt)
        max_acct = cnt
        for v in phead:
            if v == 'Permission':
                ac = "Acct" + str(cnt) + " Permission"
            if v == 'Account_Type':
                ac = "Acct" + str(cnt) + " Account_Type"
            if v == 'Perm_type':
                ac = "Acct" + str(cnt) + " Perm_type"
            if v == 'Run_as_root':
                ac = "Acct" + str(cnt) + " Run_as_root"
            try:
                ary = getattr(smbtable,ac)
            except AttributeError:
                log.warn("{} => {} , Probably more then 5 accounts for share"
                         .format(ac,v))
                continue

            ary.append(p[k][v])
            setattr(smbtable,ac,ary)
        ##now need to add blanks for acct columns if less then 4 accounts:
    thead = ['Account_Type','Permission','Perm_type','Run_as_root']
    for i in range(max_acct + 1,6):
        am = "Acct" + str(i)
        ary = getattr(smbtable,am)
        ary.append("")
        setattr(smbtable,am,ary)
        for t in thead:
            ac = "{} {}".format(am,t)
            ary = getattr(smbtable,ac)
            ary.append("")
            setattr(smbtable,ac,ary)
###########################################################################
def smb_table(isi=None):
    """\nBuild the table for the SMB shares tab."""
    s = isi.smb_shares_view
    if not s:
        return None
    for zone in s:
        for share,k in s[zone].items():
            perms(k['Permissions'])
            for key in smbheader:
                if key == 'Isilon' :
                     smbtable.Isilon.append(isi.cname)
                     continue
                if key == 'Zone' :
                     smbtable.Zone.append(zone)
                     continue
                if re.search("^Acct[0-9]",key):
                     continue
                try:
                    val = s[zone][share][key]
                except KeyError as err:
                    val = ""
                ary = getattr(smbtable,key)
                ary.append(val)
                setattr(smbtable,key,ary)
###########################################################################
def nfs_table(isi=None):
    """\nBuild the table for the NFS Exports tab."""
    n = isi.nfs_exports_list
    if not n:
        return None
    #pprint.pprint(n) ;
    for zone in n:
        for id in n[zone]:
            #print(zone,id)
            try:
               n[zone][id]['Return 32Bit File Ids'] = n[zone][id]['Return 32Bit File IDs']
            except KeyError as err:
               log.info("[{}] nfs_table() zone=>{}  id=>{} KeyError->{} BUG WITH OneFS 8.2.2.0 changed ID to Id json key for script-- Continue on"
                        .format(isi.cname,zone,id,err))
            for key in nfsheader:
                #print("id",id,"key",key)
                if key == 'Isilon' :
                     nfstable.Isilon.append(isi.cname)
                     continue
                try:
                    val = n[zone][id][key]
                except KeyError as err:
                    val = ""
                    log.error("[{}] nfs_table() zone=>{}  id=>{} KeyError->{}"
                             .format(isi.cname,zone,id,err))
                    sys.exit()
                #print(key,"=>",val)
                ary = getattr(nfstable,key)
                ary.append(val)
                setattr(nfstable,key,ary)
###########################################################################
def snapsched_table(isi=None):
    """\nBuild the sctable for the Snapshot Schedules tab."""
    sc = isi.snapshot_sched_list
    #pprint.pprint(sc) ; sys.exit()
    if not sc:
        return None
    for id in sc:
        for f in sctable.header:
            v = ""
            if f == 'Isilon':
                sctable.Isilon.append(isi.cname)
                continue
            v = sc[id][f]
            #print(f,v)
            ary  = getattr(sctable,f)
            ary.append(v)
            setattr(sctable,f,ary)
###########################################################################
def snap_table(isi=None):
    """\nBuild the stable for the Snapshots tab."""
    s = isi.snapshot_snapshots_list
    if not s:
        return None
    for id in s:
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
def fp_table(isi=None):
    """Build the filepools_table (fptable) for the Filepool_Policies tab."""
    f = isi.filepool_policies
    if not f:
        return None
    for name,i in f.items():
       for f in fpheader:
           if f == 'Isilon':
               fptable.Isilon.append(isi.cname)
               continue
           val = i.get(f,'-')
           if not val : val = '-'
           ary = getattr(fptable,f)
           ary.append(val)
           setattr(fptable,f,ary)
###########################################################################
def netpools_table(isi=None):
    """\nBuild the ntable for the Netpools tab."""
    n = isi.networks_list_pools
    if not n:
        return None
    for pool in n:
        nptable.Isilon.append(isi.cname)
        for h in npheader:
            if h == 'Isilon': continue
            try:
                v = n[pool][h]
            except KeyError:
                v = ' '
            ary = getattr(nptable,h)
            ary.append(v)
            setattr(nptable,h,ary)
###########################################################################
def qsearch(isi=None,path=None,stype=None):
    """Search and return info for a given quota directory type.
       stype = search type: 
    """
    log.debug("searching directory quotas for type=>{} path=>{}"
              .format(stype,path))
    q = isi.quota_quotas_list_csv
    if not q:
        log.warn("Unable to get quotas from " + isi.cname)
        return ' '
    try:
         q[path]
    except KeyError:
        log.debug("Directory quota not found for {}".format(path))     
        return ' '
    for i in q[path]:
        if i['Type'] != 'directory':
            continue
        try:
            found = i[stype]
            if stype == 'Files':
                return(found)
            return (util.to_gb(found))
        except KeyError:
            return ' '
###########################################################################
def search_sc_url(isi=None,zone=None,type=None,share=None):
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
    """Get smb shares and populate into prov  dict, return dict for prov_table.
    """
    prov = {}
    smb =  isi.smb_shares_view
    if not smb:
        log.info("No smb shares detected from " + isi.cname)
        return(prov)
    try:
        for zone in smb:
            for share,k in smb[zone].items():
                path = smb[zone][share]['Path']
                ts = { 'Path' : path ,
                       'Isilon' : isi.cname,
                       'Type' : 'SMB',
                       'Share name': share,
                       'Zone' : zone,
                       'Description' : ' ' + smb[zone][share]['Description'],
                       'Multi-Prot' : 'No',
                       'Map Lookup UID' : ' ',
                       'Hard GB' : qsearch(isi,path,'Hard'),
                       'Advisory GB' : qsearch(isi,path,'Adv'),
                       'Files' : qsearch(isi,path,'Files'),
                       'AppLogical GB' : qsearch(isi,path,'AppLogical'),
                       'FSLogical GB' : qsearch(isi,path,'FSLogical'),
                       'FSPhysical GB' : qsearch(isi,path,'FSPhysical'),
                       'Physical GB' : qsearch(isi,path,'Physical'),
                       'NFS_ids' : ' ',
                       'SmartConnect URL' : 
                                        search_sc_url(isi,zone,'smb',share),
                       'DFS URL' : 'DFS URL',
                       'Map_Lookup_UID' : ' '
                     }
                prov.setdefault(path,{})
                prov[path].setdefault('smb',[]).append(ts)
    except TypeError:
        pass
    return(prov)
###########################################################################
def aliases_nfs_prov(isi=None,zone=None,path=None):
    """Return a list of aliases for a zone,path. If no aliases, return a 
       blank(space) list element.
    """
    na = isi.nfs_aliases_list
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
    """Get nfs shares and populate into prov dict, return dict for prov_table.
    """
    n = isi.nfs_exports_list
    if not n:
        log.info("No NFS exports detected from " + isi.cname)
        return prov
    np = isi.nfs_paths
    for zone,paths in np.items():
        for path,i in paths.items():
            ts = { 'Path' : path ,
                   'Isilon' : isi.cname,
                   'Type' : 'NFS',
                   'Hard GB' : qsearch(isi,path,'Hard'),
                   'Advisory GB' : qsearch(isi,path,'Adv'),
                   'Files' : qsearch(isi,path,'Files'),
                   'AppLogical GB' : qsearch(isi,path,'AppLogical'),
                   'FSLogical GB' : qsearch(isi,path,'FSLogical'),
                   'FSPhysical GB' : qsearch(isi,path,'FSPhysical'),
                   'Physical GB' : qsearch(isi,path,'Physical'),
                   'Share name': 'Alias here',
                   'Zone' : zone,
                   'Multi-Prot' : 'No',
                   'NFS_ids' : i['ids'],
                   'DFS URL' : 'DFS URL here',
                   'SmartConnect URL' : search_sc_url(isi,zone,'nfs',path),
                 }
            description = ""
            mlu = []
            try:
                for id in (i['ids'].split()):
                    description += n[zone][id]['Description'] + ' '
                    mlu.append(n[zone][id]['Map Lookup UID'])
            except KeyError:
                description += "Error with NFS ID {}".format(id)
                mlu.append("Error with NFS ID {}".format(id))
            mlu = ' '.join(mlu)
            ts['Description'] = description 
            ts['Map_Lookup_UID'] = mlu
            prov.setdefault(path,{})
            al = aliases_nfs_prov(isi,zone,path)
            for alias in al:
                sharename  = alias
                ts['Share name'] = sharename
                if alias != ' ':
                     ts['SmartConnect URL'] =  search_sc_url(isi,zone,'nfs',sharename)
                prov[path].setdefault('nfs',[]).append(ts)
    return(prov)
###########################################################################
def multi_prov(prov=None):
    """Process the prov dict, marking all shares under paths with 
       nfs+smb type as Multiprotocol.
    """
    if not prov:
        return(prov)
    for path,d in prov.items():
        if len(d) > 1: #If >1, then smb + nfs
            for type,shares in prov[path].items():
                for share in shares:
                    share['Multi-Prot'] = 'Yes'
    return(prov)
###########################################################################
def match_path(path_dict=None,path_match=None):
   """Given a dict called path_dict where the keys are paths, try
       to match the patch_match. If a match is found return the value
       of the dict key, other wise return '-'"""
   if not path_dict:
      return '-'
   for policy_path, policy_name  in path_dict.items():
      match = os.path.commonpath([policy_path, path_match])
      if match == policy_path:
         return(policy_name)
   return('-')
###########################################################################
def synciq_prov(isi=None,prov=None):
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
    #make a dict of  path->policy name
    sp_paths = {}
    try:
       for id,policy in sp.items():
          sp_paths[policy['source_root_path']] = policy['name']
    except AttributeError:
       log.info("No SyncIQ policies detected from " + isi.cname)
       return False
    except KeyError as err:
       log.error("SyncIQ policies dict key error detected from {}".format(isi.cname,err))
       return False
    for path,d in prov.items():
       for type,shares in prov[path].items():
          for share in shares:
             share['SyncIQpolicy'] = match_path(sp_paths,path)
    return(prov)
###########################################################################
def get_security(isi=None,dpath=None):
    """Process the dpath dict, running a "ls -led <path>" and looking for
       DACL or SYTHENTIC ACL.
    """
    if not dpath:
        return(dpath)
    log.info("Getting share root path ACL security style")
    jfile = 'path_security'
    jobj = isi.fresh_json(jfile)
    if jobj:
        return jobj
    dpathcopy = copy.deepcopy(dpath) 
    controlre = re.compile(r'^\sCONTROL:(.+)')
    synre = re.compile(r'^\sSYNTHETIC ACL')
    for p in dpath:
        hit = 'SYNTHETIC ACL default'
        cmd = "ls -led {}".format(p)
        if not isi.cmd(cmd):
            hit = "PATH NOT FOUND"
        for line in isi.stdout:
            mo = controlre.search(line)
            if mo:
                hit = mo.group(1)
                break
            if synre.search(line):
                hit = 'SYNTHETIC ACL'
                break
        dpathcopy[p] = hit
    isi.put_json_file(ifile=jfile,icontent=dpathcopy)
    return dpathcopy
###########################################################################
def provisions_table(isi=None):
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
    prov = multi_prov(prov)
    prov = synciq_prov(isi,prov)
    if not prov:
        return
    dpath = dict((i,1) for i in prov.keys()) #dpath = dict of path=>security
    dpath = get_security(isi,dpath)
    for path in prov:
        for type,shares in prov[path].items():
            for share in shares:
                try:
                    share['Path_Security'] = dpath[path]
                except KeyError:
                    share['Path_Security'] = 'ERROR'
                
    for path in prov:
        for type,shares in prov[path].items():
            for share in shares:
                for h in pheader:
                    ary = getattr(ptable,h)
                    ary.append(share[h])
                    setattr(ptable,h,ary)
###########################################################################
def zones_table(isi=None):
   """Creates zones table from zone zones list --format json"""
   if 'ztable' not in globals():
      global zheaders 
      global ztable
      zheaders =  isi.headers_zone_zones_json
      if not zheaders:
         log.info("No access zone headers found")
         return False
      zheaders.insert(0,'Isilon')
      ztable = rpt.Table(zheaders)
   zz = isi.zone_zones_json
   if not zz:
      log.info("No access zones found")
      return False
   #log.debug(pprint.pprint(zz))
   for zone,zoneDetails in zz.items():
      log.debug("Processing {}".format(zone))
      for f in ztable.header:
            v = ""
            if f == 'Isilon':
                ztable.Isilon.append(isi.cname)
                continue
            v = str(zz[zone][f])
            ary  = getattr(ztable,f)
            ary.append(v)
            setattr(ztable,f,ary)
###########################################################################
def storagePool_table(isi=None):
   """Creates spooltable from storagepool list --format json"""
   if 'spooltable' not in globals():
      global spoolheaders 
      global spooltable
      spoolheaders =  isi.headers_storagepool_list
      if not spoolheaders:
         log.info("No storagepool headers found")
         return False
      spoolheaders.insert(0,'Isilon')
      spooltable = rpt.Table(spoolheaders)
   spl = isi.storagepool_list
   if not spl:
      log.info("No storagepools found")
      return False
   for spool,spoolDetails in spl.items():
      log.debug("Processing {}".format(spool))
      for f in spooltable.header:
            v = ""
            if f == 'Isilon':
                spooltable.Isilon.append(isi.cname)
                continue
            v = str(spl[spool][f])
            ary = getattr(spooltable,f)
            ary.append(v)
            setattr(spooltable,f,ary)
###########################################################################
def event_groups(isi=None):
   """Creates eventTable"""
   if 'eventTable' not in globals():
      global eventheaders 
      global eventTable
      eventheaders =  isi.headers_event_group_list
      if not eventheaders:
         log.info("No event group headers found")
         return False
      eventheaders.insert(0,'Isilon')
      eventTable = rpt.Table(eventheaders)
   events = isi.event_group_list
   if not events:
      log.info("No event groups found")
      return False
   for event,eventDetails in events.items():
      log.debug("Processing {}".format(event))
      for f in eventTable.header:
            v = ""
            if f == 'Isilon':
                eventTable.Isilon.append(isi.cname)
                continue
            v = str(events[event][f])
            ary = getattr(eventTable,f)
            ary.append(v)
            setattr(eventTable,f,ary)
###########################################################################
def prep_etable():
    """Check the etable for not OK status in Cluster Health.
       If not OK, turn text red. 
    """
    ary = getattr(etable,'Health')
    newary = []
    for v in ary:
        if v != 'OK':
            v = "<font color=\"red\"><strong>{}</font></strong>"\
                            .format(v)
        newary.append(v)
    setattr(etable,'Health',newary)
    discard = etable.freshdata

###########################################################################
def file_to_array(ifile=None):
    """Opens a text file and returns a list with 1 list per line.
       Try multiple encoding types before giving up
    """
    enclist = ('utf-8','latin-1','utf-16','ascii')
    try:
        for e in enclist:
            try:
                with open(ifile, mode='r', encoding=e) as fh:
                    farray = fh.read().splitlines()
                    return farray
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
def dfs_dict(dfile=None):
    """Builds a dict from dfs file.
    """
    dtext = file_to_array(dfile)
    if not dtext:
        return None

    rootre = re.compile(r'^Root Name="(.+)" State')
    linkre = re.compile(r'^\s*Link Name="(.+)" State')
    targetre = re.compile(r'^\s*Target="(.+)" State')
    dfs = {} 
    root = ''
    link = ''
    for line in dtext:
        mo = rootre.search(line)
        if mo:
             root = mo.group(1)
             continue
        mo = linkre.search(line)
        if mo:
             link = mo.group(1)
             link = "{}\\{}".format(root,link)
             continue
        mo = targetre.search(line)
        if mo and link:
             target = mo.group(1).upper()
             dfs[target] = link
             link = ''
             target= ''
    return(dfs)
###########################################################################
def dfs_col(dfile=None):
    """Adds DFS URL column to ptable and ptable.header.
       Builds the ptable.'DFS URL' attribute.
    """
    if not os.path.isfile(dfile):
        print("dfs_detect specified but no file specified or file not found")
        return
    d = dfs_dict(dfile)
    if not d:
        return None
    try:
        surl = getattr(ptable,'SmartConnect URL')
    except AttributeError as err:
        log.warn("Unable to get from ptable : {}".format(err))
        return
    nary = []
    smbre = re.compile(r'^\\\\\S')
    for u in surl:
        if not smbre.search(u):
           nary.append(' ')
           continue 
        try:
            durl = d[u.upper()]
        except KeyError:
            durl = ' '
        nary.append(durl)
    setattr(ptable,'DFS URL',nary)
    ptable.header.append('DFS URL')
###########################################################################
def prep_tables():
    """calls dfs_col if dfs_detect. (for ptable)
       Changes values in qtable to GB(including headers change) for:
    """
    clist = ['Hard','Over','AppLogical','FSLogical','FSPhysical','Physical','Adv','Soft']
    for c in clist:
        ary = getattr(qtable,c)
        nary = [] #new array
        for n in ary:
            n = util.to_gb(n)
            nary.append(n)
        newlabel = c + ' GB'
        setattr(qtable,newlabel,nary)
        delattr(qtable,c)
    newheader = []
    for q in qheader:
        val = q
        for n in clist:
            if n == q:
                val = val +  ' GB'
        newheader.append(val)
    qtable.header = newheader

    try:
        dfile = opts.dfs_detect
        dfs_col(dfile)
    except AttributeError:
        log.info("dfs_detect= not specified in .conf")
###########################################################################
def isi_main():
    """\nLaunch point for iterating through isi hosts."""
 
    log.info("Start ISILON loop")
    op=hosts.SetOpts(opts=opts,flavor='isi')
    for host in sorted(op.hosts):
        util.check_server(host,22)
        if hosts.errors_check(host): continue
        isi=hosts.Isilon(host) 
        isi.cname = isi.cluster_identity['name'] #cluster name
        isi.location = isi.cluster_contact['location']
        event_groups(isi)
        zones_table(isi)
        fp_table(isi)
        netpools_table(isi)
        provisions_table(isi)
        snapsched_table(isi)
        snap_table(isi)
        smb_table(isi)
        nfs_table(isi)
        quotastable(isi)
        emailtable(isi)
        nodestable(isi)
        storagePool_table(isi)
        hosts.errors_check(host)
###########################################################################
def mk_spreadsheet(myrpt=None):
    """\nMake spreadsheet and return file name"""
    spreadsheet = rpt.Workbook(myrpt.tmpname)
    if hosts.errorrpt:
        worksheet = spreadsheet.add_tab(name='Script Issues',
                    data=hosts.errorrpt,header=['Host','Message'])
    if etable.data:
        worksheet = spreadsheet.add_tab(name='Email rpt',
                    data=etable.data,header=etable.sheader)
        worksheet.freeze_panes(1,2)
    if eventTable.data:
        worksheet = spreadsheet.add_tab(name='Event Groups',
                    data=eventTable.data,header=eventTable.sheader)
    if ptable.data:
        worksheet = spreadsheet.add_tab(name='Provisions',
                    data=ptable.data,header=ptable.sheader)
        worksheet.freeze_panes(1,2)
        #spreadsheet.set_col_by_name(worksheet = worksheet,
                                    #colname = 'Path ',
                                    #width = 65)
        worksheet.set_col_by_name(colname = 'Path ', width = 65)
        worksheet.set_col_by_name(colname = 'Share name ', width = 65)
        worksheet.set_col_by_name(colname = 'Description ', width = 65)
    if ntable.data:
        worksheet = spreadsheet.add_tab(name='Nodes',
                    data=ntable.data,header=ntable.sheader)
    if qtable.data:
        worksheet = spreadsheet.add_tab(name='Quotas',data=qtable.data,header=qtable.sheader)
    if nfstable.data:
        worksheet = spreadsheet.add_tab(name='NFS Exports',data=nfstable.data,header=nfstable.sheader)
        worksheet.freeze_panes(1,2)
    if smbtable.data:
        worksheet = spreadsheet.add_tab(name='SMB Shares',data=smbtable.data,header=smbtable.sheader)
        worksheet.freeze_panes(1,2)
    if stable.data:
        worksheet = spreadsheet.add_tab(name='Snapshots',data=stable.data,header=stable.sheader)
        worksheet.freeze_panes(1,2)
    if sctable.data:
        worksheet = spreadsheet.add_tab(name='Snapshot Schedules',data=sctable.data,header=sctable.sheader)
        worksheet.freeze_panes(1,2)
    if fptable.data:
        worksheet = spreadsheet.add_tab(name='FilePool Policies',data=fptable.data,header=fptable.sheader)
        worksheet.freeze_panes(1,2)
    if nptable.data:
        worksheet = spreadsheet.add_tab(name='Net Pools',data=nptable.data,header=nptable.sheader)
        worksheet.freeze_panes(1,3)
    if ztable.data:
        worksheet = spreadsheet.add_tab(name='Access Zones',data=ztable.data,header=ztable.sheader)
        worksheet.freeze_panes(1,2)
    if spooltable.data:
        worksheet = spreadsheet.add_tab(name='StoragePools',
                    data=spooltable.data,header=spooltable.sheader)
        worksheet.freeze_panes(1,2)
    spreadsheet.close()
    return spreadsheet.filename
###########################################################################
def send_rpt():
    myrpt = rpt.Rpt(opts=opts)
    prep_tables()
    
    xlsxfile = mk_spreadsheet(myrpt)
    if argo.attach:
       myrpt.add_attachment(xlsxfile)
    prep_etable()
    if hosts.errorrpt:
        myrpt.add_css_heading("Errors with script","Red")
        myrpt.add_css_table(data=hosts.errorrpt,
                            header=["Host","Message"])
    smbUrl = """<a href=" """ + opts.cifs_daily_rpt_dir
    smbUrl = smbUrl  + """\\isi_rpt">Daily spreadsheet details repository</a>"""
    myrpt.add_css_heading(smbUrl)
    #myrpt.add_css_heading(opts.cifs_daily_rpt_dir + "\\isi_rpt")
    myrpt.add_css_table(data=etable.data,header=etable.header)
        
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
    log.info("START")
    global opts
    opts=hosts.SetOpts(opts=argo,main='hosts',script=True)
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
   argo=hosts.ArgParse(description="Report on Isilon Configurations.")
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
